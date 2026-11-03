/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2026 Infomaniak Network SA
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package com.infomaniak.drive.utils

import android.content.Context
import android.net.Uri
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.CursorApiResponse
import com.infomaniak.drive.data.api.ErrorCode
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.cache.OfflineFolderController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import io.realm.Realm
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import kotlin.coroutines.coroutineContext

/**
 * Recursive walk of an offline folder root against the remote API (issue #1902).
 *
 * The walk is breadth-first and cursor-paginated, and guarded against cycles with a visited set.
 * Note on memory: it is NOT bounded by pagination alone — the visited folder set, the folder
 * queue and the collected file-id list grow with the size of the folder tree (ints and one entry
 * per queued folder). What it avoids is materializing File-object subtrees or holding pages of
 * several folders at once (a single folder's pages are aggregated so its cached membership is
 * only replaced once the whole folder has been listed).
 *
 * For every folder it:
 *  1. upserts the fresh remote children into the local Realm cache,
 *  2. marks every child (files and folders) as covered ([File.isInOfflineFolder]),
 *  3. queues files without an intact offline copy for download,
 *  4. once the folder has been fully listed, drops the coverage of children that disappeared
 *     remotely (deleted or moved out), offline cache only.
 *
 * It never deletes anything outside the app offline storage and never touches explicit pins.
 */
object OfflineFolderUtils {

    data class WalkResult(
        val rootExists: Boolean,
        val completed: Boolean,
        val fileIds: List<Int>,
    )

    suspend fun walkAndMarkOfflineFolder(
        context: Context,
        rootFolderId: Int,
        userDrive: UserDrive,
        order: File.SortType = File.SortType.NAME_AZ,
    ): WalkResult = withContext(Dispatchers.IO) {
        val okHttpClient = AccountUtils.getHttpClient(userDrive.userId)
        FileController.getRealmInstance(userDrive).use { realm ->

            // The durable root intent must still be there: it may have been removed while this
            // walk was queued.
            if (!OfflineFolderController.hasActiveOfflineRoot(rootFolderId, customRealm = realm)) {
                return@withContext WalkResult(rootExists = false, completed = false, fileIds = emptyList())
            }

            val fileIds = mutableListOf<Int>()
            val visitedFolderIds = mutableSetOf(rootFolderId)
            val folderQueue = ArrayDeque(listOf(rootFolderId))
            var completed = true

            // Re-assert the pending marker on the root folder itself: it drives the folder row
            // progress indicator until this walk finishes.
            FileController.updateFile(rootFolderId, realm) { it.isMarkedAsOffline = true }

            walk@ while (folderQueue.isNotEmpty()) {
                ensureActive()
                val folderId = folderQueue.removeFirst()

                if (!OfflineFolderController.hasActiveOfflineRoot(rootFolderId, customRealm = realm)) {
                    // Root removed while walking: stop marking new files as covered.
                    return@withContext WalkResult(rootExists = false, completed = false, fileIds = fileIds)
                }

                val isRootFolder = folderId == rootFolderId
                val folderWasListed = walkSingleFolder(
                    context = context,
                    realm = realm,
                    okHttpClient = okHttpClient,
                    rootFolderId = rootFolderId,
                    folderId = folderId,
                    userDrive = userDrive,
                    order = order,
                    isRootFolder = isRootFolder,
                    fileIds = fileIds,
                    folderQueue = folderQueue,
                    visitedFolderIds = visitedFolderIds,
                )

                if (!folderWasListed) {
                    completed = false
                    break@walk
                }
            }

            // Drive-wide reconciliation net over the durable coverage membership; uses only
            // local durable state, so it is safe after an incomplete walk too.
            if (OfflineFolderController.getOfflineRoot(rootFolderId, customRealm = realm) != null) {
                sweepOrphanInheritedCoverage(context, realm, userDrive)
            }

            WalkResult(rootExists = true, completed = completed, fileIds = fileIds)
        }
    }

    /**
     * Aggregates all pages of one remote folder, then upserts and marks its children. The cache
     * membership is only replaced, and the stale reconciliation only runs, once the folder has
     * been fully listed with valid pages and a terminating response: an incomplete pagination
     * (missing data, `hasMore` without cursor, non-advancing cursor) mutates nothing and reports
     * an incomplete outcome so the walk is retried (issue #1902). Returns false when the folder
     * could not be completely listed.
     */
    private suspend fun walkSingleFolder(
        context: Context,
        realm: Realm,
        okHttpClient: OkHttpClient,
        rootFolderId: Int,
        folderId: Int,
        userDrive: UserDrive,
        order: File.SortType,
        isRootFolder: Boolean,
        fileIds: MutableList<Int>,
        folderQueue: ArrayDeque<Int>,
        visitedFolderIds: MutableSet<Int>,
    ): Boolean {
        val allPages = mutableListOf<File>()
        var lastResponse: CursorApiResponse<List<File>>? = null
        var cursor: String? = null

        while (true) {
            coroutineContext.ensureActive()

            val apiResponse = ApiRepository.getFolderFiles(
                okHttpClient = okHttpClient,
                driveId = userDrive.driveId,
                parentId = folderId,
                cursor = cursor,
                order = order,
            )

            if (!apiResponse.isSuccess()) {
                // A deleted root folder answers object_not_found on its first page: only this
                // explicit, verified missing-object response drops the durable root; transient
                // or indeterminate failures preserve it for a retry.
                if (isRootFolder && cursor == null && isRootGone(apiResponse)) {
                    removeRootCoverage(context, realm, folderId, userDrive)
                    return false
                }
                SentryLog.i(TAG, "Failed to list folder $folderId (cursor: ${cursor != null})")
                return false
            }

            val pageData = apiResponse.data
            if (pageData == null) {
                // A successful response without data is not an empty page: treat it as
                // incomplete rather than reconciling against nothing.
                SentryLog.i(TAG, "Incomplete listing for folder $folderId (null page data)")
                return false
            }
            allPages.addAll(pageData)

            when {
                apiResponse.hasMoreAndCursorExists -> {
                    val nextCursor = apiResponse.cursor
                    if (nextCursor == cursor) {
                        SentryLog.i(TAG, "Incomplete listing for folder $folderId (cursor not advancing)")
                        return false
                    }
                    cursor = nextCursor
                }
                apiResponse.hasMore -> {
                    // hasMore without a cursor cannot be continued: incomplete.
                    SentryLog.i(TAG, "Incomplete listing for folder $folderId (hasMore without cursor)")
                    return false
                }
                else -> {
                    lastResponse = apiResponse
                    break
                }
            }
        }

        val last = lastResponse ?: return false
        val remoteIds = allPages.mapTo(mutableSetOf()) { it.id }

        val completeResponse = CursorApiResponse<List<File>>(
            result = last.result,
            data = allPages,
            error = last.error,
            responseAt = last.responseAt,
            cursor = last.cursor,
            hasMore = false,
        )

        processPage(
            context = context,
            realm = realm,
            rootFolderId = rootFolderId,
            folderId = folderId,
            userDrive = userDrive,
            apiResponse = completeResponse,
            isCompleteFolder = true,
            fileIds = fileIds,
            folderQueue = folderQueue,
            visitedFolderIds = visitedFolderIds,
        )

        // Fully listed: children confirmed absent from this authoritative listing are reconciled
        // from durable coverage membership, not from the replaceable presentation child list
        // (issue #1902).
        cleanupConfirmedAbsentChildren(context, realm, folderId, userDrive, remoteIds)

        return true
    }

    private fun processPage(
        context: Context,
        realm: Realm,
        rootFolderId: Int,
        folderId: Int,
        userDrive: UserDrive,
        apiResponse: CursorApiResponse<List<File>>,
        isCompleteFolder: Boolean,
        fileIds: MutableList<Int>,
        folderQueue: ArrayDeque<Int>,
        visitedFolderIds: MutableSet<Int>,
    ) {
        val remoteFiles = apiResponse.data ?: return
        val folderProxy = FileController.getFileById(realm, folderId)

        FileController.saveRemoteFiles(
            realm = realm,
            localFolderProxy = folderProxy,
            remoteFolder = folderProxy ?: File(id = folderId, driveId = userDrive.driveId, type = File.Type.DIRECTORY.value)
                .apply { initUid() },
            apiResponse = apiResponse,
            isFirstPage = true,
            isCompleteFolder = isCompleteFolder,
        )

        val subFolderIds = mutableListOf<Int>()
        val nonFolderFiles = mutableListOf<File>()
        remoteFiles.forEach { remoteFile ->
            if (remoteFile.isFolder()) {
                if (visitedFolderIds.add(remoteFile.id)) folderQueue.add(remoteFile.id)
                subFolderIds.add(remoteFile.id)
            } else {
                nonFolderFiles.add(remoteFile)
            }
        }

        // Decide which files need a download before flagging them, since the checks below read
        // the file system for the offline copies.
        val coveredFiles = nonFolderFiles.mapNotNull { remoteFile ->
            val localFile = FileController.getFileById(realm, remoteFile.id) ?: return@mapNotNull null
            val offlineFile = localFile.getOfflineFile(context, userDrive.userId)
            val hasPendingUpload = offlineFile != null &&
                    UploadFile.hasPendingUpload(uri = Uri.fromFile(offlineFile).toString())
            val hasIntactOfflineCopy = offlineFile != null && localFile.isOfflineAndIntact(offlineFile)
            val isLocallyModified = offlineFile != null && offlineFile.exists() && OfflineFolderPolicy.isLocallyModified(
                offlineFileLastModifiedMillis = offlineFile.lastModified(),
                remoteLastModifiedAtSeconds = localFile.lastModifiedAt,
            )
            localFile to OfflineFolderPolicy.needsDownload(
                hasIntactOfflineCopy = hasIntactOfflineCopy,
                hasPendingUpload = hasPendingUpload,
                isLocallyModified = isLocallyModified,
            )
        }

        realm.executeTransaction {
            // Re-check inside the transaction: the root may have been unmarked while this folder
            // was being listed, and the coverage flags must not be (re)introduced by a stale walk.
            if (!OfflineFolderController.hasActiveOfflineRoot(rootFolderId, customRealm = realm)) {
                return@executeTransaction
            }
            // Materialize coverage on descendant folders too, so browsing a downloaded
            // subfolder cannot trigger the offline recovery heuristic (issue #1902).
            subFolderIds.forEach { subFolderId ->
                realm.where(File::class.java).equalTo(File::id.name, subFolderId).findFirst()?.let { subFolder ->
                    subFolder.isInOfflineFolder = true
                }
            }
            coveredFiles.forEach { (localFile, needsDownload) ->
                if (!localFile.isValid) return@forEach
                localFile.isInOfflineFolder = true
                if (needsDownload) {
                    localFile.isMarkedAsOffline = true
                    fileIds.add(localFile.id)
                }
            }
        }
    }

    /**
     * Reconciles the direct children of [folderId] (and the subtrees of subfolders that
     * disappeared entirely) against the freshly confirmed remote membership.
     *
     * Candidates come from the durable parentId chains and inherited flags, never from the
     * presentation child list a normal folder refresh may already have replaced (issue #1902).
     * A disappearance confirmed by the authoritative listing invalidates inherited coverage from
     * every root, subject to the edit protection inside [OfflineFolderController.clearFileCoverage];
     * obsolete cached ancestry cannot establish protection. A vanished subfolder that is itself
     * a remaining root is skipped: its own durable record owns its subtree's cleanup.
     */
    private fun cleanupConfirmedAbsentChildren(
        context: Context,
        realm: Realm,
        folderId: Int,
        userDrive: UserDrive,
        remoteFileIds: Set<Int>,
    ) {
        val result = OfflineFolderController.RemovalResult()
        val children = realm.where(File::class.java)
            .equalTo(File::parentId.name, folderId)
            .findAll()
            .createSnapshot()

        children.forEach { child ->
            if (child.id in remoteFileIds) return@forEach

            if (child.isFolder()) {
                if (OfflineFolderController.isOfflineRoot(child.id, customRealm = realm)) return@forEach
                clearConfirmedAbsentSubtree(context, realm, child.id, userDrive, result)
            } else if (OfflineFolderPolicy.shouldRemoveCoveredFileAbsentFromRemote(
                    isOffline = child.isOffline,
                    isInOfflineFolder = child.isInOfflineFolder,
                )
            ) {
                OfflineFolderController.clearFileCoverage(
                    context = context,
                    realm = realm,
                    file = child,
                    userDrive = userDrive,
                    result = result,
                    isFolder = false,
                )
            }
        }

        if (result.keptOfflineFilesCount > 0 || result.removedOfflineFilesCount > 0) {
            SentryLog.i(
                TAG,
                "Folder $folderId reconciliation: kept ${result.keptOfflineFilesCount}, removed ${result.removedOfflineFilesCount}"
            )
        }
    }

    /** Clears the inherited coverage of the whole subtree below [rootFolderId], cache only. */
    private fun clearConfirmedAbsentSubtree(
        context: Context,
        realm: Realm,
        rootFolderId: Int,
        userDrive: UserDrive,
        result: OfflineFolderController.RemovalResult,
    ) {
        val visited = mutableSetOf(rootFolderId)
        val queue = ArrayDeque(listOf(rootFolderId))

        while (queue.isNotEmpty()) {
            val currentId = queue.removeFirst()
            val currentFolder = FileController.getFileById(realm, currentId) ?: continue
            OfflineFolderController.clearFileCoverage(
                context = context,
                realm = realm,
                file = currentFolder,
                userDrive = userDrive,
                result = result,
                isFolder = true,
            )

            realm.where(File::class.java)
                .equalTo(File::parentId.name, currentId)
                .findAll()
                .createSnapshot()
                .forEach { child ->
                    if (child.isFolder()) {
                        if (visited.add(child.id)) queue.add(child.id)
                    } else if (OfflineFolderPolicy.shouldRemoveCoveredFileAbsentFromRemote(
                            isOffline = child.isOffline,
                            isInOfflineFolder = child.isInOfflineFolder,
                        )
                    ) {
                        OfflineFolderController.clearFileCoverage(
                            context = context,
                            realm = realm,
                            file = child,
                            userDrive = userDrive,
                            result = result,
                            isFolder = false,
                        )
                    }
                }
        }
    }

    /**
     * Second, drive-wide reconciliation net: inherited rows whose durable ancestry no longer
     * leads to any active offline root (moved out, root removed while their upload was pending,
     * leftovers of an interrupted cleanup) are cleared, subject to the same edit protection.
     * Independent of any presentation child list (issue #1902).
     */
    private fun sweepOrphanInheritedCoverage(context: Context, realm: Realm, userDrive: UserDrive) {
        val result = OfflineFolderController.RemovalResult()
        val orphans = realm.where(File::class.java)
            .equalTo(File::isInOfflineFolder.name, true)
            .notEqualTo(File::isOffline.name, true)
            .findAll()
            .createSnapshot()
            .filter { !OfflineFolderController.isInsideAnotherRoot(realm, it.id) }

        orphans.forEach { orphan ->
            OfflineFolderController.clearFileCoverage(
                context = context,
                realm = realm,
                file = orphan,
                userDrive = userDrive,
                result = result,
                isFolder = orphan.isFolder(),
            )
        }

        if (result.keptOfflineFilesCount > 0 || result.removedOfflineFilesCount > 0) {
            SentryLog.i(
                TAG,
                "Inherited coverage sweep: kept ${result.keptOfflineFilesCount}, removed ${result.removedOfflineFilesCount}"
            )
        }
    }

    /**
     * The root folder no longer exists remotely (deleted or trashed): drop the root record and
     * the inherited coverage below it. Explicit pins and files under another root are kept, and
     * nothing outside the offline cache is deleted.
     */
    private fun removeRootCoverage(context: Context, realm: Realm, rootFolderId: Int, userDrive: UserDrive) {
        SentryLog.i(TAG, "Offline root folder $rootFolderId disappeared remotely, removing its coverage")
        OfflineFolderController.removeOfflineRoot(folderId = rootFolderId, userDrive = userDrive, customRealm = realm)
    }

    /**
     * Only an explicit missing-object response proves the root is gone. Network failures and
     * server errors keep the durable root: the walk aborts and will be retried by the offline
     * sync.
     */
    private fun isRootGone(apiResponse: CursorApiResponse<List<File>>): Boolean {
        return apiResponse.error?.code == ErrorCode.OBJECT_NOT_FOUND
    }

    /**
     * Offline-sync hook (issue #1902): finishes the deferred cleanup of roots whose removal is
     * still pending (durable owner), then enqueues a recursive walk for every active root that
     * needs one — either it still has files waiting for download, or its last completed walk is
     * stale.
     */
    suspend fun enqueueStaleOfflineFolderSyncs(context: Context, userDrive: UserDrive, customRealm: Realm) {
        val nowMillis = System.currentTimeMillis()

        // Durable owner of the deferred cleanups: nothing to download for these roots, but their
        // leftover protected copies must be revisited until the removal completes.
        OfflineFolderController.processPendingRemovalRoots(context, userDrive, customRealm)

        OfflineFolderController.getOfflineRoots(customRealm = customRealm).forEach { root ->
            coroutineContext.ensureActive()
            if (root.isPendingRemoval) return@forEach

            val hasPendingFiles = OfflineFolderController.hasPendingFilesRecursively(
                rootFolderId = root.folderId,
                customRealm = customRealm,
            )
            if (OfflineFolderPolicy.needsSync(
                    hasPendingFiles = hasPendingFiles,
                    lastSyncedAtMillis = root.lastSyncedAt?.time,
                    nowMillis = nowMillis,
                )
            ) {
                Utils.enqueueBulkDownloadWorker(context, root.folderId, userDrive, recursive = true)
            }
        }
    }

    private val TAG = OfflineFolderUtils::class.java.simpleName
}
