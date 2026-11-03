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
            if (OfflineFolderController.getOfflineRoot(rootFolderId, customRealm = realm) == null) {
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

                if (OfflineFolderController.getOfflineRoot(rootFolderId, customRealm = realm) == null) {
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

            WalkResult(rootExists = true, completed = completed, fileIds = fileIds)
        }
    }

    /**
     * Aggregates all pages of one remote folder, then upserts and marks its children. The cached
     * membership is only replaced once the folder has been fully listed, so a mid-listing failure
     * leaves the previous children (and their coverage links) intact for later walks and for the
     * root removal traversal. Returns false when the folder could not be listed.
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

            allPages.addAll(apiResponse.data ?: emptyList())
            if (apiResponse.hasMoreAndCursorExists) {
                cursor = apiResponse.cursor
            } else {
                lastResponse = apiResponse
                break
            }
        }

        // Fully listed: capture the previous membership BEFORE replacing it, reconcile coverage
        // (including whole disappeared subfolders), then upsert the fresh children.
        val oldChildIds = FileController.getFileById(realm, folderId)
            ?.children?.createSnapshot()?.map { it.id }?.toSet() ?: emptySet()
        val remoteIds = allPages.mapTo(mutableSetOf()) { it.id }

        val last = lastResponse
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
            isCompleteFolder = !last.hasMore,
            fileIds = fileIds,
            folderQueue = folderQueue,
            visitedFolderIds = visitedFolderIds,
        )

        cleanupStaleCoveredChildren(context, realm, folderId, rootFolderId, userDrive, oldChildIds, remoteIds)

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
            if (OfflineFolderController.getOfflineRoot(rootFolderId, customRealm = realm) == null) {
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
     * Drops the coverage of children of [folderId] that are not part of [remoteFileIds] anymore.
     * Only called once the folder has been fully listed. Files (or whole disappeared subtrees)
     * are only cleared when no other remaining root covers them and no pending upload protects
     * them; [OfflineFolderController.clearFileCoverage] owns the per-file decision.
     */
    private fun cleanupStaleCoveredChildren(
        context: Context,
        realm: Realm,
        folderId: Int,
        rootFolderId: Int,
        userDrive: UserDrive,
        oldChildIds: Set<Int>,
        remoteFileIds: Set<Int>,
    ) {
        val staleIds = oldChildIds - remoteFileIds
        if (staleIds.isEmpty()) return

        val result = OfflineFolderController.RemovalResult()
        val visited = mutableSetOf<Int>()
        val queue = ArrayDeque<Int>()

        staleIds.forEach { staleId ->
            val staleChild = FileController.getFileById(realm, staleId) ?: return@forEach
            if (OfflineFolderPolicy.shouldRemoveCoveredFileAbsentFromRemote(
                    isOffline = staleChild.isOffline,
                    isInOfflineFolder = staleChild.isInOfflineFolder,
                ) || staleChild.isFolder()
            ) {
                if (staleChild.isFolder()) {
                    if (visited.add(staleId)) queue.add(staleId)
                } else {
                    clearIfNotCoveredByOtherRoot(context, realm, staleChild, rootFolderId, userDrive, result)
                }
            }
        }

        // A disappeared subfolder takes its whole subtree with it.
        while (queue.isNotEmpty()) {
            val subFolderId = queue.removeFirst()
            val subFolder = FileController.getFileById(realm, subFolderId) ?: continue
            clearIfNotCoveredByOtherRoot(context, realm, subFolder, rootFolderId, userDrive, result, isFolder = true)
            realm.where(File::class.java)
                .equalTo(File::parentId.name, subFolderId)
                .findAll()
                .createSnapshot()
                .forEach { child ->
                    if (child.isFolder()) {
                        if (visited.add(child.id)) queue.add(child.id)
                    } else {
                        clearIfNotCoveredByOtherRoot(context, realm, child, rootFolderId, userDrive, result)
                    }
                }
        }

        if (result.keptOfflineFilesCount > 0) {
            SentryLog.i(TAG, "Kept ${result.keptOfflineFilesCount} offline copies during stale cleanup")
        }
    }

    private fun clearIfNotCoveredByOtherRoot(
        context: Context,
        realm: Realm,
        file: File,
        rootFolderId: Int,
        userDrive: UserDrive,
        result: OfflineFolderController.RemovalResult,
        isFolder: Boolean = false,
    ) {
        // Recheck remaining roots before dropping a moved/deleted descendant: it may still be
        // covered by another root (e.g. an ancestor of the walked root or a nested one).
        val isCoveredByOtherRoot = OfflineFolderController.isInsideAnotherRoot(
            realm,
            file.id,
            excludedFolderId = rootFolderId,
        )
        if (isCoveredByOtherRoot || OfflineFolderController.isOfflineRoot(file.id, customRealm = realm)) return

        OfflineFolderController.clearFileCoverage(
            context = context,
            realm = realm,
            file = file,
            userDrive = userDrive,
            result = result,
            isFolder = isFolder,
        )
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
     * Enqueues a recursive walk for every offline root of [userDrive] that needs one: either it
     * still has files waiting for download, or its last completed walk is stale.
     */
    suspend fun enqueueStaleOfflineFolderSyncs(context: Context, userDrive: UserDrive, customRealm: Realm) {
        val nowMillis = System.currentTimeMillis()
        OfflineFolderController.getOfflineRoots(customRealm = customRealm).forEach { root ->
            coroutineContext.ensureActive()
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
