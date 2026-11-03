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
import com.infomaniak.core.network.models.exceptions.NetworkException
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.CursorApiResponse
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.cache.OfflineFolderController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.MediaUtils.deleteInMediaScan
import com.infomaniak.drive.utils.MediaUtils.isMedia
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
 * queue and the collected file ids all grow with the size of the folder tree (file ids as ints,
 * one row per queued folder). What the walk avoids is materializing whole subtrees of File
 * objects or holding all pages of a listing at once. For every folder it:
 *  1. upserts the fresh remote children into the local Realm cache,
 *  2. marks every non-pinned child as covered ([File.isInOfflineFolder]),
 *  3. queues children without an intact offline copy for download,
 *  4. once the folder has been fully listed, removes the offline copies of children that
 *     disappeared remotely (deleted or moved out), offline cache only.
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
     * Lists one remote folder completely (all pages), upserting and marking its children.
     * Returns false when the folder could not be listed (network or API error).
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
        var cursor: String? = null
        val remoteFileIds = mutableSetOf<Int>()
        var isFirstPage = true

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
                // A deleted root folder makes its first page fail: stop covering it and let the
                // offline sync remove the root record.
                if (isRootFolder && isFirstPage && isRootGone(apiResponse)) {
                    removeRootCoverage(context, realm, folderId, userDrive)
                    return false
                }
                SentryLog.i(TAG, "Failed to list folder $folderId (cursor: ${cursor != null})")
                return false
            }

            processPage(
                context = context,
                realm = realm,
                rootFolderId = rootFolderId,
                folderId = folderId,
                userDrive = userDrive,
                apiResponse = apiResponse,
                isFirstPage = isFirstPage,
                fileIds = fileIds,
                folderQueue = folderQueue,
                visitedFolderIds = visitedFolderIds,
                remoteFileIds = remoteFileIds,
            )

            if (apiResponse.hasMoreAndCursorExists) {
                cursor = apiResponse.cursor
                isFirstPage = false
            } else {
                break
            }
        }

        // The folder is now fully listed: children previously covered but absent from the remote
        // listing have been deleted or moved out, their offline copy goes away (cache only).
        cleanupStaleCoveredChildren(context, realm, folderId, userDrive, remoteFileIds)

        return true
    }

    private fun processPage(
        context: Context,
        realm: Realm,
        rootFolderId: Int,
        folderId: Int,
        userDrive: UserDrive,
        apiResponse: CursorApiResponse<List<File>>,
        isFirstPage: Boolean,
        fileIds: MutableList<Int>,
        folderQueue: ArrayDeque<Int>,
        visitedFolderIds: MutableSet<Int>,
        remoteFileIds: MutableSet<Int>,
    ) {
        val remoteFiles = apiResponse.data ?: return
        val folderProxy = FileController.getFileById(realm, folderId)

        FileController.saveRemoteFiles(
            realm = realm,
            localFolderProxy = folderProxy,
            remoteFolder = folderProxy ?: File(id = folderId, driveId = userDrive.driveId, type = File.Type.DIRECTORY.value)
                .apply { initUid() },
            apiResponse = apiResponse,
            isFirstPage = isFirstPage,
            isCompleteFolder = false,
        )

        val nonFolderFiles = remoteFiles.filterNot { it.isFolder() }
        remoteFiles.filter { it.isFolder() }.forEach { subFolder ->
            if (visitedFolderIds.add(subFolder.id)) folderQueue.add(subFolder.id)
        }

        // Decide which files need a download before flagging them, since the checks below read
        // the file system for the offline copies.
        val coveredFiles = nonFolderFiles.mapNotNull { remoteFile ->
            val localFile = FileController.getFileById(realm, remoteFile.id) ?: return@mapNotNull null
            val offlineFile = localFile.getOfflineFile(context, userDrive.userId)
            // Intactness check without requiring the explicit pin: covered files (isOffline ==
            // false) still count when their offline copy matches the remote metadata.
            val hasIntactOfflineCopy = offlineFile != null && offlineFile.exists()
                    && !localFile.isObsoleteOrNotIntact(offlineFile)
            localFile to OfflineFolderPolicy.needsDownload(
                isOffline = localFile.isOffline,
                hasIntactOfflineCopy = hasIntactOfflineCopy,
            )
        }

        realm.executeTransaction {
            // Re-check inside the transaction: the root may have been unmarked while this page was
            // being fetched, and the coverage flags must not be (re)introduced by a stale walk.
            if (OfflineFolderController.getOfflineRoot(rootFolderId, customRealm = realm) == null) {
                return@executeTransaction
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

        remoteFileIds.addAll(nonFolderFiles.map { it.id })
    }

    /**
     * Removes the inherited coverage (and the offline copy) of children of [folderId] that are
     * not part of [remoteFileIds] anymore. Only called once the folder has been fully listed.
     * Copies backing a pending upload (an offline edit not yet sent) are kept, flags included,
     * until the upload completes; a later walk then clears them.
     */
    private fun cleanupStaleCoveredChildren(
        context: Context,
        realm: Realm,
        folderId: Int,
        userDrive: UserDrive,
        remoteFileIds: Set<Int>,
    ) {
        val folder = FileController.getFileById(realm, folderId) ?: return
        val staleChildren = folder.children.createSnapshot().filter { child ->
            !child.isFolder()
                    && child.id !in remoteFileIds
                    && OfflineFolderPolicy.shouldRemoveCoveredFileAbsentFromRemote(
                isOffline = child.isOffline,
                isInOfflineFolder = child.isInOfflineFolder,
            )
        }
        if (staleChildren.isEmpty()) return

        val (childrenToClear, childrenKeptForUpload) = staleChildren.partition { child ->
            val offlineFile = child.getOfflineFile(context, userDrive.userId)
            offlineFile != null && UploadFile.hasPendingUpload(uri = Uri.fromFile(offlineFile).toString())
        }

        realm.executeTransaction {
            childrenToClear.forEach { child ->
                if (child.isValid) {
                    child.isInOfflineFolder = false
                    child.isMarkedAsOffline = false
                }
            }
        }

        childrenToClear.forEach { child ->
            child.getOfflineFile(context, userDrive.userId)?.let { offlineFile ->
                runCatching { if (offlineFile.exists()) offlineFile.delete() }
                if (child.isMedia()) child.deleteInMediaScan(context, userDrive)
            }
        }
        if (childrenKeptForUpload.isNotEmpty()) {
            SentryLog.i(TAG, "Kept ${childrenKeptForUpload.size} offline copies backing pending uploads")
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
     * Best-effort detection of a root folder that is gone remotely. Network failures are not
     * "root gone": the walk simply aborts and will be retried by the offline sync.
     */
    private fun isRootGone(apiResponse: CursorApiResponse<List<File>>): Boolean {
        return apiResponse.error?.exception !is NetworkException
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
