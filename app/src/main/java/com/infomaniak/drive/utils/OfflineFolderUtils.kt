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

                when (walkSingleFolder(
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
                )) {
                    FolderWalkOutcome.INCOMPLETE -> {
                        // The folder's membership is unknown: leave its coverage untouched,
                        // keep the walk incomplete, but continue the other queued folders
                        // (issue: round 7 #15) — one child's persistent listing failure must
                        // not starve independent siblings. The loop top re-asserts the active
                        // root before every folder, so a removed root still terminates the
                        // walk immediately.
                        completed = false
                        continue@walk
                    }
                    // Deferred moves keep their old metadata and durable membership; the
                    // collected files still count as safe walk progress (issue: round 6 #10).
                    FolderWalkOutcome.DEFERRED_MOVES -> completed = false
                    FolderWalkOutcome.COMPLETED -> Unit
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

    /** Outcome of one folder's walk pass (issue: round 6 #10). */
    private enum class FolderWalkOutcome {
        /** Fully listed and reconciled. */
        COMPLETED,
        /** Fully listed; only the move reconciliation is deferred. Independent folders continue. */
        DEFERRED_MOVES,
        /** The listing itself is incomplete: the folder's membership is unknown. */
        INCOMPLETE,
    }

    /**
     * Aggregates all pages of one remote folder, then upserts and marks its children. The cache
     * membership is only replaced, and the stale reconciliation only runs, once the folder has
     * been fully listed with valid pages and a terminating response: an incomplete pagination
     * (missing data, `hasMore` without cursor, non-advancing cursor) mutates nothing and reports
     * [FolderWalkOutcome.INCOMPLETE] so the walk is retried (issue #1902). A fully listed folder
     * whose move reconciliation deferred reports [FolderWalkOutcome.DEFERRED_MOVES] — its
     * membership is coherent and independent folders keep walking (issue: round 6 #10).
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
    ): FolderWalkOutcome {
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
                    return FolderWalkOutcome.INCOMPLETE
                }
                SentryLog.i(TAG, "Failed to list folder $folderId (cursor: ${cursor != null})")
                return FolderWalkOutcome.INCOMPLETE
            }

            val pageData = apiResponse.data
            if (pageData == null) {
                // A successful response without data is not an empty page: treat it as
                // incomplete rather than reconciling against nothing.
                SentryLog.i(TAG, "Incomplete listing for folder $folderId (null page data)")
                return FolderWalkOutcome.INCOMPLETE
            }
            allPages.addAll(pageData)

            when {
                apiResponse.hasMoreAndCursorExists -> {
                    val nextCursor = apiResponse.cursor
                    if (nextCursor == cursor) {
                        SentryLog.i(TAG, "Incomplete listing for folder $folderId (cursor not advancing)")
                        return FolderWalkOutcome.INCOMPLETE
                    }
                    cursor = nextCursor
                }
                apiResponse.hasMore -> {
                    // hasMore without a cursor cannot be continued: incomplete.
                    SentryLog.i(TAG, "Incomplete listing for folder $folderId (hasMore without cursor)")
                    return FolderWalkOutcome.INCOMPLETE
                }
                else -> {
                    lastResponse = apiResponse
                    break
                }
            }
        }

        val last = lastResponse ?: return FolderWalkOutcome.INCOMPLETE
        val remoteIds = allPages.mapTo(mutableSetOf()) { it.id }

        // Capture every locally owned copy (explicit pins included — the walk would otherwise
        // replace their metadata too — and directories excluded, since directories are not file
        // content) at its OLD location, BEFORE any metadata replacement (issue: round 4 #9).
        data class OldOwnedCopy(val fileId: Int, val oldOffline: IOFile, val oldName: String, val oldParentId: Int, val oldLastModifiedAt: Long)
        val oldOwnedCopies = allPages.mapNotNull { remoteFile ->
            val oldRow = FileController.getFileById(realm, remoteFile.id) ?: return@mapNotNull null
            if (oldRow.isFolder()) return@mapNotNull null
            val oldOffline = oldRow.getOfflineFile(context, userDrive.userId)?.takeIf { it.exists() } ?: return@mapNotNull null
            OldOwnedCopy(remoteFile.id, oldOffline, oldRow.name, oldRow.parentId, oldRow.lastModifiedAt)
        }

        // Preserve BEFORE committing replacement metadata: files whose reconciliation did not
        // complete keep their OLD metadata (excluded from this page's commit) so no edited bytes
        // are ever stranded at an untracked path (issues: round 4 #2, #5, #9).
        val unresolvedMoveIds = mutableSetOf<Int>()
        oldOwnedCopies.forEach { old ->
            val newIdentity = allPages.firstOrNull { it.id == old.fileId } ?: return@forEach
            // Location changes are detected on the RESOLVED offline paths (issue: round 5 #7),
            // exactly like the per-file download manager: a remote path string can never equal
            // the local offline directory path, so the previous comparison classified every
            // unchanged file as a move and pushed clean copies through reconciliation.
            val newOffline = newIdentity.getOfflineFile(context, userDrive.userId)
            val locationChanged = newOffline == null || newOffline.path != old.oldOffline.path
            if (locationChanged && !OfflineFolderController.reconcileMovedInheritedCopy(
                    context = context,
                    oldOfflineFile = old.oldOffline,
                    oldIdentityName = old.oldName,
                    oldIdentityParentId = old.oldParentId,
                    oldLastModifiedAt = old.oldLastModifiedAt,
                    newIdentity = newIdentity,
                    userDrive = userDrive,
                )
            ) {
                unresolvedMoveIds.add(old.fileId)
            }
        }
        if (unresolvedMoveIds.isNotEmpty()) {
            SentryLog.i(TAG, "Deferred ${unresolvedMoveIds.size} move(s) whose old copies are not safely preserved yet")
        }

        val committableFiles = allPages.filterNot { it.id in unresolvedMoveIds }

        // Deferred moves keep their presentation membership (issue: round 5 #8): the retained
        // OLD row joins the commit list so the folder listing does not lose the file while its
        // reconciliation is deferred. Managed rows are merged with themselves by
        // saveRemoteFiles and re-flagged at their old location, which never schedules a
        // re-download: deferred copies are dirty or upload-protected by definition.
        val presentationFiles = if (unresolvedMoveIds.isEmpty()) {
            committableFiles
        } else {
            committableFiles + unresolvedMoveIds.mapNotNull { FileController.getFileById(realm, it) }
        }

        val completeResponse = CursorApiResponse<List<File>>(
            result = last.result,
            data = presentationFiles,
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
        // (issue #1902). The FULL membership set is passed (issue: round 5 #8): deferred moves
        // are remotely PRESENT files whose reconciliation is unfinished — subtracting them would
        // classify retained sources as confirmed deletions and clear their coverage.
        cleanupConfirmedAbsentChildren(context, realm, folderId, userDrive, remoteIds)

        if (unresolvedMoveIds.isNotEmpty()) {
            // The move reconciliation is incomplete: the affected files keep their old metadata
            // and location this round, and the root stays incomplete for retry (issue: round
            // 5 #8/#9). The folder itself WAS fully listed — independent folders keep walking
            // so one occupied destination cannot starve the whole root (issue: round 6 #10).
            return FolderWalkOutcome.DEFERRED_MOVES
        }

        return FolderWalkOutcome.COMPLETED
    }

    /**
     * Commits the (already preservation-filtered) remote children of [folderId] into the local
     * cache and materializes the folder coverage on them.
     */
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
            userDrive = userDrive,
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
            // Stop at every remaining active root encountered deeper in the subtree: its own
            // record and walk establish (or disprove) its coverage independently (issue: round
            // 3 #11).
            if (currentId != rootFolderId && OfflineFolderController.hasActiveOfflineRoot(currentId, customRealm = realm)) {
                continue
            }
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
            // Include the item's own active root row: a top-level root is not an orphan, and a
            // pending-removal root's subtree stays owned by its record until its cleanup
            // completes (issue: round 3 #10).
            .filter { !OfflineFolderController.isCoveredByRootIncludingSelf(it.id, customRealm = realm) }

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
