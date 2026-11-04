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

        val completeResponse = CursorApiResponse<List<File>>(
            result = last.result,
            data = allPages,
            error = last.error,
            responseAt = last.responseAt,
            cursor = last.cursor,
            hasMore = false,
        )

        // Explicit per-file outcomes (issues: round 13 #8, round 14 #8): the commit itself
        // reports which rows did NOT take the listing's identity, classified at the commit —
        // no post-commit inference from resolved paths, and no conflation of an unresolved
        // rejection with a proven supersession. Every sibling continues regardless: the
        // outcomes are per-file, never an abort.
        val commitOutcomes = processPage(
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

        // DEFERRALS keep the walk incomplete (issue: round 14 #8): an ambiguous identity tie
        // rejected for a missing observation is NOT a completed reconciliation — its retained
        // old copy can be intact and produce no download, so treating it as complete would
        // advance root completion over unfinished work. A PROVEN supersession is the listing
        // losing the race: the next listing refetches it and does not defer the walk.
        val deferredMoveIds = commitOutcomes.deferredIds

        if (deferredMoveIds.isNotEmpty()) {
            // The reconciliation is incomplete: the affected files keep their old metadata
            // and location this round, and the root stays incomplete for retry (issue: round
            // 5 #8/#9). The folder itself WAS fully listed — independent folders keep walking
            // so one occupied destination cannot starve the whole root (issue: round 6 #10).
            SentryLog.i(TAG, "Deferred ${deferredMoveIds.size} file(s) whose old copies are not safely preserved yet")
            return FolderWalkOutcome.DEFERRED_MOVES
        }

        return FolderWalkOutcome.COMPLETED
    }

    /**
     * Commits the (already preservation-filtered) remote children of [folderId] into the local
     * cache and materializes the folder coverage on them. Returns [FileController.saveRemoteFiles]'s
     * explicit per-file commit outcomes (issues: round 13 #8, round 14 #8).
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
    ): FileController.ListingCommitOutcomes {
        val remoteFiles = apiResponse.data ?: return FileController.ListingCommitOutcomes(emptySet(), emptySet())
        val folderProxy = FileController.getFileById(realm, folderId)

        val commitOutcomes = FileController.saveRemoteFiles(
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
        return commitOutcomes
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
    /**
     * Freshness predicate for remote-listing cleanup (issue: round 9 #6): the authoritative
     * listing of a parent authorizes clearing only a row whose LIVE parent relationship still
     * matches — a row moved under another parent between collection and clearing belongs to
     * that other subtree now, and this listing does not speak for it. A row deleted in the
     * meantime is skipped as well (nothing left to clear).
     */
    private fun listingClearable(expectedParentId: Int): (Realm, File) -> Boolean = { currentRealm, candidate ->
        FileController.getFileById(currentRealm, candidate.id)?.parentId == expectedParentId
    }

    /**
     * Destructive-decision predicate for the flat children cleanup (issues: round 11 #8,
     * round 12 #9): the listing-parent match of [listingClearable] PLUS a revalidation,
     * inside the committing transaction, that no root covering the candidate CHANGED INTENT
     * after [traversalStartedGeneration]. Roots whose intent predates the traversal stay the
     * listing's maintenance responsibility (the walk must keep clearing stale rows beneath
     * them); a root created or revived after the traversal collected this child is a newly
     * authoritative owner the collection checks never saw — it stops this decision.
     */
    private fun listingAbsentClearable(expectedParentId: Int, traversalStartedGeneration: Long): (Realm, File) -> Boolean =
        { currentRealm, candidate ->
            listingClearable(expectedParentId)(currentRealm, candidate) &&
                !OfflineFolderController.hasActiveRootCoveringSince(candidate.id, currentRealm, traversalStartedGeneration)
        }

    /**
     * Destructive-decision predicate for the absent-subtree traversal (issues: round 11
     * #8/#10, round 12 #10, round 13 #6), evaluated inside the committing transaction on the
     * refreshed live row:
     *  - the ORIGINAL boundary's listed-parent authority still holds — the boundary folder
     *    must still hang from [boundaryListedParentId], the parent its listing spoke for;
     *    F's own move beneath another subtree (or beneath a pre-existing root) invalidates
     *    every remaining descendant, which the candidate-to-boundary chain alone cannot see;
     *  - the candidate's CURRENT ancestry must reach the boundary through live parent links,
     *    with an INDEPENDENT active root along the chain checked BEFORE the boundary is
     *    treated as reached (that root's own walk owns the coverage);
     *  - the boundary candidate ITSELF is handled explicitly — the chain walk starts at the
     *    candidate's parent, so a self-started chain can never reach itself;
     *  - no root covering the candidate changed intent after [traversalStartedGeneration]
     *    (issue: round 12 #9).
     */
    private fun absentSubtreeClearable(
        boundaryFolderId: Int,
        boundaryListedParentId: Int,
        traversalStartedGeneration: Long,
    ): (Realm, File) -> Boolean = { currentRealm, candidate ->
        val boundaryHolds = FileController.getFileById(currentRealm, boundaryFolderId)?.parentId == boundaryListedParentId
        var reachesBoundary = candidate.id == boundaryFolderId
        var currentId: Int? = FileController.getFileById(currentRealm, candidate.id)?.parentId
        val visited = mutableSetOf(candidate.id)
        while (!reachesBoundary && currentId != null && visited.add(currentId)) {
            // Independent-root ownership FIRST, on every chain node INCLUDING the boundary
            // (issue: round 14 #9): an active root at any node — or at the boundary itself —
            // owns the coverage, and reaching the boundary must not short-circuit that
            // check the way the previous order did.
            if (OfflineFolderController.hasActiveOfflineRoot(currentId, customRealm = currentRealm)) break
            if (currentId == boundaryFolderId) {
                reachesBoundary = true
                break
            }
            currentId = FileController.getFileById(currentRealm, currentId)?.parentId
        }
        boundaryHolds && reachesBoundary &&
            // FRESH ownership checks that do not depend on the generation (issue: round 14
            // #9): the boundary's own record and the candidate's own record (the
            // boundary-self case bypasses the chain walk entirely) are re-read INSIDE this
            // committing transaction — a root committed after the walk's last outer
            // transaction is still seen here, so the generation stamp alone never decides.
            !OfflineFolderController.hasActiveOfflineRoot(boundaryFolderId, customRealm = currentRealm) &&
            !OfflineFolderController.hasActiveOfflineRoot(candidate.id, customRealm = currentRealm) &&
            !OfflineFolderController.hasActiveRootCoveringSince(candidate.id, currentRealm, traversalStartedGeneration)
    }

    private fun cleanupConfirmedAbsentChildren(
        context: Context,
        realm: Realm,
        folderId: Int,
        userDrive: UserDrive,
        remoteFileIds: Set<Int>,
    ) {
        val result = OfflineFolderController.RemovalResult()
        // The traversal's freshness horizon (issues: round 11 #8, round 12 #9): a root whose
        // intent changed AFTER this generation stamp — created, revived or removed — is a
        // newly authoritative owner the collection checks never saw, and every destructive
        // decision below revalidates against it.
        val traversalStartedGeneration = OfflineFolderController.currentRootIntentGeneration()
        val children = realm.where(File::class.java)
            .equalTo(File::parentId.name, folderId)
            .findAll()
            .createSnapshot()

        children.forEach { child ->
            if (child.id in remoteFileIds) return@forEach

            if (child.isFolder()) {
                if (OfflineFolderController.isOfflineRoot(child.id, customRealm = realm)) return@forEach
                clearConfirmedAbsentSubtree(context, realm, child.id, folderId, userDrive, result, traversalStartedGeneration)
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
                    // The listing of [folderId] is this cleanup's authority (issue: round 9 #6),
                    // revalidated against roots committed after collection (issue: round 11 #8).
                    isStillClearable = listingAbsentClearable(folderId, traversalStartedGeneration),
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
        listedParentId: Int,
        userDrive: UserDrive,
        result: OfflineFolderController.RemovalResult,
        traversalStartedGeneration: Long,
    ) {
        val visited = mutableSetOf(rootFolderId)
        // folderId to the parent the traversal reached it from — the only parent relationship
        // this cleanup's listing authority covers (issue: round 9 #6).
        val queue = ArrayDeque<Pair<Int, Int>>()
        queue.add(rootFolderId to listedParentId)

        while (queue.isNotEmpty()) {
            val (currentId, collectedParentId) = queue.removeFirst()
            // Active-root revalidation at EVERY queue node, INCLUDING the boundary folder
            // itself (issue: round 11 #8): a root record committed after the traversal decided
            // [rootFolderId] was not a root owns the subtree's coverage now — its own walk
            // establishes it, so this traversal stops entirely instead of clearing beneath
            // the new authority. A deeper root keeps its independent walk (issue: round 3 #11).
            if (OfflineFolderController.hasActiveOfflineRoot(currentId, customRealm = realm)) {
                if (currentId == rootFolderId) break
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
                // The original absent-subtree boundary, its listed-parent authority and the
                // new-root check travel INSIDE the decision (issues: round 11 #8/#10, 13 #6).
                isStillClearable = absentSubtreeClearable(rootFolderId, listedParentId, traversalStartedGeneration),
            )
            // The subtree's original listing authority dies with the folder's rejection
            // (issue: round 10 #6): if the folder was not cleared because its live parent
            // moved — another subtree owns it now — descending would clear children whose
            // only "authority" is an immediate-parent match against the moved row. The
            // boundary is revalidated at EVERY level, not assumed from the top; the same
            // boundary also gates every descendant's destructive decision below, so a move
            // BETWEEN queue iterations stops the remaining descendants too (issue: round 11 #10).
            if (!listingClearable(collectedParentId)(realm, currentFolder)) continue

            realm.where(File::class.java)
                .equalTo(File::parentId.name, currentId)
                .findAll()
                .createSnapshot()
                .forEach { child ->
                    if (child.isFolder()) {
                        if (visited.add(child.id)) queue.add(child.id to currentId)
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
                            isStillClearable = absentSubtreeClearable(rootFolderId, listedParentId, traversalStartedGeneration),
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
                // Fresh coverage predicate INSIDE the clearing transaction (issue: round 8
                // #8): a root covering this row after the snapshot filter ran stops the
                // clearing — the filter alone saw only the traversal-time state.
                isStillClearable = { currentRealm, candidate ->
                    !OfflineFolderController.isCoveredByRootIncludingSelf(candidate.id, customRealm = currentRealm)
                },
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
