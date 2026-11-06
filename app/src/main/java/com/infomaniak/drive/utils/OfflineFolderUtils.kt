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
import com.infomaniak.drive.utils.Utils.ROOT_ID
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
        /**
         * The root-intent generation captured BEFORE the walk's first request (issue: round
         * 18 #20/#25): the batch's root completion compares against it, so a root removed
         * and revived — or a newer walk started — while this one ran is never completed by
         * this older batch.
         */
        val intentGeneration: Long = 0L,
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

            // REQUEST-TIME AUTHORITY (issues: round 18 #20/#25): the root's intent
            // generation is captured BEFORE any listing request — the walk's completion (and
            // its destructive absent-child cleanup) compare against THIS stamp, so changes
            // that happen while the walk is in flight are newer work's business, not this
            // walk's to complete or clear.
            val intentGeneration = OfflineFolderController.rootIntentGeneration(userDrive, rootFolderId)

            // REQUEST-TIME SUBTREE TOKENS (issue: round 22 #11): the absent-subtree
            // traversal collects its candidates from the CURRENT realm after HTTP — a
            // descendant moved into the subtree after the request has no request-time
            // observation and must never be cleared by an older response. Every row inside
            // the walk root's tree as it stood BEFORE the first listing request (depth-one
            // detached copies) is the candidate token set for every folder's cleanup in
            // this walk. A deeper folder's own listing is younger than this capture, so
            // its cleanup is CONSERVATIVE: rows that joined between the capture and that
            // listing are deferred (the next walk re-captures), never over-cleared — a row
            // that LEFT the subtree fails the expected-parent check instead. The map's
            // parent links also reconstruct each subtree-cleanup boundary's REQUEST-TIME
            // ancestor chain, pinning its RESOLVED LOCATION: an ancestor above a listed
            // parent moving changes the boundary's resolved offline path without changing
            // its parentId, and the old response is not the authority for the relocated
            // location either.
            val subtreeObservedById = run {
                val map = HashMap<Int, File>()
                // THE WALK ROOT'S OWN REQUEST-TIME ROW (issue: round 23 #11): the map
                // previously held descendants only — the root itself was absent, so the
                // reconstructed request ancestry stopped at the root and the root's own
                // relocation/publication since the request was invisible to the tokens.
                FileController.getFileById(realm, rootFolderId)?.let { rootRow ->
                    map.putIfAbsent(rootRow.id, realm.copyFromRealm(rootRow, 1))
                    // THE WALK ROOT'S REQUEST-TIME ANCESTRY (issue: round 24 #11): the map
                    // previously stopped at the walk root — ancestors ABOVE it were absent,
                    // so the boundary's reconstructed request chain could not verify
                    // relocations above the root (an ancestor moving changes the resolved
                    // offline path without changing any parentId inside the subtree). The
                    // complete chain up to its zero-parent boundary (bounded) is captured
                    // with the same pre-request detachment.
                    var ancestorId: Int? = rootRow.parentId
                    val visitedAncestors = mutableSetOf<Int>()
                    while (ancestorId != null && ancestorId > 0 && visitedAncestors.add(ancestorId)) {
                        val ancestorRow = FileController.getFileById(realm, ancestorId) ?: break
                        map.putIfAbsent(ancestorRow.id, realm.copyFromRealm(ancestorRow, 1))
                        ancestorId = ancestorRow.parentId
                    }
                }
                val folderStack = ArrayDeque(listOf(rootFolderId))
                val visitedFolders = mutableSetOf<Int>()
                while (folderStack.isNotEmpty()) {
                    val currentId = folderStack.removeFirst()
                    if (!visitedFolders.add(currentId)) continue
                    realm.where(File::class.java)
                        .equalTo(File::parentId.name, currentId)
                        .findAll()
                        .createSnapshot()
                        .forEach { row ->
                            map.putIfAbsent(row.id, realm.copyFromRealm(row, 1))
                            if (row.isFolder()) folderStack.addLast(row.id)
                        }
                }
                map
            }
            // PER-CANDIDATE PUBLICATION/INTENT TOKENS (issue: round 23 #11): the request-
            // time provenance record's (version, content hash) and the row's intent flag —
            // a new publication, a restoration, or a re-pin since the request changes the
            // current token, and the old absence authority no longer clears the candidate.
            val subtreeRecordTokensById = run {
                // NULLABLE VALUES (issue: round 25 #11): a ZERO token is an explicit
                // NO-RECORD capture; NULL is a capture-time READ FAILURE — the destructive
                // predicates treat null as never-matching (defer), so absence and failure
                // are distinguished on both sides.
                val tokens = HashMap<Int, OfflineFolderUtils.AbsentChildPublicationToken?>()
                subtreeObservedById.values.forEach { observed ->
                    if (!observed.isFolder()) {
                        // The provenance registry is keyed by the SCOPED java.io.File (issue:
                        // round 24 #1): the model row is resolved to its own offline path under
                        // the walk's captured scope before the record is read — never the
                        // active account. The capture and every validation build their tokens
                        // through the same helper.
                        tokens[observed.id] = publicationTokenOf(context, observed, userDrive.userId)
                    }
                }
                tokens
            }

            val fileIds = mutableListOf<Int>()
            val visitedFolderIds = mutableSetOf(rootFolderId)
            val folderQueue = ArrayDeque(listOf(rootFolderId))
            var completed = true

            // Re-assert the pending marker on the root folder itself: it drives the folder row
            // progress indicator until this walk finishes.
            FileController.updateFile(rootFolderId, realm) {
                it.isMarkedAsOffline = true
                it.bumpOfflineIntentGeneration()
            }

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
                    subtreeObservedById = subtreeObservedById,
                    subtreeRecordTokensById = subtreeRecordTokensById,
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

            WalkResult(rootExists = true, completed = completed, fileIds = fileIds, intentGeneration = intentGeneration)
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
        subtreeObservedById: Map<Int, File>,
        subtreeRecordTokensById: Map<Int, AbsentChildPublicationToken?>,
    ): FolderWalkOutcome {
        val allPages = mutableListOf<File>()
        var lastResponse: CursorApiResponse<List<File>>? = null
        var cursor: String? = null
        // THE EXECUTING CONTEXT IS CAPTURED ONCE (issue: round 24 #1): the non-suspending
        // checkCancelled callback below cannot read the suspend `coroutineContext` property —
        // the walk's actual context is captured here, before the first request, and every
        // callback invocation checks THIS job.
        val walkContext = coroutineContext

        // REQUEST-TIME ROOT GENERATION (issue: round 18 #25): captured BEFORE the first
        // listing request — the absent-child cleanup's freshness horizon is the REQUEST's,
        // not the cleanup's; a root removed and revived (or a newer walk started) while the
        // response was in flight predates this stamp.
        val requestGeneration = OfflineFolderController.rootIntentGeneration(userDrive, rootFolderId)

        // COMPLETE PRE-REQUEST OBSERVATIONS (issues: round 16 #16, round 19 #25): the
        // folder's children as they stood before the FIRST listing request — detached at
        // depth one, so the embedded version survives (issue: round 16 #9). The basis is
        // the DURABLE PARENT MEMBERSHIP (the parentId chains), captured BEFORE the HTTP
        // request — the previous presentation children-list basis was cleared and rebuilt
        // by every first-page commit, so a retained pending candidate whose link a
        // previous pass had dropped lost its observation, and with it its cleanup retry
        // authority. They anchor tied identity changes through the shared boundary and
        // the committing transaction, and they are ALSO the cleanup's candidate authority
        // (issue: round 18 #25): a child whose membership was established AFTER the
        // request (a move-in, or a newer walk's publication) is not in this map and is
        // never cleared by this response.
        val observedById = realm.where(File::class.java)
            .equalTo(File::parentId.name, folderId)
            .findAll()
            .associateBy({ child -> child.id }) { child -> realm.copyFromRealm(child, 1) }

        while (true) {
            coroutineContext.ensureActive()

            val apiResponse = ApiRepository.getFolderFiles(
                okHttpClient = okHttpClient,
                driveId = userDrive.driveId,
                parentId = folderId,
                cursor = cursor,
                order = order,
            )

            // CANCELLATION IMMEDIATELY POST-HTTP, BEFORE ANY EFFECT (issue: round 19 #26):
            // the loop-top check ran BEFORE the request — a cancelled walk whose response
            // just arrived must not run any effect from it (the root-removal path in
            // particular).
            coroutineContext.ensureActive()

            if (!apiResponse.isSuccess()) {
                // A deleted root folder answers object_not_found on its first page: only this
                // explicit, verified missing-object response drops the durable root; transient
                // or indeterminate failures preserve it for a retry.
                if (isRootFolder && cursor == null && isRootGone(apiResponse)) {
                    removeRootCoverage(context, realm, folderId, userDrive, requestGeneration)
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
        // Post-HTTP cancellation before the commit/cleanup effects (issue: round 19 #26).
        coroutineContext.ensureActive()
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
            observedById = observedById,
            checkCancelled = { walkContext.ensureActive() },
        )

        // Fully listed: children confirmed absent from this authoritative listing are reconciled
        // from durable coverage membership, not from the replaceable presentation child list
        // (issue #1902). The FULL membership set is passed (issue: round 5 #8): deferred moves
        // are remotely PRESENT files whose reconciliation is unfinished — subtracting them would
        // classify retained sources as confirmed deletions and clear their coverage.
        // CANCELLATION BEFORE POST-RESPONSE MUTATION (issue: round 18 #25): the destructive
        // cleanup never runs for a cancelled walk.
        coroutineContext.ensureActive()
        // REQUEST-TIME AUTHORITY (issue: round 18 #25): the cleanup's freshness horizon and
        // its candidate observations are the REQUEST's, not the cleanup's — the root
        // generation and the pre-request children were captured BEFORE the listing, so a
        // root removed and revived (or a child whose membership was established by a newer
        // walk or move) while the response was in flight predates this stamp and is never
        // cleared by it.
        cleanupConfirmedAbsentChildren(
            context, realm, folderId, userDrive, remoteIds, requestGeneration, observedById,
            subtreeObservedById, subtreeRecordTokensById,
        )

        // DEFERRALS keep the walk incomplete (issue: round 14 #8): an ambiguous identity tie
        // rejected for a missing observation is NOT a completed reconciliation — its retained
        // old copy can be intact and produce no download, so treating it as complete would
        // advance root completion over unfinished work. A PROVEN supersession is the listing
        // losing the race: the next listing refetches it and does not defer the walk.
        val deferredMoveIds = commitOutcomes.deferredIds

        if (deferredMoveIds.isNotEmpty()) {
            // NEW AUTHORITATIVE RESOLUTION for deferred tie-ids (issues: round 18 #4/#21,
            // replacing the round-17 local-anchor re-commit): the previous design grafted a
            // POST-RESPONSE local row onto the ALREADY-RECEIVED listing response as its
            // "missing" observation — a fabricated request observation. Trace: the request
            // observed F at A; another operation commits F at B with tied timestamps and a
            // local edit; the listing correctly rejects the stale A response; the retry then
            // observed B AFTER receiving A, passed the same-observation check, and could
            // register B's edit under A's identity and delete/move B's source. An
            // already-received response's observation is IMMUTABLE — it is never replaced.
            // Instead, each selected deferred id gets a NEW details request issued AFTER its
            // detached pre-request observation is captured, and THAT response is committed
            // against THAT observation through the same preservation boundary and the
            // in-transaction conditional acceptance — as a SINGLE-FILE commit, never
            // [processPage] (its isFirstPage=true commit would replace the folder's whole
            // membership). A move-in candidate's old row (the reason every listing of this
            // folder deferred it) is the genuine pre-request observation of its NEW request,
            // so the livelock the round-17 design addressed stays resolved. UNRESOLVED ids
            // (no row, failed request, superseded/preservation deferral) keep the walk
            // incomplete for the next pass.
            // ALL UNRESOLVED IDS ARE TRACKED (issue: round 19 #19): an id leaves this set
            // only when ITS OWN resolution is accepted — the folder completes only when
            // the set is EMPTY, never because one resolution succeeded.
            val unresolvedIds = deferredMoveIds.toMutableSet()
            for (deferredId in fairDeferredSelection(deferredMoveIds, folderId, DEFERRED_RESOLUTION_BUDGET)) {
                coroutineContext.ensureActive()
                // PRE-REQUEST observation (issue: round 18 #4): the candidate's current local
                // row, detached BEFORE the resolving request — never grafted onto the old
                // response.
                val preObservation = FileController.getFileById(realm, deferredId)
                    ?.let { row -> realm.copyFromRealm(row, 1) } ?: continue
                val detailsResponse = runCatching {
                    ApiRepository.getFileDetails(
                        file = File(id = deferredId, driveId = userDrive.driveId),
                        okHttpClient = okHttpClient,
                    )
                }.getOrNull()
                // CANCELLATION AFTER EACH RESOLVING HTTP (issue: round 20 #22): the
                // pre-request check ran before the blocking call — a cancelled walk whose
                // response just arrived must not run snapshot registration, relocation,
                // metadata commits or coverage writes from it. Cancellation PROPAGATES
                // (never an ordinary resolution failure).
                coroutineContext.ensureActive()
                val details = detailsResponse?.data?.takeIf { it.id == deferredId } ?: continue
                // ...and AGAIN AFTER the coordination wait (issue: round 21 #22): the
                // previous second check ran BEFORE the lock acquisition, so a cancellation
                // while blocked still allowed preservation and commits afterward. The
                // executing context is captured in the suspend caller and checked with the
                // non-suspending extension INSIDE the section, after all required locks
                // are held, before any side effect.
                val executingContext = coroutineContext
                val accepted = synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    OfflineFileCoordination.withLocksFor(userDrive, listOf(deferredId, folderId).distinct()) {
                        executingContext.ensureActive()
                        val committable = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                            realm = realm,
                            remoteFiles = listOf(details),
                            userDrive = userDrive,
                            observedById = mapOf(deferredId to preObservation),
                        ).first()
                        if (committable !== details) {
                            // Preservation deferred (stale gate or unfinished preservation):
                            // the id stays deferred for the next pass.
                            false
                        } else {
                            val committed = FileController.updateExistingFileIfNotOlder(
                                newFile = committable,
                                realm = realm,
                                userDrive = userDrive,
                                observed = preObservation,
                            )
                            if (committed) {
                                // RESOLVED MOVE-IN MATERIALIZATION (issue: round 19 #19):
                                // a single-file commit bypasses [processPage], whose child
                                // commits link the row under the container, materialize
                                // inherited coverage and mark the download intent — without
                                // this a resolved move-in stayed stranded from traversal
                                // with no coverage and no download.
                                realm.executeTransaction {
                                    val freshRow = FileController.getFileById(realm, deferredId)
                                        ?: return@executeTransaction
                                    val container = FileController.getFileById(realm, folderId)
                                    if (container != null && freshRow.parentId == folderId &&
                                        container.children.none { it.id == deferredId }
                                    ) {
                                        container.children.add(freshRow)
                                    }
                                    // COVERAGE + DOWNLOAD INTENT (mirrors processPage's
                                    // materialization): flagged covered only under the
                                    // walk's STILL-ACTIVE root; the pending marker drives
                                    // the worker for a copy that is not proven current and
                                    // not owned by a pending upload.
                                    if (!freshRow.isFolder() &&
                                        OfflineFolderController.hasActiveOfflineRoot(rootFolderId, customRealm = realm) &&
                                        OfflineFolderController.isInsideActiveRoot(realm, deferredId)
                                    ) {
                                        freshRow.isInOfflineFolder = true
                                        val offlineFile = freshRow.getOfflineFile(context, userDrive.userId)
                                        // THE COMPLETION TIER (review-53 §4, stage 5), as in
                                        // the walk's download decision: the marker's work
                                        // completes only at the content-verified tier — a
                                        // metadata-current (v2) copy still owes its
                                        // verification re-drive, and the worker's no-spin gate
                                        // bounds the body re-download.
                                        val needsDownload = offlineFile == null ||
                                            !DownloadOfflineFileManager.offlineCopyIsServerContentVerified(
                                                context, freshRow, offlineFile, userDrive,
                                            )
                                        if (needsDownload && (offlineFile == null ||
                                                !UploadFile.hasPendingUpload(uri = Uri.fromFile(offlineFile).toString()))
                                        ) {
                                            freshRow.isMarkedAsOffline = true
                                            freshRow.bumpOfflineIntentGeneration()
                                            fileIds.add(freshRow.id)
                                        }
                                    }
                                }
                            }
                            committed
                        }
                    }
                }
                if (accepted) unresolvedIds.remove(deferredId)
            }
            if (unresolvedIds.isNotEmpty()) {
                // ANY UNRESOLVED ID KEEPS THE FOLDER INCOMPLETE (issue: round 19 #19): one
                // accepted resolution is that file's resolution, not the folder's
                // completion — the affected files keep their old metadata and location,
                // and the root stays incomplete for retry (issue: round 5 #8/#9). The
                // folder itself WAS fully listed — independent folders keep walking so one
                // occupied destination cannot starve the whole root (issue: round 6 #10).
                SentryLog.i(TAG, "Deferred ${unresolvedIds.size} file(s) whose old copies are not safely preserved yet")
                return FolderWalkOutcome.DEFERRED_MOVES
            }
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
        observedById: Map<Int, File>,
        checkCancelled: () -> Unit = {},
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
            observedById = observedById,
            // THE WALK'S EXECUTING CONTEXT, POST-LOCKS (issue: round 23 #19): the
            // coroutine context is captured in the suspend caller; the save re-checks it
            // after the complete coordination set is held.
            checkCancelled = checkCancelled,
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
            // THE COMPLETION-TIER COPY CHECK (review-53 §4, stage 5): the walk's skip
            // decision claims the copy EQUALS THE SERVER'S CURRENT CONTENT — only the
            // content-verified tier may skip a download. A metadata-current (v2) copy is
            // re-queued: its transfer either verifies it (a supported server hash
            // converges the copy to the verified tier) or the no-spin gate and the
            // attempt observation bound the re-drive without a body re-download. The
            // receipt-based edit/pending-upload protections below are unchanged — they
            // classify locally, where the local receipt is the adequate authority.
            val hasIntactOfflineCopy = offlineFile != null &&
                DownloadOfflineFileManager.offlineCopyIsServerContentVerified(context, localFile, offlineFile, userDrive)
            val isLocallyModified = offlineFile != null && offlineFile.exists() && OfflineFolderPolicy.isLocallyModified(
                offlineFileLastModifiedMillis = offlineFile.lastModified(),
                // Edit detection judges against the PUBLISHED byte baseline, not accepted
                // server metadata (issue: round 15 #6) — retained old-clean bytes after a
                // backward-dated newer generation commits are not a local edit.
                remoteLastModifiedAtSeconds = localFile.effectivePublishedBaseline,
            )
            localFile to OfflineFolderPolicy.needsDownload(
                hasIntactOfflineCopy = hasIntactOfflineCopy,
                hasPendingUpload = hasPendingUpload,
                isLocallyModified = isLocallyModified,
            )
        }

        // Coverage materialization is an OWNERSHIP write: it joins the ROOT-INTENT protocol
        // (issue: round 15 #13) so a concurrent SAF deletion that requires D's ancestry
        // cannot interleave between its descendant check and its delete.
        synchronized(OfflineFileCoordination.rootIntentMonitor) {
        realm.executeTransaction {
            // Re-check inside the transaction: the root may have been unmarked while this folder
            // was being listed, and the coverage flags must not be (re)introduced by a stale walk.
            if (!OfflineFolderController.hasActiveOfflineRoot(rootFolderId, customRealm = realm)) {
                return@executeTransaction
            }
            // COMPLETE-ANCESTRY revalidation per candidate, INSIDE this later monitored
            // transaction (issue: round 16 #18): the metadata commit and the coverage
            // materialization are separate sections, and SAF can move or delete the
            // intermediate directory D in between (its newly listed child C was not yet
            // marked owned). Root existence alone is insufficient — a candidate whose CURRENT
            // durable ancestry no longer reaches this root (or an independent active root)
            // is not marked: its rows are the orphan sweep's or a later listing's to judge.
            fun reachesRootOrIndependentRoot(candidateId: Int): Boolean {
                var currentId: Int? = candidateId
                val visited = mutableSetOf(candidateId)
                while (currentId != null && currentId != ROOT_ID) {
                    if (currentId != rootFolderId &&
                        OfflineFolderController.hasActiveOfflineRoot(currentId, customRealm = realm)
                    ) {
                        return true
                    }
                    if (currentId == rootFolderId) return true
                    val row = FileController.getFileById(realm, currentId) ?: return false
                    currentId = row.parentId.takeIf { visited.add(it) }
                }
                return false
            }
            // Materialize coverage on descendant folders too, so browsing a downloaded
            // subfolder cannot trigger the offline recovery heuristic (issue #1902).
            subFolderIds.forEach { subFolderId ->
                realm.where(File::class.java).equalTo(File::id.name, subFolderId).findFirst()?.let { subFolder ->
                    if (reachesRootOrIndependentRoot(subFolderId)) subFolder.isInOfflineFolder = true
                }
            }
            coveredFiles.forEach { (localFile, needsDownload) ->
                if (!localFile.isValid) return@forEach
                if (!reachesRootOrIndependentRoot(localFile.id)) return@forEach
                localFile.isInOfflineFolder = true
                if (needsDownload) {
                    localFile.isMarkedAsOffline = true
                    localFile.bumpOfflineIntentGeneration()
                    fileIds.add(localFile.id)
                }
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
    private fun listingAbsentClearable(
        userDrive: UserDrive,
        expectedParentId: Int,
        traversalStartedGeneration: Long,
    ): (Realm, File) -> Boolean =
        { currentRealm, candidate ->
            listingClearable(expectedParentId)(currentRealm, candidate) &&
                !OfflineFolderController.hasActiveRootCoveringSince(userDrive, candidate.id, currentRealm, traversalStartedGeneration)
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
        boundaryObservation: File?,
        subtreeObservedById: Map<Int, File>,
        subtreeRecordTokensById: Map<Int, AbsentChildPublicationToken?>,
        predicateContext: Context,
        predicateUserDrive: UserDrive,
    ): (Realm, File) -> Boolean = { currentRealm, candidate ->
        val liveBoundary = FileController.getFileById(currentRealm, boundaryFolderId)
        // RESOLVED-LOCATION AUTHORITY (issues: round 22 #11, round 25 #10): the boundary's
        // request-time ancestor chain — reconstructed from the pre-request subtree map's
        // parent links — must be a prefix of its LIVE chain, compared as (id, NAME) pairs:
        // an ancestor RENAME preserves every id while changing the resolved offline path,
        // which the id-only comparison could not see. An ancestor above the listed parent
        // moving changes the boundary's resolved offline path WITHOUT changing its parentId
        // (the listed-parent check alone cannot see it); the old response is not the
        // authority for the relocated or renamed location. A live chain that comes up short
        // (a missing ancestor row) also defers.
        val requestAncestry = buildList {
            var current: Int? = subtreeObservedById[boundaryFolderId]?.parentId
            while (current != null && current > 0) {
                add(current to subtreeObservedById[current]?.name)
                current = subtreeObservedById[current]?.parentId
            }
        }
        val liveAncestry = if (liveBoundary == null) emptyList() else buildList {
            var current: Int? = liveBoundary.parentId
            val visited = mutableSetOf<Int>()
            while (current != null && current > 0 && visited.add(current)) {
                val row = FileController.getFileById(currentRealm, current)
                add(current to row?.name)
                current = row?.parentId
            }
        }
        val boundaryLocationHolds = liveAncestry.size >= requestAncestry.size &&
            requestAncestry.withIndex().all { (index, requestAncestor) ->
                liveAncestry[index] == requestAncestor
            }
        val boundaryHolds = liveBoundary != null &&
            liveBoundary.parentId == boundaryListedParentId &&
            boundaryLocationHolds &&
            // The boundary's PRE-REQUEST OBSERVATION is carried into every queued
            // descendant's destructive predicate and compared INSIDE this committing
            // transaction (issues: round 21 #21, round 22 #11): a boundary restored or
            // re-created since the request (same listed parent, newer identity) is a newer
            // generation this response is not the authority for — the listed-parent check
            // alone let its CURRENT descendants be cleared by the old traversal. The
            // observation comparison covers the publication identity too (the materialized
            // published baseline is part of the observation).
            (boundaryObservation == null || FileController.isSameObservation(liveBoundary, boundaryObservation))
        // CANDIDATE REQUEST-TIME TOKEN (issue: round 22 #11): the traversal's candidates
        // are collected from the CURRENT realm after HTTP — a descendant moved INTO the
        // subtree after the request has no request-time observation and is never cleared
        // by this older response (fresh authority reconciles it); a descendant that moved
        // WITHIN the subtree fails the expected-parent match (its request-time parent
        // differs from its current one); a newer generation (publication, restore) fails
        // the full observation comparison.
        val requestObservation = subtreeObservedById[candidate.id]
        // THE COMPLETE CANDIDATE-TO-BOUNDARY REQUEST CHAIN (issue: round 23 #11): the
        // immediate-parent match alone let a candidate whose ANCESTORS moved since the
        // request pass — the request-time chain (reconstructed from the map's parent
        // links) must equal the live chain up to the boundary.
        val requestChainToBoundary = run {
            val chain = mutableListOf<Int>()
            var current: Int? = requestObservation?.parentId
            while (current != null && current > 0) {
                chain.add(current)
                if (current == boundaryFolderId) break
                current = subtreeObservedById[current]?.parentId
            }
            chain
        }
        val liveChainToBoundary = run {
            val chain = mutableListOf<Int>()
            var current: Int? = FileController.getFileById(currentRealm, candidate.id)?.parentId
            val visitedIds = mutableSetOf<Int>()
            while (current != null && current > 0 && visitedIds.add(current)) {
                chain.add(current)
                if (current == boundaryFolderId) break
                current = FileController.getFileById(currentRealm, current)?.parentId
            }
            chain
        }
        // PUBLICATION/RESTORATION AND INTENT TOKENS (issues: round 23 #11, round 24 #11,
        // round 25 #10/#11): the current record's COMPLETE publication token — version,
        // content hash and the fresh per-publication identity — must EQUAL the request-time
        // token through the SHARED builder (an explicit no-record capture is the zero token
        // on BOTH sides; a read failure on either side is null and never matches). A new
        // publication, a restoration with its FRESH identity, or a re-publication of the
        // SAME bytes since the request changes the token — and the durable intent stamp
        // (bumped by every marker transition) plus the marker flag must be unchanged: a
        // re-pin after the request is newer work's business.
        // BYTE-PUBLICATION TOKENS APPLY TO FILES ONLY (issue: round 26 #14): the capture
        // populates publication tokens only for non-folder rows — a DIRECTORY has no byte
        // publication, so no token can ever match and requiring one made every directory
        // candidate fail unconditionally (its stale inherited coverage survived
        // authoritative absence forever). A folder's identity, marker/generation and
        // ancestry validation above remain full-strength.
        val currentRecordToken = if (candidate.isFolder()) {
            null
        } else {
            runCatching {
                publicationTokenOf(predicateContext, candidate, predicateUserDrive.userId)
            }.getOrNull()
        }
        val requestRecordToken = subtreeRecordTokensById[candidate.id]
        val recordTokenHolds = candidate.isFolder() ||
            (requestRecordToken != null &&
                currentRecordToken != null &&
                currentRecordToken == requestRecordToken)
        val candidateTokenHolds = requestObservation != null &&
            requestObservation.parentId == candidate.parentId &&
            FileController.isSameObservation(candidate, requestObservation) &&
            requestChainToBoundary == liveChainToBoundary &&
            requestObservation.isMarkedAsOffline == candidate.isMarkedAsOffline &&
            requestObservation.offlineIntentStamp == candidate.offlineIntentStamp &&
            recordTokenHolds
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
        boundaryHolds && candidateTokenHolds && reachesBoundary &&
            // FRESH ownership checks that do not depend on the generation (issue: round 14
            // #9): the boundary's own record and the candidate's own record (the
            // boundary-self case bypasses the chain walk entirely) are re-read INSIDE this
            // committing transaction — a root committed after the walk's last outer
            // transaction is still seen here, so the generation stamp alone never decides.
            !OfflineFolderController.hasActiveOfflineRoot(boundaryFolderId, customRealm = currentRealm) &&
            !OfflineFolderController.hasActiveOfflineRoot(candidate.id, customRealm = currentRealm) &&
            !OfflineFolderController.hasActiveRootCoveringSince(predicateUserDrive, candidate.id, currentRealm, traversalStartedGeneration)
    }

    private fun cleanupConfirmedAbsentChildren(
        context: Context,
        realm: Realm,
        folderId: Int,
        userDrive: UserDrive,
        remoteFileIds: Set<Int>,
        requestGeneration: Long,
        observedById: Map<Int, File>,
        subtreeObservedById: Map<Int, File>,
        subtreeRecordTokensById: Map<Int, AbsentChildPublicationToken?>,
    ) {
        val result = OfflineFolderController.RemovalResult()
        // REQUEST-TIME freshness horizon (issues: round 11 #8, round 12 #9, round 18 #25):
        // the stamp is the LISTING REQUEST's — a root whose intent changed after the
        // request (created, revived or removed) is a newly authoritative owner the request
        // never saw, and every destructive decision below revalidates against it. The
        // previous capture ran when the cleanup BEGAN — after the listing and the commit —
        // so a revived root whose change predated the late stamp passed the check while an
        // old in-flight response cleared its newly published children.
        val traversalStartedGeneration = requestGeneration
        // THE FOLDER'S REQUEST-TIME LOCATION HOLDS (issue: round 24 #11): the request-time
        // ancestor chain — reconstructed from the pre-request subtree map, which now
        // captures the ancestry ABOVE the walk root — must be a prefix of the folder's
        // LIVE chain. An ancestor relocating changes the resolved offline path of every
        // row beneath it without changing their parentIds; the old absence response is
        // not the authority for the relocated location, and the whole cleanup defers.
        // (id, NAME) PAIRS (issue: round 25 #10): an ancestor RENAME preserves every id
        // while changing the resolved path — the id-only comparison could not see it.
        val requestFolderChain = buildList {
            var current: Int? = subtreeObservedById[folderId]?.parentId
            val seen = mutableSetOf<Int>()
            while (current != null && current > 0 && seen.add(current)) {
                add(current to subtreeObservedById[current]?.name)
                current = subtreeObservedById[current]?.parentId
            }
        }
        val liveFolderChain = buildList {
            var current: Int? = FileController.getFileById(realm, folderId)?.parentId
            val seen = mutableSetOf<Int>()
            while (current != null && current > 0 && seen.add(current)) {
                val row = FileController.getFileById(realm, current)
                add(current to row?.name)
                current = row?.parentId
            }
        }
        val folderLocationHolds = liveFolderChain.size >= requestFolderChain.size &&
            requestFolderChain.withIndex().all { (index, requestAncestor) -> liveFolderChain[index] == requestAncestor }
        if (!folderLocationHolds) {
            SentryLog.i(TAG, "Folder $folderId reconciliation deferred: its request-time location no longer holds")
            return
        }
        val children = realm.where(File::class.java)
            .equalTo(File::parentId.name, folderId)
            .findAll()
            .createSnapshot()

        children.forEach { child ->
            if (child.id in remoteFileIds) return@forEach
            // CANDIDATE AUTHORITY IS THE REQUEST'S (issue: round 18 #25): a child absent
            // from the pre-request observations had its membership established AFTER the
            // request — a move-in, or a newer walk's publication — and this older response
            // is not its listing authority; it is never cleared here.
            if (child.id !in observedById) return@forEach

            if (child.isFolder()) {
                if (OfflineFolderController.isOfflineRoot(child.id, customRealm = realm)) return@forEach
                // The boundary's PRE-REQUEST identity must still describe the live folder
                // (issue: round 19 #25): a folder restored or re-created since the request
                // is a newer identity this response is not the authority for.
                val liveChild = FileController.getFileById(realm, child.id)
                val observedChild = observedById[child.id]
                if (liveChild == null || observedChild == null ||
                    !FileController.isSameObservation(liveChild, observedChild)
                ) {
                    return@forEach
                }
                // The boundary's PRE-REQUEST observation is the ORIGINAL destructive-
                // authority token (issue: round 21 #21): it travels through every queued
                // descendant and is compared inside each final destructive section — the
                // outer pre-check above runs outside the clearing coordination.
                clearConfirmedAbsentSubtree(
                    context,
                    realm,
                    child.id,
                    folderId,
                    userDrive,
                    result,
                    traversalStartedGeneration,
                    boundaryObservation = observedChild,
                    subtreeObservedById = subtreeObservedById,
                    subtreeRecordTokensById = subtreeRecordTokensById,
                )
            } else if (OfflineFolderPolicy.shouldRemoveCoveredFileAbsentFromRemote(
                    isOffline = child.isOffline,
                    isInOfflineFolder = child.isInOfflineFolder,
                )
            ) {
                val fileClearable = listingAbsentClearable(userDrive, folderId, traversalStartedGeneration)
                OfflineFolderController.clearFileCoverage(
                    context = context,
                    realm = realm,
                    file = child,
                    userDrive = userDrive,
                    result = result,
                    isFolder = false,
                    // The listing of [folderId] is this cleanup's authority (issue: round 9 #6),
                    // revalidated against roots changed after the REQUEST (issue: round 18
                    // #25) — and the candidate's FULL PRE-REQUEST IDENTITY is compared
                    // THROUGH the destroying transaction (issue: round 19 #25): a restore
                    // or newer generation committed since the request is not this
                    // response's to clear.
                    isStillClearable = { currentRealm, candidate ->
                        fileClearable(currentRealm, candidate) &&
                            observedById[candidate.id]?.let { observed ->
                                FileController.isSameObservation(candidate, observed)
                            } == true &&
                            // THE REQUEST-TIME MARKER AND DURABLE INTENT STAMP (issue:
                            // round 25 #10): isSameObservation excludes the marker — a
                            // verification marker SET after the listing request was cleared
                            // by the older absence response; the durable intent stamp is
                            // bumped by every marker transition, so an unmark/repin (ABA)
                            // no longer passes either.
                            observedById[candidate.id]?.isMarkedAsOffline == candidate.isMarkedAsOffline &&
                            observedById[candidate.id]?.offlineIntentStamp == candidate.offlineIntentStamp &&
                            // THE PUBLICATION/RESTORATION TOKEN ON THE DIRECT LEAF TOO
                            // (issues: round 24 #11, round 25 #11): the current record's
                            // complete publication token must equal the request-time token
                            // through the SHARED builder (explicit absence = the zero token
                            // on both sides; a read failure on either side defers), inside
                            // this final coordinated decision.
                            (runCatching {
                                publicationTokenOf(context, candidate, userDrive.userId)
                            }.getOrNull()?.let { current ->
                                current == subtreeRecordTokensById[candidate.id]
                            } == true) &&
                            // THE FOLDER'S REQUEST-TIME LOCATION INSIDE THE DECISION (issue:
                            // round 25 #10): the (id, name) ancestor chain — reconstructed
                            // from the pre-request subtree map, which captures the ancestry
                            // above the walk root — must still be a prefix of the live chain
                            // HERE, not only in the outer pre-check.
                            run {
                                val requestChain = buildList {
                                    var current: Int? = subtreeObservedById[folderId]?.parentId
                                    val seenChained = mutableSetOf<Int>()
                                    while (current != null && current > 0 && seenChained.add(current)) {
                                        add(current to subtreeObservedById[current]?.name)
                                        current = subtreeObservedById[current]?.parentId
                                    }
                                }
                                val liveChain = buildList {
                                    var current: Int? = FileController.getFileById(currentRealm, folderId)?.parentId
                                    val seenChained = mutableSetOf<Int>()
                                    while (current != null && current > 0 && seenChained.add(current)) {
                                        val row = FileController.getFileById(currentRealm, current)
                                        add(current to row?.name)
                                        current = row?.parentId
                                    }
                                }
                                liveChain.size >= requestChain.size &&
                                    requestChain.withIndex().all { (index, requestAncestor) ->
                                        liveChain[index] == requestAncestor
                                    }
                            }
                    },
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
        boundaryObservation: File?,
        subtreeObservedById: Map<Int, File>,
        subtreeRecordTokensById: Map<Int, AbsentChildPublicationToken?>,
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
                // The original absent-subtree boundary, its listed-parent authority, its
                // PRE-REQUEST observation token and the new-root check travel INSIDE the
                // decision (issues: round 11 #8/#10, 13 #6, round 21 #21).
                isStillClearable = absentSubtreeClearable(
                    rootFolderId,
                    listedParentId,
                    traversalStartedGeneration,
                    boundaryObservation,
                    subtreeObservedById,
                    subtreeRecordTokensById,
                    context,
                    userDrive,
                ),
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
                            isStillClearable = absentSubtreeClearable(
                                rootFolderId,
                                listedParentId,
                                traversalStartedGeneration,
                                boundaryObservation,
                                subtreeObservedById,
                                subtreeRecordTokensById,
                                context,
                                userDrive,
                            ),
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
    private fun removeRootCoverage(
        context: Context,
        realm: Realm,
        rootFolderId: Int,
        userDrive: UserDrive,
        requestGeneration: Long,
    ) {
        // EXACT REQUEST-GENERATION AUTHORITY UNDER THE INTENT MONITOR (issue: round 19
        // #26): the OBJECT_NOT_FOUND answer belongs to the request whose generation was
        // captured BEFORE it — a root removed and REVIVED (or a newer walk started) while
        // the response was in flight is a newer authority this response says nothing
        // about. The removal runs under the intent monitor, and only while the root's
        // generation is still the request's.
        synchronized(OfflineFileCoordination.rootIntentMonitor) {
            if (OfflineFolderController.rootIntentChangedSince(userDrive, rootFolderId, requestGeneration)) {
                SentryLog.i(
                    TAG,
                    "Root $rootFolderId intent changed while the listing was in flight; removal deferred to the newer authority",
                )
                return
            }
            SentryLog.i(TAG, "Offline root folder $rootFolderId disappeared remotely, removing its coverage")
            OfflineFolderController.removeOfflineRoot(folderId = rootFolderId, userDrive = userDrive, customRealm = realm)
        }
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

    /**
     * The COMPLETE publication token of [row] at its offline path, with an EXPLICIT
     * absent-record representation (issue: round 25 #11): NO record is the zero token —
     * the SAME representation the request-time capture uses, so a covered file with
     * missing bytes and no sidecar compares EQUAL at capture and validation instead of
     * never clearing. A record that EXISTS but cannot be read is NULL — never equal to the
     * absent token: absence and read failure are distinguished, and a read failure defers
     * the destructive decision.
     */
    private fun publicationTokenOf(context: Context, row: File, userId: Int): AbsentChildPublicationToken? {
        val offlineFile = row.getOfflineFile(context, userId) ?: return AbsentChildPublicationToken(0, null, 0)
        if (!DownloadOfflineFileManager.provenanceRecordFileExists(context, offlineFile)) {
            return AbsentChildPublicationToken(0, null, 0)
        }
        val record = DownloadOfflineFileManager.readPublishedProvenance(context, offlineFile) ?: return null
        return AbsentChildPublicationToken(record.version, record.contentHash, record.publicationId)
    }

    /**
     * The request-time publication/restoration token of an absent-child candidate (issue:
     * round 24 #11): the provenance record's version, content hash AND fresh
     * per-publication identity. The (version, hash) pair alone cannot distinguish a later
     * publication or restoration of the SAME bytes — the publication identity is written
     * fresh by every publication, so any publication or restoration after the request
     * changes the token and the old absence authority no longer clears the candidate.
     */
    internal data class AbsentChildPublicationToken(
        val version: Int,
        val contentHash: String?,
        val publicationId: Long,
    )

    /**
     * Bounded authoritative re-fetches per folder pass for deferred move ties (issue: round
     * 17 #17): one pathological folder cannot spin the walk into an unbounded request loop;
     * the remainder stays deferred for the next listing.
     */
    internal const val DEFERRED_RESOLUTION_BUDGET = 4

    /**
     * Rotating resolution cursor per folder (issue: round 18 #21): a FAIR slice of the
     * deferred ids per pass — the previous `sorted().take(budget)` let permanent
     * preservation conflicts occupy the budget on every pass and starve a valid later
     * candidate forever. The cursor advances by the budget each pass and wraps, so every
     * deferred id eventually gets its resolving request; a pass that resolves ANY of its
     * slice keeps the walk going, and a fully-unresolved slice reports the deferral.
     */
    private val deferredResolutionCursors = java.util.concurrent.ConcurrentHashMap<Int, Int>()

    internal fun fairDeferredSelection(deferredIds: Set<Int>, folderId: Int, budget: Int): List<Int> {
        val ordered = deferredIds.sorted()
        if (ordered.size <= budget) return ordered
        // merge returns the NEW cursor value (old + budget; absent counts as 0).
        val newCursor = deferredResolutionCursors.merge(folderId, budget, Int::plus) ?: budget
        val offset = (newCursor - budget).mod(ordered.size)
        return List(budget) { index -> ordered[(offset + index) % ordered.size] }
    }
}
