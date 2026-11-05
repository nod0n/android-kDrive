/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2023-2026 Infomaniak Network SA
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
package com.infomaniak.drive.data.cache

import android.util.Log
import androidx.collection.ArrayMap
import androidx.collection.arrayMapOf
import com.infomaniak.core.common.cancellable
import com.infomaniak.core.network.networking.DefaultHttpClientProvider
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.ApiRoutes
import com.infomaniak.drive.data.api.ErrorCode
import com.infomaniak.drive.data.api.CursorApiResponse
import com.infomaniak.drive.data.cache.FileController.saveRemoteFiles
import com.infomaniak.drive.data.cache.FolderFilesProvider.SourceRestrictionType.ONLY_FROM_REMOTE
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.File.SortType
import com.infomaniak.drive.data.models.FileAction
import com.infomaniak.drive.data.models.FileActivityType
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.models.file.SpecialFolder.SharedWithMe
import com.infomaniak.drive.data.services.MqttClientWrapper
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.FileId
import com.infomaniak.drive.utils.OfflineFileCoordination
import com.infomaniak.drive.utils.OfflineFolderUtils
import com.infomaniak.drive.utils.Utils.ROOT_ID
import io.realm.Realm
import io.realm.RealmQuery
import io.sentry.Sentry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import java.util.Calendar

object FolderFilesProvider {

    private val TAG = FolderFilesProvider::class.java.simpleName

    // Bump this when we want to force-refresh files that are too old.
    // Example: We did it when we added Categories & Colored folders, to automatically display them when updating the app.
    private const val MIN_VERSION_CODE = 5_00_010_01

    /** Missing-ancestor details requests per activity resolution pass (issue: round 23 #15) — bounded, retried next pass. */
    private const val MISSING_ANCESTRY_RESOLUTION_BUDGET = 8

    private val minDateToIgnoreCache = Calendar.getInstance().apply { add(Calendar.MONTH, -2) }.timeInMillis / 1000 // 3 month

    fun getFiles(
        folderFilesProviderArgs: FolderFilesProviderArgs,
        checkCancelled: () -> Unit = {},
        sequenceHadUnresolved: Boolean = false,
    ): FolderFilesProviderResult? {
        val realm = folderFilesProviderArgs.realm ?: FileController.getRealmInstance(folderFilesProviderArgs.userDrive)
        try {
            val folderProxy = FileController.getFileById(realm, folderFilesProviderArgs.folderId)
            val sourceRestrictionType = folderFilesProviderArgs.sourceRestrictionType
            val needToLoadFromRemote = needToLoadFromRemote(sourceRestrictionType, folderProxy)
            SentryLog.i(
                TAG,
                "getFiles with folder: ${folderProxy?.id}, sourceRestrictionType: $sourceRestrictionType, needToLoadFromRemote: $needToLoadFromRemote"
            )
            val files = when {
                needToLoadFromRemote && sourceRestrictionType != SourceRestrictionType.ONLY_FROM_LOCAL -> {
                    // THE SEQUENCE'S UNRESOLVED STATE REACHES THE FINAL TRANSACTION
                    // (issue: round 26 #19): the ordinary listing's accumulated flag rides
                    // getFiles → loadFromRemote → handleRemoteFiles → saveRemoteFiles, so
                    // the final page's SAME transaction commits the durable incompleteness —
                    // never a post-hoc downgrade an interruption can outrun.
                    loadFromRemote(realm, folderProxy, folderFilesProviderArgs, checkCancelled, sequenceHadUnresolved)
                }
                folderFilesProviderArgs.isFirstPage -> {
                    loadFromLocal(realm, folderProxy, folderFilesProviderArgs.withChildren, folderFilesProviderArgs.order)
                }
                else -> {
                    SentryLog.i(TAG, "getFiles: files is null")
                    null
                }
            }
            SentryLog.i(TAG, "Load files finished")
            return files
        } finally {
            if (folderFilesProviderArgs.realm == null) realm.close()
        }
    }

    suspend fun loadRootFiles(userDrive: UserDrive, hasNetwork: Boolean) = withContext(Dispatchers.IO) {
        if (hasNetwork) {
            runCatching {
                getFiles(
                    FolderFilesProviderArgs(
                        folderId = ROOT_ID,
                        isFirstPage = true,
                        order = SortType.NAME_AZ,
                        sourceRestrictionType = ONLY_FROM_REMOTE,
                        userDrive = userDrive,
                    ),
                    // The suspend caller's captured executing context (issue: round 22 #20).
                    checkCancelled = { coroutineContext.ensureActive() },
                )
            }.cancellable().onFailure { throwable ->
                SentryLog.e(TAG, "recursiveDownload failed", throwable)
            }.getOrNull()
        }
    }

    suspend fun loadSharedWithMeFiles(
        folderFilesProviderArgs: FolderFilesProviderArgs,
        onRecursionStart: (() -> Unit)? = null,
    ) {
        with(folderFilesProviderArgs) {
            val rootFolder = File(id = SharedWithMe.id, name = "/")
            val okHttpClient = AccountUtils.getHttpClient(userDrive.userId)
            // The cancellation callback runs inside the non-suspending walk code, so the
            // suspend caller's context is captured here once and checked through it.
            val executingContext = coroutineContext

            FileController.getRealmInstance(userDrive).use { realm ->
                loadSharedWithMeFilesRec(
                    realm = realm,
                    okHttpClient = okHttpClient,
                    folderFilesProviderArgs = this,
                    isRoot = folderId == ROOT_ID,
                    rootFolder = rootFolder,
                    onRecursionStart = onRecursionStart,
                    // CANCELLATION AFTER EACH RESOLVING HTTP (issue: round 20 #22): the
                    // suspend caller's job is checked between the resolving responses and
                    // the side effects.
                    checkCancelled = { executingContext.ensureActive() },
                )
            }
        }
    }

    private tailrec fun loadSharedWithMeFilesRec(
        realm: Realm,
        okHttpClient: OkHttpClient,
        isRoot: Boolean,
        folderFilesProviderArgs: FolderFilesProviderArgs,
        cursor: String? = null,
        rootFolder: File? = null,
        onRecursionStart: (() -> Unit)? = null,
        observedById: Map<Int, File> = emptyMap(),
        accumulatedDeferredIds: Set<Int> = emptySet(),
        checkCancelled: () -> Unit = {},
    ) {
        val folderId = if (isRoot) SharedWithMe.id else folderFilesProviderArgs.folderId
        // PRE-REQUEST observations (issue: round 18 #21): the container's children detached
        // BEFORE the first page of this request sequence — the same anchor the recursive
        // walker commits against, so a same-folder identity tie (e.g. a same-second rename
        // between the request and the commit) resolves through the shared boundary instead
        // of deferring on every pass. The map is captured ONCE per sequence and threaded
        // through every page and the recursion: all pages belong to one request sequence
        // and share its immutable pre-request state. A row moving INTO this folder from
        // another container is not among the pre-request children — its observation is
        // unknowable before the response names it; it defers to the next listing pass,
        // exactly as before (the offline walk resolves such candidates with fresh
        // details requests instead, issue: round 18 #4).
        val sequenceObservations = if (cursor == null) {
            if (isRoot) {
                // PSEUDOCONTAINER MEMBERSHIP (issue: round 21 #17): the Shared ROOT is not a
                // real parent — its members are the pseudocontainer's durable PRESENTATION
                // children list, never a parentId == pseudocontainer-id query (shared rows'
                // real parents are their actual shared folders, so the previous query
                // observed nothing and every root candidate lost its retry authority).
                FileController.getFileById(realm, folderId)?.children?.createSnapshot()
                    ?.associateBy({ child -> child.id }) { child -> realm.copyFromRealm(child, 1) }
                    ?: emptyMap()
            } else {
                // DURABLE PRE-HTTP MEMBERSHIP (issue: round 19 #25): the same basis as the
                // walk's observations — the presentation children list is cleared and rebuilt
                // by every first-page commit, so a retained pending candidate whose link a
                // previous pass dropped lost its observation and its retry authority.
                realm.where(File::class.java)
                    .equalTo(File::parentId.name, folderId)
                    .findAll()
                    .associateBy({ child -> child.id }) { child -> realm.copyFromRealm(child, 1) }
            }
        } else {
            observedById
        }
        val apiResponse = if (isRoot) {
            ApiRepository.getSharedWithMeFiles(
                okHttpClient = okHttpClient,
                order = folderFilesProviderArgs.order,
                cursor = cursor
            )
        } else {
            ApiRepository.getFolderFiles(
                okHttpClient = okHttpClient,
                driveId = folderFilesProviderArgs.userDrive.driveId,
                parentId = folderFilesProviderArgs.folderId,
                cursor = cursor,
                order = folderFilesProviderArgs.order,
            )
        }
        val folderProxy = FileController.getFileById(realm, folderId)

        // POST-HTTP CHECK (issue: round 22 #20): the initial shared-page handling previously
        // proceeded from the response straight to the save — a cancellation while the HTTP
        // blocked was followed by commits. The executing-context check runs here and before
        // every effect below.
        checkCancelled()

        fun saveFiles() = saveRemoteFiles(
            realm = realm,
            localFolderProxy = folderProxy,
            remoteFolder = if (folderFilesProviderArgs.folderId == ROOT_ID) rootFolder else folderProxy,
            apiResponse = apiResponse,
            isFirstPage = cursor == null,
            isCompleteFolder = !apiResponse.hasMore,
            userDrive = folderFilesProviderArgs.userDrive,
            observedById = sequenceObservations,
            // THE INITIAL PAGE'S POST-LOCKS CHECK (issue: round 23 #19): the save checks
            // the executing context again INSIDE saveRemoteFiles, after the complete
            // coordination set is held, immediately before the boundary/transaction.
            checkCancelled = checkCancelled,
            // THE SEQUENCE'S UNRESOLVED STATE RIDES THE FINAL-PAGE TRANSACTION (issue:
            // round 25 #17): earlier pages' deferred candidates make this page's durable
            // completeness false in the SAME transaction that commits it — never a
            // post-hoc downgrade a crash can outrun.
            sequenceHadUnresolved = accumulatedDeferredIds.isNotEmpty(),
        )

        when {
            apiResponse.hasMoreAndCursorExists -> {
                // WHOLE-PAGINATION OUTCOMES (issue: round 21 #17): every page's deferred
                // ids accumulate into the sequence's single resolution pass — only the
                // final page previously supplied candidates, discarding earlier pages'.
                val pageOutcomes = saveFiles()
                if (cursor == null) onRecursionStart?.invoke()
                loadSharedWithMeFilesRec(
                    realm = realm,
                    okHttpClient = okHttpClient,
                    isRoot = isRoot,
                    folderFilesProviderArgs = folderFilesProviderArgs,
                    cursor = apiResponse.cursor,
                    observedById = sequenceObservations,
                    accumulatedDeferredIds = accumulatedDeferredIds + pageOutcomes.deferredIds,
                    checkCancelled = checkCancelled,
                )
            }
            apiResponse.isSuccess() -> {
                val commitOutcomes = saveFiles()
                // ONE budgeted resolution pass for the WHOLE SEQUENCE's deferred candidates
                // (issues: round 19 #20, round 21 #17) — ordinary and shared listings
                // resolve their move-in candidates exactly like the offline walk: fresh
                // pre-observed details requests, fair per-item outcomes. Unresolved
                // candidates wait for the NEXT listing pass; this never converges in place.
                val unresolvedCount = resolveDeferredCandidatesOnce(
                    realm = realm,
                    okHttpClient = okHttpClient,
                    folderId = folderId,
                    userDrive = folderFilesProviderArgs.userDrive,
                    deferredIds = accumulatedDeferredIds + commitOutcomes.deferredIds,
                    checkCancelled = checkCancelled,
                )
                if (unresolvedCount > 0) {
                    // INCOMPLETE RECONCILIATION OUTLIVES PAGINATION (issue: round 22 #17,
                    // round 23 #16): the shared sequence previously IGNORED the resolver's
                    // returned count entirely — a fully listed folder with unresolved
                    // candidates stayed durably complete, and cached completeness
                    // (needToLoadFromRemote) suppressed any redrive. The same durable
                    // marker as the ordinary route: the next visit re-drives the remote
                    // load and the budgeted resolver, independent of pagination; a later
                    // fully resolving pass re-persists the pagination completeness through
                    // saveRemoteFiles.
                    FileController.updateFile(folderId, realm) { folderRow ->
                        folderRow.isComplete = false
                    }
                }
            }
        }
    }

    /**
     * ONE budgeted resolution pass for a listing's deferred candidates (issue: round 19
     * #20): each selected id gets a NEW details request issued after its detached
     * pre-request observation is captured, and THAT response is committed against THAT
     * observation through the shared preservation boundary — the same protocol as the
     * offline walk's resolution (round 18 #4), with FAIR per-item outcomes (a rotating
     * selection, one outcome per candidate). The accepted resolution is linked under this
     * container — a single-file commit bypasses the listing's child linking. ONE pass per
     * listing: unresolved candidates wait for the NEXT listing pass — this loop never
     * converges in place (no loop-until-done), and the listing's own commit outcome is
     * unaffected.
     */
    /**
     * ONE budgeted resolution pass over [deferredIds]; returns the number of candidates that
     * remain UNRESOLVED (budget-skipped, failed fetches, rejected or uncommitted resolutions)
     * — the caller durably marks the folder's reconciliation incomplete from it (issue: round
     * 22 #17).
     */
    private fun resolveDeferredCandidatesOnce(
        realm: Realm,
        okHttpClient: OkHttpClient,
        folderId: Int,
        userDrive: UserDrive,
        deferredIds: Set<Int>,
        checkCancelled: () -> Unit = {},
    ): Int {
        if (deferredIds.isEmpty()) return 0
        val resolvedIds = mutableSetOf<Int>()
        for (deferredId in OfflineFolderUtils.fairDeferredSelection(
            deferredIds,
            folderId,
            OfflineFolderUtils.DEFERRED_RESOLUTION_BUDGET,
        )) {
            val preObservation = FileController.getFileById(realm, deferredId)
                ?.let { row -> realm.copyFromRealm(row, 1) } ?: continue
            // THE CANDIDATE'S REAL DRIVE (issue: round 21 #17): a shared listing's
            // container scope names the presentation drive — the candidate's ACTUAL drive
            // (from its own row) scopes its details request and its byte locks; the
            // container scope keeps scoping the container's lock.
            val candidateDriveId = preObservation.driveId.takeIf { it > 0 } ?: userDrive.driveId
            val candidateScope = UserDrive(
                userId = userDrive.userId,
                driveId = candidateDriveId,
                sharedWithMe = userDrive.sharedWithMe,
            )
            val details = runCatching {
                ApiRepository.getFileDetails(
                    file = File(id = deferredId, driveId = candidateDriveId),
                    okHttpClient = okHttpClient,
                )
            }.getOrNull()?.data?.takeIf { it.id == deferredId } ?: continue
            // Cancellation after the resolving HTTP, before any side effect (issue: round
            // 20 #22) — and AGAIN after the coordination wait (issue: round 21 #22): the
            // previous second check would have run BEFORE the lock acquisition, so a
            // cancellation while blocked still allowed preservation and commits afterward.
            checkCancelled()
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withLocksFor(candidateScope, listOf(deferredId)) {
                    OfflineFileCoordination.withLocksFor(userDrive, listOf(folderId)) {
                        // POST-LOCK-WAIT check (issue: round 21 #22): the caller's executing
                        // context rides the cancellation callback.
                        checkCancelled()
                        // THE CANDIDATE'S ACTUAL SCOPE THROUGH PRESERVATION AND UPDATE
                        // (issue: round 22 #18): the details request and the row lock already
                        // use candidateScope — preservation (snapshot registration, offline
                        // path derivation, ownership identity) and the conditional update
                        // previously still received the presentation CONTAINER scope, so a
                        // shared candidate from drive D2 displayed under D1 snapshotted
                        // under D1 and mutated through D1-keyed locks no D2 worker serializes
                        // with. The container scope keeps only the presentation membership
                        // lock; the row-then-container nesting order is this resolver's
                        // consistent combined order.
                        val committable = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                            realm = realm,
                            remoteFiles = listOf(details),
                            userDrive = candidateScope,
                            observedById = mapOf(deferredId to preObservation),
                        ).first()
                        if (committable === details) {
                            val committed = FileController.updateExistingFileIfNotOlder(
                                newFile = committable,
                                realm = realm,
                                userDrive = candidateScope,
                                observed = preObservation,
                            )
                            if (committed) {
                                // MEMBERSHIP: real ancestry for a REAL container; PRESENTATION
                                // membership for the Shared ROOT pseudocontainer (issue: round
                                // 21 #17) — real-parent equality with the pseudocontainer id is
                                // impossible for shared rows (their parents are their actual
                                // shared folders), which previously rejected every root link.
                                // SAF SHARED ROOT PRESENTATION (issue: round 22 #17): the SAF
                                // shared root's container id is ROOT_ID (with the sharedWithMe
                                // scope), not SharedWithMe.id — the presentation-membership
                                // recognition previously matched only the latter, so a resolved
                                // shared-root candidate never linked.
                                val pseudoContainerMembership = folderId == SharedWithMe.id ||
                                    (folderId == ROOT_ID && userDrive.sharedWithMe)
                                var membershipAccepted = false
                                realm.executeTransaction {
                                    val freshRow = FileController.getFileById(realm, deferredId)
                                        ?: return@executeTransaction
                                    val container = FileController.getFileById(realm, folderId)
                                    val membershipOk = pseudoContainerMembership || freshRow.parentId == folderId
                                    if (container != null && membershipOk &&
                                        container.children.none { it.id == deferredId }
                                    ) {
                                        container.children.add(freshRow)
                                    }
                                    // An already-linked member is equally accepted: the link
                                    // only runs when absent, acceptance is the membership
                                    // itself.
                                    if (container != null && membershipOk) membershipAccepted = true
                                }
                                // ACTUAL ACCEPTED OUTCOMES (issue: round 23 #16): the
                                // resolver's returned count never counted resolutions —
                                // `resolvedIds` was never written, so every nonempty
                                // deferred set reported fully unresolved even after
                                // accepted commits, durably marking fully reconciled
                                // folders incomplete. Membership acceptance (committed AND
                                // linked under the container) is the resolution outcome;
                                // budget-skips, failed fetches, boundary rejections and
                                // uncommitted resolutions stay unresolved.
                                if (membershipAccepted) resolvedIds.add(deferredId)
                            }
                        }
                    }
                }
            }
        }
        return deferredIds.size - resolvedIds.size
    }

    fun getCloudStorageFiles(
        realm: Realm,
        folderId: Int,
        userDrive: UserDrive,
        sortType: SortType,
        isFirstPage: Boolean = true,
        transaction: (files: ArrayList<File>) -> Unit,
        okHttpClient: OkHttpClient? = null,
        checkCancelled: () -> Unit = {},
        sequenceHadUnresolved: Boolean = false,
    ) {
        val folderProxy = FileController.getFileById(realm, folderId)
        val folderFilesProviderArgs = FolderFilesProviderArgs(
            folderId = folderId,
            isFirstPage = isFirstPage,
            order = sortType,
            realm = realm,
            sourceRestrictionType = ONLY_FROM_REMOTE,
            userDrive = userDrive,
        )
        val currentOkHttpClient = okHttpClient ?: runBlocking { AccountUtils.getHttpClient(userDrive.userId) }
        val folderFilesProviderResult = loadCloudStorageFromRemote(
            realm = realm,
            folderProxy = folderProxy,
            folderFilesProviderArgs = folderFilesProviderArgs,
            okHttpClient = currentOkHttpClient,
            checkCancelled = checkCancelled,
            sequenceHadUnresolved = sequenceHadUnresolved,
        )

        transaction(folderFilesProviderResult?.folderFiles ?: arrayListOf())

        // CROSS-PAGE UNRESOLVED ACCUMULATION (issue: round 24 #17): the flag rides the
        // whole listing sequence — a successful FINAL page re-marks the folder incomplete
        // when ANY page's reconciliation left candidates unresolved, so a later page can
        // never erase earlier deferred work and the redrive stays independent of
        // pagination completion.
        val sequenceUnresolved = sequenceHadUnresolved ||
            (folderFilesProviderResult?.unresolvedReconciliation == true)
        if (folderFilesProviderResult?.isComplete == false) {
            getCloudStorageFiles(
                realm = realm,
                folderId = folderId,
                userDrive = userDrive,
                sortType = sortType,
                isFirstPage = false,
                transaction = transaction,
                okHttpClient = currentOkHttpClient,
                checkCancelled = checkCancelled,
                sequenceHadUnresolved = sequenceUnresolved,
            )
        } else if (folderFilesProviderResult != null && sequenceUnresolved) {
            // POST-SEQUENCE DOWNGRADE (issue: round 22 #17) — retained alongside the
            // in-transaction flag (issue: round 25 #17): the transaction already wrote
            // the unresolved completeness for carried state and this page's own
            // substitutions; this write covers an edge the transaction could not see
            // (this page resolved clean but the resolver's post-save pass left
            // candidates unresolved). Idempotent over the same value.
            FileController.updateFile(folderId, realm) { folderRow -> folderRow.isComplete = false }
        }
    }

    fun tryLoadActivitiesFromFolder(folder: File, userDrive: UserDrive, activitiesJob: Job): Boolean {
        val realm = FileController.getRealmInstance(userDrive)
        val folderProxy = FileController.getFileById(realm, folder.id) ?: return false
        if (!folderProxy.isComplete) return false

        val okHttpClient = runBlocking { AccountUtils.getHttpClient(userDrive.userId, 30) }
        val result = loadActivitiesFromFolderRec(activitiesJob, folderProxy, userDrive, okHttpClient)

        realm.close()

        return result.isNotEmpty()
    }

    private fun loadFromRemote(
        realm: Realm,
        folderProxy: File?,
        folderFilesProviderArgs: FolderFilesProviderArgs,
        checkCancelled: () -> Unit = {},
        sequenceHadUnresolved: Boolean = false,
    ): FolderFilesProviderResult? = with(Dispatchers.IO) {

        SentryLog.i(TAG, "loadFromRemote")
        val userDrive = folderFilesProviderArgs.userDrive
        val (okHttpClient, driveId) = runBlocking { AccountUtils.getHttpClient(userDrive.userId) } to userDrive.driveId

        // COMPLETE PRE-REQUEST OBSERVATIONS (issue: round 16 #16): the container's children as
        // they stood before the listing HTTP request, detached at depth one (embedded version
        // included, issue: round 16 #9). They anchor tied identity changes through the shared
        // boundary and the committing transaction, so ordinary listings converge on
        // same-second renames instead of deferring them on every pass.
        val observedById = folderProxy?.children
            ?.associateBy({ child -> child.id }) { child -> realm.copyFromRealm(child, 1) }
            ?: emptyMap()

        val apiResponse = when {
            folderFilesProviderArgs.folderId == ROOT_ID -> {
                ApiRepository.getFolderFiles(
                    okHttpClient = okHttpClient,
                    driveId = driveId,
                    parentId = folderFilesProviderArgs.folderId,
                    cursor = if (folderFilesProviderArgs.isFirstPage) null else folderProxy?.cursor,
                    order = folderFilesProviderArgs.order,
                )

            }
            else -> {
                ApiRepository.getListingFiles(
                    okHttpClient = okHttpClient,
                    driveId = driveId,
                    parentId = folderFilesProviderArgs.folderId,
                    cursor = if (folderFilesProviderArgs.isFirstPage) null else folderProxy?.cursor,
                    order = folderFilesProviderArgs.order,
                ).let {
                    CursorApiResponse(
                        result = it.result,
                        data = it.data?.files,
                        error = it.error,
                        responseAt = it.responseAt,
                        cursor = it.cursor,
                        hasMore = it.hasMore,
                    )
                }
            }
        }

        ensureActive()

        handleRemoteFiles(
            realm,
            apiResponse,
            folderFilesProviderArgs,
            folderProxy,
            okHttpClient = okHttpClient,
            observedById = observedById,
            checkCancelled = checkCancelled,
            sequenceHadUnresolved = sequenceHadUnresolved,
        )
    }

    private fun loadCloudStorageFromRemote(
        realm: Realm,
        folderProxy: File?,
        folderFilesProviderArgs: FolderFilesProviderArgs,
        okHttpClient: OkHttpClient,
        checkCancelled: () -> Unit = {},
        sequenceHadUnresolved: Boolean = false,
    ): FolderFilesProviderResult? = with(Dispatchers.IO) {
        val userDrive = folderFilesProviderArgs.userDrive

        // COMPLETE PRE-REQUEST OBSERVATIONS (issue: round 16 #16), as in [loadFromRemote].
        val observedById = folderProxy?.children
            ?.associateBy({ child -> child.id }) { child -> realm.copyFromRealm(child, 1) }
            ?: emptyMap()

        val apiResponse = when {
            folderFilesProviderArgs.folderId == ROOT_ID && userDrive.sharedWithMe -> {
                ApiRepository.getSharedWithMeFiles(
                    okHttpClient = okHttpClient,
                    order = folderFilesProviderArgs.order,
                    cursor = if (folderFilesProviderArgs.isFirstPage) null else folderProxy?.cursor,
                )
            }
            else -> {
                ApiRepository.getFolderFiles(
                    okHttpClient = okHttpClient,
                    driveId = userDrive.driveId,
                    parentId = folderFilesProviderArgs.folderId,
                    cursor = if (folderFilesProviderArgs.isFirstPage) null else folderProxy?.cursor,
                    order = folderFilesProviderArgs.order,
                )
            }
        }

        // POST-HTTP CHECK (issue: round 22 #20): the caller's captured executing context
        // (the SAF cursor's job, the suspend listing's job) — the previous bare
        // ensureActive() ran against the with-block's Job-LESS dispatcher receiver and
        // therefore checked nothing.
        checkCancelled()

        // The CAPTURED account client rides the shared resolution (issue: round 22 #19):
        // the default-parameter client authenticated details resolution against the
        // GLOBALLY ACTIVE account instead of the listing's captured one.
        handleRemoteFiles(
            realm,
            apiResponse,
            folderFilesProviderArgs,
            folderProxy,
            okHttpClient = okHttpClient,
            observedById = observedById,
            checkCancelled = checkCancelled,
            sequenceHadUnresolved = sequenceHadUnresolved,
        )
    }

    private fun handleRemoteFiles(
        realm: Realm,
        apiResponse: CursorApiResponse<List<File>>,
        folderFilesProviderArgs: FolderFilesProviderArgs,
        folderProxy: File?,
        okHttpClient: OkHttpClient = DefaultHttpClientProvider.okHttpClient,
        observedById: Map<Int, File> = emptyMap(),
        checkCancelled: () -> Unit = {},
        sequenceHadUnresolved: Boolean = false,
    ): FolderFilesProviderResult? {
        val userDrive = folderFilesProviderArgs.userDrive
        val apiResponseData = apiResponse.data
        val folderWithChildren = folderFilesProviderArgs.withChildren

        val localFolder = folderProxy?.freeze()
            ?: ApiRepository.getFileDetails(
                File(id = folderFilesProviderArgs.folderId, driveId = userDrive.driveId),
                okHttpClient
            ).data
            ?: return null

        return when {
            apiResponseData != null -> {
                val isCompleteFolder = !apiResponse.hasMore || apiResponseData.count() < ApiRoutes.PER_PAGE
                val commitOutcomes = saveRemoteFiles(
                    realm = realm,
                    localFolderProxy = folderProxy,
                    remoteFolder = localFolder,
                    apiResponse = apiResponse,
                    isFirstPage = folderFilesProviderArgs.isFirstPage,
                    isCompleteFolder = isCompleteFolder,
                    userDrive = userDrive,
                    observedById = observedById,
                    // POST-LOCKS CHECK (issue: round 23 #19): the ordinary/SAF route's
                    // executing-context callback reaches the save's coordinated section.
                    checkCancelled = checkCancelled,
                    // THE SEQUENCE'S UNRESOLVED STATE RIDES THE FINAL-PAGE TRANSACTION
                    // (issue: round 25 #17): earlier pages' unresolved reconciliation
                    // makes this page's durable completeness false in the SAME
                    // transaction that commits it.
                    sequenceHadUnresolved = sequenceHadUnresolved,
                )
                // ORDINARY/SAF LISTING RESOLUTION (issue: round 21 #17): the shared route's
                // budgeted resolver previously never ran for the ordinary and SAF paths —
                // their move-in candidates deferred forever. ONE pass per listing, same
                // protocol (fresh pre-observed details, fair per-item outcomes); unresolved
                // candidates wait for the next listing pass.
                var unresolvedDeferredCount = 0
                if (commitOutcomes.deferredIds.isNotEmpty()) {
                    unresolvedDeferredCount = resolveDeferredCandidatesOnce(
                        realm = realm,
                        okHttpClient = okHttpClient,
                        folderId = folderFilesProviderArgs.folderId,
                        userDrive = userDrive,
                        deferredIds = commitOutcomes.deferredIds,
                        checkCancelled = checkCancelled,
                    )
                    if (unresolvedDeferredCount > 0) {
                        // INCOMPLETE RECONCILIATION OUTLIVES PAGINATION (issue: round 22 #17):
                        // the folder's persisted completeness previously came from pagination
                        // alone — a fully listed folder with unresolved deferred candidates
                        // looked complete, a nonempty cached folder then bypassed remote
                        // loading (needToLoadFromRemote), and candidates beyond the budget
                        // never received another resolution pass. The folder row is durably
                        // marked INCOMPLETE whenever unresolved candidates remain; the next
                        // visit re-drives the remote load and the budgeted resolver (fair
                        // redrive), independent of pagination completion. A later fully
                        // resolving pass re-persists the pagination completeness through
                        // saveRemoteFiles.
                        FileController.updateFile(folderFilesProviderArgs.folderId, realm) { folderRow ->
                            folderRow.isComplete = false
                        }
                    }
                }
                val folderFiles = if (folderWithChildren) ArrayList(apiResponseData) else arrayListOf()
                FolderFilesProviderResult(
                    folder = localFolder,
                    folderFiles = folderFiles,
                    isComplete = isCompleteFolder,
                    unresolvedReconciliation = unresolvedDeferredCount > 0,
                )
            }
            folderFilesProviderArgs.isFirstPage -> {
                loadFromLocal(realm, folderProxy, folderWithChildren, folderFilesProviderArgs.order)
            }
            else -> {
                null
            }
        }
    }

    private fun loadFromLocal(
        realm: Realm,
        folderProxy: File?,
        withChildren: Boolean,
        order: SortType
    ): FolderFilesProviderResult? {
        SentryLog.i(TAG, "loadFromLocal")
        val localFolderWithoutChildren = folderProxy?.let { realm.copyFromRealm(it, 1) } ?: return null
        val sortedFolderFiles = if (withChildren) FileController.getLocalSortedFolderFiles(folderProxy, order) else arrayListOf()
        return FolderFilesProviderResult(folder = localFolderWithoutChildren, folderFiles = sortedFolderFiles, isComplete = true)
    }

    private fun needToLoadFromRemote(sourceRestrictionType: SourceRestrictionType, folderProxy: File?): Boolean {

        fun hasDuplicatesFiles(query: RealmQuery<File>): Boolean = query.count() != query.distinct(File::id.name).count()

        return sourceRestrictionType == ONLY_FROM_REMOTE
                || folderProxy == null
                || folderProxy.children.isEmpty()
                || !folderProxy.isComplete
                || folderProxy.versionCode <= MIN_VERSION_CODE
                || folderProxy.children.where().let(::hasDuplicatesFiles)
                || minDateToIgnoreCache >= folderProxy.responseAt
    }

    /**
     * Pagination-pass state (issue: round 16 #17): a deferral on ANY page retains the folder
     * cursor for the WHOLE pass — a later fully-handled page must never persist a cursor
     * beyond an earlier unresolved action.
     */
    private class FolderActivitiesPassState {
        var anyPageDeferred = false
    }

    private tailrec fun loadActivitiesFromFolderRec(
        activitiesJob: Job,
        folderProxy: File,
        userDrive: UserDrive,
        okHttpClient: OkHttpClient,
        cursor: String? = folderProxy.cursor,
        returnResponse: ArrayMap<Int, FileAction> = arrayMapOf(),
        passState: FolderActivitiesPassState = FolderActivitiesPassState(),
    ): Map<out Int, FileAction> {
        SentryLog.i(TAG, "loadActivitiesFromFolderRec with folderId ${folderProxy.id} cursor: ${cursor != null}")
        val realm = folderProxy.realm
        // DETACHED pre-request observations (issues: round 15 #10, round 16 #9): the
        // container's children as they stood when the listing/activities request was issued —
        // managed objects would refresh together with the rows inside each activity's
        // coordination and defeat tie resolution, leaving tied directory changes deferred
        // forever. The copy is DEPTH ONE (issue: round 16 #9): a depth-zero copy drops the
        // embedded FileVersion, and the tie-resolution observation comparison needs the
        // version identifier the live row carries — an observation missing its own version
        // defers every legitimate tied rename.
        val observedChildren = folderProxy.children.associateBy({ child -> child.id }) { child ->
            realm.copyFromRealm(child, 1)
        }
        val apiResponse = ApiRepository.getListingFiles(
            okHttpClient = okHttpClient,
            driveId = userDrive.driveId,
            parentId = folderProxy.id,
            cursor = cursor,
            order = SortType.NAME_AZ,
        )

        if (!apiResponse.isSuccess()) return returnResponse

        activitiesJob.ensureActive()

        val apiResponseData = apiResponse.data

        var allActivitiesHandled = true
        if (apiResponseData != null && apiResponseData.actions.isNotEmpty()) {
            val actionsFiles = apiResponseData.actionsFiles.associateBy(File::id)
            SentryLog.i(TAG, "loadActivitiesFromFolderRec: actions ${apiResponseData.actions.count()}")
            apiResponseData.actions.asReversed().forEach { fileActivity ->
                allActivitiesHandled = fileActivity.applyFileAction(
                    realm,
                    actionsFiles,
                    returnResponse,
                    folderProxy,
                    userDrive,
                    observedChildren,
                    // THE EXECUTING JOB'S CALLBACK (issue: round 24 #18): the ancestor
                    // resolver's additional requests and coordinated commits check the
                    // ACTIVITIES pass's job — after each HTTP and inside each lock section.
                    checkCancelled = { activitiesJob.ensureActive() },
                ) && allActivitiesHandled
            }
        }

        // Cross-page deferral memory (issue: round 16 #17): a deferral on THIS page marks the
        // whole pagination pass unresolved, so a later fully-handled page cannot persist a
        // cursor beyond the earlier action.
        if (!allActivitiesHandled) passState.anyPageDeferred = true

        // The folder cursor advances ONLY when every activity of the WHOLE pagination pass
        // was applied (issues: round 15 #10, round 16 #17): a deferred tied change retains
        // unresolved work — the next activities fetch re-runs from the retained cursor and
        // the periodic folder walk is the resolving refresh. The write happens ONCE at the
        // pass's final page: per-page persistence previously let a fully-handled later page
        // advance the cursor past an earlier deferred page's action, permanently skipping
        // the unresolved work (whose duplicate suppression inside the pass cannot hide it).
        if (allActivitiesHandled) {
            SentryLog.d(TAG, "loadActivitiesFromFolderRec: page fully handled for folder ${folderProxy.id}")
        } else {
            SentryLog.i(
                TAG,
                "loadActivitiesFromFolderRec: deferred folder ${folderProxy.id} activities on this page; pass cursor retained for fair retry"
            )
        }

        return if (apiResponse.hasMoreAndCursorExists) {
            SentryLog.i(TAG, "loadActivitiesFromFolderRec: loading next page")
            // Loading the next page, then the cursor is required
            loadActivitiesFromFolderRec(
                activitiesJob = activitiesJob,
                folderProxy = folderProxy,
                cursor = apiResponse.cursor,
                userDrive = userDrive,
                okHttpClient = okHttpClient,
                returnResponse = returnResponse,
                passState = passState,
            )
        } else {
            SentryLog.i(TAG, "loadActivitiesFromFolderRec: no more pages")
            if (!passState.anyPageDeferred) {
                if (apiResponse.responseAt > 0L) {
                    FileController.updateFile(folderProxy.id, realm) { file ->
                        file.responseAt = apiResponse.responseAt
                        apiResponse.cursor?.let { file.cursor = it }
                    }
                } else {
                    Sentry.captureMessage("response at is null") { scope -> scope.setExtra("data", apiResponse.toString()) }
                }
            } else {
                SentryLog.i(
                    TAG,
                    "loadActivitiesFromFolderRec: folder ${folderProxy.id} pass deferred on an earlier page; cursor retained for fair retry"
                )
            }
            returnResponse
        }
    }

    /** Returns whether the activity was fully applied — a deferral retains the folder cursor (round 15 #10). */
    private fun FileAction.applyFileAction(
        realm: Realm,
        actionFiles: Map<Int, File>,
        returnResponse: ArrayMap<Int, FileAction>,
        currentFolder: File,
        userDrive: UserDrive,
        observedChildren: Map<Int, File>,
        checkCancelled: () -> Unit = {},
    ): Boolean {
        val actionFile = actionFiles[fileId]

        return when (actionType) {
            FileActivityType.FILE_DELETE,
            FileActivityType.FILE_MOVE_OUT,
            FileActivityType.FILE_TRASH -> {
                // We used to have this condition, but it doesn't exist on the ios side, according to commit it was an api fix.
                // returnResponse[fileId]?.createdAt?.time == createdAt.time
                if (returnResponse[fileId] == null) {
                    // A DEFERRED destructive action is not recorded as handled (issue: round
                    // 16 #15/#17): its unresolved work must stay visible to the retained
                    // cursor and to later duplicates.
                    val handled = removeAction(realm, currentFolder, actionFile, userDrive, observedChildren[fileId])
                    if (handled) returnResponse[fileId] = this
                    handled
                } else {
                    true
                }
            }
            else -> {
                if (actionType == null) {
                    SentryLog.e(TAG, "The action with value '$actionString' is unknown")
                }
                // The file has not yet been managed and is not the parent folder.
                if (returnResponse[fileId] == null && actionFile?.id != currentFolder.id) {
                    val handled = upsertAction(
                        realm, currentFolder, actionFile, userDrive, observedChildren[fileId],
                        checkCancelled,
                    )
                    returnResponse[fileId] = this
                    handled
                } else {
                    true
                }
            }
        }
    }

    /**
     * Destructive folder-activity removal with complete request authority (issue: round 16
     * #15): the removal applies only against the FRESH live row through the complete action
     * predicate — the request observation resolves identity ties. A payload-less folder
     * activity carries NO timestamp at all ([FileAction] has none): no deletion authority can
     * be established for it, so it defers — the copy and the unresolved cursor are retained
     * for a payload-bearing retry. Returns false when deferred.
     */
    private fun FileAction.removeAction(
        realm: Realm,
        currentFolder: File,
        actionFile: File?,
        userDrive: UserDrive,
        observed: File?,
    ): Boolean {
        // [FileAction] carries no timestamp: a payload-less destructive activity has NO
        // deletion authority of its own (issue: round 16 #15) — but the SERVER has.
        // FRESH OBSERVATION IMMEDIATELY BEFORE THE PROBE (issue: round 19 #21): the
        // caller's children-map observation can be stale — or ABSENT, in which case the
        // previous null guard SKIPPED the freshness check entirely and the probe's
        // absence could erase a row restored or moved since. The observation is the
        // row's identity captured HERE, immediately before the probe, at FUNCTION scope
        // so the coordinated section below compares against it.
        val preProbeObservation = if (actionFile == null) {
            FileController.getFileById(realm, fileId)?.let { realm.copyFromRealm(it, 1) }
        } else {
            null
        }
        if (actionFile == null) {
            // RESOLVED ABSENCE IS CONSUMED (issue: round 18 #18): a later pass may already
            // have removed the row — the event has nothing left to delete and must not
            // block the folder cursor forever on an immutable event that will never grow
            // its missing payload.
            if (FileController.getFileById(realm, fileId) == null) return true
            // NEW AUTHORITATIVE RESOLVING PATH (issue: round 18 #18): the resolving probe
            // answers CURRENT (a live remote row — the event is stale or misattributed),
            // VERIFIED MISSING (the explicit OBJECT_NOT_FOUND code — the same
            // missing-object authority every other route uses: deletion authority is
            // ESTABLISHED and the coordinated section runs), or INDETERMINATE (transport
            // and other errors — defer). The previous behavior rejected every payload-less
            // destructive activity unconditionally, so a verified-absent object deferred
            // forever.
            val okHttpClient = runCatching {
                runBlocking { AccountUtils.getHttpClient(userDrive.userId) }
            }.getOrNull() ?: return false
            val probe = runCatching {
                ApiRepository.getFileDetails(
                    file = File(id = fileId, driveId = userDrive.driveId),
                    okHttpClient = okHttpClient,
                )
            }.getOrNull()
            when {
                probe == null -> return false
                probe.data != null -> {
                    // VERIFIED CURRENT OBJECT CONSUMES THE STALE DELETION (issue: round
                    // 19 #21): the object exists server-side — this deletion event is
                    // stale or misattributed and is consumed WITHOUT any delete; the
                    // retained row is the current state, and the cursor moves past an
                    // event that can never grow its payload.
                    return true
                }
                probe.error?.code == ErrorCode.OBJECT_NOT_FOUND -> Unit
                else -> return false
            }
        }
        // ONE coordinated operation (issue: round 17 #12): intent monitor → ordered locks
        // (container + file) → fresh Realm; the COMPLETE action predicate re-applies against
        // the FRESH row INSIDE the section (a restore or newer generation committed since the
        // pre-gate read defers the deletion), preservation runs before any byte or row
        // deletion, and a directory's descendant-ownership revalidates INSIDE the deleting
        // transaction.
        // The section's outcome is the function's outcome (issue: round 18 compile
        // corrections): the synchronized block's value must be RETURNED — an unreturned
        // last expression leaves the Boolean function without a return statement.
        return synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withLocksFor(userDrive, listOf(currentFolder.id, fileId)) {
                runCatching { realm.refresh() }
                val liveRow = FileController.getFileById(realm, fileId) ?: return@withLocksFor true
                if (actionFile == null) {
                    // VERIFIED-MISSING authority, freshness still required (issues: round 18
                    // #18, round 19 #21): the fresh row must still match the FRESH
                    // PRE-PROBE observation — a restore or move committed since the probe
                    // defers; the probe's server absence applies only to the row the probe
                    // was issued against. A null observation can no longer SKIP this
                    // check: the capture above guarantees one for any row that existed
                    // before the probe, and a row that vanished since defers to the
                    // section's own null-row consumption below.
                    if (preProbeObservation != null && !FileController.isSameObservation(liveRow, preProbeObservation)) {
                        return@withLocksFor false
                    }
                } else if (FileController.isStaleForAuthoritativeAction(liveRow, actionFile, null, observed)) {
                    return@withLocksFor false
                }
                val parentRow = FileController.getFileById(realm, currentFolder.id)
                if (parentRow == null) return@withLocksFor false
                when (actionType) {
                    FileActivityType.FILE_MOVE_OUT -> {
                        realm.executeTransaction { parentRow.children.removeAll { it.id == fileId } }
                        true
                    }
                    else -> {
                        val context = Realm.getApplicationContext() ?: return@withLocksFor false
                        if (liveRow.isFolder()) {
                            // Required ancestry revalidated INSIDE the deleting transaction
                            // (issue: round 17 #12): coverage or pin writers can establish
                            // required descendants up to the last moment.
                            var deletable = false
                            realm.executeTransaction {
                                deletable = !OfflineFolderController.hasLocallyOwnedDescendant(liveRow.id, realm)
                                if (deletable) liveRow.deleteFromRealm()
                            }
                            deletable
                        } else {
                            when (OfflineFolderController.preserveInheritedCopyBeforeRemoval(liveRow, userDrive, realm)) {
                                OfflineFolderController.PreservationOutcome.CONTENT_SAFE -> {
                                    runCatching { liveRow.deleteCaches(context, userDrive) }
                                    realm.executeTransaction { if (liveRow.isValid) liveRow.deleteFromRealm() }
                                    true
                                }
                                // Preservation pending: row, bytes and cursor retained.
                                OfflineFolderController.PreservationOutcome.PRESERVED -> false
                            }
                        }
                    }
                }
            }
        }
    }

    private fun FileAction.upsertAction(
        realm: Realm,
        currentFolder: File,
        actionFile: File?,
        userDrive: UserDrive,
        observed: File?,
        checkCancelled: () -> Unit = {},
    ): Boolean {
        if (actionFile == null) {
            // SCOPED RESOLVING FETCH (issue: round 19 #21): a payload-less update/unknown
            // activity previously deferred forever on an immutable event that will never
            // grow its missing payload, blocking the folder cursor. The scoped details
            // fetch answers like the removal probe: a live object is committed from the
            // fetched AUTHORITATIVE current state (the event's intent — refresh this row —
            // is thereby satisfied) with the pre-fetch observation resolving ties, a
            // verified-missing object CONSUMES the event (nothing left to update), and an
            // indeterminate transport defers with the cursor retained.
            val preFetchObservation = FileController.getFileById(realm, fileId)?.let { realm.copyFromRealm(it, 1) }
            val okHttpClient = runCatching {
                runBlocking { AccountUtils.getHttpClient(userDrive.userId) }
            }.getOrNull() ?: return false
            val probe = runCatching {
                ApiRepository.getFileDetails(
                    file = File(id = fileId, driveId = userDrive.driveId),
                    okHttpClient = okHttpClient,
                )
            }.getOrNull()
            // THE EXECUTING CONTEXT AFTER THE RESPONSE (issue: round 25 #18): a cancelled
            // caller stops BEFORE any coordinated effect, with only the stateless probe
            // discarded — no row, cursor or byte effect has run.
            checkCancelled()
            val details = probe?.data?.takeIf { it.id == fileId }
            return when {
                details != null -> {
                    // MISSING-ANCESTRY RESOLUTION, BOUNDED AND FAIR (issue: round 23 #15):
                    // the activity path previously fetched only the event's own file and
                    // deferred on a missing parent FOREVER — nothing in the retry ever
                    // fetched the parent, so the same immutable activity blocked the
                    // cursor indefinitely. The fetched row's ancestry is resolved here,
                    // OUTSIDE the coordination section: each missing ancestor gets ONE
                    // details request (no retry loop), and the fetched ancestors are
                    // committed TOP-DOWN through the coordinated upsert, each requiring
                    // its own resolved chain — an interior fetch failure defers that
                    // ancestor and everything below it, and the event stays deferred,
                    // never consumed. The child commit below re-validates the complete
                    // usable chain (the strengthened resolved-parent acceptance).
                    resolveMissingAncestryOutsideLocks(
                        realm = realm,
                        fetchedFile = details,
                        okHttpClient = okHttpClient,
                        userDrive = userDrive,
                        checkCancelled = checkCancelled,
                    )
                    // COMPLETE ASCENDING LOCK SET ONCE (issue: round 22 #14): the previous
                    // withFileLock(file) pre-held the CHILD lock while the upsert then
                    // requested lower-ID parent locks — a concrete lock-order deadlock
                    // against any concurrent ascending acquirer (activity holds 100 wants
                    // 50; Gallery holds 50 wants 100, monitor retained). The caller now
                    // acquires the intent monitor and the COMPLETE set — the fetched row's
                    // new parent, the observed previous parent and the row itself — in ONE
                    // ascending acquisition, then runs the boundary and the coordinated
                    // upsert body (which acquires nothing itself).
                    val coordinatedIds = buildList {
                        add(fileId)
                        preFetchObservation?.parentId?.takeIf { it > 0 }?.let(::add)
                        details.parentId.takeIf { it > 0 }?.let(::add)
                    }.distinct()
                    synchronized(OfflineFileCoordination.rootIntentMonitor) {
                        OfflineFileCoordination.withLocksFor(userDrive, coordinatedIds) {
                            // POST-LOCK-WAIT CHECK IN THE CHILD'S OWN SECTION (issue: round
                            // 25 #18): a cancelled caller never runs the boundary or the
                            // coordinated upsert after waiting on the lock set.
                            checkCancelled()
                            val committable = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                                realm = realm,
                                remoteFiles = listOf(details),
                                userDrive = userDrive,
                                observedById = fileId.let { id ->
                                    preFetchObservation?.let { obs -> mapOf(id to obs) } ?: emptyMap()
                                },
                            ).first()
                            if (committable !== details) {
                                false
                            } else {
                                // ABSENCE-AWARE upsert (issues: round 21 #18, round 22
                                // #14/#15): an authoritative details answer for an UNCACHED
                                // row must make progress; the upsert reconciles the
                                // authoritative membership (destination link + obsolete old
                                // backlink) and backfills an inserted ancestor's existing
                                // children in the same coordinated operation. A missing
                                // parent row DEFERS (the cursor stays) instead of consuming
                                // an authoritative event for unusable ancestry.
                                FileController.upsertFetchedFileIfNotOlderInCoordinatedSection(
                                    newFile = committable,
                                    realm = realm,
                                    userDrive = userDrive,
                                    observed = preFetchObservation,
                                )
                            }
                        }
                    }
                }
                probe?.error?.code == ErrorCode.OBJECT_NOT_FOUND -> true
                else -> false
            }
        } else {
            if (actionFile.isImporting()) MqttClientWrapper.start(actionFile.externalImport?.id)
            return FileController.upsertActionFile(realm, currentFolder.id, actionFile, userDrive, observed = observed)
        }
    }

    /**
     * ONE bounded, fair pass resolving a fetched row's missing ancestry (issues: round 23
     * #15, round 24 #15/#16/#18): climbs the parent chain to its usable boundary — a zero
     * parent, the fetch budget, or a CACHED ancestor whose OWN chain is already complete —
     * fetching each MISSING ancestor with a single details request OUTSIDE any lock, then
     * committing the fetched ancestors TOP-DOWN through the coordinated validate-preserve-
     * commit protocol.
     *
     * CACHED HOLES DO NOT TERMINATE THE CLIMB (issue: round 24 #15): a cached ancestor
     * with an incomplete chain of its own no longer stops the walk — the climb continues
     * THROUGH it, so every pass makes durable progress on holes above cached rows instead
     * of re-fetching the same boundary forever.
     *
     * EXPLICIT ABSENCE/IDENTITY BEFORE EACH REQUEST, VALIDATE-PRESERVE-COMMIT UNDER THE
     * COMPLETE LOCK SET (issue: round 24 #16): each ancestor's pre-request observation is
     * captured (null = documented absence) BEFORE its details request; the commit re-reads
     * the current graph under the monitor + the ancestor's lock set, runs the shared
     * preservation boundary against that observation, and upserts with the observation as
     * the tie anchor — a strictly newer response can no longer change an ancestor's
     * identity over rows another request inserted during the HTTP without reconciling
     * them.
     *
     * SCAFFOLD ACCEPTANCE (issue: round 24 #15): the fetched ancestors commit with
     * requireResolvedParent = false — with more missing ancestors than the fetch budget,
     * the topmost fetched ancestor's own parent is still absent, and requiring its
     * complete chain committed NOTHING on every pass (each pass re-fetched the same first
     * eight). Committed scaffold rows are durable progress; the event's own commit still
     * requires the complete usable chain.
     *
     * THE EXECUTING CONTEXT IS CHECKED (issue: round 24 #18): the callback rides every
     * request and every coordinated commit — after each HTTP and inside each lock section.
     * No retry loop: ancestors beyond the budget or with indeterminate fetches wait for
     * the next pass, exactly like the event they unblock.
     */
    internal fun resolveMissingAncestryOutsideLocks(
        realm: Realm,
        fetchedFile: File,
        okHttpClient: OkHttpClient,
        userDrive: UserDrive,
        checkCancelled: () -> Unit = {},
    ) {
        val fetchedAncestors = ArrayDeque<Pair<File, File?>>() // ancestor to its PRE-REQUEST observation
        val climbedIds = mutableSetOf<Int>() // CYCLE CHECK (issues: round 25 #15, round 26 #17)
        var cursorRow: File = fetchedFile
        var fetchBudget = MISSING_ANCESTRY_RESOLUTION_BUDGET
        // FINITE CACHED ITERATION, NO FIXED DEPTH CAP (issue: round 26 #17): the climb
        // terminates structurally — at the root, at a cycle (the visited set), or at the
        // separate network budget — never at an arbitrary hop count. A hole above any fixed
        // budget was previously unreachable forever, because every invocation restarted
        // from the same descendant and burned its bound on cached hops.
        while (true) {
            val parentId = cursorRow.parentId.takeIf { it > 0 } ?: break
            // CYCLE CHECK: a repeated id is a corrupted cycle — the climb defers instead
            // of circling.
            if (!climbedIds.add(parentId)) break
            val cached = FileController.getFileById(realm, parentId)
            if (cached != null) {
                // A CACHED BOUNDARY ONLY WHEN ITS OWN CHAIN IS COMPLETE (issue: round 24
                // #15); an interior hole is climbed THROUGH, never mistaken for resolved
                // ancestry.
                if (FileController.usableAncestryChain(realm, parentId)) break
                cursorRow = cached
                continue
            }
            if (fetchBudget-- <= 0) break
            checkCancelled()
            // EXPLICIT ABSENCE/IDENTITY CAPTURED IMMEDIATELY BEFORE THE REQUEST AND
            // CARRIED WITH THE RESPONSE (issue: round 25 #14): the observation each
            // ancestor's commit is validated against is the state its OWN request was
            // issued from — an absent row is a documented absence, an existing row its
            // depth-one identity. The previous capture ran in the commit loop, AFTER
            // every ancestor HTTP had completed, so a row another request inserted
            // during the climb was mistaken for the state the request was issued from.
            val preRequestObservation = FileController.getFileById(realm, parentId)
                ?.let { row -> realm.copyFromRealm(row, 1) }
            val parent = runCatching {
                ApiRepository.getFileDetails(
                    file = File(
                        id = parentId,
                        driveId = cursorRow.driveId.takeIf { it > 0 } ?: userDrive.driveId,
                    ),
                    okHttpClient = okHttpClient,
                )
            }.getOrNull()?.data?.takeIf { it.id == parentId } ?: break // indeterminate: defer as before
            checkCancelled()
            fetchedAncestors.addFirst(parent to preRequestObservation) // top-down commit order
            cursorRow = parent
        }
        for ((ancestor, preRequestObservation) in fetchedAncestors) {
            checkCancelled()
            val committed = synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withLocksFor(
                    userDrive,
                    buildList {
                        add(ancestor.id)
                        ancestor.parentId.takeIf { it > 0 }?.let(::add)
                    }.distinct(),
                ) {
                    // POST-LOCK-WAIT CHECK (issue: round 24 #18).
                    checkCancelled()
                    runCatching { realm.refresh() }
                    // THE SHARED PRESERVATION BOUNDARY AGAINST THE CAPTURED OBSERVATION
                    // (issue: round 24 #16): the raw upsert bypassed the caller-level
                    // validate-preserve-commit protocol — a newer response changing an
                    // ancestor's identity over rows inserted during the HTTP no longer
                    // skips it.
                    val committable = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                        realm = realm,
                        remoteFiles = listOf(ancestor),
                        userDrive = userDrive,
                        observedById = preRequestObservation?.let { mapOf(ancestor.id to it) } ?: emptyMap(),
                    ).first()
                    if (committable !== ancestor) {
                        false
                    } else {
                        // SCAFFOLD ACCEPTANCE (issue: round 24 #15): requireResolvedParent
                        // = false — the scaffold commits over its own (possibly still
                        // absent) parent so every pass makes durable progress; the event's
                        // own acceptance still validates the complete usable chain.
                        FileController.upsertFetchedFileIfNotOlderInCoordinatedSection(
                            newFile = committable,
                            realm = realm,
                            userDrive = userDrive,
                            observed = preRequestObservation,
                            requireResolvedParent = false,
                        )
                    }
                }
            }
            if (!committed) break
        }
    }

    data class FolderFilesProviderArgs(
        val folderId: FileId,
        val isFirstPage: Boolean = true,
        val order: SortType = SortType.NAME_AZ,
        val realm: Realm? = null,
        val sourceRestrictionType: SourceRestrictionType = SourceRestrictionType.UNRESTRICTED,
        val userDrive: UserDrive = UserDrive(),
        val withChildren: Boolean = true,
    )

    data class FolderFilesProviderResult(
        val folder: File,
        val folderFiles: ArrayList<File>,
        val isComplete: Boolean,
        val cursor: String? = null,
        /**
         * THIS page's budgeted resolution pass left candidates unresolved (issue: round 24
         * #17): the page-walk accumulates the flag across the whole listing sequence — a
         * successful FINAL page must not erase the durable incompleteness an earlier
         * page's unresolved candidates established.
         */
        val unresolvedReconciliation: Boolean = false,
    )

    enum class SourceRestrictionType {
        UNRESTRICTED,
        ONLY_FROM_LOCAL,
        ONLY_FROM_REMOTE,
    }
}
