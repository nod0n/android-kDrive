/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2024-2025 Infomaniak Network SA
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
import android.os.ParcelFileDescriptor
import androidx.concurrent.futures.await
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.work.ListenableWorker
import androidx.work.WorkInfo
import androidx.work.WorkManager
import androidx.work.WorkQuery
import com.google.gson.reflect.TypeToken
import com.infomaniak.core.auth.networking.AuthHttpClientProvider
import com.infomaniak.core.common.cancellable
import com.infomaniak.core.network.api.ApiController
import com.infomaniak.core.network.models.ApiResponse
import com.infomaniak.core.network.models.exceptions.NetworkException
import com.infomaniak.core.network.networking.HttpUtils
import com.infomaniak.core.network.networking.ManualAuthorizationRequired
import com.infomaniak.core.network.utils.ApiErrorCode.Companion.translateError
import com.infomaniak.core.network.utils.await
import com.infomaniak.core.notifications.notifyCompat
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.R
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.ApiRoutes
import com.infomaniak.drive.data.api.ProgressResponseBody
import com.infomaniak.drive.data.api.UploadTask
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.cache.OfflineFolderController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.services.BaseDownloadWorker
import com.infomaniak.drive.data.services.BulkDownloadWorker
import com.infomaniak.drive.data.services.DownloadWorker
import com.infomaniak.drive.utils.MediaUtils.deleteInMediaScan
import com.infomaniak.drive.utils.MediaUtils.isMedia
import com.infomaniak.drive.utils.NotificationUtils.downloadProgressNotification
import io.realm.Realm
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.invoke
import kotlinx.coroutines.withContext
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import kotlin.coroutines.cancellation.CancellationException
import kotlin.coroutines.coroutineContext
import okhttp3.Request
import okhttp3.Response
import java.util.UUID

/**
 * TYPED outcome of a cache promotion for its caller (issue: round 19 #24): the previous
 * Boolean collapsed "this transfer's intent token was fulfilled" into the same `true` as
 * the conflicts and supersessions, whose pending markers the caller then erased. Only
 * [FULFILLED] authorizes the marker consumption; [DEFERRED] retains the marker and the
 * root's incompleteness; [FAILED] falls through to the fresh download branch.
 */
enum class CachePromotionOutcome { FULFILLED, DEFERRED, FAILED }

/**
 * TYPED generation establishment for a completed download (issues: round 22 #23,
 * round 23 #22, round 24 #19/#20, round 25 #19): the download route carries no revision
 * token, so the only available establishment is a post-download details comparison — and a
 * LATER details response is not response-bound identity (nothing here establishes a
 * server guarantee excluding cached/stale or ABA observations).
 *
 * NO KNOWN-GENERATION PROOF FROM POST-DETAILS ALONE (issue: round 25 #19): the API
 * supplies neither a response-bound generation identifier nor an actually version-bound
 * download route ([ApiRoutes.downloadFile] serves the CURRENT revision only; [FileVersion]
 * carries no download url), so even a positive-version match between the metadata capture
 * and the later details response establishes nothing about the BYTES — a stale/cached
 * same-size byte response followed by matching current details must not receive a
 * known-generation proof. The positive-version branch therefore returns
 * [COARSE_UNRESOLVED] like every other metadata-only comparison, and [ESTABLISHED] is
 * RESERVED for a future response-bound identity or version-bound route (unreachable
 * until such a route exists). Every unresolved publication records an explicitly
 * UNRESOLVED (version-0) proof and returns an unresolved outcome; the durable intent is
 * retained. The convergence cost of this honesty — a periodic re-download of files whose
 * generation no available route can bind — is documented in findings §44 for the
 * orchestrator; a version-bound download route is the real fix.
 *
 * [FAILED] means the bytes' generation is UNKNOWABLE — the caller discards the bytes and
 * preserves the intent.
 *
 * TOP LEVEL, NOT COMPANION-NESTED (round-24 build correction): external callers
 * (SAF, workers) qualify it through the file's package — a companion-nested type is
 * not reachable as `DownloadOfflineFileManager.GenerationEstablishment`.
 */
enum class GenerationEstablishment { ESTABLISHED, COARSE_UNRESOLVED, FAILED }

class DownloadOfflineFileManager(
    private val userDrive: UserDrive,
    private val filesCount: Int,
    private val downloadWorker: BaseDownloadWorker,
    private val notificationManagerCompat: NotificationManagerCompat,
    /**
     * True when this manager downloads files on behalf of an offline folder walk (issue #1902):
     * completion never manufactures an explicit pin, and a copy whose coverage disappeared
     * while the download was in flight is dropped instead of being kept.
     */
    private val isInheritedDownload: Boolean = false,
) {
    private var lastDownloadedFile: IOFile? = null
    private var currentFile: File? = null
    private var filesDownloaded = 0

    /**
     * Whether the most recent [execute] transfer was rejected as superseded (issue: round 9
     * #8): the bulk caller must exclude such a transfer from its completion-marker clearing
     * AND from its batch's synchronized verdict, because a newer generation owns the file's
     * pending state and its replacement is still outstanding. The manager's own result maps
     * the outcome to success (the newer transfer's bytes win, nothing failed here), which is
     * why this status must travel beside it.
     */
    var lastTransferSuperseded: Boolean = false
        private set

    /**
     * Whether this transfer's downloaded bytes were discarded because their generation could
     * not be established (issue: round 24 #20): the worker converts that failure into
     * RETRYABLE work instead of a terminal failure — the durable intent (the pending marker)
     * stays, and WorkManager re-drives the transfer with fresh state.
     */
    var lastTransferGenerationUnresolved: Boolean = false
        private set

    private var lastUpdateProgressMillis = System.currentTimeMillis()

    /**
     * Whether this transfer's metadata commit was DEFERRED to the verification outcome
     * (issue: round 23 #7): a boundary-substituted transfer fetches comparison bytes with
     * the row uncommitted; a converged publication (the staged copy published) then
     * advances the historical row through the conditional acceptance, so the published
     * record identifies the row on the next pass.
     */
    private var verificationMetadataDeferred = false

    /**
     * THE HISTORICAL VERIFICATION'S CAPTURED OLD OBSERVATION (issue: round 24 #7): when a
     * boundary substitution defers the metadata, the transfer fetches comparison bytes for
     * the UNCOMMITTED historical row. The publication's supersession CAS then proves the
     * row has not moved SINCE THE CAPTURE — the fetched response's identity difference is
     * the very subject of the comparison, never a supersession. Null for ordinary
     * transfers (the response itself is the CAS basis).
     */
    private var verificationBasis: File? = null

    /** THE VERIFIED OLD-SOURCE HASH (issue: round 27 #3): the basis-location copy's
     * content hash at the moment the verification comparison CONVERGED — the
     * retirement revalidates the old path's bytes against exactly this value under
     * its own locks, so an intervening edit in the release/reacquire gap is
     * preserved, never deleted. Null when no basis copy was verified. */
    private var verifiedBasisSourceHash: String? = null

    suspend fun execute(
        context: Context,
        file: File,
        onProgress: (progress: Int, fileId: Int) -> Unit,
    ): ListenableWorker.Result {

        // Reset the per-transfer cleanup state BEFORE any early return (issue: round 3 #13): a
        // previous transfer's file must never be examined or deleted by this transfer's cleanup.
        currentFile = file
        lastDownloadedFile = null
        lastTransferSuperseded = false
        lastTransferGenerationUnresolved = false
        verificationMetadataDeferred = false
        verificationBasis = null
        verifiedBasisSourceHash = null

        // CAPTURED CACHE PUBLICATION TOKEN (issue: round 22 #21): the pre-download cache
        // cleanup validates the cache's CURRENT self-certification, but a later writer can
        // replace the cache and create a NEW self-certifying record under the SAME captured
        // row metadata — the cleanup would then delete that later publication. The cache's
        // exact token is captured BEFORE any await point (the details HTTP below is the
        // window); the deletion requires that exact token and bytes.
        val capturedCacheToken = capturedCachePublicationToken(context, file.getCacheFile(context, userDrive))

        // Fetch first WITHOUT committing metadata, so an inherited copy at the old location can
        // be preserved (migrated, or snapshotted against its OLD baseline) before the remote
        // rename/move upsert would forget that location (issues: round 3 #7, round 7 #2). The
        // shared upsert boundary covers both moved and unchanged paths; if preservation is
        // incomplete, the transfer aborts with the OLD metadata intact: nothing is stranded,
        // and the offline sync retries (issue: round 4 #9).
        val remoteFile = fetchRemoteFileDetails(context, file.id)
        // ONE coordinated operation (issue: round 11 #2): the ROOT-INTENT monitor precedes the
        // file lock (issue: round 16 #20) — the details response can CHANGE THE LOCATION
        // (parentId), a commit that reparents rows and alters the ancestry coverage
        // resolution walks; holding only the file lock let this reparenting interleave with
        // SAF's ancestry decisions (delete D after finding no owned descendants, then this
        // phase commits F under the deleted D). Order: intent monitor → file lock →
        // Realm/FS mutation — never the monitor from inside an already-held file lock.
        val committed = synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive, remoteFile.id) {
                // COMPLETE DESTINATION-CHAIN validation BEFORE any preservation side
                // effect (issue: round 18 #3, restoring the round-17 #8 intent: the
                // boundary call below PRESERVES — migrates, snapshots or deletes — the old
                // copy, so the incoming metadata's complete destination ancestry must be
                // refreshed and validated FIRST; the previous order ran the check after
                // the boundary, so a response moving F beneath an uncached ancestor could
                // move F's clean bytes — or snapshot and delete its dirty source — before
                // the check returned false, leaving the old row at the emptied location).
                // The same validation is re-applied to the boundary-committed identity
                // before the metadata commit.
                fun destinationChainExists(parentId: Int, realm: Realm): Boolean {
                    var currentId: Int? = parentId
                    val visited = mutableSetOf<Int>()
                    while (currentId != null && currentId > 0) {
                        if (!visited.add(currentId)) return false
                        if (FileController.getFileById(realm, currentId) == null) return false
                        currentId = FileController.getFileById(realm, currentId)?.parentId
                    }
                    return true
                }
                val prePreservationChainOk = remoteFile.parentId <= 0 ||
                    FileController.getRealmInstance(userDrive).use { realm ->
                        runCatching { realm.refresh() }
                        destinationChainExists(remoteFile.parentId, realm)
                    }
                if (!prePreservationChainOk) {
                    SentryLog.i(TAG, "Deferring refresh of file ${file.id}: its destination chain is incomplete")
                    return@withFileLock false
                }
                var skipMetadataCommit = false
                val commitFile = FileController.getRealmInstance(userDrive).use { realm ->
                    // The transfer's basis [file] is the request-observed state (issue: round 13
                    // #1): a tied-timestamp identity change is accepted only while the live row
                    // still matches it. The boundary result is DETACHED before the block closes
                    // (issue: round 22 #1 pattern) — a managed old row read after `use` would
                    // hit a closed realm.
                    val boundaryResult = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                        realm = realm,
                        remoteFiles = listOf(remoteFile),
                        userDrive = userDrive,
                        observedById = mapOf(remoteFile.id to file),
                    ).first()
                    if (boundaryResult === remoteFile) remoteFile else realm.copyFromRealm(boundaryResult, 1)
                }
                if (commitFile !== remoteFile) {
                    // NON-DESTRUCTIVE VERIFICATION DOWNLOAD (issues: round 22 #6, round 23
                    // #7): a boundary substitution defers the METADATA, not the
                    // verification — a separate staged copy is fetched and compared against
                    // the retained bytes at the (non-destructive) publication: equal bytes
                    // certify the retained generation, different bytes are retained. The
                    // historical row and bytes are never replaced first, and the metadata
                    // commit is skipped (the row stays as-is; a converged publication is
                    // committed by the NEXT pass, whose boundary sees the certified copy
                    // as replaceable). METADATA EQUALITY IS NO LONGER A PREREQUISITE:
                    // a legacy row with an unknown version against details with a known
                    // one — or an older row followed by a newer remote revision — now
                    // obtains comparison bytes. Only a genuinely STALE payload (the live
                    // row is NEWER than the fetch — a newer generation already owns the
                    // file) still defers outright.
                    if (FileController.isSupersededGeneration(commitFile, remoteFile)) {
                        SentryLog.i(TAG, "Deferring refresh of file ${file.id}: its old offline copy is not safely preserved yet")
                        return@withFileLock false
                    }
                    skipMetadataCommit = true
                    verificationMetadataDeferred = true
                    // THE CAPTURED OLD OBSERVATION (issue: round 24 #7): the uncommitted
                    // boundary result is the verification's CAS basis — the publication
                    // proves the row has not moved since THIS state, and the staged
                    // comparison (not the response's identity difference) decides
                    // convergence.
                    verificationBasis = commitFile
                }
                // The SAME complete-chain validation holds THROUGH the commit (issue: round
                // 17 #8): a chain broken during preservation defers without the metadata
                // write.
                val commitChainOk = commitFile.parentId <= 0 ||
                    FileController.getRealmInstance(userDrive).use { realm ->
                        destinationChainExists(commitFile.parentId, realm)
                    }
                if (!commitChainOk) {
                    SentryLog.i(TAG, "Deferring refresh of file ${file.id}: its destination chain broke during preservation")
                    return@withFileLock false
                }

                // Conditional replacement (issue: round 10 #4): a delayed response from an older
                // fetch must not roll back a newer generation another worker already committed —
                // otherwise this transfer's later publication compares against the rolled-back row,
                // passes, and leaves bytes and metadata describing different revisions. The
                // acceptance decision reads the live row INSIDE the committing transaction
                // (issue: round 11 #5) against the request-observed basis (issue: round 13 #1).
                FileController.getRealmInstance(userDrive).use { realm ->
                    if (skipMetadataCommit) {
                        // Non-destructive verification (issue: round 22 #6): the historical
                        // row stays AS-IS — no metadata commit — while the transfer fetches
                        // its separate staged copy for the publication-time comparison.
                        true
                    } else {
                        FileController.updateExistingFileIfNotOlder(newFile = commitFile, realm = realm, userDrive = userDrive, observed = file)
                    }
                }
            }
        }
        if (!committed) {
            SentryLog.i(TAG, "Deferring refresh of file ${file.id}: a newer generation was committed while the metadata response was in flight")
            return ListenableWorker.Result.failure()
        }

        val file = remoteFile
        currentFile = file
        val cacheFile = file.getCacheFile(context, userDrive)
        val offlineFile = file.getOfflineFile(context, userDrive.userId)
        if (offlineFile == null) {
            SentryLog.e(TAG, "offline must not be null")
            return ListenableWorker.Result.failure()
        }

        // POSITIVE destination ownership for the early-success paths (issue: round 15 #11):
        // pending/dirty fast paths return success without otherwise proving who owns the
        // destination — foreign or unknown ownership defers like a conflict, so the bulk
        // retains the root's incompleteness instead of completing F over another row's
        // protected bytes.
        // THE CAPTURED OLD PHYSICAL SOURCE IS THE VERIFICATION SUBJECT (issue: round 26
        // #11): in verification mode the retained historical row still OWNS the captured
        // basis location (A) while the fetched identity names the accepted rename's
        // destination (B) — resolving the owner ONLY at B rejected the verification before
        // its comparison could ever run (a hashless historical copy at A stayed deferred on
        // every retry after a remote rename; threading a verification basis into the later
        // publication never made that path reachable). The gate therefore also admits the
        // row when it exclusively owns the CAPTURED BASIS location; the verification's own
        // publication CAS still validates the basis comparison and the conditional identity
        // relocation under the coordinated locks.
        val destinationOwner = FileController.getRealmInstance(userDrive).use { realm ->
            OfflineFolderController.destinationOwnerRow(realm, context, userDrive, file, offlineFile)
                ?: verificationBasis
                    ?.takeIf { it.id == file.id }
                    ?.getOfflineFile(context, userDrive.userId)
                    ?.let { basisOffline ->
                        OfflineFolderController.destinationOwnerRow(
                            realm, context, userDrive, verificationBasis!!, basisOffline,
                        )
                    }
        }
        if (destinationOwner == null || destinationOwner.id != file.id) {
            lastTransferSuperseded = true
            return ListenableWorker.Result.success()
        }
        // Protect offline edits (issue #1902): never replace the bytes backing a pending upload
        // or a locally newer copy; the sync layer uploads them and a later walk re-evaluates.
        // Each early-success path consumes its OWN pending marker and validates the
        // observation INSIDE the consuming coordination (issues: round 18 #20, round 21
        // #20) — a pre-lock check can be stale by the time the marker is consumed, and a
        // stale observation falls through to the ordinary paths instead of reporting
        // success while consuming a newer request's re-mark.
        if (consumeEarlySuccessIntent(context, file) { live, liveOffline ->
                // EXACT OWNERSHIP, NOT A URI RECORD (issue: round 23 #9): the pending
                // branch previously required only existence — an old URI record after the
                // bytes changed (or a partial edit copy) fulfilled the request. The exact
                // upload-record identity (name/folder/mtime/hash/fileId) must match the
                // CURRENT bytes.
                liveOffline.exists() &&
                    UploadFile.sha256OrNull(liveOffline)?.let { currentHash ->
                        UploadFile.hasUploadRecordFor(
                            fileName = live.name,
                            remoteFolder = live.parentId,
                            driveId = userDrive.driveId,
                            userId = userDrive.userId,
                            fileModifiedAt = java.util.Date(liveOffline.lastModified()),
                            pendingOnly = true,
                            contentHash = currentHash,
                            originalFileId = live.id,
                        )
                    } == true
            }
        ) {
            return ListenableWorker.Result.success()
        }
        if (consumeEarlySuccessIntent(context, file) { live, liveOffline ->
                // COMPLETE CONTENT CLASSIFICATION (issue: round 23 #9): the edit branch
                // previously accepted any newer mtime — certified published bytes at
                // mtime 20 above a lagging Realm baseline 10 satisfied it after newer
                // remote metadata was accepted, the refresh was skipped and its marker
                // cleared. The edit must be POSITIVELY established: an ANCHORED baseline
                // (the mtime comparison is meaningless against the fallback), no
                // certifying record (a certified copy is the published generation
                // regardless of mtime), and no positively foreign record on the path.
                liveOffline.exists() &&
                    (live.publishedCleanBaseline ?: 0L) > 0L &&
                    run {
                        val liveProof = readPublishedProvenance(context, liveOffline)
                        val liveHash = UploadFile.sha256OrNull(liveOffline)
                        // A FAILED READ IS UNRESOLVED, NOT "NOT CERTIFIED" (issue: round
                        // 24 #9): unreadable bytes can never be positively established —
                        // the previous null-hash fall-through let an existing unreadable
                        // file with an anchored baseline and a newer mtime clear the
                        // marker and return success.
                        liveHash != null &&
                            liveProof?.positivelyForeignTo(live, userDrive) != true &&
                            !(liveProof?.identifies(live, userDrive) == true &&
                                liveProof.contentHash != null &&
                                liveProof.contentHash == liveHash) &&
                            OfflineFolderPolicy.isLocallyModified(
                                offlineFileLastModifiedMillis = liveOffline.lastModified(),
                                // The LIVE row's PUBLISHED byte baseline, not accepted server
                                // metadata (issues: round 15 #6, round 22 #8).
                                remoteLastModifiedAtSeconds = live.effectivePublishedBaseline,
                            )
                    }
            }
        ) {
            return ListenableWorker.Result.success()
        }

        when (moveCacheFileIfIntact(context, file, cacheFile, offlineFile)) {
            // The EXACT fulfilled file-intent token is consumed INSIDE the publication's
            // coordinated commit (issue: round 20 #20) — the FULFILLED arm runs no
            // post-publication clear: an unpin could delete the published bytes and a
            // re-pin establish a new marker between the publication's locks releasing
            // and the late call.
            CachePromotionOutcome.FULFILLED -> {
                return ListenableWorker.Result.success()
            }
            // Deferred outcomes (conflict, superseded, protected/invalidated) retain the
            // pending marker and the root's incompleteness; the batch's work is done for
            // this file either way.
            CachePromotionOutcome.DEFERRED -> return ListenableWorker.Result.success()
            // A failed promotion falls through to the fresh download branch.
            CachePromotionOutcome.FAILED -> Unit
        }

        if (consumeEarlySuccessIntent(context, file) { live, liveOffline ->
                // The scoped current-copy proof (issues: round 21 #25, round 22 #8)
                // re-established INSIDE the consuming section, against the transfer's
                // captured account and the LIVE row's own location.
                live.isOfflineAndProvenCurrent(context, liveOffline, userDrive)
            }
        ) {
            // We can have this case for example when we try to put a lot of files at once in offline mode
            // and for some reason, the worker is cancelled after a long time, the worker is restarted
            filesDownloaded += 1
            return ListenableWorker.Result.success()
        }

        onProgress(0, file.id)

        // The previous published copy is never deleted upfront: the fresh download is staged in
        // an app-owned directory and published by an atomic rename, so a failed transfer never
        // destroys it (issue: round 3 #2/#4).
        // CONDITIONAL cache cleanup (issues: round 17 #6, round 18 #11): only a cache still
        // representing THIS transfer's source revision goes — a later publication's bytes are
        // never removed by this older transfer — and the exact check and the deletion run
        // under the intent monitor and the file lock, so another publication can no longer
        // replace the cache between the check and the deletion.
        synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive, file.id) {
                if (cacheFile.exists() &&
                    (cacheFile.lastModified() / 1_000L) == file.lastModifiedAt &&
                    file.isIntactFile(cacheFile) &&
                    // EXACT CAPTURED PUBLICATION IDENTITY (issue: round 21 #24): coarse
                    // size/mtime alone cannot distinguish this transfer's cache from a
                    // DIFFERENT generation a concurrent writer published with the same
                    // size/mtime — and this cleanup also runs when promotion was REJECTED
                    // for an unproven cache. Only the cache's own provenance record
                    // certifying THESE bytes for THIS row's exact generation authorizes
                    // the deletion; a different or unproven cache is preserved.
                    cacheBytesProvenCurrent(context, cacheFile, file, userDrive) &&
                    // THE EXACT CAPTURED TOKEN (issue: round 22 #21): the deletion requires
                    // the cache to still carry THIS transfer's captured publication token and
                    // the same bytes — a later writer's new self-certifying record (same
                    // captured row metadata, different bytes) does not match and is
                    // preserved.
                    capturedCacheTokenStillMatches(context, cacheFile, capturedCacheToken)
                ) {
                    deletePublishedProvenance(context, cacheFile)
                    runCatching { cacheFile.delete() }
                }
            }
        }

        return if (currentFile?.rights?.canRead == false) {
            ListenableWorker.Result.failure()
        } else {
            runCatching {
                startOfflineDownload(context, file, offlineFile, onProgress)
            }.cancellable().onFailure { exception ->
                SentryLog.e(TAG, "An error has occurred", exception)
            }.getOrDefault(ListenableWorker.Result.failure())
        }
    }

    /**
     * Cleanup only ever concerns this transfer's owned staged file: it is deleted on failure and
     * renamed on success, so at cleanup time an existing staged file is always residue.
     */
    fun cleanLastDownloadedFile() {
        lastDownloadedFile?.let { staged ->
            runCatching { if (staged.exists()) staged.delete() }
        }
    }

    /**
     * Fetches the fresh remote metadata WITHOUT committing it to Realm: the caller preserves any
     * old inherited copy first, then upserts (issue: round 3 #7).
     */
    private suspend fun fetchRemoteFileDetails(
        context: Context,
        fileId: Int,
    ): File {
        // Always use the captured account's HTTP client: the walk may target another account/drive
        // than the currently active one (issue #1902).
        val fileDetails = Dispatchers.IO {
            ApiRepository.getFileDetails(
                file = File(id = fileId, driveId = userDrive.driveId),
                okHttpClient = AccountUtils.getHttpClient(userDrive.userId),
            )
        }
        val remoteFile = fileDetails.data
        return if (fileDetails.isSuccess() && remoteFile != null) {
            remoteFile
        } else {
            if (fileDetails.error?.exception is NetworkException) throw UploadTask.NetworkException()

            val translateError = fileDetails.translateError()
            val responseGsonType = object : TypeToken<ApiResponse<File>>() {}.type
            val translatedErrorText = context.getString(translateError)
            val responseJson = ApiController.gson.toJson(fileDetails, responseGsonType)
            throw RemoteFileException("$responseJson $translatedErrorText")
        }
    }

    /**
     * Promotes an intact cache file through the same staged, validated publication as network
     * downloads (issue: round 3 #3): partial promotion bytes never appear at the offline path,
     * and the final coordinated recheck runs before the atomic replace.
     */
    private suspend fun moveCacheFileIfIntact(
        context: Context,
        file: File,
        cacheFile: IOFile,
        offlineFile: IOFile,
    ): CachePromotionOutcome = withContext(Dispatchers.IO) {
        // ASYNC-PREPARATION / HELD-PUBLICATION split (round-16 compile correction): Kotlin
        // forbids a suspension point inside a critical section, so the SHARED provenance
        // validation THROUGH STAGING (issues: round 16 #11/#12) runs as ONE SYNCHRONOUS
        // section under the ROOT-INTENT monitor and the file lock — the cache bytes and
        // their provenance sidecar cannot change between validation and staging, and the
        // monitor-first order matches the global protocol. The staged publication is
        // SUSPEND, so it runs OUTSIDE the critical section: its own coordinated core
        // (publishStagedOfflineFile) re-acquires the SAME monitor and locks in the SAME
        // order and revalidates identity, ownership, intent and protection before its
        // rename, so no outcome check is lost.
        val staged = synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive, file.id) {
                // EXACT current generation provenance (issues: round 14 #14, round 15 #9,
                // round 16 #11): exact mtime and size do not establish WHICH generation the
                // cached bytes are — the sidecar written at cache publication must match
                // this transfer's authoritative (details-sourced) version and baseline.
                // Unavailable or mismatched provenance downloads the current revision.
                if (!cacheBytesProvenCurrent(context, cacheFile, file, userDrive)) {
                    null
                } else {
                    val candidate = newStagedFile(context, file)
                    // EXPLICIT Boolean success AFTER the actual copy (round-16 compile fix #2):
                    // File.copyTo returns the destination File, so getOrDefault(false) against
                    // it inferred a File/Boolean least-upper-bound instead of a condition.
                    val copied = runCatching {
                        cacheFile.copyTo(candidate, overwrite = true)
                        true
                    }.getOrDefault(false)
                    if (copied) {
                        candidate
                    } else {
                        runCatching { if (candidate.exists()) candidate.delete() }
                        null
                    }
                }
            }
        } ?: return@withContext CachePromotionOutcome.FAILED

        val published = runCatching {
            publishStagedOfflineFile(context, file, offlineFile, staged)
        }.onFailure { exception ->
            SentryLog.e(TAG, "Cache promotion failed for ${offlineFile.path}", exception)
        }.getOrDefault(PublicationOutcome.FAILED)
        // Cache promotion is a transfer path too: its superseded/conflict outcomes must reach
        // the bulk caller like the network branch's (issues: round 10 #7, round 14 #13), or
        // the batch clears pending work and reports the root synchronized.
        // Unresolved outcomes propagate to Bulk completion (issue: round 17 #7): an
        // invalidated transfer's root incompleteness is retained like a conflict.
        lastTransferSuperseded = published in UNRESOLVED_PUBLICATION_OUTCOMES

        runCatching { if (staged.exists()) staged.delete() }
        // TYPED promotion outcomes for the CALLER (issue: round 19 #24): the previous
        // Boolean collapsed PUBLISHED (this transfer's intent token WAS fulfilled — the
        // marker may clear) into the same `true` as the conflicts and supersessions
        // (whose pending marker the caller then ERASED, contradicting their own
        // retain-the-marker contract).
        when (published) {
            // Completion (flag write, media scan, invalidation) and the EXACT file-intent
            // consumption already ran inside the per-file lock via the publication's
            // coordinated commit (issues: round 5 #2, round 20 #20) — the caller runs no
            // post-publication marker clear.
            PublicationOutcome.PUBLISHED -> CachePromotionOutcome.FULFILLED
            // Skipped outcomes leave the existing bytes and timestamps untouched (issue:
            // round 4 #1). Marker completion is NOT unconditional here (issue: round 17 #7):
            // SKIPPED_PROTECTED-for-this-file cleared its marker INSIDE the validating
            // section (publishLockedCore, under the locks, generation+intent validated);
            // SKIPPED_INVALIDATED never clears — a revived root or newer request that
            // established pending work after the skip decision owns its marker.
            PublicationOutcome.SKIPPED_PROTECTED, PublicationOutcome.SKIPPED_INVALIDATED -> {
                filesDownloaded += 1
                CachePromotionOutcome.DEFERRED
            }
            PublicationOutcome.SKIPPED_CONFLICT -> {
                // The destination holds protected content owned by another row (issue: round
                // 14 #13): deferred — the pending marker STAYS so the work and the root's
                // incompleteness are retained until the conflict resolves.
                CachePromotionOutcome.DEFERRED
            }
            PublicationOutcome.SKIPPED_SUPERSEDED -> {
                // A newer generation owns the file's pending state and completion (issue:
                // round 8 #9): this transfer's skip clears no marker and runs no completion.
                CachePromotionOutcome.DEFERRED
            }
            // UNRESOLVED generation (issue: round 20 #14): the bytes are published but the
            // generation is unproven — the promotion is NOT fulfilled (the marker stays,
            // the root's incompleteness is retained via the unresolved-outcome set).
            PublicationOutcome.PUBLISHED_UNRESOLVED -> CachePromotionOutcome.DEFERRED
            PublicationOutcome.FAILED -> CachePromotionOutcome.FAILED
        }
    }

    /**
     * Final coordinated publication AND completion (issue: round 5 #2): the completion flag
     * write, its exact-revision-verified invalidation and the media scan all run INSIDE the same
     * per-file lock as the rename, so an edit becoming a pending upload (or a root removal
     * retaining it) between publication and completion can neither be overwritten nor deleted.
     */
    private suspend fun publishStagedOfflineFile(
        context: Context,
        file: File,
        target: IOFile,
        staged: IOFile,
        verificationBasis: File? = null,
        explicitUnresolvedGeneration: Boolean = false,
    ): PublicationOutcome = publishStagedOfflineFile(
        userDrive = userDrive,
        isInheritedDownload = isInheritedDownload,
        file = file,
        target = target,
        staged = staged,
        onPublishedLocked = { completePublishedFileLocked(context, file, target) },
        verificationBasis = verificationBasis,
        explicitUnresolvedGeneration = explicitUnresolvedGeneration,
        // THE COMPARISON-TIME BASIS HASH (issue: round 27 #3) rides this transfer's own
        // instance state — the companion core cannot hold it, so the wrapper binds the
        // callback; the retirement below reads exactly the hash its publication verified.
        onVerifiedBasisHash = { verifiedBasisSourceHash = it },
    )

    /**
     * Completion core for bytes this transfer just published, executed while the per-file lock
     * is still held. The redundant timestamp write is gone: the staged file was already
     * timestamped with the remote revision before the atomic rename.
     */
    private fun completePublishedFileLocked(context: Context, file: File, target: IOFile) {
        var coverageInvalidated = false

        // All validation happens INSIDE the committing transaction (issue: round 3 #9): current
        // explicit pin and durable ACTIVE root ancestry are re-derived here; a materialized
        // inherited flag alone never authorizes retention.
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction {
                val liveFile = FileController.getFileById(realm, file.id)
                val pinned = liveFile?.isOffline == true
                val ancestorRootCovers = liveFile != null && OfflineFolderController.isInsideActiveRoot(realm, file.id)

                when {
                    isInheritedDownload && !pinned && !ancestorRootCovers -> {
                        coverageInvalidated = true
                    }
                    isInheritedDownload && ancestorRootCovers -> {
                        if (liveFile != null && liveFile.isValid) liveFile.isInOfflineFolder = true
                    }
                    else -> {
                        if (liveFile != null && liveFile.isValid) {
                            // A file covered by an offline folder keeps its inherited state: it
                            // must not become an explicit pin, otherwise removing the folder
                            // could not clean it up without stripping separately pinned files.
                            liveFile.isOffline = liveFile.isOffline || !liveFile.isInOfflineFolder
                        }
                    }
                }
            }
        }

        if (coverageInvalidated) {
            // Coverage disappeared while this transfer ran. Delete the target only if it is
            // still exactly the revision this transfer published: anything else (an edit that
            // became a pending upload, for instance) is owned by someone else now.
            val isExactPublishedRevision = target.exists() && target.lastModified() / 1_000L == file.lastModifiedAt
            if (isExactPublishedRevision) {
                deletePublishedProvenance(context, target)
                runCatching { target.delete() }
                if (file.isMedia()) file.deleteInMediaScan(context, userDrive)
            }
        } else {
            if (file.isMedia()) MediaUtils.scanFile(context, target)
        }
        fileDownloaded(context, file.id)
    }

    private suspend fun startOfflineDownload(
        context: Context,
        file: File,
        offlineFile: java.io.File,
        onProgress: (progress: Int, fileId: Int) -> Unit,
    ): ListenableWorker.Result = withContext(Dispatchers.IO) {

        val okHttpClient = AccountUtils.getHttpClient(userDrive.userId, null)

        // THE NO-SPIN GATE (issue: round 26 #20), BEFORE any transfer: with no supported
        // response-bound identity route, an already-published EXPLICITLY UNRESOLVED copy
        // covering the CURRENT remote generation gains nothing from another identical
        // download. The durable unresolved state is retained (marker stays, no completion
        // claim, no intent clear) and the attempt defers — evidence (a metadata advance, a
        // lost or non-certifying record/copy) re-justifies the transfer on a later pass.
        if (unresolvedPublicationAlreadyCoversCurrentGeneration(
                context = context,
                file = file,
                target = offlineFile,
                userDrive = userDrive,
                okHttpClient = okHttpClient,
            )
        ) {
            lastTransferGenerationUnresolved = true
            SentryLog.i(
                downloadWorker.workerTag(),
                "Download of ${file.id} deferred: an explicitly unresolved publication already covers this generation and no new evidence exists",
            )
            // THE DEFERRED VERIFICATION ADVANCE STILL PROGRESSES (issues: round 27 #4,
            // round 28 #4/#5): suppression must not strand a converged verification whose
            // CONDITIONAL METADATA ADVANCE was rejected on a tied-rename tie. The
            // resumption is BOUND to the outstanding durable operation: a DETACHED
            // pre-request observation of the live row is captured BEFORE the details
            // request (the previous shape read the managed row AFTER the response and
            // passed it as its own observation — a tied identity commit during the HTTP
            // validated the row against itself and accepted stale B); the coordinated
            // section re-derives the destination proof binding (the version-0 unresolved
            // publication must still certify the target for this identity at the fetched
            // generation), validates the live row against the detached observation, and
            // runs the conditional acceptance through the normal boundary. Cancellation
            // is checked after the response and inside the acquired section, and is
            // REthrown (never consumed by runCatching).
            run {
                val preRequestObservation = FileController.getRealmInstance(userDrive).use { obsRealm ->
                    runCatching { obsRealm.refresh() }
                    FileController.getFileById(obsRealm, file.id)?.let { obsRealm.copyFromRealm(it, 1) }
                } ?: return@withContext ListenableWorker.Result.failure()
                val freshForAdvance = runCatching {
                    ApiRepository.getFileDetails(
                        file = File(id = file.id, driveId = userDrive.driveId),
                        okHttpClient = okHttpClient,
                    ).data?.takeIf { it.id == file.id }
                }.getOrElse { if (it is kotlin.coroutines.cancellation.CancellationException) throw it else null }
                // THE EXECUTING CONTEXT AFTER THE RESPONSE (issue: round 28 #5).
                ensureActive()
                if (freshForAdvance == null) return@withContext ListenableWorker.Result.failure()
                synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    OfflineFileCoordination.withFileLock(userDrive, file.id) {
                        // POST-LOCK-WAIT CHECK (issue: round 28 #5).
                        ensureActive()
                        FileController.getRealmInstance(userDrive).use { realm ->
                            runCatching { realm.refresh() }
                            val live = FileController.getFileById(realm, file.id) ?: return@withFileLock
                            // THE OBSERVATION MUST STILL MATCH (issue: round 28 #4): a tied
                            // identity commit during the request is newer work's business.
                            if (!FileController.isSameObservation(live, preRequestObservation)) {
                                return@withFileLock
                            }
                            // THE DURABLE OPERATION BINDING (issue: round 28 #4): the
                            // target must STILL carry the outstanding version-0 unresolved
                            // publication for this identity, certifying its bytes, at the
                            // fetched generation — otherwise this resumption has no
                            // operation to advance.
                            val bindingProof = readPublishedProvenance(context, offlineFile)
                            val bindingHolds = bindingProof != null &&
                                bindingProof.version == 0 &&
                                bindingProof.identifies(live, userDrive) &&
                                bindingProof.certifiesBytes(offlineFile) &&
                                bindingProof.revisedAt == freshForAdvance.revisedAt &&
                                bindingProof.lastModifiedAt == freshForAdvance.lastModifiedAt
                            if (!bindingHolds) return@withFileLock
                            FileController.updateExistingFileIfNotOlder(
                                newFile = freshForAdvance,
                                realm = realm,
                                userDrive = userDrive,
                                observed = preRequestObservation,
                            )
                        }
                    }
                }
            }
            return@withContext ListenableWorker.Result.failure()
        }

        val response = downloadFileResponseAsync(
            fileUrl = ApiRoutes.downloadFile(file),
            okHttpClient = okHttpClient,
            downloadInterceptor = downloadProgressInterceptor(
                getMostRecentLastUpdate = { lastUpdateProgressMillis },
                onLastUpdateChange = { lastUpdate -> lastUpdateProgressMillis = lastUpdate },
                onProgress = { progress ->
                    ensureActive()

                    onProgress(progress, file.id)

                    if (downloadWorker.isForOneFile()) {
                        updateDownloadNotification(
                            context = context,
                            contentTitle = file.name,
                            contentText = "%d%%".format(progress),
                            progressPercent = progress,
                        )
                    }

                    SentryLog.d(downloadWorker.workerTag(), "download $progress%")
                },
            )
        )

        if (!response.isSuccessful) return@withContext ListenableWorker.Result.failure()

        // Download into a unique, app-owned staged file (issue: round 3 #2): never a sibling of
        // the offline path (a user file could live there), never shared between concurrent
        // workers. In-progress bytes are never visible at the offline path.
        val staged = newStagedFile(context, file)
        lastDownloadedFile = staged
        makeSureFileExists(staged) ?: return@withContext ListenableWorker.Result.failure()

        runCatching {
            val remoteDataHasBeenSaved = saveRemoteData(downloadWorker.workerTag(), response, staged)

            // Validate before publishing (size when the remote metadata carries one), then hand
            // over to the final coordinated publication (intent, pending uploads, target
            // revision, atomic replace).
            val sizeIsValidated = file.size == null || file.isIntactFile(staged)
            // GENERATION-BOUND RESPONSE VALIDATION (issue: round 22 #23): the download route
            // carries no revision token — the staged bytes are whatever the server served at
            // request time, while the staged record would claim the METADATA fetch's
            // generation; a server advance between the two would record R2 bytes as R1, and
            // equal-size content could then satisfy PROVEN_CURRENT and root completion.
            // Hashing the bytes proves byte stability, not the asserted server generation.
            // The version-carrying details route is therefore re-fetched AFTER the download
            // (OUTSIDE every lock): a generation that no longer matches — or a validation
            // that cannot complete — leaves the bytes' generation UNKNOWABLE. The staged
            // copy is discarded and the intent retained (the pending marker stays for the
            // re-drive); the next pass re-fetches the metadata and re-downloads. No
            // known-version proof is manufactured from an unbound response.
            val generation = reestablishDownloadedGeneration(
                file = file,
                userDrive = userDrive,
                okHttpClient = okHttpClient,
            )
            if (remoteDataHasBeenSaved && staged.exists() && sizeIsValidated && generation == GenerationEstablishment.FAILED) {
                runCatching { staged.delete() }
                // TYPED UNRESOLVED, RETRYABLE (issue: round 24 #20): a discarded
                // unestablishable generation is deferred work, never a terminal failure —
                // the worker converts this failure into a retry and the durable intent
                // (the pending marker) stays.
                lastTransferGenerationUnresolved = true
                SentryLog.i(
                    downloadWorker.workerTag(),
                    "Download of ${file.id} discarded: its generation could not be established against the server",
                )
                return@withContext ListenableWorker.Result.failure()
            }
            if (remoteDataHasBeenSaved && staged.exists() && sizeIsValidated) {
                // THE TYPED RESULT DRIVES THE PUBLICATION (issue: round 25 #20): an
                // unestablished generation forces the EXPLICITLY UNRESOLVED publication
                // (version-0 record, unresolved outcome, no intent clear, no completion)
                // INDEPENDENTLY of the historical CAS basis — a known-version verification
                // basis whose fresh establishment was coarse must not let the publication
                // compute unresolvedness from the OLD known versions and report PUBLISHED.
                val outcome = publishStagedOfflineFile(
                    context = context,
                    file = file,
                    target = offlineFile,
                    staged = staged,
                    verificationBasis = verificationBasis,
                    explicitUnresolvedGeneration = generation != GenerationEstablishment.ESTABLISHED,
                )
                // Public superseded/conflict status for the bulk caller (issues: round 9 #8,
                // round 14 #13).
                lastTransferSuperseded = outcome in UNRESOLVED_PUBLICATION_OUTCOMES
                // CONDITIONAL METADATA ADVANCE (issue: round 23 #7): a verification whose
                // bytes CONVERGED published the staged copy under the fetched generation —
                // the historical row is advanced through the same conditional acceptance
                // under the coordination, so the published record identifies the row on
                // the next pass (the comparison was resolved BEFORE the metadata moved).
                if (verificationMetadataDeferred &&
                    (outcome == PublicationOutcome.PUBLISHED || outcome == PublicationOutcome.PUBLISHED_UNRESOLVED)
                ) {
                    var advanceAccepted = false
                    synchronized(OfflineFileCoordination.rootIntentMonitor) {
                        OfflineFileCoordination.withFileLock(userDrive, file.id) {
                            FileController.getRealmInstance(userDrive).use { realm ->
                                runCatching { realm.refresh() }
                                val liveRow = FileController.getFileById(realm, file.id)
                                // THE OLD SOURCE IS CLASSIFIED AND PRESERVED BEFORE ANY
                                // IDENTITY ADVANCE (issues: round 27 #3, round 28 #2/#3):
                                // the row still OWNS A at this moment — an intervening or
                                // unreadable revision is durably registered under the
                                // CURRENT (pre-advance) identity and the advancement is
                                // SKIPPED (the row keeps its old location, so the retained
                                // revision's ownership stays discoverable); hash equality
                                // alone is CONTENT EVIDENCE, never deletion authority — the
                                // retirement additionally requires fresh URI-consumer,
                                // recovery, exclusive-ownership and non-foreign authority.
                                val basisPath = verificationBasis
                                    ?.getOfflineFile(context, userDrive.userId)?.path
                                // THE EXECUTING CONTEXT INSIDE THE ACQUIRED SECTION
                                // (issue: round 29 #2): the publication released its
                                // coordination; a cancelled worker waiting here must not
                                // classify, snapshot, advance or retire.
                                ensureActive()
                                var retirementAuthorized = true
                                if (basisPath != null && basisPath != offlineFile.path) {
                                    val basisCopy = IOFile(basisPath)
                                    if (basisCopy.exists()) {
                                        // AUTHORITY PRE-FLIGHT BEFORE ANY CLASSIFICATION OR
                                        // SNAPSHOT (issues: round 29 #1): the live identity,
                                        // the old location, exclusive ownership, non-foreign
                                        // provenance, recovery exclusions and URI consumers
                                        // are validated FIRST — the previous order registered
                                        // every differing/unreadable source as the live row's
                                        // edit BEFORE the ownership checks could reject a
                                        // relocated row or a newly recovery-managed path, and
                                        // the recovery-resolution override belonged to
                                        // validated reconciliation, not this flow.
                                        val currentBasisHash = UploadFile.sha256OrNull(basisCopy)
                                        val basisUri = android.net.Uri.fromFile(basisCopy).toString()
                                        val preAdvanceRow = liveRow?.let { realm.copyFromRealm(it, 1) }
                                        val authorityHolds = preAdvanceRow != null &&
                                            OfflineFolderController.exclusivePathOwner(
                                                realm, context, userDrive, basisPath,
                                            ) == preAdvanceRow.id &&
                                            readPublishedProvenance(context, basisCopy)
                                                ?.positivelyForeignTo(preAdvanceRow, userDrive) != true &&
                                            !UploadFile.hasPendingUpload(uri = basisUri) &&
                                            !UploadFile.hasRetainedUnsentUpload(uri = basisUri) &&
                                            !hasUnresolvedRecovery(context, userDrive, file.id, basisPath)
                                        when {
                                            // AUTHORITY LOST: the row moved, the path was
                                            // re-owned, a foreign record names it, a URI
                                            // consumer or recovery owns it — nothing at A is
                                            // this operation's to classify; the advancement is
                                            // skipped so the row keeps its old owning
                                            // location.
                                            !authorityHolds -> {
                                                retirementAuthorized = false
                                                SentryLog.i(
                                                    TAG,
                                                    "Old-path authority lost for ${file.id}; advancement deferred",
                                                )
                                            }
                                            // THE EXACT VERIFIED REVISION: the retirement
                                            // candidate (subject to the advancement below).
                                            currentBasisHash != null && currentBasisHash == verifiedBasisSourceHash -> Unit
                                            // A READABLE DIFFERING REVISION WITH FULL
                                            // AUTHORITY: classified through the COMMON content
                                            // classification — a PROVEN edit registers as the
                                            // row's snapshot (NO recovery-resolution override);
                                            // foreign, clean and uncertain bytes are deferred
                                            // unregistered. UNREADABLE bytes are UNCERTAIN,
                                            // never a proven edit.
                                            currentBasisHash != null -> {
                                                val classifiedEdit = currentBasisHash != verifiedBasisSourceHash &&
                                                    OfflineFolderController.protectionStateOf(
                                                        preAdvanceRow ?: liveRow ?: return@withFileLock,
                                                        basisCopy,
                                                        userDrive,
                                                    ) == OfflineFolderController.CopyProtection.DIRTY
                                                if (classifiedEdit && preAdvanceRow != null) {
                                                    val registered = runCatching {
                                                        OfflineFolderController.registerDirtySnapshot(
                                                            context = context,
                                                            file = preAdvanceRow,
                                                            offlineFile = basisCopy,
                                                            userDrive = userDrive,
                                                        )
                                                    }.getOrDefault(false)
                                                    if (!registered) retirementAuthorized = false
                                                } else {
                                                    retirementAuthorized = false
                                                }
                                                SentryLog.i(
                                                    TAG,
                                                    "Old-path revision for ${file.id} classified; advancement deferred",
                                                )
                                            }
                                            // UNREADABLE OLD BYTES (issue: round 29 #1):
                                            // uncertain — the advancement defers, nothing is
                                            // registered, nothing is deleted.
                                            else -> {
                                                retirementAuthorized = false
                                                SentryLog.i(
                                                    TAG,
                                                    "Unreadable old-path bytes for ${file.id}; advancement deferred",
                                                )
                                            }
                                        }
                                        // RE-CHECK BEFORE THE COMMIT/UNLINK FOLLOWING
                                        // POTENTIALLY LENGTHY HASH/SNAPSHOT WORK (issue:
                                        // round 29 #2).
                                        ensureActive()
                                    }
                                }
                                if (retirementAuthorized) {
                                    advanceAccepted = FileController.updateExistingFileIfNotOlder(
                                        newFile = file,
                                        realm = realm,
                                        userDrive = userDrive,
                                        // THE CAPTURED HISTORICAL OBSERVATION (issue: round
                                        // 27 #4): the advancement was issued from the retained
                                        // A basis — the previous observed = file (the fetched B
                                        // identity) could never match a live A row with tied
                                        // updatedAt, so a legitimate rename's advancement was
                                        // rejected on every attempt (and the no-spin gate then
                                        // kept the row at A indefinitely).
                                        observed = verificationBasis ?: file,
                                    )
                                    // THE OLD PHYSICAL SOURCE IS RETIRED WITH THE ACCEPTED
                                    // IDENTITY (issue: round 25 #9): only the exact verified
                                    // revision, under full current authority, is retired.
                                    if (advanceAccepted && basisPath != null && basisPath != offlineFile.path) {
                                        val basisCopy = IOFile(basisPath)
                                        if (basisCopy.exists()) {
                                            deletePublishedProvenance(context, basisCopy)
                                            runCatching { basisCopy.delete() }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                when (outcome) {
                    PublicationOutcome.FAILED -> return@withContext ListenableWorker.Result.failure()
                    // Completion (flag writes, timestamp, media scan) only runs for bytes this
                    // transfer actually published; skipped outcomes leave the existing content
                    // untouched (issue: round 4 #1).
                    PublicationOutcome.PUBLISHED -> onProgress(100, file.id)
                    // UNRESOLVED generation (issue: round 20 #14): bytes are published but
                    // the generation is unproven — NO 100, no completion; the transfer's
                    // work is done (success) while lastTransferSuperseded (set membership)
                    // retains the batch's and root's incompleteness.
                    PublicationOutcome.PUBLISHED_UNRESOLVED -> Unit
                    PublicationOutcome.SKIPPED_PROTECTED, PublicationOutcome.SKIPPED_INVALIDATED -> {
                        // Marker completion is conditional INSIDE the validating section
                        // (issue: round 17 #7); an invalidated old transfer clears nothing.
                    }
                    PublicationOutcome.SKIPPED_CONFLICT -> {
                        // A protected destination owned by another row defers (issue: round
                        // 14 #13): the pending marker stays so the work is retained.
                    }
                    PublicationOutcome.SKIPPED_SUPERSEDED -> {
                        // A newer generation owns the file's pending state and completion
                        // (issue: round 8 #9): this transfer's skip must not clear its marker
                        // out from under the newer transfer.
                    }
                }
                ListenableWorker.Result.success()
            } else {
                runCatching { staged.delete() }
                ListenableWorker.Result.failure()
            }
        }.cancellable().getOrElse {
            runCatching { staged.delete() }
            throw it
        }
    }

    /**
     * Clears only the download-pending marker, without touching bytes, timestamps or coverage
     * flags (issue: round 4 #1).
     */
    private fun clearPendingMarker(fileId: Int) {
        FileController.markFileAsOffline(fileId = fileId, isMarkedAsOffline = false, userDrive = userDrive)
    }

    /**
     * Consumes THIS transfer's pending-download marker CONDITIONALLY, validating the
     * early-success observation INSIDE the same coordinated section (issues: round 18 #20,
     * round 21 #20): the marker is cleared only when the live row is still at the
     * generation this transfer refreshed against AND the transfer's early-success
     * condition still holds after the locks. A pre-lock observation can be stale by the
     * time the marker is consumed — upload completion followed by unpin/delete/re-pin
     * establishes a newer marker for the same server metadata, which a metadata-only
     * generation comparison cannot distinguish — so a stale observation is reported to
     * the caller (false), which falls through to the ordinary promotion/download paths
     * instead of reporting success while consuming a newer intent. Runs under the intent
     * monitor and the file lock.
     *
     * FRESH INPUTS, NEVER THE CAPTURED ROW/PATH (issue: round 22 #8): the condition
     * receives the LIVE row and the offline path the LIVE row resolves to — a captured
     * row/path pair validates a same-second moved or path-reused location while clearing
     * the CURRENT row's marker. The live row must still hold the offline intent, and the
     * consumed path's provenance must not be positively foreign (another identity's
     * bytes are never this row's early success).
     */
    private fun consumeEarlySuccessIntent(
        context: Context,
        file: File,
        condition: (live: File, liveOffline: IOFile) -> Boolean,
    ): Boolean =
        synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive, file.id) {
                FileController.getRealmInstance(userDrive).use { realm ->
                    runCatching { realm.refresh() }
                    val live = FileController.getFileById(realm, file.id) ?: return@use false
                    if (FileController.isSupersededGeneration(live, file) || live.updatedAt > file.updatedAt) return@use false
                    if (!live.isOffline && !live.isInOfflineFolder) return@use false
                    val liveOffline = live.getOfflineFile(context, userDrive.userId) ?: return@use false
                    if (readPublishedProvenance(context, liveOffline)?.positivelyForeignTo(live, userDrive) == true) {
                        return@use false
                    }
                    // FRESH RECOVERY EXCLUSION (issue: round 24 #9): an unresolved durable
                    // recovery governing this row or its path defers the early success —
                    // the sweep owns the bytes, and no intent is consumed over them.
                    if (hasUnresolvedRecovery(context, userDrive, live.id, liveOffline.absolutePath)) {
                        return@use false
                    }
                    if (!condition(live, liveOffline)) return@use false
                    if (live.isMarkedAsOffline) {
                        FileController.markFileAsOffline(fileId = file.id, isMarkedAsOffline = false, userDrive = userDrive)
                    }
                    true
                }
            }
        }

    private fun makeSureFileExists(offlineFile: java.io.File): Unit? {
        val parentExists = offlineFile.parentFile?.exists()

        runCatching {
            if (parentExists == false) offlineFile.parentFile?.mkdirs()
            // This line is here to help some devices that don't succeed in automatically creating the file…
            offlineFile.createNewFile()
        }.onFailure {
            SentryLog.e(TAG, "Failed to create a new file", it) { scope ->
                scope.setExtra("does parent exist", parentExists.toString())
            }

            return null
        }

        return Unit
    }

    private fun fileDownloaded(context: Context, fileId: Int) {
        FileController.markFileAsOffline(fileId = fileId, isMarkedAsOffline = false, userDrive = userDrive)
        lastUpdateProgressMillis = System.currentTimeMillis()
        filesDownloaded += 1

        val progressPercent = (filesDownloaded * 100) / filesCount
        downloadWorker.downloadNotification()?.let { notification ->
            val notificationContentTitle = notification.titleResId?.let { titleResId ->
                context.getString(titleResId, progressPercent)
            }
            val notificationContentText = notification.contentResId?.let { contentResId ->
                context.resources.getQuantityString(
                    contentResId,
                    filesDownloaded,
                    filesDownloaded,
                    filesCount
                )
            }
            updateDownloadNotification(
                context = context,
                contentTitle = notificationContentTitle,
                contentText = notificationContentText,
                progressPercent = progressPercent
            )
        }
    }

    private fun updateDownloadNotification(
        context: Context,
        contentTitle: String?,
        contentText: String?,
        progressPercent: Int
    ) {
        val downloadNotification = downloadWorker.downloadNotification()
        downloadNotification?.notification?.apply {
            setContentTitle(contentTitle)
            setContentText(contentText)
            setProgress(100, progressPercent, false)
            notificationManagerCompat.notifyCompat(downloadNotification.id, this)
        }
    }

    companion object {
        private const val MAX_INTERVAL_BETWEEN_PROGRESS_UPDATE_MS = 1000L
        private val TAG = DownloadOfflineFileManager::class.java.simpleName

        private const val STAGING_FOLDER = "offline_staging"

        internal enum class PublicationOutcome {
            PUBLISHED,
            SKIPPED_PROTECTED,
            SKIPPED_CONFLICT,
            SKIPPED_INVALIDATED,
            SKIPPED_SUPERSEDED,
            FAILED,

            /** Bytes published, generation UNRESOLVED (issue: round 20 #14): a both-unknown
             * coarse-baseline tie cannot prove the published generation — never a
             * completion, never a 100, never a marker clear, never a root completion.
             * Typed so every public caller distinguishes it from PUBLISHED. */
            PUBLISHED_UNRESOLVED,
        }

        /** Outcomes that must retain Bulk/root incompleteness (issues: round 17 #7, round 20 #14). */
        private val UNRESOLVED_PUBLICATION_OUTCOMES =
            setOf(
                PublicationOutcome.SKIPPED_SUPERSEDED,
                PublicationOutcome.SKIPPED_CONFLICT,
                PublicationOutcome.SKIPPED_INVALIDATED,
                PublicationOutcome.PUBLISHED_UNRESOLVED,
            )

        /** Suffix of the old SIBLING sidecar experiment — never written or deleted again (round 17 #1). */
        private const val CACHE_PROVENANCE_SUFFIX = ".offlinegen"

        /** Registry directory for published-generation provenance, OUTSIDE the user-file namespace (round 17 #1). */
        private const val PROVENANCE_FOLDER = "offline_provenance"

        private fun provenanceRoot(context: Context): IOFile =
            IOFile(File.getOfflineFolder(context).parentFile, PROVENANCE_FOLDER)

        /**
         * The provenance record of [publishedFile], stored OUTSIDE the user-file namespace
         * (issue: round 17 #1): the previous sibling `${name}.offlinegen` collided with real
         * user files (a synced `report.offlinegen` could be truncated by publishing `report`,
         * or deleted by clearing it, with no ownership or protection check). The record is
         * keyed by the SHA-256 of the published file's ABSOLUTE path — which encodes the
         * captured account, drive and exact published location — never by a suffix reserved
         * inside directories holding user content.
         */
        /**
         * Collision-resistant registry key (issue: round 24 #5): the SHA-256 of the
         * absolute path. The proofmove relocation records previously used the path's
         * 32-bit `hashCode`, letting unrelated paths share one record.
         */
        private fun provenanceKeyHex(publishedFile: IOFile): String {
            val digest = java.security.MessageDigest.getInstance("SHA-256")
                .digest(publishedFile.absolutePath.toByteArray())
            return digest.joinToString("") { "%02x".format(it) }
        }

        private fun provenanceFileFor(context: Context, publishedFile: IOFile): IOFile {
            val hex = provenanceKeyHex(publishedFile)
            return IOFile(IOFile(provenanceRoot(context), hex.substring(0, 2)), "$hex.gen")
        }

        /**
         * The registry directories are CREATED AND VERIFIED before any record write (issue:
         * round 18 #2): neither the registry root nor the two-character fan-out prefix exists
         * on a normal first use — an unchecked `writeText` at a missing directory fails
         * silently, so writes verify the prefix directory and readers/writers both treat an
         * unverifiable registry as "no provenance". Returns the record file, or null when the
         * directories cannot be established.
         */
        private fun verifiedProvenanceFileFor(context: Context, publishedFile: IOFile): IOFile? = runCatching {
            val recordFile = provenanceFileFor(context, publishedFile)
            val prefixDir = recordFile.parentFile
            if (prefixDir == null || (!prefixDir.exists() && !prefixDir.mkdirs())) return null
            recordFile
        }.getOrNull()

        /**
         * The identity recorded with published bytes (issues: round 16 #12/#13, round 17 #13,
         * round 18 #2/#12, round 20 #3, round 24 #11): the FULL owner and generation identity
         * — producing version, modification baseline and the accepted revision identity
         * (revisedAt, updatedAt), the owning ACCOUNT, drive and file row, the exact published
         * location, and the IMMUTABLE CONTENT IDENTITY (the SHA-256 of the bytes as
         * published — a metadata-only record can never authorize destructive cleanliness
         * after mutable access; a same-size same-second edit breaks the identity) — plus the
         * PUBLICATION IDENTITY: a fresh random value written by EVERY publication, so a
         * later publication or restoration of the SAME generation is distinguishable from
         * the recorded one (a request-time token comparison can never be satisfied by a
         * re-publication's coarse-equal record).
         */
        internal data class PublishedProvenance(
            val version: Int,
            val lastModifiedAt: Long,
            val revisedAt: Long,
            val updatedAt: Long,
            val userId: Int,
            val driveId: Int,
            val fileId: Int,
            val contentHash: String?,
            /** Fresh per-publication value (0 = a pre-nonce record): the request-time
             * publication/restoration token's distinguishing field (issue: round 24 #11). */
            val publicationId: Long = 0,
            val absolutePath: String,
        ) {
            /** STRICT owner identity (issue: round 20 #11): the record must name THIS row,
             * THIS drive, AND the caller's CAPTURED ACCOUNT — a null scope can never verify
             * account ownership, so it never grants a currentness or ownership claim. A
             * path-reused record inherited from another file (or another account's
             * shared-drive row matching on coarse metadata) is not provenance. */
            internal fun identifies(file: File, userDrive: UserDrive?): Boolean =
                userDrive != null && fileId == file.id && driveId == file.driveId &&
                    userId == userDrive.userId

            /** Row-and-drive identity only, WITHOUT an account claim (round 20 #11): the
             * scopeless readability pre-check — matches prove at most that the record
             * describes this row, never account ownership or currentness. */
            internal fun rowIdentifies(file: File): Boolean =
                fileId == file.id && driveId == file.driveId

            /** POSITIVELY foreign (issue: round 19 #12): the record names a DIFFERENT row,
             * drive, or — when the caller's account scope is known — a different account. An
             * unknown scope (userDrive == null) is unverifiable, not foreign. */
            internal fun positivelyForeignTo(file: File, userDrive: UserDrive?): Boolean =
                fileId != file.id || driveId != file.driveId || (userDrive != null && userId != userDrive.userId)

            /** IMMUTABLE BYTE IDENTITY (issues: round 20 #3/#24): the record's content hash
             * is present and equals the CURRENT bytes' hash — these bytes ARE the
             * generation the record published, whatever the row's accepted metadata now
             * says. A hashless (legacy) record proves nothing about the bytes. */
            internal fun certifiesBytes(bytes: IOFile): Boolean =
                contentHash != null && contentHash == UploadFile.sha256OrNull(bytes)
        }

        /**
         * EXPLICIT VERSIONED FRAMING (issue: round 25 #12): every NEW record starts with the
         * literal "v2|" marker, so the format is never inferred from the total number of
         * '|'-split components — a legacy nine-field record whose final path contains '|'
         * splits into ten-plus components and must still parse as LEGACY (its location is
         * the re-joined remainder after the known prefix fields). The body after the marker
         * is: version|lastModifiedAt|revisedAt|updatedAt|userId|driveId|fileId|contentHash|
         * publicationId|location(re-joined).
         */
        private const val PROVENANCE_FRAME = "v2|"
        private const val SIDECAR_FRAME = "s2|"
        private val KNOWN_STORAGE_KINDS = setOf("offline", "cache", "converted")

        private fun PublishedProvenance.recordText(target: IOFile): String =
            PROVENANCE_FRAME + "$version|$lastModifiedAt|$revisedAt|$updatedAt|$userId|$driveId|$fileId|${contentHash ?: ""}|$publicationId|${target.absolutePath}"

        /** A fresh per-publication identity (issue: round 24 #11): 64 random bits — two
         * publications colliding is negligible, and a re-publication of the SAME generation
         * always differs from the recorded one. */
        private fun newPublicationId(): Long = UUID.randomUUID().leastSignificantBits

        /**
         * Associates PUBLISHED bytes (cache or offline destination) with the content generation
         * that produced them and the owner that published them. The registry directories are
         * created and verified first; a failed write is reported, never silently ignored. The
         * bytes' IMMUTABLE CONTENT IDENTITY is hashed at write time (issue: round 20 #3): a
         * record without a computable byte identity is not written — it could authorize
         * nothing.
         */
        private fun writePublishedProvenance(
            context: Context,
            target: IOFile,
            file: File,
            userDrive: UserDrive,
            unresolvedRecord: Boolean = false,
        ): Boolean {
            val recordFile = verifiedProvenanceFileFor(context, target) ?: return false
            return runCatching {
                val provenance = PublishedProvenance(
                    // EXPLICITLY UNRESOLVED PROOF (issue: round 24 #19): a publication whose
                    // generation could not be established (an unrestablishable route, or the
                    // both-unknown coarse tie) records VERSION 0 — the copy stays non-current
                    // and no known-generation claim is issued from metadata equality.
                    version = if (unresolvedRecord) 0 else file.version?.number ?: 0,
                    lastModifiedAt = file.lastModifiedAt,
                    revisedAt = file.revisedAt,
                    updatedAt = file.updatedAt,
                    userId = userDrive.userId,
                    driveId = userDrive.driveId,
                    fileId = file.id,
                    contentHash = UploadFile.sha256OrNull(target),
                    publicationId = newPublicationId(),
                    absolutePath = target.absolutePath,
                )
                if (provenance.contentHash == null) return@runCatching false
                recordFile.writeText(provenance.recordText(target))
                true
            }.getOrDefault(false)
        }

        /**
         * PREPARES the staged record and reads it back BEFORE any byte is replaced (issue:
         * round 18 #2): a publication proceeds only when its provenance is established and
         * verifiable at the staged path — a missing staged record is failure, never a
         * successful movement of "nothing", on BOTH the offline and the plain-cache paths.
         */
        private fun prepareStagedProvenance(
            context: Context,
            staged: IOFile,
            file: File,
            userDrive: UserDrive,
            unresolvedRecord: Boolean = false,
        ): Boolean {
            if (!writePublishedProvenance(context, staged, file, userDrive, unresolvedRecord)) return false
            // THE DURABLE ATTEMPTED OBSERVATION (issues: round 28 #13 and its follow-up):
            // the unresolved record's version field is BY DESIGN zero (the unresolved
            // marker), which discarded the attempted generation's real version; the retry
            // decision needs the COMPLETE available remote observation — version, NULLABLE
            // size, the three timestamps, AND THE CAPTURED LOCATION (the destination the
            // attempt was published to; a relocated or path-reused record must never
            // compare another destination's observation) — persisted SEPARATELY from the
            // byte provenance. It is evidence only for "can another attempt gain
            // anything", never a byte-generation proof.
            if (unresolvedRecord) {
                val observationText = (
                    listOf(
                        "1",
                        (file.version?.number ?: 0).toString(),
                        file.size?.toString() ?: "",
                        file.revisedAt.toString(),
                        file.lastModifiedAt.toString(),
                        file.updatedAt.toString(),
                    ) + staged.absolutePath
                    ).joinToString("|")
                if (!atomicWriteText(IOFile(provenanceFileFor(context, staged).path + ".obs"), observationText)) {
                    return false
                }
            }
            return readPublishedProvenance(context, staged) != null
        }

        /**
         * Associates ALREADY-VERIFIED bytes (issue: round 19 #13) — the upload-completion
         * reconciliation hash-proved them to BE the uploaded revision — with their own
         * generation identity, written TOGETHER with the row's baseline advance. The
         * record's modification baseline is the VERIFIED BYTE baseline, never the row's
         * possibly-lagging accepted metadata, so the copy's readability coheres with its
         * own generation; the row's version and revision identity keep the record from
         * falsely proving currentness against metadata that has not accepted the new
         * revision yet. The proof records the ALREADY-VERIFIED immutable uploaded hash,
         * never a fresh sample of the mutable original (issue: round 21 #13): an external
         * editor holding write permission outside these Kotlin locks could write new bytes
         * between the caller's verification and the write — a re-sampled hash would
         * certify that edit as uploaded-clean content and advance the baseline over it.
         * The original is revalidated against the SAME identity here; a later edit fails
         * the write (the caller defers) and, should it land after even this check,
         * mismatches the written proof and stays unuploaded/protected.
         */
        internal fun writeVerifiedRevisionProvenance(
            context: Context,
            target: IOFile,
            row: File,
            userDrive: UserDrive,
            verifiedLastModifiedAt: Long,
            verifiedContentHash: String,
        ): Boolean {
            val recordFile = verifiedProvenanceFileFor(context, target) ?: return false
            return runCatching {
                if (UploadFile.sha256OrNull(target) != verifiedContentHash) return@runCatching false
                val provenance = PublishedProvenance(
                    version = row.version?.number ?: 0,
                    lastModifiedAt = verifiedLastModifiedAt,
                    revisedAt = row.revisedAt,
                    updatedAt = row.updatedAt,
                    userId = userDrive.userId,
                    driveId = userDrive.driveId,
                    fileId = row.id,
                    contentHash = verifiedContentHash,
                    publicationId = newPublicationId(),
                    absolutePath = target.absolutePath,
                )
                recordFile.writeText(provenance.recordText(target))
                readPublishedProvenance(context, target) != null
            }.getOrDefault(false)
        }

        internal fun readPublishedProvenance(context: Context, publishedFile: IOFile): PublishedProvenance? = runCatching {
            parseProvenanceText(
                provenanceFileFor(context, publishedFile).readText().trim(),
                expectedPath = publishedFile.absolutePath,
            )
        }.getOrNull()?.takeIf { it.absolutePath == publishedFile.absolutePath }

        /**
         * Parses one provenance record text (issues: round 20 #3, round 24 #11, round 25
         * #12, round 26 #15): an explicit "v2|" frame selects the framed format (the
         * publication identity at a KNOWN body index). Anything else is LEGACY, and the
         * format is NEVER inferred from the total split count — a path containing '|'
         * changes the count under every grammar. The three actual legacy grammars are the
         * eight-field hashless record, the nine-field hash record, and the unframed
         * TEN-field record with a publication id that the immediately preceding version
         * wrote; each candidate's location is the re-joined remainder, and THE EXPECTED
         * CANONICAL PATH decides between them (the caller validates the record names the
         * exact path it was read at — an ambiguous component count is resolved by that
         * identity, never by field position alone). Without an expected path the most
         * specific grammar wins; the caller's location check still guards.
         */
        private fun parseProvenanceText(text: String, expectedPath: String? = null): PublishedProvenance {
            val framed = text.startsWith(PROVENANCE_FRAME)
            val parts = text.removePrefix(PROVENANCE_FRAME).split("|")
            if (framed) {
                require(parts.size >= 10)
                return PublishedProvenance(
                    version = parts[0].trim().toInt(),
                    lastModifiedAt = parts[1].trim().toLong(),
                    revisedAt = parts[2].trim().toLong(),
                    updatedAt = parts[3].trim().toLong(),
                    userId = parts[4].trim().toInt(),
                    driveId = parts[5].trim().toInt(),
                    fileId = parts[6].trim().toInt(),
                    contentHash = parts[7].trim().takeIf { it.isNotEmpty() },
                    publicationId = parts[8].trim().toLongOrNull() ?: 0L,
                    // The location is the LAST field and re-joins any '|' inside it.
                    absolutePath = parts.drop(9).joinToString("|"),
                )
            }
            require(parts.size >= 8)
            val candidates = buildList {
                if (parts.size >= 10) {
                    // TEN-FIELD legacy grammar (the preceding version's unframed format):
                    // the publication identity is PRESERVED, never zeroed.
                    add(
                        PublishedProvenance(
                            version = parts[0].trim().toInt(),
                            lastModifiedAt = parts[1].trim().toLong(),
                            revisedAt = parts[2].trim().toLong(),
                            updatedAt = parts[3].trim().toLong(),
                            userId = parts[4].trim().toInt(),
                            driveId = parts[5].trim().toInt(),
                            fileId = parts[6].trim().toInt(),
                            contentHash = parts[7].trim().takeIf { it.isNotEmpty() },
                            publicationId = parts[8].trim().toLongOrNull() ?: 0L,
                            absolutePath = parts.drop(9).joinToString("|"),
                        )
                    )
                }
                if (parts.size >= 9) {
                    // NINE-FIELD legacy grammar: hash present, no publication id.
                    add(
                        PublishedProvenance(
                            version = parts[0].trim().toInt(),
                            lastModifiedAt = parts[1].trim().toLong(),
                            revisedAt = parts[2].trim().toLong(),
                            updatedAt = parts[3].trim().toLong(),
                            userId = parts[4].trim().toInt(),
                            driveId = parts[5].trim().toInt(),
                            fileId = parts[6].trim().toInt(),
                            contentHash = parts[7].trim().takeIf { it.isNotEmpty() },
                            publicationId = 0L,
                            absolutePath = parts.drop(8).joinToString("|"),
                        )
                    )
                }
                // EIGHT-FIELD legacy grammar: hashless, the location from field seven on.
                add(
                    PublishedProvenance(
                        version = parts[0].trim().toInt(),
                        lastModifiedAt = parts[1].trim().toLong(),
                        revisedAt = parts[2].trim().toLong(),
                        updatedAt = parts[3].trim().toLong(),
                        userId = parts[4].trim().toInt(),
                        driveId = parts[5].trim().toInt(),
                        fileId = parts[6].trim().toInt(),
                        contentHash = null,
                        publicationId = 0L,
                        absolutePath = parts.drop(7).joinToString("|"),
                    )
                )
            }
            return expectedPath?.let { expected -> candidates.firstOrNull { it.absolutePath == expected } }
                ?: candidates.first()
        }

        /**
         * A FRESH RESTORATION IDENTITY (issue: round 25 #10): proof text written back by a
         * restoration or an old-record re-establishment is re-serialized with a NEW
         * publication id — reusing the old text's id made a same-bytes restoration
         * invisible to the publication-token comparison (the ABA the token exists to
         * break). Null when the retained text is unparseable (the caller retains state).
         */
        private fun refreshedRecordText(recordText: String, target: IOFile): String? = runCatching {
            parseProvenanceText(recordText, expectedPath = target.absolutePath)
                .copy(publicationId = newPublicationId())
                .recordText(target)
        }.getOrNull()

        internal fun deletePublishedProvenance(context: Context, publishedFile: IOFile) {
            runCatching { provenanceFileFor(context, publishedFile).delete() }
        }

        /**
         * Whether a provenance record FILE exists for [publishedFile] (issue: round 25 #11):
         * lets callers distinguish an explicit ABSENCE (no record — the conservative
         * zero-token) from a READ FAILURE (a record exists but cannot be parsed — never
         * equal to the absent token, so cleanup defers).
         */
        internal fun provenanceRecordFileExists(context: Context, publishedFile: IOFile): Boolean = runCatching {
            provenanceFileFor(context, publishedFile).exists()
        }.getOrDefault(true)

        /**
         * ATOMIC RECORD WRITE (issue: round 25 #6): the text is written to a unique
         * temporary file in the same directory and RENAMED into place — an interrupted
         * write can no longer leave a torn (undecodable) record at the operational path.
         */
        private fun atomicWriteText(target: IOFile, text: String): Boolean = runCatching {
            val temp = IOFile(target.parentFile, "${target.name}.${UUID.randomUUID()}.tmp")
            temp.writeText(text)
            if (!temp.renameTo(target)) {
                runCatching { temp.delete() }
                return@runCatching false
            }
            true
        }.getOrDefault(false)

        /**
         * THE ACTUAL DESTINATION KIND of a publication target (issue: round 27 #9): the
         * same resolution [File.getStoredFile] uses — the ordinary offline path, the
         * onlyoffice CONVERTED-PDF cache, or the plain per-scope cache. A converted or
         * plain-cache target is never mislabeled "offline" (its recovery could then never
         * re-resolve its own destination).
         */
        private fun destinationStorageKind(context: Context, file: File, target: IOFile, userDrive: UserDrive): String =
            when (target.absolutePath) {
                file.getOfflineFile(context, userDrive.userId)?.absolutePath -> "offline"
                file.getConvertedPdfCache(context, userDrive).absolutePath -> "converted"
                else -> "cache"
            }

        private class RecoveredScope(
            val userId: Int,
            val driveId: Int,
            val fileId: Int,
            val targetPath: String,
            /** The destination's Realm flavor (issue: round 26 #5): shared-Realm rows never
             * resolve in the ordinary per-drive realm. Legacy sidecars predate the field. */
            val sharedWithMe: Boolean = false,
            /** The destination's storage kind (issue: round 26 #5): ordinary-cache recovery
             * targets are validated against the row's cache file, never the offline path. */
            val storageKind: String = "offline",
        )

        /**
         * INDEPENDENTLY DURABLE SCOPED INDEX (issues: round 25 #6, round 26 #5): a single-line
         * scope sidecar written ATOMICALLY before each operation record — an undecodable
         * record still names its owner scope, its destination's realm flavor and storage
         * kind, and its target path, so identities DEMONSTRABLY outside that scope keep
         * progressing instead of failing closed forever, and the sidecar is the repair key
         * for a corrupt record's disposition. Format v2:
         * `userId|driveId|fileId|shared(0|1)|kind|targetPath` (the path re-joined — it may
         * contain `|`); the pre-review-26 format was `userId|driveId|fileId|targetPath`.
         */
        private fun writeScopeSidecar(
            recordFile: IOFile,
            userId: Int,
            driveId: Int,
            fileId: Int,
            targetPath: String,
            sharedWithMe: Boolean = false,
            storageKind: String = "offline",
        ): Boolean = atomicWriteText(
            IOFile(recordFile.path + ".scope"),
            // EXPLICIT FRAMING (issue: round 27 #10): the previous unframed format was
            // ambiguous — a legacy four-field record whose PATH contains '|' has six or
            // more components and was misread as shared/kind fields. New sidecars carry
            // the "s2|" frame; the location is always the re-joined tail.
            "s2|$userId|$driveId|$fileId|${if (sharedWithMe) 1 else 0}|$storageKind|$targetPath",
        )

        /**
         * The parsed scope sidecar of [recordFile], or null when absent/undecodable
         * (issue: round 27 #10): a framed "s2|" record parses its typed fields with
         * VALIDATION (shared ∈ {0,1}, kind ∈ the known set — an invalid value is
         * undecodable, never silently re-interpreted). An UNFRAMED record is legacy: the
         * COMPLETE tail after the identity fields is the PATH (re-joined — it may contain
         * '|'), with the documented conservative defaults; a six-field unframed record
         * from the immediately preceding version is recognized ONLY when its shared/kind
         * fields carry valid values. When [expectedFileId]/[expectedTargetPath] are
         * supplied (the caller has the operation body), the sidecar's identity/path are
         * CROSS-CHECKED against them — a mismatched index is undecodable for this
         * operation (fail closed), never silently trusted.
         */
        private fun readScopeSidecar(
            recordFile: IOFile,
            expectedFileId: Int? = null,
            expectedTargetPath: String? = null,
        ): RecoveredScope? = runCatching {
            val text = IOFile(recordFile.path + ".scope").readText().trim()
            val framed = text.startsWith(SIDECAR_FRAME)
            val parts = text.removePrefix(SIDECAR_FRAME).split("|")
            val scope = if (framed) {
                require(parts.size >= 6)
                val shared = parts[3].trim()
                val kind = parts[4].trim()
                require(shared == "0" || shared == "1")
                require(kind in KNOWN_STORAGE_KINDS)
                RecoveredScope(
                    userId = parts[0].trim().toInt(),
                    driveId = parts[1].trim().toInt(),
                    fileId = parts[2].trim().toInt(),
                    sharedWithMe = shared == "1",
                    storageKind = kind,
                    targetPath = parts.drop(5).joinToString("|"),
                )
            } else if (parts.size >= 6 && parts[3].trim() in setOf("0", "1") &&
                parts[4].trim() in KNOWN_STORAGE_KINDS
            ) {
                // LEGACY SIX-FIELD (the immediately preceding version's unframed format):
                // recognized only through VALID typed values; the tail is the path.
                RecoveredScope(
                    userId = parts[0].trim().toInt(),
                    driveId = parts[1].trim().toInt(),
                    fileId = parts[2].trim().toInt(),
                    sharedWithMe = parts[3].trim() == "1",
                    storageKind = parts[4].trim(),
                    targetPath = parts.drop(5).joinToString("|"),
                )
            } else {
                // LEGACY FOUR-FIELD: the COMPLETE tail is the path — never kind-split.
                require(parts.size >= 4)
                RecoveredScope(
                    userId = parts[0].trim().toInt(),
                    driveId = parts[1].trim().toInt(),
                    fileId = parts[2].trim().toInt(),
                    targetPath = parts.drop(3).joinToString("|"),
                )
            }
            // THE CANONICAL CROSS-CHECK (issue: round 27 #10): the index must agree with
            // the operation body it scopes.
            if (expectedFileId != null && scope.fileId != expectedFileId) return@runCatching null
            if (expectedTargetPath != null && scope.targetPath != expectedTargetPath) return@runCatching null
            scope
        }.getOrNull()

        /**
         * THE DURABLE DISPOSAL (issue: round 26 #8): the scope index belongs to the
         * operation's lifecycle — a successful body disposition removes its sidecar too,
         * so uniquely named indexes do not accumulate permanently across ordinary
         * replacements and the unresolved-download retry loop.
         */
        private fun disposeRecoveryRecord(recordFile: IOFile): Boolean {
            // DIRECT BODY DELETION (issue: round 27 #1): an already-absent body counts as
            // disposed (a caller may have removed it first); an actual deletion FAILURE
            // propagates — the operation state is retained, never half-disposed. The
            // previous body line called this very function with the same argument and
            // recursed to StackOverflowError on every disposal.
            val bodyDisposed = if (!recordFile.exists()) true else runCatching { recordFile.delete() }.getOrDefault(false)
            if (!bodyDisposed) return false
            // THE SIDECAR ONLY AFTER CONFIRMED BODY DISPOSITION (issue: round 27 #1): the
            // index never outlives its operation and never disappears before the body; an
            // absent sidecar (a legacy record) is already disposed.
            val sidecar = IOFile(recordFile.path + ".scope")
            return !sidecar.exists() || runCatching { sidecar.delete() }.getOrDefault(false)
        }

        /** THE TRI-STATE SIDECAR AUTHORITY (issue: round 28 #10): an ABSENT sidecar is
         * LEGACY authority (resolvable from the canonical path + live owner); an existing
         * sidecar that is malformed or fails its body cross-check is INVALID — it never
         * substitutes another realm/kind; a decodable, cross-checked sidecar is RESOLVED. */
        private sealed class ScopedAuthority {
            object AbsentLegacy : ScopedAuthority()
            object Invalid : ScopedAuthority()
            class Resolved(val scope: RecoveredScope) : ScopedAuthority()
        }

        private fun readScopeAuthority(
            recordFile: IOFile,
            expectedFileId: Int? = null,
            expectedTargetPath: String? = null,
        ): ScopedAuthority {
            val sidecarFile = IOFile(recordFile.path + ".scope")
            if (!sidecarFile.exists()) return ScopedAuthority.AbsentLegacy
            val scope = readScopeSidecar(recordFile, expectedFileId, expectedTargetPath)
                ?: return ScopedAuthority.Invalid
            return ScopedAuthority.Resolved(scope)
        }

        /**
         * LEGACY AUTHORITY RESOLUTION (issue: round 28 #10): a record with NO scope
         * sidecar resolves its destination kind and realm flavor from its VALIDATED
         * CANONICAL target path and the LIVE EXCLUSIVE owner, across BOTH captured flavors
         * and all three destination kinds (offline/cache/converted — the previous blanket
         * ordinary-offline default gave legacy cache/shared records no convergence route).
         * Returns the resolved authority, or null when AMBIGUOUS (no resolver matches a
         * live exclusive owner in either flavor): the caller defers explicitly, never
         * substituting defaults. On success the resolved authority is PERSISTED in the
         * framed s2| form — later passes use captured, not re-derived, authority.
         */
        private fun resolveLegacyScopeAuthority(
            context: Context,
            recordFile: IOFile,
            fileId: Int,
            userId: Int,
            driveId: Int,
            targetPath: String,
        ): RecoveredScope? = runCatching {
            for (sharedWithMe in listOf(false, true)) {
                val scope = UserDrive(userId = userId, driveId = driveId, sharedWithMe = sharedWithMe)
                FileController.getRealmInstance(scope).use { realm ->
                    runCatching { realm.refresh() }
                    val row = FileController.getFileById(realm, fileId) ?: return@use
                    val resolvers = sequenceOf(
                        "offline" to row.getOfflineFile(context, userId, sharedWithMe = sharedWithMe)?.absolutePath,
                        "cache" to row.getCacheFile(context, scope).absolutePath,
                        "converted" to row.getConvertedPdfCache(context, scope).absolutePath,
                    )
                    for ((kind, resolvedPath) in resolvers) {
                        if (resolvedPath == targetPath &&
                            appContextForOwnership()?.let { ctx ->
                                OfflineFolderController.exclusivePathOwner(realm, ctx, scope, targetPath)
                            } == row.id
                        ) {
                            // PERSIST THE RESOLVED FRAMED AUTHORITY (best effort: a failed
                            // write leaves the record legacy — a later pass re-resolves).
                            runCatching {
                                writeScopeSidecar(recordFile, userId, driveId, fileId, targetPath, sharedWithMe, kind)
                            }
                            return@runCatching RecoveredScope(
                                userId = userId,
                                driveId = driveId,
                                fileId = fileId,
                                targetPath = targetPath,
                                sharedWithMe = sharedWithMe,
                                storageKind = kind,
                            )
                        }
                    }
                }
            }
            null // genuine ambiguity: the caller defers explicitly
        }.getOrNull()

        /**
         * CURRENT CANONICAL OWNERSHIP AUTHORITY (issues: round 25 #4, round 26 #5): the
         * record's owner row must still EXIST in the record's ACTUAL Realm flavor, still
         * resolve to the record's exact target path AT ITS CAPTURED STORAGE KIND, and still
         * hold the path EXCLUSIVELY — locking the identity named by an old record never
         * establishes that the owner still controls the target. A shared-Realm destination
         * is looked up in the shared realm (the ordinary per-drive realm never held the
         * row); an ordinary-cache destination is validated against the row's id-keyed cache
         * file (structurally exclusive — the name-based path-conflict query does not apply
         * to cache paths). Read fresh under the caller's coordination, for every restore
         * and proof rewrite.
         */
        private fun currentOwnerHolds(
            context: Context,
            recordScope: UserDrive,
            recordFileId: Int,
            target: IOFile,
            sharedWithMe: Boolean = false,
            storageKind: String = "offline",
        ): Boolean = runCatching {
            val realmScope = UserDrive(
                userId = recordScope.userId,
                driveId = recordScope.driveId,
                sharedWithMe = sharedWithMe,
            )
            FileController.getRealmInstance(realmScope).use { realm ->
                runCatching { realm.refresh() }
                val row = FileController.getFileById(realm, recordFileId) ?: return@runCatching false
                val ownedPath = when (storageKind) {
                    "cache" -> row.getCacheFile(context, realmScope).absolutePath
                    "converted" -> row.getConvertedPdfCache(context, realmScope).absolutePath
                    else ->
                        // THE CAPTURED REALM FLAVOR RESOLVES THE PATH (issue: round 27
                        // #9d): a shared row whose stored path is blank previously derived
                        // its remote path through an ORDINARY UserDrive — the row was
                        // invisible there and the resolution failed forever.
                        row.getOfflineFile(context, recordScope.userId, sharedWithMe = sharedWithMe)?.absolutePath
                }
                val pathOwned = ownedPath == target.absolutePath
                // ID-KEYED DESTINATIONS ARE STRUCTURALLY EXCLUSIVE (issue: round 28 #9):
                // the plain cache and the converted-PDF cache are both keyed by the row's
                // own id — the name-derived offline path-conflict query does not apply to
                // either (a converted target previously failed it forever).
                val exclusivelyOwned = storageKind != "offline" ||
                    appContextForOwnership()?.let { ctx ->
                        OfflineFolderController.exclusivePathOwner(realm, ctx, realmScope, target.path)
                    } == row.id
                pathOwned && exclusivelyOwned
            }
        }.getOrDefault(false)

        /**
         * Whether [prefixFile]'s COMPLETE bytes equal the head of [fullFile] (issue: round
         * 25 #3): the ambiguity gate for legacy in-place restore output — a readable
         * strict prefix of the immutable backup could equally be a user truncation.
         */
        /**
         * THREE-VALUED PREFIX COMPARISON (issue: round 27 #12): TRUE = a proven strict
         * prefix; FALSE = PROVEN non-prefix content (a complete, successful comparison
         * including the reliable length bound); NULL = READ/EOF UNCERTAINTY — the
         * comparison could not complete (an unreadable backup, a short read, an I/O
         * failure), which is NEVER permission to treat the target as a genuine edit: a
         * partial in-place restore's output must retain the complete backup. The previous
         * Boolean folded read failures into "not a prefix".
         */
        private fun filePrefixMatches(prefixFile: IOFile, fullFile: IOFile): Boolean? = runCatching {
            val prefixLength = prefixFile.length()
            if (prefixLength <= 0L) return@runCatching null
            if (prefixLength >= fullFile.length()) return@runCatching false
            prefixFile.inputStream().use { a ->
                fullFile.inputStream().use { b ->
                    val bufA = ByteArray(64 * 1024)
                    val bufB = ByteArray(64 * 1024)
                    var remaining = prefixLength
                    while (remaining > 0) {
                        val chunk = minOf(remaining, bufA.size.toLong()).toInt()
                        var readA = 0
                        while (readA < chunk) {
                            val r = a.read(bufA, readA, chunk - readA)
                            if (r < 0) return@runCatching null
                            readA += r
                        }
                        var readB = 0
                        while (readB < chunk) {
                            val r = b.read(bufB, readB, chunk - readB)
                            if (r < 0) return@runCatching null
                            readB += r
                        }
                        for (i in 0 until chunk) {
                            if (bufA[i] != bufB[i]) return@runCatching false
                        }
                        remaining -= chunk
                    }
                    true
                }
            }
        }.getOrNull()

        /**
         * Moves the provenance record of [source]'s published bytes onto [target], rewriting
         * the record's location identity (issue: round 18 #12). An UNPROVEN source never
         * confirms — or leaves — a target record: provenance-less bytes INVALIDATE the
         * destination's record and the move fails, instead of reporting success for moving
         * nothing. True only when a verified record now names [target].
         */
        /**
         * Durable RELOCATION RECOVERY STATE (issue: round 23 #14): the original proof text
         * and the exact destination path, written BEFORE the destination proof is
         * attempted — a failed relocation leaves the state on disk and the reconciliation
         * sweep re-attempts the destination proof (the bytes are already at the target);
         * a verified destination proof disposes it. The old proof is never destroyed
         * without recoverable relocation state.
         */
        private fun proofRelocationRecordFile(context: Context, target: IOFile): IOFile =
            IOFile(stagingDirectory(context), "proofmove_${provenanceKeyHex(target)}.recovery")

        private fun writeProofRelocationRecord(
            context: Context,
            target: IOFile,
            proofText: String,
            sharedWithMe: Boolean = false,
            storageKind: String = "offline",
        ): Boolean = runCatching {
            val recordFile = proofRelocationRecordFile(context, target)
            // ATOMIC WRITE + SCOPE SIDECAR FIRST (issues: round 25 #6, round 26 #5): the
            // scope index exists before the record, so an interrupted or later-corrupted
            // record is still scoped (identities demonstrably outside it progress), and it
            // names the destination's actual Realm flavor (the proofmove target is always
            // an offline-path publication).
            val proofFields = proofText.removePrefix(PROVENANCE_FRAME).split("|")
            val sidecarWritten = writeScopeSidecar(
                recordFile,
                userId = proofFields.getOrNull(4)?.trim()?.toIntOrNull() ?: 0,
                driveId = proofFields.getOrNull(5)?.trim()?.toIntOrNull() ?: 0,
                fileId = proofFields.getOrNull(6)?.trim()?.toIntOrNull() ?: 0,
                targetPath = target.absolutePath,
                sharedWithMe = sharedWithMe,
                storageKind = storageKind,
            )
            if (!sidecarWritten) return@runCatching false
            atomicWriteText(recordFile, listOf("1", target.absolutePath, proofText).joinToString("\n"))
        }.getOrDefault(false)

        internal fun movePublishedProvenance(
            context: Context,
            source: IOFile,
            target: IOFile,
            sharedWithMe: Boolean = false,
            storageKind: String = "offline",
        ): Boolean {
            val provenance = readPublishedProvenance(context, source)
            if (provenance == null) {
                deletePublishedProvenance(context, target)
                return false
            }
            // THE RELOCATION STATE EXISTS BEFORE THE DESTINATION WRITE IS ATTEMPTED
            // (issue: round 23 #14): every failure path below leaves a durable record of
            // the original proof and its exact destination; only a VERIFIED destination
            // proof disposes it.
            val relocationStateWritten = writeProofRelocationRecord(
                context,
                target,
                provenance.recordText(target),
                sharedWithMe = sharedWithMe,
                storageKind = storageKind,
            )
            if (!relocationStateWritten) {
                // Without recoverable state the source proof is NOT destroyed: the old
                // record stays at the source path (the caller's bytes have not moved yet
                // in every current caller that reaches this branch — the destination
                // registry could not even be established).
                return false
            }
            val recordFile = verifiedProvenanceFileFor(context, target) ?: run {
                return false
            }
            val written = runCatching {
                recordFile.writeText(provenance.recordText(target))
                true
            }.getOrDefault(false)
            if (!written) {
                // A partial write must never lend a truncated generation (issue: round 22
                // #12). The relocation state RETAINS the original proof for the sweep; the
                // source record is invalidated only when the relocation state exists (the
                // sweep re-writes the destination proof — a record at the vacated source
                // path must never survive to lend its generation).
                disposeRecoveryRecord(recordFile)
                deletePublishedProvenance(context, source)
                return false
            }
            // VERIFIED DESTINATION PROOF (issue: round 23 #14): the written record must
            // read back before any state is disposed.
            if (readPublishedProvenance(context, target) == null) {
                disposeRecoveryRecord(recordFile)
                deletePublishedProvenance(context, source)
                return false
            }
            // THE ATTEMPTED OBSERVATION TRAVELS WITH THE RECORD (issue: round 28 #13):
            // the durable remote-observation sidecar is re-keyed to the destination (an
            // unresolved relocation keeps its retry evidence); the source's copy dies with
            // the source record.
            runCatching {
                val sourceObs = IOFile(provenanceFileFor(context, source).path + ".obs")
                val targetObs = IOFile(provenanceFileFor(context, target).path + ".obs")
                if (sourceObs.exists()) {
                    targetObs.writeText(sourceObs.readText())
                    runCatching { sourceObs.delete() }
                }
            }
            deletePublishedProvenance(context, source)
            runCatching { proofRelocationRecordFile(context, target).delete() }
            return true
        }

        /**
         * The EXACT cache publication token captured at staging or edit initiation (issue:
         * round 22 #21): the record's content hash and version plus the bytes' own hash. A
         * later writer can replace the cache and create a NEW self-certifying record under
         * the SAME captured row metadata — cleanup that validates current self-certification
         * alone would then delete that later publication.
         */
        data class CachePublicationToken(val contentHash: String?, val version: Int, val bytesHash: String?)

        internal fun capturedCachePublicationToken(context: Context, cacheFile: IOFile): CachePublicationToken? =
            if (cacheFile.exists()) {
                val record = readPublishedProvenance(context, cacheFile)
                CachePublicationToken(
                    contentHash = record?.contentHash,
                    version = record?.version ?: 0,
                    bytesHash = UploadFile.sha256OrNull(cacheFile),
                )
            } else null

        /** The deletion gate (issue: round 22 #21): only THAT exact token and bytes still. */
        internal fun capturedCacheTokenStillMatches(
            context: Context,
            cacheFile: IOFile,
            captured: CachePublicationToken?,
        ): Boolean = captured != null && cacheFile.exists() &&
            capturedCachePublicationToken(context, cacheFile) == captured

        /**
         * Whether [cacheFile]'s bytes are PROVEN to be the current content generation of [file]
         * (issues: round 15 #9, round 16 #11): exact size, exact modification baseline, and the
         * generation provenance record matching the row's authoritative version and baseline —
         * AND naming THIS row (and the caller's account, when known) as the bytes' owner
         * (issue: round 18 #12). EVERY cache promotion — worker and UI alike — requires this;
         * missing, mismatched or foreign provenance must enqueue a fresh download instead of
         * publishing unknown-generation bytes under current metadata.
         */
        internal fun cacheBytesProvenCurrent(
            context: Context,
            cacheFile: IOFile,
            file: File,
            userDrive: UserDrive? = null,
        ): Boolean {
            if (!(cacheFile.exists() && file.isIntactFile(cacheFile) && (cacheFile.lastModified() / 1_000L) == file.lastModifiedAt)) {
                return false
            }
            val provenance = readPublishedProvenance(context, cacheFile)
            val transferVersion = file.version?.number ?: 0
            return transferVersion > 0 && provenance != null && provenance.identifies(file, userDrive) &&
                // IMMUTABLE BYTE IDENTITY (issue: round 20 #3): the plain-cache validation
                // inherits the same same-second-edit weakness the offline path had — the
                // seconds-resolution mtime and size comparison alone cannot tell a
                // same-size edit from the published generation. The record's content
                // identity must match the current bytes.
                provenance.certifiesBytes(cacheFile) &&
                provenance.version == transferVersion && provenance.lastModifiedAt == file.lastModifiedAt
        }

        /**
         * App-owned staging directory on the SAME volume as the offline storage (so publication
         * is an atomic rename), but OUTSIDE it: remote file paths can never resolve here, so no
         * user content can be staged-over or mistaken for residue (issue: round 3 #2).
         */
        private fun stagingDirectory(context: Context): IOFile =
            IOFile(File.getOfflineFolder(context).parentFile, STAGING_FOLDER).apply { if (!exists()) mkdirs() }

        /**
         * A unique staged file per transfer: identical downloads by concurrent workers (e.g. an
         * ancestor and a nested root) never collide (issue: round 3 #2).
         */
        internal fun newStagedFile(context: Context, file: File): IOFile =
            IOFile(stagingDirectory(context), "${file.id}_${file.driveId}_${UUID.randomUUID()}.part")

        private fun recoveryRecordFile(context: Context, recoveryFile: IOFile): IOFile =
            IOFile(stagingDirectory(context), "${recoveryFile.name}.recovery")

        /**
         * DURABLE ROLLBACK ASSOCIATION (issues: round 21 #9/#10, round 22 #2): every
         * recovery backup is written together with a sidecar record naming its target, the
         * retained OLD proof, the target's pre-publication mtime, its OWNER scope
         * (userId/driveId) and file identity, and the backup's IMMUTABLE content hash — an
         * interrupted publication leaves a self-describing, scope-addressable recovery
         * state that the reconciliation can coordinate under the OWNER's locks and
         * validate against the exact backed-up generation. A backup without a computable
         * hash is never kept (it could not be verified against anything).
         */
        private fun writeRecoveryRecord(
            context: Context,
            recoveryFile: IOFile,
            target: IOFile,
            oldRecordText: String?,
            oldTargetMtimeMillis: Long,
            file: File,
            userDrive: UserDrive,
            backupHash: String,
            expectedPublicationHash: String?,
            expectedPublicationRecordText: String?,
        ): Boolean = runCatching {
            // RECORD v3 (issue: round 23 #2): the operation's EXPECTED TARGET PUBLICATION
            // (the staged generation's content hash and the proof the target will carry)
            // rides the record — the reconciliation can then distinguish INTERRUPTED
            // PUBLICATION OUTPUT (the target already holds the expected bytes: complete
            // the publication with its captured proof) from a PROVEN INTERVENING EDIT,
            // and never auto-uploads arbitrary changed targets.
            val recordFile = recoveryRecordFile(context, recoveryFile)
            // ATOMIC WRITE + SCOPE SIDECAR FIRST (issues: round 25 #6, round 26 #5): the
            // scope index exists before the record itself, and names the destination's
            // ACTUAL realm flavor and storage kind — an interrupted cache or shared-Realm
            // publication is later validated against its captured destination, never
            // against a universally resolved ordinary offline path.
            val capturedStorageKind = destinationStorageKind(context, file, target, userDrive)
            if (!writeScopeSidecar(
                    recordFile,
                    userDrive.userId,
                    userDrive.driveId,
                    file.id,
                    target.absolutePath,
                    sharedWithMe = userDrive.sharedWithMe,
                    storageKind = capturedStorageKind,
                )
            ) {
                return@runCatching false
            }
            atomicWriteText(
                recordFile,
                listOf(
                    "3",
                    target.absolutePath,
                    recoveryFile.absolutePath,
                    oldTargetMtimeMillis.toString(),
                    oldRecordText ?: "",
                    file.id.toString(),
                    userDrive.userId.toString(),
                    userDrive.driveId.toString(),
                    backupHash,
                    expectedPublicationHash ?: "",
                    expectedPublicationRecordText ?: "",
                ).joinToString("\n")
            )
        }.getOrDefault(false)

        private class RecoveryRecord(
            val recordFile: IOFile,
            val targetPath: String,
            val recoveryPath: String,
            val oldTargetMtimeMillis: Long,
            val oldRecordText: String?,
            val fileId: Int,
            val userId: Int,
            val driveId: Int,
            val backupHash: String?,
            val expectedPublicationHash: String?,
            val expectedPublicationRecordText: String?,
            val legacy: Boolean,
            /** The owner scope was recovered from validated durable state (issue: round 24
             * #3) — a record whose scope could not be established is never locked, queried
             * against a fabricated account, or mutated; it is retained as unresolved. */
            val scopeResolved: Boolean = true,
        )

        /**
         * The (userId, driveId) embedded in a canonical offline path
         * `<offlineRoot>/<userId>/<driveId>/…` (issue: round 24 #3): the durable owner/path
         * index that exists independently of record-payload parsing. Legacy v1 recovery
         * records carry no owner identity, but their TARGET PATH does. Null when the path
         * is not a canonical offline path under any account.
         */
        private fun legacyScopeFromPath(targetPath: String): Pair<Int, Int>? = runCatching {
            val context = Realm.getApplicationContext() ?: return@runCatching null
            val rootPath = File.getOfflineFolder(context).canonicalPath
            val canonical = IOFile(targetPath).canonicalPath
            if (!canonical.startsWith(rootPath + IOFile.separator)) return@runCatching null
            val segments = canonical.removePrefix(rootPath + IOFile.separator).split(IOFile.separator)
            val userId = segments.getOrNull(0)?.trim()?.toIntOrNull() ?: return@runCatching null
            val driveId = segments.getOrNull(1)?.trim()?.toIntOrNull() ?: return@runCatching null
            if (userId <= 0 || driveId <= 0) return@runCatching null
            userId to driveId
        }.getOrNull()

        /**
         * Versioned recovery-record parsing (issue: round 23 #3): v3 (11 fields, the
         * expected target publication), v2 (9 fields, round 22) and — CONSERVATIVELY — the
         * legacy five-field v1. A v1 record carried no owner identity and no backup hash:
         * the identity is derived from the backup file's own name (newStagedFile embeds
         * id_driveId) and the immutable backup hash is computed from the backup itself at
         * reconciliation. THE OWNER SCOPE is recovered from the record's VALIDATED CANONICAL
         * TARGET PATH (issue: round 24 #3) — never a fabricated account 0: a v1 record
         * whose path does not establish a scope consistent with the staged name stays
         * unparseable (unresolved, never deleted, never locked, never auto-registered).
         */
        private fun readRecoveryRecord(recordFile: IOFile): RecoveryRecord? = runCatching {
            val text = recordFile.readText()
            val v3 = text.split("\n", limit = 11)
            if (v3.size == 11 && v3[0].trim() == "3") {
                return@runCatching RecoveryRecord(
                    recordFile = recordFile,
                    targetPath = v3[1],
                    recoveryPath = v3[2],
                    oldTargetMtimeMillis = v3[3].trim().toLongOrNull() ?: return null,
                    oldRecordText = v3[4].takeIf { it.isNotEmpty() },
                    fileId = v3[5].trim().toIntOrNull() ?: return null,
                    userId = v3[6].trim().toIntOrNull() ?: return null,
                    driveId = v3[7].trim().toIntOrNull() ?: return null,
                    backupHash = v3[8].trim().takeIf { it.isNotEmpty() },
                    expectedPublicationHash = v3[9].trim().takeIf { it.isNotEmpty() },
                    expectedPublicationRecordText = v3[10].takeIf { it.isNotEmpty() },
                    legacy = false,
                )
            }
            val v2 = text.split("\n", limit = 9)
            if (v2.size == 9 && v2[0].trim() == "2") {
                return@runCatching RecoveryRecord(
                    recordFile = recordFile,
                    targetPath = v2[1],
                    recoveryPath = v2[2],
                    oldTargetMtimeMillis = v2[3].trim().toLongOrNull() ?: return null,
                    oldRecordText = v2[4].takeIf { it.isNotEmpty() },
                    fileId = v2[5].trim().toIntOrNull() ?: return null,
                    userId = v2[6].trim().toIntOrNull() ?: return null,
                    driveId = v2[7].trim().toIntOrNull() ?: return null,
                    backupHash = v2[8].trim().takeIf { it.isNotEmpty() },
                    expectedPublicationHash = null,
                    expectedPublicationRecordText = null,
                    legacy = false,
                )
            }
            val v1 = text.split("\n", limit = 5)
            if (v1.size == 5 && v1[0].trim() == "1") {
                val recoveryName = IOFile(v1[2]).name
                val nameParts = recoveryName.split("_")
                val derivedFileId = nameParts.getOrNull(0)?.trim()?.toIntOrNull() ?: return null
                val derivedDriveId = nameParts.getOrNull(1)?.trim()?.toIntOrNull() ?: return null
                // SCOPE FROM THE VALIDATED CANONICAL LOCATION (issue: round 24 #3): the
                // path's embedded account is cross-checked against the staged name's drive
                // id — a contradiction (or an unparseable path) leaves the record WITHOUT
                // an owner scope: it stays retained and unresolved, never reconciled under
                // account 0 and never mutated.
                val pathScope = legacyScopeFromPath(v1[1])
                val scopeResolved = pathScope != null && pathScope.second == derivedDriveId
                return@runCatching RecoveryRecord(
                    recordFile = recordFile,
                    targetPath = v1[1],
                    recoveryPath = v1[2],
                    oldTargetMtimeMillis = v1[3].trim().toLongOrNull() ?: return null,
                    oldRecordText = v1[4].takeIf { it.isNotEmpty() },
                    fileId = derivedFileId,
                    userId = pathScope?.first ?: 0,
                    driveId = pathScope?.second ?: derivedDriveId,
                    backupHash = null,
                    expectedPublicationHash = null,
                    expectedPublicationRecordText = null,
                    legacy = true,
                    scopeResolved = scopeResolved,
                )
            }
            null
        }.getOrNull()

        /**
         * The file ids with a PENDING durable recovery in [userDrive]'s scope (issues:
         * round 22 #3, round 23 #3): a per-file durable EXCLUSION from edit
         * classification, registration and replacement — partial bytes from an interrupted
         * publication must never be classified as an edit, snapshotted, or replaced while
         * the recovery is unresolved. Unrelated files progress independently. A LEGACY v1
         * record (no owner identity) matches by its name-derived (fileId, driveId) pair
         * under any userId. Returns NULL when the recovery state CANNOT be enumerated (an
         * I/O failure): the caller must treat null as "exclude conservatively" — an
         * enumeration failure never means "no pending recovery".
         */
        internal fun pendingRecoveryFileIds(context: Context, userDrive: UserDrive): Set<Int>? {
            val files = runCatching {
                stagingDirectory(context)
                    .listFiles { f -> f.name.endsWith(".recovery") && !f.name.startsWith("proofmove_") }
            }.getOrNull()
            // ENUMERATION FAILURE IS UNRESOLVED, NEVER ABSENCE (issue: round 24 #3): the
            // previous mapNotNull chain also made a null listing and every UNREADABLE
            // record disappear — both now fail the whole per-scope enumeration closed
            // (the caller defers the pass; the sweep owns the unresolved state).
            if (files == null) return null
            val ids = mutableSetOf<Int>()
            for (recordFile in files) {
                val record = readRecoveryRecord(recordFile)
                if (record == null) {
                    // UNDECODABLE RECORD, INDEPENDENTLY SCOPED (issue: round 25 #6): the
                    // scope sidecar decides — a corrupt record demonstrably outside THIS
                    // scope no longer blocks this enumeration (the previous decode
                    // failure failed the whole pass closed, blocking unrelated files and
                    // accounts forever); inside the scope (or an unsidecarred record)
                    // still fails closed.
                    val scope = readScopeSidecar(recordFile) ?: return null
                    if (scope.driveId == userDrive.driveId && scope.userId == userDrive.userId) {
                        ids.add(scope.fileId)
                    }
                    continue
                }
                when {
                    // THE RECOVERED LEGACY SCOPE IS HONORED (issue: round 26 #7): a record
                    // whose scope was RESOLVED — including a legacy v1 record whose account
                    // was conclusively recovered from its validated canonical path — is
                    // matched by its recovered user id, so two signed-in accounts on the
                    // same drive/file ids no longer share exclusions. Only an
                    // UNATTRIBUTABLE record (scope unresolved) stays account-independent.
                    record.scopeResolved && record.userId == userDrive.userId &&
                        record.driveId == userDrive.driveId -> ids.add(record.fileId)
                    !record.scopeResolved && record.driveId == userDrive.driveId -> ids.add(record.fileId)
                    else -> Unit
                }
            }
            return ids.filter { it > 0 }.toSet()
        }

        /**
         * Whether an UNRESOLVED durable recovery still governs [fileId] or the bytes at
         * [targetPath] (issue: round 23 #3): re-read INSIDE the final mutation locks — the
         * batch-captured exclusion set predates the HTTP/lock window, and a recovery
         * created during it must still exclude the file. Matches by owner identity OR by
         * the recorded target path (covering legacy records whose identity could not be
         * derived). An enumeration/read failure fails CLOSED (true): unresolved recovery
         * state is never silently absent.
         */
        internal fun hasUnresolvedRecovery(
            context: Context,
            userDrive: UserDrive,
            fileId: Int,
            targetPath: String?,
        ): Boolean {
            val files = runCatching {
                stagingDirectory(context).listFiles { f -> f.name.endsWith(".recovery") }
            }.getOrNull()
            // ENUMERATION FAILURE IS UNRESOLVED, NEVER ABSENCE (issue: round 24 #3): the
            // previous `?.any{} == true` turned a null listing into a fail-OPEN answer
            // (getOrDefault(true) only guarded thrown exceptions, not null).
            if (files == null) return true
            for (recordFile in files) {
                if (recordFile.name.startsWith("proofmove_")) {
                    // PENDING RELOCATION STATE GOVERNS ITS TARGET PATH (issue: round 24
                    // #5): an unresolved proofmove record means the destination's proof is
                    // mid-relocation. A DAMAGED BODY IS INDEPENDENTLY SCOPED (issue: round
                    // 26 #6): the scope index decides exactly like an ordinary corrupt
                    // record — identities/paths demonstrably outside it progress; inside
                    // it (or without a sidecar) fails closed. The previous blanket
                    // fail-closed blocked unrelated files and accounts forever.
                    val parts = runCatching { recordFile.readText().split("\n", limit = 3) }.getOrNull()
                    if (parts == null || parts.size < 3 || parts[0].trim() != "1") {
                        val scope = readScopeSidecar(recordFile) ?: return true
                        val scopeIdentity = scope.fileId == fileId &&
                            scope.driveId == userDrive.driveId && scope.userId == userDrive.userId
                        val scopePath = targetPath != null && IOFile(scope.targetPath).absolutePath == targetPath
                        if (scopeIdentity || scopePath) return true
                        continue
                    }
                    if (targetPath != null && IOFile(parts[1]).absolutePath == targetPath) return true
                    continue
                }
                val record = readRecoveryRecord(recordFile)
                if (record == null) {
                    // DECODING FAILURE, INDEPENDENTLY SCOPED (issues: round 24 #3, round 25
                    // #6): an unreadable record still blocks the identities its scope
                    // sidecar names (or fails closed without one), but identities
                    // DEMONSTRABLY outside its scope progress — the previous blanket
                    // fail-closed blocked unrelated files and accounts forever.
                    val scope = readScopeSidecar(recordFile) ?: return true
                    val scopeIdentity = scope.fileId == fileId &&
                        scope.driveId == userDrive.driveId && scope.userId == userDrive.userId
                    val scopePath = targetPath != null && IOFile(scope.targetPath).absolutePath == targetPath
                    if (scopeIdentity || scopePath) return true
                    continue
                }
                // THE RECOVERED LEGACY SCOPE IS HONORED (issue: round 26 #7): a resolved
                // scope — including a conclusively recovered legacy v1 account — is compared
                // by user id; only an UNATTRIBUTABLE record stays account-independent.
                val identityMatches = record.fileId == fileId &&
                    record.driveId == userDrive.driveId &&
                    (if (record.scopeResolved) record.userId == userDrive.userId else true)
                if (identityMatches || (targetPath != null && IOFile(record.targetPath).absolutePath == targetPath)) {
                    return true
                }
            }
            return false
        }

        /**
         * Reconciles interrupted publications left by crashes or failed restores (issues:
         * round 21 #9/#10, round 22 #2/#3): each record is reconciled under its OWNER
         * scope's intent → file-lock protocol (the previous sweep ran globally without any
         * captured scope, coordination or ownership validation, so it could overwrite a
         * later edit or another account's current file). For every record —
         *  - a backup whose target carries a readable, SELF-CERTIFYING proof is a completed
         *    publication (or an already-restored target): the recovery state is disposed;
         *  - a target still at the BACKED-UP generation (its immutable hash and mtime match
         *    the record) was never mutated: the retained proof is (re-)established and the
         *    recovery disposed — no byte copy is needed;
         *  - a CHANGED target is a NEWER REVISION (a later edit or another identity's
         *    content) and is NEVER overwritten: the intervening revision is explicitly
         *    PRESERVED (snapshotted as the row's edit — the same universal preservation
         *    path); only a verified preservation disposes the recovery, a failed one
         *    retains the record and backup as unresolved durable state;
         *  - a partial/unreadable target whose bytes match neither the backup nor a
         *    self-certifying publication keeps the recovery pending for a verified
         *    restore: the restore is (re-)attempted and only a VERIFIED restore (bytes
         *    hashed against the backup's own content, mtime restored, old proof written
         *    back and read) disposes the state.
         * Returns true only when every record is resolved; the caller threads the
         * per-file pending set as the durable exclusion (round 22 #3).
         */
        internal fun reconcileInterruptedRecoveries(context: Context): Boolean {
            val records = runCatching {
                stagingDirectory(context).listFiles { f -> f.name.endsWith(".recovery") }?.toList() ?: emptyList()
            }.getOrDefault(emptyList())
            var allResolved = true
            for (recordFile in records) {
                // PROOF-RELOCATION RECOVERY (issues: round 23 #14, round 24 #5): a proofmove
                // record holds the original proof and its exact destination. The retained
                // proof's OWN identity (owner scope + expected content hash, parsed from the
                // proof text) coordinates the re-attempt under the OWNER's locks, and the
                // target must still hold THE PROOF'S OWN BYTES before the proof may be
                // (re-)written onto them — a failed publication whose rollback already
                // restored the previous generation is SUPERSEDED: its proof is disposed,
                // never written over the restored bytes.
                if (recordFile.name.startsWith("proofmove_")) {
                    val proofMoveResolved = runCatching {
                        val parts = recordFile.readText().split("\n", limit = 3)
                        if (parts.size != 3 || parts[0].trim() != "1") return@runCatching false
                        val target = IOFile(parts[1])
                        val proofText = parts[2]
                        // FRAMING-AWARE FIELD PARSING (issue: round 25 #12): the owner and
                        // content-hash fields sit at the same BODY indices in both the framed
                        // and the legacy format once the "v2|" marker is stripped — the
                        // location is the re-joined tail in both and is never parsed here.
                        val proofFields = proofText.removePrefix(PROVENANCE_FRAME).split("|")
                        val proofUserId = proofFields.getOrNull(4)?.trim()?.toIntOrNull()
                        val proofDriveId = proofFields.getOrNull(5)?.trim()?.toIntOrNull()
                        val proofFileId = proofFields.getOrNull(6)?.trim()?.toIntOrNull()
                        val proofContentHash = proofFields.getOrNull(7)?.trim()?.takeIf { it.isNotEmpty() }
                        when {
                            proofUserId == null || proofDriveId == null || proofFileId == null ||
                                proofContentHash == null ->
                                // An unattributable or unverifiable retained proof is never
                                // written onto unknown bytes: the state stays for a later
                                // sweep (issue: round 24 #5).
                                false
                            !target.exists() ->
                                // The destination bytes are gone — the relocation has no
                                // subject (a rollback or a later writer removed them). The
                                // retained proof text cannot recreate bytes: the superseded
                                // relocation state is disposed unverified.
                                disposeRecoveryRecord(recordFile)
                            else -> {
                                val scope = UserDrive(userId = proofUserId, driveId = proofDriveId)
                                // THE CAPTURED REALM FLAVOR (issue: round 26 #5): the sidecar
                                // names the destination's realm — a shared-scope relocation
                                // validates its owner in the shared realm.
                                // THE TRI-STATE SIDECAR AUTHORITY (issue: round 28 #10):
                                // an invalid CURRENT index defers the replay (no substituted
                                // realm/kind); an ABSENT (legacy) index resolves from the
                                // canonical path + live exclusive owner and persists the
                                // framed authority; ambiguity defers.
                                val moveScopeShared: Boolean
                                val moveScopeKind: String
                                when (val moveAuthority = readScopeAuthority(
                                    recordFile,
                                    expectedFileId = proofFileId,
                                    expectedTargetPath = target.absolutePath,
                                )) {
                                    is ScopedAuthority.Invalid -> return@runCatching false
                                    is ScopedAuthority.AbsentLegacy -> {
                                        val resolved = resolveLegacyScopeAuthority(
                                            context,
                                            recordFile,
                                            proofFileId,
                                            proofUserId,
                                            proofDriveId,
                                            target.absolutePath,
                                        ) ?: return@runCatching false
                                        moveScopeShared = resolved.sharedWithMe
                                        moveScopeKind = resolved.storageKind
                                    }
                                    is ScopedAuthority.Resolved -> {
                                        moveScopeShared = moveAuthority.scope.sharedWithMe
                                        moveScopeKind = moveAuthority.scope.storageKind
                                    }
                                }
                                val resolved = synchronized(OfflineFileCoordination.rootIntentMonitor) {
                                    OfflineFileCoordination.withFileLock(scope, proofFileId) {
                                        val targetHash = UploadFile.sha256OrNull(target)
                                        when {
                                            // A FAILED TARGET HASH IS UNRESOLVED (issue: round 26
                                            // #4): the retained proof is never rewritten onto
                                            // bytes whose identity could not be read, and the
                                            // operation record is never disposed over them — the
                                            // previous null-hash fallthrough entered the write
                                            // branch WITHOUT any ownership check.
                                            targetHash == null -> false
                                            // HASH EQUALITY IS CONTENT EVIDENCE, NOT OWNERSHIP
                                            // AUTHORITY (issue: round 25 #4): the recorded
                                            // identity must STILL exclusively own the target's
                                            // canonical path before the proof may be rewritten.
                                            targetHash != proofContentHash -> {
                                                // SUPERSEDED BY A VERIFIED ROLLBACK OR LATER
                                                // PUBLICATION (issue: round 25 #5): a crash
                                                // between the rollback's record disposal and the
                                                // relocation disposition left a restored target
                                                // plus this retained failed-operation proof. When
                                                // the target now carries a VERIFIED proof for the
                                                // record's own identity, the relocation state is
                                                // retired (the previous mismatch arm returned false
                                                // forever, permanently blocking the path); otherwise
                                                // it stays unresolved.
                                                val currentProof = readPublishedProvenance(context, target)
                                                val supersededByVerified = currentProof != null &&
                                                    currentProof.certifiesBytes(target) &&
                                                    currentProof.identifies(
                                                        File(id = proofFileId, driveId = proofDriveId),
                                                        scope,
                                                    ) &&
                                                    currentOwnerHolds(
                                                        context, scope, proofFileId, target,
                                                        sharedWithMe = moveScopeShared,
                                                        storageKind = moveScopeKind,
                                                    )
                                                if (supersededByVerified) {
                                                    disposeRecoveryRecord(recordFile)
                                                } else false
                                            }
                                            !currentOwnerHolds(
                                                context, scope, proofFileId, target,
                                                sharedWithMe = moveScopeShared,
                                                storageKind = moveScopeKind,
                                            ) ->
                                                // The path's ownership moved since the record was
                                                // written: the retained proof is never written over
                                                // another owner's state (issue: round 25 #4).
                                                false
                                            else -> runCatching {
                                                // THE EXPECTED PUBLICATION IDENTITY (issue: round
                                                // 26 #4): a target that already carries a
                                                // self-certifying proof with a DIFFERENT
                                                // publication id is owned by a NEWER publication or
                                                // restoration — replaying the retained proof would
                                                // replace that fresh identity with the older
                                                // operation's. The superseded operation is disposed
                                                // WITHOUT replaying its proof; an equal identity (or
                                                // an absent target proof) is the recovery of the
                                                // operation's own state and may be written.
                                                val currentProof = readPublishedProvenance(context, target)
                                                val retainedPublicationId =
                                                    proofFields.getOrNull(8)?.trim()?.toLongOrNull() ?: 0L
                                                if (currentProof != null && currentProof.certifiesBytes(target) &&
                                                    currentProof.publicationId != retainedPublicationId
                                                ) {
                                                    return@runCatching disposeRecoveryRecord(recordFile)
                                                }
                                                val recordTarget = verifiedProvenanceFileFor(context, target)
                                                val written = recordTarget != null && runCatching {
                                                    atomicWriteText(recordTarget, proofText)
                                                }.getOrDefault(false) &&
                                                    readPublishedProvenance(context, target) != null
                                                if (written) disposeRecoveryRecord(recordFile)
                                                written
                                            }.getOrDefault(false)
                                        }
                                    }
                                }
                                resolved
                            }
                        }
                    }.getOrDefault(false)
                    if (!proofMoveResolved) allResolved = false
                    continue
                }
                val record = readRecoveryRecord(recordFile)
                if (record == null) {
                    // An unparseable (or legacy-format) record is never deleted: it may
                    // still guard a needed backup, and dropping it could lose the only
                    // copy of a generation.
                    allResolved = false
                    continue
                }
                // NO OWNER SCOPE, NO MUTATION (issue: round 24 #3): a legacy record whose
                // scope could not be recovered from its validated canonical path is never
                // locked against a fabricated account 0 and never reconciled — the record
                // and its backup stay retained, unresolved durable state.
                if (!record.scopeResolved) {
                    allResolved = false
                    continue
                }
                val recoveryFile = IOFile(record.recoveryPath)
                val target = IOFile(record.targetPath)
                val recordScope = UserDrive(userId = record.userId, driveId = record.driveId)
                val resolved = synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    OfflineFileCoordination.withFileLock(recordScope, record.fileId) {
                        runCatching { reconcileOneRecovery(context, record, recordScope, recoveryFile, target) }.getOrDefault(false)
                    }
                }
                if (!resolved) allResolved = false
            }
            // ORPHAN SIDECAR RECONCILIATION (issues: round 26 #8, round 27 #11): a scope
            // index whose record body no longer exists has nothing left to scope — the
            // operation was disposed (or its record never completed its interrupted
            // write). The classification is SERIALIZED WITH OPERATION CREATION/DISPOSAL
            // under the intent monitor — the writers publish the sidecar BEFORE the body
            // (a corrupt body must stay scoped), so an unsynchronized sweep could delete
            // the index of an operation being created right now, losing the captured
            // destination flavor/kind for a later crash. Under the monitor, every
            // coordinated writer's (sidecar, body) pair is atomic relative to this
            // classification; the body-absence recheck runs inside it too. A pending
            // operation's index (its body still present) is never touched; an undeletable
            // orphan is harmless (it is never enumerated) and does not fail the pass.
            runCatching {
                synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    stagingDirectory(context).listFiles { f -> f.name.endsWith(".scope") }?.forEach { sidecar ->
                        if (!IOFile(sidecar.path.removeSuffix(".scope")).exists()) {
                            runCatching { sidecar.delete() }
                        }
                    }
                }
            }
            return allResolved
        }

        private fun reconcileOneRecovery(
            context: Context,
            record: RecoveryRecord,
            recordScope: UserDrive,
            recoveryFile: IOFile,
            target: IOFile,
        ): Boolean {
            // THE CAPTURED REALM FLAVOR AND DESTINATION KIND (issue: round 26 #5), for
            // every ownership validation in this arm set: the scope sidecar written before
            // the record carries the destination's ACTUAL realm flavor and storage kind —
            // a cache or shared-Realm destination is validated against its captured
            // identity, never against a universally resolved ordinary offline path. A
            // legacy record without a sidecar keeps the documented conservative defaults
            // (the same ordinary-offline semantics its scope resolution already used);
            // nothing here falls back to the current account or invents a scope.
            // THE TRI-STATE SIDECAR AUTHORITY (issue: round 28 #10): an INVALID CURRENT
            // index (malformed, or failing its body cross-check) defers this record with
            // NO substituted realm/kind — wrong authority is never compiled from a broken
            // index. An ABSENT (legacy) index resolves its kind/flavor from the validated
            // canonical path + the LIVE EXCLUSIVE owner across captured flavors and all
            // three destination kinds, and PERSISTS the resolved framed authority; genuine
            // ambiguity defers explicitly. The identity fields always come from the record
            // itself, never the sidecar.
            val capturedShared: Boolean
            val capturedStorageKind: String
            when (val authority = readScopeAuthority(
                record.recordFile,
                expectedFileId = record.fileId,
                expectedTargetPath = record.targetPath,
            )) {
                is ScopedAuthority.Invalid -> return false
                is ScopedAuthority.AbsentLegacy -> {
                    val resolved = resolveLegacyScopeAuthority(
                        context,
                        record.recordFile,
                        record.fileId,
                        record.userId,
                        record.driveId,
                        record.targetPath,
                    ) ?: return false
                    capturedShared = resolved.sharedWithMe
                    capturedStorageKind = resolved.storageKind
                }
                is ScopedAuthority.Resolved -> {
                    capturedShared = authority.scope.sharedWithMe
                    capturedStorageKind = authority.scope.storageKind
                }
            }
            if (!recoveryFile.exists()) {
                // The backup is gone: a completed publication (or completed restore)
                // leaves the target with its own readable proof; anything else is
                // unresolved durable state.
                if (readPublishedProvenance(context, target) == null) return false
                disposeRecoveryRecord(record.recordFile)
                return true
            }
            // SELF-CERTIFYING DISPOSAL REQUIRES THE RECORDED OWNER (issue: round 23 #2):
            // a target whose certifying record names ANOTHER identity (or another file)
            // is not this recovery's completed publication — the state stays unresolved.
            val targetProof = if (target.exists()) readPublishedProvenance(context, target) else null
            if (targetProof != null && targetProof.certifiesBytes(target) &&
                targetProof.identifies(
                    File(id = record.fileId, driveId = record.driveId),
                    UserDrive(userId = record.userId, driveId = record.driveId),
                )
            ) {
                runCatching { recoveryFile.delete() }
                disposeRecoveryRecord(record.recordFile)
                return true
            }
            // The backup's IMMUTABLE verification basis (issue: round 23 #2): the record's
            // hash when carried; a legacy record's backup is hashed NOW (the backup file
            // is never mutated after creation). An unverifiable backup stays unresolved.
            val backupHash = record.backupHash ?: UploadFile.sha256OrNull(recoveryFile)
            if (backupHash == null) return false
            val currentTargetHash = if (target.exists()) UploadFile.sha256OrNull(target) else null
            // INTERRUPTED PUBLICATION OUTPUT (issue: round 23 #2): the target already
            // carries the EXPECTED publication's bytes — the rename completed before the
            // record move. The publication is COMPLETED with its captured expected proof
            // (never re-classified as an edit, never re-downloaded); an unreadable
            // expected-proof write leaves the state unresolved.
            if (currentTargetHash != null && record.expectedPublicationHash == currentTargetHash) {
                // CURRENT OWNERSHIP AND PUBLICATION AUTHORITY (issue: round 25 #4): hash
                // equality is content evidence only — the recorded owner must still
                // exclusively hold the canonical path, and the target's current record must
                // not be a DIFFERENT later publication's proof (completing the interrupted
                // operation over it would regress a newer publication token).
                val currentRecord = readPublishedProvenance(context, target)
                if (!currentOwnerHolds(
                        context, recordScope, record.fileId, target,
                        sharedWithMe = capturedShared,
                        storageKind = capturedStorageKind,
                    )
                ) {
                    return false
                }
                if (currentRecord != null && currentRecord.contentHash != record.expectedPublicationHash) return false
                if (record.expectedPublicationRecordText != null) {
                    if (!atomicWriteText(provenanceFileFor(context, target), record.expectedPublicationRecordText)) return false
                    if (readPublishedProvenance(context, target) == null) return false
                }
                runCatching { recoveryFile.delete() }
                disposeRecoveryRecord(record.recordFile)
                return true
            }
            if (backupHash != null && currentTargetHash != null && currentTargetHash == backupHash) {
                // The target is STILL the backed-up generation (the rename never happened
                // or a previous restore completed): establish the retained proof and
                // dispose — never a blind byte copy over an unknown state.
                // CURRENT OWNERSHIP AUTHORITY (issue: round 25 #4), as in every proof
                // rewrite; the re-established record carries a FRESH publication identity
                // (issue: round 25 #10) so a same-bytes re-establishment is visible to the
                // publication-token comparison.
                if (!currentOwnerHolds(
                        context, recordScope, record.fileId, target,
                        sharedWithMe = capturedShared,
                        storageKind = capturedStorageKind,
                    )
                ) {
                    return false
                }
                if (record.oldRecordText != null) {
                    val refreshed = refreshedRecordText(record.oldRecordText, target) ?: return false
                    if (!atomicWriteText(provenanceFileFor(context, target), refreshed)) return false
                    if (readPublishedProvenance(context, target) == null) return false
                } else {
                    deletePublishedProvenance(context, target)
                }
                runCatching { recoveryFile.delete() }
                disposeRecoveryRecord(record.recordFile)
                return true
            }
            if (target.exists() && currentTargetHash != null) {
                // RESTORATION AUTHORITY IS NEVER PREFIX SIMILARITY (issue: round 24 #2):
                // the previous strict-prefix arm auto-restored a shorter target whose
                // bytes matched the backup's prefix — but a legitimate truncating edit
                // (including to zero bytes, before upload registration) satisfies the
                // same predicate, and prefix equality cannot establish which operation
                // produced the bytes. Restores are STAGED AND ATOMIC (the verified stage
                // replaces the target in one rename), so interrupted-restore output can
                // no longer exist at the target: a changed readable target routes through
                // the PROVEN EDIT arm below or stays unresolved — it is never overwritten.
                // PROVEN INTERVENING EDIT ONLY (issue: round 23 #2): the bytes differ from
                // BOTH the backup and the expected publication. They are a genuine newer
                // revision only when the target is NEWER than the backed-up generation AND
                // the current row EXCLUSIVELY owns the recorded target path (the record's
                // file id alone never authorized registration). Anything else — older or
                // equal-mtime different bytes, a contested or re-owned path — stays
                // UNRESOLVED: never auto-uploaded, never overwritten; the recovery state
                // is retained for a later sweep.
                val newerThanBackedUp = target.lastModified() > record.oldTargetMtimeMillis
                // RESTORE-PROTOCOL AMBIGUITY IS NEVER AN EDIT (issues: round 25 #3, round 26
                // #3): EVERY record version (v1–v3) may describe an interrupted IN-PLACE
                // restore — the earlier builds restored with recoveryFile.copyTo(target,
                // overwrite = true) while already writing v2/v3 records, so `legacy` cannot
                // identify the restoration protocol. A changed target that is a readable
                // STRICT PREFIX of the complete backup — or EMPTY (zero-byte partial output,
                // which the prefix comparison rejects by length) — cannot be distinguished
                // from a genuine user truncation by ownership or timestamp: auto-registering
                // it uploads partial restore output and the sweep then DISCARDS the complete
                // backup. The ambiguous output and the complete backup are BOTH retained
                // unresolved; only independent operation evidence (the expected-publication
                // arm above) or a verified resolution outside this arm may resolve them.
                val targetLength = runCatching { target.length() }.getOrDefault(-1L)
                val prefixRead = filePrefixMatches(target, recoveryFile)
                // (issues: round 25 #3, round 26 #3, round 27 #12): a proven strict prefix
                // (true) AND a READ/EOF UNCERTAINTY (null — e.g. an unreadable backup)
                // both retain the ambiguous output and the complete backup unresolved;
                // only a PROVEN non-prefix comparison (false) may proceed toward edit
                // registration — the previous Boolean let a backup whose bytes could not
                // be read register its partial target as an edit before the backup's
                // deletion.
                if (prefixRead != false || targetLength == 0L) return false
                // THE CAPTURED REALM FLAVOR AND DESTINATION KIND (issue: round 27 #9c):
                // the intervening-edit branch resolves the row and its path through the
                // SAME captured scope the record's sidecar names — the previous
                // ordinary-realm lookup and offline-path resolution could never match a
                // shared or cache/converted destination, leaving an attributable live
                // owner excluded indefinitely.
                val armScope = UserDrive(
                    userId = recordScope.userId,
                    driveId = recordScope.driveId,
                    sharedWithMe = capturedShared,
                )
                val rowOwnsTarget = runCatching {
                    FileController.getRealmInstance(armScope).use { realm ->
                        runCatching { realm.refresh() }
                        val row = FileController.getFileById(realm, record.fileId)
                        val ownedPath = when (capturedStorageKind) {
                            "cache" -> row?.getCacheFile(context, armScope)?.absolutePath
                            "converted" -> row?.getConvertedPdfCache(context, armScope)?.absolutePath
                            else -> row?.getOfflineFile(context, recordScope.userId, sharedWithMe = capturedShared)?.absolutePath
                        }
                        row != null && ownedPath == target.absolutePath &&
                            // DESTINATION-APPROPRIATE OWNERSHIP (issue: round 28 #9): an
                            // ID-keyed cache/converted destination is structurally
                            // exclusive; only offline paths run the name-based
                            // path-conflict query (which previously failed cache targets
                            // forever).
                            (capturedStorageKind != "offline" ||
                                appContextForOwnership()?.let { ctx ->
                                    OfflineFolderController.exclusivePathOwner(realm, ctx, armScope, target.path)
                                } == row.id)
                    }
                }.getOrDefault(false)
                if (!newerThanBackedUp || !rowOwnsTarget) return false
                val preserved = OfflineFolderController.registerDirtySnapshot(
                    context = context,
                    file = FileController.getRealmInstance(armScope).use { realm ->
                        runCatching { realm.refresh() }
                        FileController.getFileById(realm, record.fileId)?.let { realm.copyFromRealm(it, 1) }
                    } ?: return false,
                    offlineFile = target,
                    userDrive = armScope,
                    allowPendingRecoveryResolution = true,
                )
                if (!preserved) return false
                runCatching { recoveryFile.delete() }
                disposeRecoveryRecord(record.recordFile)
                return true
            } else if (!target.exists()) {
                // A MISSING target: the VERIFIED restore is the only resolution. Its
                // verified outcome IS the branch's result: a failed restore stays
                // unresolved (the recovery state is retained for the next sweep).
                // CURRENT OWNERSHIP AUTHORITY (issue: round 25 #4): the restore onto a
                // path the recorded owner no longer exclusively holds is deferred — the
                // changed-target branch's live ownership check applies here too.
                if (!currentOwnerHolds(
                        context, recordScope, record.fileId, target,
                        sharedWithMe = capturedShared,
                        storageKind = capturedStorageKind,
                    )
                ) {
                    return false
                }
                return restoreRecoveryCopy(
                    context = context,
                    recoveryFile = recoveryFile,
                    target = target,
                    recordFile = record.recordFile,
                    oldRecordText = record.oldRecordText,
                    oldTargetMtimeMillis = record.oldTargetMtimeMillis,
                    expectedBackupHash = backupHash,
                )
            } else {
                // An UNREADABLE CHANGED target (issue: round 23 #2): never overwritten —
                // the unreadable bytes could be mid-write content of another writer; the
                // state stays unresolved for a later sweep.
                return false
            }
        }

        private fun appContextForOwnership(): android.content.Context? = Realm.getApplicationContext()

        /**
         * RESTORES the retained old bytes with their retained proof and verifies the result
         * before disposing the recovery state (issues: round 21 #9, round 24 #2): the BACKUP
         * is validated against its immutable recorded hash BEFORE anything is mutated — a
         * damaged backup never reaches the target — and the restore is STAGED: the backup is
         * copied to a verified stage, timestamped, and published by an ATOMIC RENAME, so a
         * failed restore leaves the existing target bytes and proof untouched (the previous
         * in-place copyTo destroyed the existing target before the verification could fail).
         * On any failure the recovery backup and its durable record are RETAINED for the
         * reconciliation sweep; on success — the published bytes re-hashed against the
         * backup's own content hash, mtime restored, proof written back and read — they are
         * disposed.
         */
        private fun restoreRecoveryCopy(
            context: Context,
            recoveryFile: IOFile,
            target: IOFile,
            recordFile: IOFile,
            oldRecordText: String?,
            oldTargetMtimeMillis: Long,
            expectedBackupHash: String? = null,
        ): Boolean = runCatching {
            // BACKUP INTEGRITY BEFORE ANY TARGET MUTATION (issue: round 24 #2): the backup
            // must still hash to the record's immutable hash (a corrupted backup never
            // "restores" successfully — and never overwrites the target first).
            val backupHashNow = UploadFile.sha256OrNull(recoveryFile)
            if (backupHashNow == null || (expectedBackupHash != null && backupHashNow != expectedBackupHash)) {
                return@runCatching false
            }
            val stage = IOFile(stagingDirectory(context), "restore_${UUID.randomUUID()}.part")
            try {
                val stagedOk = runCatching {
                    recoveryFile.copyTo(stage, overwrite = true)
                    stage.exists() && stage.length() == recoveryFile.length() &&
                        UploadFile.sha256OrNull(stage) == backupHashNow
                }.getOrDefault(false)
                if (!stagedOk) return@runCatching false
                if (!stage.setLastModified(oldTargetMtimeMillis)) return@runCatching false
                // ATOMIC PUBLICATION of the verified stage: the rename replaces the target
                // in one step; a failed rename leaves the target exactly as it was.
                if (!stage.renameTo(target)) return@runCatching false
                // VERIFIED RESULT: the published bytes are re-hashed against the immutable
                // backup hash before any state is disposed.
                if (UploadFile.sha256OrNull(target) != backupHashNow) return@runCatching false
                if (oldRecordText != null) {
                    // A FRESH RESTORATION IDENTITY (issue: round 25 #10): the re-written
                    // proof carries a NEW publication id — the reused old text's id made a
                    // same-bytes restoration invisible to the publication-token comparison.
                    val refreshed = refreshedRecordText(oldRecordText, target)
                        ?: return@runCatching false
                    if (!atomicWriteText(provenanceFileFor(context, target), refreshed)) return@runCatching false
                    if (readPublishedProvenance(context, target) == null) return@runCatching false
                }
                // THE ROLLBACK DISPOSES THE RELOCATION STATE ITSELF (issue: round 25 #5):
                // the previous order — the caller deleting the failed proof-move record
                // separately AFTER this restore disposed its own recovery record — left a
                // crash window whose residue (restored R1 plus the retained R2 relocation
                // proof) the sweep then mismatched forever. Disposing here closes the
                // window; any caller's own deletion is an idempotent no-op.
                disposeRecoveryRecord(proofRelocationRecordFile(context, target))
                runCatching { recoveryFile.delete() }
                disposeRecoveryRecord(recordFile)
                true
            } finally {
                // The unpublished stage never lingers (issue: round 24 #21 discipline).
                runCatching { if (stage.exists()) stage.delete() }
                runCatching { deletePublishedProvenance(context, stage) }
            }
        }.getOrDefault(false)

        /**
         * Shared, coordinated publication of a staged file into its final offline location
         * (issue: round 4 #4): used by network downloads, worker cache promotion AND the UI
         * promotion, all through the shared per-file coordination so uploads, cleanup, moves and
         * completion cannot interleave with the publication.
         */
        private suspend fun publishStagedOfflineFile(
            userDrive: UserDrive,
            isInheritedDownload: Boolean,
            file: File,
            target: IOFile,
            staged: IOFile,
            onPublishedLocked: (() -> Unit)? = null,
            verificationBasis: File? = null,
            explicitUnresolvedGeneration: Boolean = false,
            onVerifiedBasisHash: ((String?) -> Unit)? = null,
        ): PublicationOutcome = withContext(Dispatchers.IO) {
            // ROOT-INTENT coordination for publication (issue: round 12 #8): an ancestry-
            // dependent publication must serialize with ancestry-changing writers (folder
            // moves take the monitor before their locks) — a monitored UI folder move could
            // otherwise change ancestry after this publication validates its destination,
            // because it locks the ancestor rather than this publishing leaf. The monitor is
            // acquired BEFORE the ordered file locks and retained through the destination
            // ownership read, the final supersession decision and the filesystem rename
            // inside publishLockedCore. Order: intent monitor → ascending file locks →
            // Realm/FS mutation.
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                // Destination ownership (issue: round 6 #3): the row owning the existing bytes at
                // [target] is resolved BEFORE locking, then this publication holds BOTH the
                // incoming file's and the owner's coordination locks in a stable ascending-id
                // order, so the owner's mutators (cleanup, reconciliation, its own publications)
                // are serialized with it and no lock ordering can deadlock.
                val appContext = Realm.getApplicationContext()
                val preLockOwner = appContext?.let { ctx ->
                    FileController.getRealmInstance(userDrive).use { realm ->
                        OfflineFolderController.destinationOwnerRow(realm, ctx, userDrive, file, target)
                    }
                }
                val lockedOwnerId: Int? = preLockOwner?.id

                // Both locks (incoming + established owner) in ONE ascending-id acquisition,
                // constant stack (issues: round 11 #1, round 11 #12): the same ordered protocol as
                // every graph commit and cleanup acquisition, so no lock cycle is possible, and
                // the explicit-lock loop cannot overflow the stack the way nested monitors did.
                OfflineFileCoordination.withLocksFor(userDrive, listOfNotNull(file.id, preLockOwner?.id)) {
                    publishLockedCore(
                        userDrive = userDrive,
                        isInheritedDownload = isInheritedDownload,
                        file = file,
                        target = target,
                        staged = staged,
                        onPublishedLocked = onPublishedLocked,
                        lockedOwnerId = lockedOwnerId,
                        verificationBasis = verificationBasis,
                        explicitUnresolvedGeneration = explicitUnresolvedGeneration,
                        checkCancelled = { coroutineContext.ensureActive() },
                        onVerifiedBasisHash = onVerifiedBasisHash,
                    )
                }
            }
        }

        /**
         * Publication body, executed while the coordination locks of the incoming file (and of
         * the established destination owner, if any) are held. [checkCancelled] restores the
         * final cancellation check inside the locks (issue: round 7 #11): Java monitor
         * acquisition does not respond to coroutine cancellation, so a cancelled transfer
         * waiting for an ownership lock must re-check after acquiring them and immediately
         * before the publication.
         */
        private fun publishLockedCore(
            userDrive: UserDrive,
            isInheritedDownload: Boolean,
            file: File,
            target: IOFile,
            staged: IOFile,
            onPublishedLocked: (() -> Unit)?,
            lockedOwnerId: Int?,
            checkCancelled: () -> Unit,
            verificationBasis: File? = null,
            explicitUnresolvedGeneration: Boolean = false,
            onVerifiedBasisHash: ((String?) -> Unit)? = null,
        ): PublicationOutcome {
            checkCancelled()
            // HISTORICAL VERIFICATION BASIS (issue: round 24 #7): a verification transfer's
            // authority is the CAPTURED OLD OBSERVATION (the uncommitted row state the
            // verification was captured against), not the fetched response — the response's
            // identity difference from the historical row is the very subject of the staged
            // comparison, never a supersession. Ordinary transfers CAS against the response.
            val casBasis = verificationBasis ?: file
            // Live intent: an inherited download requires a current pin or durable root
            // ancestry (a materialized inherited flag alone never authorizes retention); a
            // pin download accepts its own pending marker or explicit pin as the user's
            // request. The same realm read also decides whether this transfer's captured
            // identity/revision has been superseded (issues: round 8 #9, round 9 #7).
            val appContextForCheck = Realm.getApplicationContext()
            var supersededGeneration = false
            var unresolvedGeneration = false
            val intentOk = FileController.getRealmInstance(userDrive).use { realm ->
                val live = FileController.getFileById(realm, file.id)
                val pinned = live?.isOffline == true
                val ancestryCovers = OfflineFolderController.isInsideActiveRoot(realm, file.id)
                // Superseded rejection — full identity/generation CAS (issues: round 8 #9,
                // round 9 #7, round 10 #4): the transfer is stale when the live row holds a
                // NEWER SERVER GENERATION ([File.revisedAt] — the modification time alone is
                // not monotonic, a re-upload can move it backwards; the mtime comparison only
                // covers legacy rows whose revisedAt was never populated), OR a different
                // location identity (direct rename/move, or an ancestor-chain change that
                // moved the resolved offline path). Publishing at the old location and
                // completing against the new row would desync bytes and metadata. The check
                // and the rename form one compare-and-set: identity/revision writers take the
                // same per-file lock before their transactions (round 10 #3). In VERIFICATION
                // mode the CAS basis is the captured old observation (issue: round 24 #7).
                supersededGeneration = live != null && (
                    live.revisedAt > casBasis.revisedAt ||
                        (live.revisedAt == 0L && live.lastModifiedAt > casBasis.lastModifiedAt) ||
                        live.parentId != casBasis.parentId ||
                        live.name != casBasis.name ||
                        // Accepted content-generation identity (issues: round 12 #6, round
                        // 13 #9): the committed metadata and this transfer must describe the
                        // SAME revision. UNKNOWN version data (listing rows omit the
                        // optional expansion) is not a generation difference — only two
                        // KNOWN, differing version numbers supersede; the COMPLETE content
                        // baseline (modification time AND size) decides otherwise, so a
                        // tied-timestamp same-size-differs revision still supersedes the
                        // staged bytes.
                        ((live.version?.number ?: 0) > 0 && (casBasis.version?.number ?: 0) > 0 &&
                            live.version?.number != casBasis.version?.number) ||
                        // Ambiguous version provenance (issue: round 14 #7): when exactly ONE
                        // side carries the authoritative version and the complete coarse
                        // baseline ties, timestamp/size equality alone cannot prove the same
                        // content generation — an inherited or omitted version must not
                        // falsely prove equality with an in-flight transfer. The publication
                        // defers (SKIPPED_SUPERSEDED) and the retry obtains the authoritative
                        // identity: details routes carry the version expansion, so the
                        // worker's refetch and the deferred markers' refresh resolve it.
                        // Both sides UNKNOWN remains the documented coarse-baseline case.
                        (((live.version?.number ?: 0) > 0) != ((casBasis.version?.number ?: 0) > 0) &&
                            live.lastModifiedAt == casBasis.lastModifiedAt && live.size == casBasis.size) ||
                        live.lastModifiedAt != casBasis.lastModifiedAt ||
                        live.size != casBasis.size ||
                        run {
                            val livePath = appContextForCheck?.let { ctx -> live.getOfflineFile(ctx, userDrive.userId)?.path }
                            // THE VERIFICATION'S LOCATION AUTHORITY IS THE CAPTURED BASIS
                            // (issue: round 25 #9): after an accepted rename, the historical
                            // row still owns the OLD location while the fetched identity (and
                            // the publication target) is the NEW one — the verification
                            // proves the row has not moved since the capture (its path still
                            // equals the basis's), and accepting the new identity is the very
                            // change the converged publication commits. Ordinary transfers
                            // keep the exact target-path CAS.
                            val basisPath = if (verificationBasis != null) {
                                appContextForCheck?.let { ctx -> casBasis.getOfflineFile(ctx, userDrive.userId)?.path }
                            } else null
                            livePath == null || livePath != (basisPath ?: target.path)
                        }
                    )
                // UNRESOLVED GENERATION (issue: round 19 #14): when NEITHER side carries a
                // version, the coarse baseline tie is the only generation evidence — the
                // publication may still proceed (the bytes were fetched fresh from the
                // server's current revision, and the record's version-0 identity keeps the
                // copy non-current), but it is UNRESOLVED, NOT current: the file/root
                // completion callbacks assert a generation identity both-unknown cannot
                // prove, so they are skipped and a later details-resolved pass completes.
                // The assignment precedes the conditional so the lambda's TAIL stays the
                // Boolean the caller's `if (!intentOk)` needs (round-19 build fix).
                unresolvedGeneration = explicitUnresolvedGeneration ||
                    (live != null &&
                        (live.version?.number ?: 0) <= 0 && (casBasis.version?.number ?: 0) <= 0)
                if (isInheritedDownload) {
                    pinned || ancestryCovers
                } else {
                    pinned || live?.isMarkedAsOffline == true || ancestryCovers
                }
            }

            if (!intentOk) {
                runCatching { staged.delete() }
                return PublicationOutcome.SKIPPED_INVALIDATED
            }
            if (supersededGeneration) {
                runCatching { staged.delete() }
                return PublicationOutcome.SKIPPED_SUPERSEDED
            }

            // The existing copy may have been edited, registered for upload, or re-owned by
            // another row during the transfer: those bytes always win over the freshly
            // downloaded ones (issue: round 6 #3).
            // PENDING-RECOVERY EXCLUSION OUTSIDE TARGET EXISTENCE (issues: round 23 #3,
            // round 24 #4): the check ACTUALLY runs before the existence branch now — a
            // missing target with an unresolved recovery is recovery-managed state (a crash
            // removed the bytes mid-recovery) and the publication must never treat the path
            // as free. Re-read under the publication's final locks, by identity AND by
            // target path; an enumeration failure fails CLOSED.
            val appContextForRecovery = Realm.getApplicationContext()
            val pendingRecoveryExcludes = appContextForRecovery != null &&
                hasUnresolvedRecovery(appContextForRecovery, userDrive, file.id, target.absolutePath)
            if (target.exists()) {
                val appContext = Realm.getApplicationContext()
                // THE OWNER ROW'S STABLE SNAPSHOT (round-24 build correction): the
                // DestinationOwner DTO carries only the ownership/baseline fields — the
                // exact-identity ownership query below needs the ACTUAL observed row's
                // name and parentId, read here from the SAME realm the ownership was
                // resolved in, detached so it survives the session, and never invented
                // or weakened. One val resolution (DTO + detached row snapshot) — no
                // captured mutable state.
                val ownerResolution: Pair<OfflineFolderController.DestinationOwner, File?>? = appContext?.let { ctx ->
                    FileController.getRealmInstance(userDrive).use { realm ->
                        OfflineFolderController.destinationOwnerRow(realm, ctx, userDrive, file, target)?.let { owner ->
                            owner to (FileController.getFileById(realm, owner.id)?.let { realm.copyFromRealm(it, 1) })
                        }
                    }
                }
                val liveOwner = ownerResolution?.first
                val liveOwnerRow: File? = ownerResolution?.second

                // REGISTRY OWNER CONFLICT (issue: round 20 #11): the destination's
                // provenance record POSITIVELY names a different owner — the bytes
                // are another identity's published content. Realm ownership and
                // mtime alone never authorize overwriting them; the publication
                // defers as a conflict until the registry conflict resolves.
                val registryConflict = appContext != null &&
                    readPublishedProvenance(appContext, target)
                        ?.positivelyForeignTo(file, userDrive) == true
                // NON-DESTRUCTIVE STAGED COMPARISON ELIGIBILITY (issue: round 22 #6): set by
                // the classification below when the retained bytes carry no content proof —
                // the freshly staged copy may then CERTIFY them without replacing them first.
                var targetContentUnproven = false
                val targetProtected = UploadFile.hasPendingUpload(uri = Uri.fromFile(target).toString()) ||
                        UploadFile.hasRetainedUnsentUpload(uri = Uri.fromFile(target).toString()) ||
                        registryConflict || pendingRecoveryExcludes || when {
                    // Without an application context the ownership cannot be established: preserve.
                    appContext == null -> true
                    // Unknown ownership (no row claims the path — e.g. new-file/path-reuse
                    // residue): preserve, never overwrite — EXCEPT a verification's NEW
                    // location (issue: round 25 #9): a converged verification publishes at
                    // the fetched identity's path, typically a location no row owns yet; its
                    // authority is the CAPTURED BASIS (the live row still owning the old
                    // location), validated inside the verification arm below.
                    liveOwner == null && verificationBasis == null -> true
                    // The owner changed after the locks were taken: its mutators are not
                    // serialized with this publication — defer and let the next attempt lock
                    // the current owner correctly.
                    liveOwner != null && liveOwner.id != lockedOwnerId -> true
                    // Known owner (or a verification's unowned new location): protect against
                    // the owner's PUBLISHED byte baseline (issue: round 15 #6), never the
                    // incoming revision — the previous baseline declared another file's older
                    // dirty copy unprotected and overwrote its sole edit, and accepted
                    // metadata alone must not reclassify retained old-clean bytes as an edit.
                    else -> run {
                        val currentTargetHash = UploadFile.sha256OrNull(target)
                        // HISTORICAL VERIFICATION (issues: round 24 #7, round 25 #9): the
                        // staged comparison itself decides. THE OLD PHYSICAL SOURCE IS THE
                        // COMPARISON SUBJECT: after an accepted rename the retained copy
                        // lives at the CAPTURED BASIS's location while the publication target
                        // is the new identity's path — the staged bytes are compared against
                        // the basis-location copy (the live row's own retained bytes), never
                        // a destination-only existence gate. Equal bytes converge (the
                        // publication proceeds at the NEW location and the conditional
                        // metadata advance accepts the new identity); different or unreadable
                        // comparison bytes are RETAINED. The new location's own existing
                        // bytes — when any — must be absent or byte-identical (never a bare
                        // rename over foreign or unproven content).
                        if (verificationBasis != null) {
                            val stagedHash = UploadFile.sha256OrNull(staged)
                            targetContentUnproven = false
                            val basisLocationCopy = appContext?.let { ctx ->
                                casBasis.getOfflineFile(ctx, userDrive.userId)
                            }?.takeIf { it.exists() && it.path != target.path }
                            val comparisonHash = when {
                                basisLocationCopy != null -> UploadFile.sha256OrNull(basisLocationCopy)
                                currentTargetHash != null -> currentTargetHash
                                else -> null
                            } ?: return@run true
                            if (stagedHash == null || comparisonHash != stagedHash) return@run true
                            // THE VERIFIED OLD-SOURCE HASH (issue: round 27 #3): the
                            // retirement revalidates the old path against exactly this
                            // comparison-time hash; an intervening edit in the
                            // release/reacquire gap is preserved, never deleted. The hash
                            // is threaded to the CALLER (this coordinated core runs in the
                            // companion and holds no transfer-instance state).
                            onVerifiedBasisHash?.invoke(if (basisLocationCopy != null) comparisonHash else null)
                            // THE NEW LOCATION NEVER RECEIVES A BARE OVERWRITE (issue: round
                            // 25 #9, same authority as the transfer): existing bytes at the
                            // target must equal the staged copy.
                            return@run currentTargetHash != null && currentTargetHash != stagedHash
                        }
                        // ONE COMPLETE CONTENT CLASSIFICATION, CLOSED OVER THE REALM
                        // (issues: round 22 #1, round 22 #5): the ownership comparison is
                        // computed INSIDE the scoped-realm block and only the Boolean
                        // escapes; a REFRESH FAILURE defers to the protected side. The
                        // classification matches preservation's:
                        //  - CERTIFIED clean bytes (the owner's record certifies them) are
                        //    the published generation — replaceable regardless of mtime (a
                        //    stale baseline COMMIT no longer blocks the requested refresh
                        //    with a false SKIPPED_PROTECTED);
                        //  - PROVEN hash drift (the owner's record, different content) is an
                        //    edit — protected;
                        //  - a HASHLESS record identifying the owner proves nothing about
                        //    content: unproven historical — RETAINED (a same-size same-mtime
                        //    historical edit was previously considered replaceable);
                        //  - NO identifying record at all: unknown historical — RETAINED.
                        // ORDINARY MODE ONLY (round 25 #9): the verification arm above
                        // returned, so a null owner here is the compiler-unreachable case —
                        // preserved defensively as protected.
                        val classificationOwner = liveOwner ?: return@run true
                        val ownerProof = readPublishedProvenance(appContext, target)
                        val ownerRowIdentifies = runCatching {
                            FileController.getRealmInstance(userDrive).use { ownerRealm ->
                                ownerRealm.refresh()
                                val ownerRow = FileController.getFileById(ownerRealm, classificationOwner.id)
                                ownerRow != null && ownerProof?.identifies(ownerRow, userDrive) == true
                            }
                        }.getOrDefault(false)
                        // The record's content hash hoisted NULLABLY (round-22 build fix):
                        // the when-branches are independent — branch two's ownerProof
                        // non-null check does not smart-cast branch three's receiver. A
                        // null record (or a hashless one) classifies through the same arms
                        // as before; nothing is weakened.
                        val ownerContentHash = ownerProof?.contentHash
                        var classified = when {
                            currentTargetHash == null -> true
                            ownerProof != null && ownerRowIdentifies && ownerProof.certifiesBytes(target) -> false
                            ownerRowIdentifies && ownerContentHash != null &&
                                ownerContentHash != currentTargetHash -> true
                            else -> true
                        }
                        // STAGED COMPARISON (issue: round 22 #6): only UNPROVEN
                        // destinations (no identifying record, or a hashless identifying
                        // record) converge through the staged copy — positively foreign
                        // records and proven drift never do. When the freshly staged copy is
                        // byte-identical to the retained bytes, the retained generation IS
                        // the fetched one: the publication proceeds and its certifying
                        // record replaces the hashless one (convergence). Different or
                        // unreadable bytes stay protected.
                        targetContentUnproven = classified &&
                            (ownerProof == null || (ownerRowIdentifies && ownerProof.contentHash == null))
                        if (targetContentUnproven && currentTargetHash != null) {
                            val stagedHash = UploadFile.sha256OrNull(staged)
                            if (stagedHash != null && stagedHash == currentTargetHash) {
                                classified = false
                                targetContentUnproven = false
                            }
                        }
                        classified
                    }
                }

                if (targetProtected) {
                    runCatching { staged.delete() }
                    // A REGISTRY CONFLICT IS ALWAYS A CONFLICT (issue: round 21 #11): the
                    // bytes at this path positively belong to another identity's published
                    // generation — even when Realm's path attribution names THIS file (a
                    // reused path), the protected content is NOT this file's own usable
                    // content. It returns SKIPPED_CONFLICT, clears no marker, and never
                    // reports a success over foreign bytes.
                    if (registryConflict) return PublicationOutcome.SKIPPED_CONFLICT
                    // The recovery case defers like a conflict: the durable intent stays
                    // and the sweep resolves the file (issue: round 22 #3).
                    if (pendingRecoveryExcludes) return PublicationOutcome.SKIPPED_CONFLICT
                    // Usable protected content FOR THIS FILE vs a destination conflict
                    // (issues: round 14 #13, round 23 #6, round 24 #6): the protection
                    // proves F's own VERIFIED-USABLE content only when the destination's
                    // owner row is F itself AND the content is POSITIVELY owned — the
                    // exact upload-record identity (name/folder/mtime/size/hash/fileId)
                    // proven over the CURRENT bytes. URI presence, a recovery exclusion
                    // and preservation alone never fulfill the transfer: a truncated or
                    // unreadable original whose record no longer matches is preserved but
                    // stays a conflict — the intent is RETAINED (the marker stays), the
                    // dialog never reports 100 and the bulk counts the work unresolved.
                    val ownedByThisFile = liveOwner != null && liveOwner.id == file.id && liveOwner.id == lockedOwnerId
                    val protectedContentUsable = ownedByThisFile && liveOwnerRow != null &&
                        run {
                            val currentProtectedHash = UploadFile.sha256OrNull(target)
                            currentProtectedHash != null &&
                                UploadFile.hasDurableOriginalRevisionOwnership(
                                    fileName = liveOwnerRow.name,
                                    remoteFolder = liveOwnerRow.parentId,
                                    driveId = userDrive.driveId,
                                    userId = userDrive.userId,
                                    fileModifiedAt = java.util.Date(target.lastModified()),
                                    fileSize = target.length(),
                                    contentHash = currentProtectedHash,
                                    originalFileId = file.id,
                                )
                        }
                    return if (ownedByThisFile && protectedContentUsable) {
                        // The transfer's own generation and intent were validated INSIDE this
                        // section (superseded/intent checks above) — clearing THIS transfer's
                        // pending marker here cannot erase a newer request's marker (issue:
                        // round 17 #7).
                        FileController.markFileAsOffline(fileId = file.id, isMarkedAsOffline = false, userDrive = userDrive)
                        PublicationOutcome.SKIPPED_PROTECTED
                    } else {
                        PublicationOutcome.SKIPPED_CONFLICT
                    }
                }
            } else if (pendingRecoveryExcludes) {
                // MISSING TARGET UNDER UNRESOLVED RECOVERY (issue: round 24 #4): the path
                // is recovery-managed state — never treated as free. The sweep owns the
                // resolution; the durable intent is retained (no marker clear).
                runCatching { staged.delete() }
                return PublicationOutcome.SKIPPED_CONFLICT
            } else if (verificationBasis != null) {
                // THE OLD PHYSICAL SOURCE IS COMPARED INDEPENDENTLY OF DESTINATION
                // EXISTENCE (issue: round 27 #2): the previous verification comparison
                // existed only inside the target-exists block — with the renamed
                // destination B absent locally, the publication skipped the comparison
                // entirely, published the staged generation at B and let the metadata
                // advance retire the CAPTURED BASIS's copy at A, deleting an uncompared
                // historical/local revision. An ABSENT destination in verification mode
                // still requires the basis comparison: differing OR unreadable old bytes
                // are RETAINED (the durable intent stays) and no retirement is
                // authorized.
                val basisCopy = appContextForCheck?.let { ctx ->
                    casBasis.getOfflineFile(ctx, userDrive.userId)
                }?.takeIf { it.exists() && it.path != target.path }
                val stagedHash = UploadFile.sha256OrNull(staged)
                val basisHash = basisCopy?.let { UploadFile.sha256OrNull(it) }
                if (basisCopy != null && (stagedHash == null || basisHash == null || basisHash != stagedHash)) {
                    runCatching { staged.delete() }
                    // A DEFERRED OUTCOME, NEVER A PROTECTED SUCCESS (issue: round 28 #1):
                    // the comparison could not converge (differing or unreadable old
                    // bytes) — the work is UNRESOLVED: the single-file worker retries and
                    // the batch/root keeps its incompleteness. The previous
                    // SKIPPED_PROTECTED is reserved for a positively verified usable copy
                    // FULFILLING the request; a failed comparison fulfills nothing (B is
                    // absent, the pending marker stays).
                    return PublicationOutcome.SKIPPED_CONFLICT
                }
                // A CONVERGED BASIS COMPARISON: the verified old-source hash rides to
                // the retirement's revalidation (issue: round 27 #3), threaded to the
                // caller (this core holds no transfer-instance state).
                onVerifiedBasisHash?.invoke(if (basisCopy != null) basisHash else null)
            } else {
                onVerifiedBasisHash?.invoke(null)
            }

            staged.setLastModified(file.getLastModifiedInMilliSecond())
            val provenanceContext = appContextForCheck
            if (provenanceContext == null) {
                runCatching { staged.delete() }
                return PublicationOutcome.FAILED
            }
            // PREPARE the staged record BEFORE the bytes are replaced (issue: round 18 #2):
            // the registry directories are created and verified and the staged record reads
            // back — a publication whose provenance cannot be established never moves its
            // bytes, instead of replacing the target first and deleting it afterwards.
            if (!prepareStagedProvenance(provenanceContext, staged, file, userDrive, unresolvedRecord = explicitUnresolvedGeneration)) {
                runCatching { staged.delete() }
                return PublicationOutcome.FAILED
            }
            // Final cancellation check AFTER the registry I/O, BEFORE the first mutation
            // (issues: round 7 #11, round 19 #11): a cancelled publication mutates nothing —
            // neither the destination's record nor its bytes.
            checkCancelled()
            // The OLD proof is retained for rollback (issue: round 19 #11): the atomicity of
            // the replacement covers the PROOF as well as the bytes — a rename failure must
            // leave the retained bytes carrying their generation's provenance.
            val oldRecordFile = provenanceFileFor(provenanceContext, target)
            val oldRecordText = if (oldRecordFile.exists()) runCatching { oldRecordFile.readText() }.getOrNull() else null
            // RECOVERABLE OLD BYTES (issue: round 20 #10): the atomic rename REPLACES the
            // old bytes — a post-rename failure (the target-provenance move failing)
            // previously deleted the NEW bytes, leaving the destination EMPTY with the old
            // bytes gone. The old bytes are backed up in the app-owned staging directory
            // until the operation commits; every post-rename failure restores them with
            // their retained proof. The backup carries a DURABLE sidecar record (issues:
            // round 21 #9/#10, round 22 #2) naming its target, the old proof, the target's
            // pre-publication mtime, its OWNER scope and file identity, and the backup's
            // IMMUTABLE content hash — the reconciliation coordinates under the owner's
            // locks and validates against the exact backed-up generation.
            val oldTargetMtime = if (target.exists()) target.lastModified() else 0L
            val recoveryFile = if (target.exists()) newStagedFile(provenanceContext, file) else null
            // copyTo returns the DESTINATION file, not a success flag — the backup is
            // validated explicitly (complete bytes) after the copy (issue: round 21 #1).
            val recoveryOk = recoveryFile == null || runCatching {
                target.copyTo(recoveryFile, overwrite = true)
                recoveryFile.exists() && recoveryFile.length() == target.length()
            }.getOrDefault(false)
            // A backup without a computable content hash is never kept (issue: round 22
            // #2): the reconciliation could not verify anything against it.
            val backupHash = if (recoveryFile != null) UploadFile.sha256OrNull(recoveryFile) else null
            // A backup without its durable association is never kept (issues: round 21
            // #9/#10, round 22 #2): an anonymous staged file cannot be reconciled after
            // restart.
            // THE EXPECTED TARGET PUBLICATION (issue: round 23 #2): the staged
            // generation's hash and the proof the target will carry ride the record, so
            // the reconciliation can complete an interrupted publication instead of
            // mis-classifying its output as an edit.
            val expectedPublicationHash = UploadFile.sha256OrNull(staged)
            val expectedPublicationRecordText = readPublishedProvenance(provenanceContext, staged)?.recordText(target)
            val recoveryRecorded = recoveryFile == null || (backupHash != null &&
                writeRecoveryRecord(
                    provenanceContext,
                    recoveryFile,
                    target,
                    oldRecordText,
                    oldTargetMtime,
                    file,
                    userDrive,
                    backupHash,
                    expectedPublicationHash,
                    expectedPublicationRecordText,
                ))
            if (!recoveryOk || !recoveryRecorded) {
                runCatching { recoveryFile?.delete() }
                recoveryFile?.let { runCatching { recoveryRecordFile(provenanceContext, it).delete() } }
                runCatching { staged.delete() }
                return PublicationOutcome.FAILED
            }
            // Cancellation AFTER the backup I/O, BEFORE the first mutation (issue: round
            // 21 #22): the backup copy can be long; a cancelled publication must not
            // proceed from it to the proof invalidation and the rename. The exception
            // propagates; the untouched target keeps its bytes and proof, and the sweep
            // reconciles the (never-mutated) recovery state.
            checkCancelled()
            // The OLD provenance is invalidated BEFORE the bytes are replaced (issue: round
            // 16 #12): a crash between the rename and the new sidecar write must never leave
            // the new bytes carrying the previous generation's provenance.
            deletePublishedProvenance(provenanceContext, target)
            if (staged.renameTo(target)) {
                // The staged record travels onto the target with the rename (issue: round 18
                // #2): provenance and bytes are published together — a failed move now
                // RESTORES the recoverable old bytes and their proof (issue: round 20 #10)
                // instead of deleting the new bytes with the old ones already gone.
                if (!movePublishedProvenance(
                        provenanceContext,
                        staged,
                        target,
                        sharedWithMe = userDrive.sharedWithMe,
                        storageKind = destinationStorageKind(provenanceContext, file, target, userDrive),
                    )
                ) {
                    val restored = recoveryFile != null && restoreRecoveryCopy(
                        context = provenanceContext,
                        recoveryFile = recoveryFile,
                        target = target,
                        recordFile = recoveryRecordFile(provenanceContext, recoveryFile),
                        oldRecordText = oldRecordText,
                        oldTargetMtimeMillis = oldTargetMtime,
                    )
                    if (restored) {
                        // THE ROLLBACK SUPERSEDES THE FAILED PUBLICATION'S PROOF (issue:
                        // round 24 #5): a verified restore put the previous generation and
                        // its proof back — the relocation record for the failed proof is
                        // disposed here, so the next sweep can never write it over the
                        // restored bytes.
                        runCatching { proofRelocationRecordFile(provenanceContext, target).delete() }
                    }
                    if (!restored) {
                        if (recoveryFile == null) {
                            // Nothing recoverable existed before (fresh publication): fail closed.
                            runCatching { target.delete() }
                        }
                        // A FAILED restore RETAINS the recovery copy and its durable record
                        // (issue: round 21 #9): the sweep reconciles them; deleting them
                        // here could lose the only copy of the old generation. The pending
                        // relocation record stays too: the sweep validates it against the
                        // target's actual bytes before any write (issue: round 24 #5).
                    }
                    return PublicationOutcome.FAILED
                }
                runCatching { recoveryFile?.delete() }
                recoveryFile?.let { runCatching { recoveryRecordFile(provenanceContext, it).delete() } }
                // Record the PUBLISHED bytes' clean baseline separately from the accepted
                // server metadata (issue: round 15 #6): edit detection judges the copy
                // against the baseline its bytes were actually written under until a
                // successful replacement advances it — metadata acceptance alone must not
                // reclassify retained old-clean bytes as a local edit.
                FileController.getRealmInstance(userDrive).use { realm ->
                    realm.executeTransaction {
                        FileController.getFileById(realm, file.id)?.publishedCleanBaseline = file.lastModifiedAt
                    }
                }
                // UNRESOLVED BEFORE ANY INTENT CONSUMPTION (issue: round 21 #14): the
                // durable pending marker is the single-file work's retry intent — an
                // unresolved generation must leave it in place for the durable re-drive.
                // The previous order cleared the marker and then returned the unresolved
                // outcome, so a single-file verification could terminate successfully
                // without retained retry intent.
                if (unresolvedGeneration) {
                    return PublicationOutcome.PUBLISHED_UNRESOLVED
                }
                // EXACT FILE-INTENT CONSUMPTION (issue: round 20 #20): the fulfilled
                // transfer's pending marker is consumed INSIDE the publication's
                // coordinated commit — the CAS above validated this transfer's identity
                // against the live row under the same locks, so the marker (if any)
                // belongs to this generation's request, whose bytes are now published.
                // The previous post-publication clear ran OUTSIDE coordination: an unpin
                // could delete the published bytes and a re-pin establish a new marker
                // for the same revision before the late call compared only server
                // metadata — and erased the new intent.
                FileController.markFileAsOffline(fileId = file.id, isMarkedAsOffline = false, userDrive = userDrive)
                // Run the owner's completion while the per-file lock is still held (issue:
                // round 5 #2): the unresolved arm above already returned, so completion
                // fires only for a PROVEN generation — no completion callback (coverage/
                // flag writes, downloaded notification, media scan, promotion-cache
                // cleanup) may run over a generation neither side could prove. The bytes
                // of an unresolved publication stay published (the record's version-0
                // identity keeps the copy non-current); a later pass with the
                // authoritative identity completes the file.
                onPublishedLocked?.invoke()
                return PublicationOutcome.PUBLISHED
            } else {
                // A FAILED RENAME NEVER DELETES THE OLD TARGET (issue: round 19 #11): the
                // rename touched nothing, so the retained bytes stay — and their proof is
                // rolled back with them, keeping the destination's generation intact.
                if (oldRecordText != null) {
                    runCatching { provenanceFileFor(provenanceContext, target).writeText(oldRecordText) }
                }
                runCatching { recoveryFile?.delete() }
                SentryLog.e(TAG, "Failed to publish staged file to ${target.path}")
                runCatching { staged.delete() }
                return PublicationOutcome.FAILED
            }
        }

        /**
         * UI cache promotion (issue: round 4 #8): routes through the same protected, coordinated
         * publication as the workers. The caller sets the explicit pin BEFORE calling so the
         * intent check sees the user's request; the cache source is deleted only after a
         * confirmed publication. Returns the full [PublicationOutcome] (issue: round 13 #13):
         * a superseded or invalidated publication is NOT a success the UI may promote — the
         * caller decides what remains of the user's intent per outcome.
         */
        internal suspend fun promoteCacheToOffline(
            context: Context,
            file: File,
            cacheFile: IOFile,
            offlineFile: IOFile,
            userDrive: UserDrive,
        ): PublicationOutcome {
            // AUTHORITATIVE version resolution for ambiguous transfers (issue: round 16 #13):
            // a presentation row's unknown version against an unknown live version left only
            // the coarse baseline, and two same-second, same-size revisions passed as
            // "current". The details route carries the version expansion; resolving the
            // identity before publication turns the ambiguous comparison into a
            // known-generation one (the stale-operation risk is re-checked against the live
            // row under the publication locks anyway).
            val authoritativeFile = if ((file.version?.number ?: 0) > 0) file else {
                runCatching {
                    // The SUPPLIED scope's account client (issue: round 22 #19): the
                    // promotion's resolving details request previously used the default
                    // client — the globally active account — while the publication ran in
                    // the supplied userDrive.
                    ApiRepository.getFileDetails(
                        File(id = file.id, driveId = userDrive.driveId),
                        AccountUtils.getHttpClient(userDrive.userId),
                    ).data
                }.getOrNull()?.takeIf { details -> details.id == file.id } ?: file
            }
            // HELD-STAGING / SUSPEND-PUBLICATION split (round-16 compile correction): the
            // SHARED provenance validation THROUGH STAGING (issues: round 16 #11/#12) runs as
            // ONE SYNCHRONOUS section under the ROOT-INTENT monitor and the file lock — every
            // cache promotion, UI included, requires the generation sidecar to match the
            // authoritative identity, and the cache bytes cannot change between validation
            // and staging. The staged publication is SUSPEND, so it runs OUTSIDE the critical
            // section and re-acquires the SAME monitor and locks in the SAME order inside its
            // coordinated core; missing or mismatched provenance fails and the caller
            // enqueues a fresh download.
            val staged = newStagedFile(context, authoritativeFile)
            // THE CAPTURED PROMOTION TOKEN (issue: round 23 #20): captured ATOMICALLY WITH
            // STAGING — inside the same monitor+lock section that copies the cache — so a
            // writer that replaces the cache after the section (certifying different bytes
            // under the same captured row metadata) can no longer satisfy the cleanup.
            var capturedPromotionToken: CachePublicationToken? = null
            val stagedOk = synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withFileLock(userDrive, file.id) {
                    // EXPLICIT Boolean success AFTER the actual copy (round-16 compile fix
                    // #2), as in moveCacheFileIfIntact: File.copyTo's File return must not
                    // merge with the failure default into a non-Boolean condition.
                    cacheBytesProvenCurrent(context, cacheFile, authoritativeFile, userDrive) && runCatching {
                        cacheFile.copyTo(staged, overwrite = true)
                        capturedPromotionToken = capturedCachePublicationToken(context, cacheFile)
                        true
                    }.getOrDefault(false)
                }
            }
            if (!stagedOk) {
                runCatching { if (staged.exists()) staged.delete() }
                SentryLog.i(TAG, "Cache promotion refused for ${offlineFile.path}: unproven generation")
                return PublicationOutcome.FAILED
            }
            val outcome = runCatching {
                publishStagedOfflineFile(
                    userDrive,
                    isInheritedDownload = false,
                    authoritativeFile,
                    offlineFile,
                    staged,
                    onPublishedLocked = {
                        // EXACT-GENERATION cache cleanup INSIDE the publication's locks
                        // (issues: round 17 #6, round 18 #11): the promoted cache goes only
                        // when it still IS the staged source revision, and the exact check
                        // and the deletion run under the SAME monitor and file locks that
                        // serialized the publication — another publication can no longer
                        // replace the cache between the check and the deletion.
                        if (cacheFile.exists() &&
                            (cacheFile.lastModified() / 1_000L) == authoritativeFile.lastModifiedAt &&
                            authoritativeFile.isIntactFile(cacheFile) &&
                            // EXACT CAPTURED PUBLICATION IDENTITY (issues: round 21 #24,
                            // round 23 #20): the promotion's cleanup runs under the
                            // publication's locks and requires the token captured ATOMICALLY
                            // WITH STAGING — a concurrent writer's new self-certifying record
                            // (same captured row metadata, different bytes) does not match.
                            cacheBytesProvenCurrent(context, cacheFile, authoritativeFile, userDrive) &&
                            capturedCacheTokenStillMatches(context, cacheFile, capturedPromotionToken)
                        ) {
                            deletePublishedProvenance(context, cacheFile)
                            runCatching { cacheFile.delete() }
                        }
                    },
                )
            }.onFailure { exception ->
                SentryLog.e(TAG, "Cache promotion failed for ${offlineFile.path}", exception)
            }.getOrDefault(PublicationOutcome.FAILED)

            runCatching { if (staged.exists()) staged.delete() }
            return outcome
        }

        /**
         * The SHARED plain-cache publication protocol (issue: round 18 #11): a completely
         * staged copy is published to a plain cache destination through ONE synchronous
         * section under the file lock — timestamp, prepared-and-verified provenance record,
         * atomic rename, provenance move — with the cancellation re-check INSIDE the lock
         * (issue: round 18 #23) and staging cleanup on every path. Used by the network
         * re-download route AND the SAF data writer, whose previous direct write into the
         * cache truncated bytes a concurrent promotion had already validated and staged.
         */
        internal suspend fun publishStagedCacheCopy(
            context: Context,
            file: File,
            target: IOFile,
            userDrive: UserDrive,
            staged: IOFile,
            generationResolved: Boolean = true,
        ): PublicationOutcome = try {
            // THE MONITOR PRECEDES THE FILE LOCK (issue: round 28 #11): this writer
            // creates and disposes scope-indexed operation records (sidecar BEFORE body),
            // so its (sidecar, body) publication pair must be atomic relative to the
            // sweep's orphan classification — which holds the intent monitor. The
            // previous file-lock-only section could interleave with the monitor-held
            // sweep: the index of an operation being created was deleted before its body
            // existed, and a later crash left recovery without its captured
            // flavor/kind. Consistent monitor→lock ordering with every other coordinated
            // section; no lock-order inversion is introduced (the monitor is acquired
            // FIRST, as the protocol requires).
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive, file.id) {
                // Cancellation re-check AFTER the blocking lock (issue: round 18 #23):
                // lock acquisition does not respond to coroutine cancellation — a
                // cancelled transfer that waited here must not replace a newer cache
                // and report PUBLISHED. The exception PROPAGATES (never converted
                // into an ordinary outcome) and the staged bytes are cleaned up in
                // the finally below.
                coroutineContext.ensureActive()
                // PLAIN-CACHE RECOVERY EXCLUSION (issue: round 24 #4): the plain-cache
                // publication previously had no equivalent of the coordinated
                // publication's pending-recovery exclusion — an unresolved durable
                // recovery governing this row or its path keeps those bytes out of
                // replacement here too: the staged copy is discarded by the finally and
                // the transfer fails without consuming any intent.
                if (hasUnresolvedRecovery(context, userDrive, file.id, target.absolutePath)) {
                    return@withFileLock PublicationOutcome.FAILED
                }
                runCatching {
                    staged.setLastModified(file.getLastModifiedInMilliSecond())
                    // PREPARE the staged record BEFORE the bytes are replaced (issue:
                    // round 18 #2): the registry directories are created and verified
                    // and the staged record reads back — a missing staged record is
                    // failure, never a successful move of "nothing". An UNRESOLVED
                    // generation records VERSION 0 (issue: round 24 #19/#20): no
                    // known-generation proof is issued from metadata equality.
                    if (!prepareStagedProvenance(context, staged, file, userDrive, unresolvedRecord = !generationResolved)) {
                        PublicationOutcome.FAILED
                    } else {
                        // Cancellation AFTER the registry I/O, BEFORE the first mutation
                        // (issue: round 19 #11): a cancelled publication mutates nothing —
                        // neither the destination's record nor its bytes.
                        coroutineContext.ensureActive()
                        // The OLD proof is retained for rollback (issue: round 19 #11): the
                        // atomicity of the replacement covers the PROOF as well as the bytes.
                        val oldRecordFile = provenanceFileFor(context, target)
                        val oldRecordText = if (oldRecordFile.exists()) runCatching { oldRecordFile.readText() }.getOrNull() else null
                        // RECOVERABLE OLD BYTES (issue: round 20 #10), as in the coordinated
                        // publication: the rename replaces the old bytes — a post-rename
                        // proof-move failure restores them with their retained proof. The
                        // backup carries its DURABLE sidecar record (issues: round 21 #9,
                        // #10).
                        val oldTargetMtime = if (target.exists()) target.lastModified() else 0L
                        val recoveryFile = if (target.exists()) newStagedFile(context, file) else null
                        // copyTo returns the DESTINATION file, not a success flag — the
                        // backup is validated explicitly (complete bytes) after the copy
                        // (issue: round 21 #1).
                        val recoveryOk = recoveryFile == null || runCatching {
                            target.copyTo(recoveryFile, overwrite = true)
                            recoveryFile.exists() && recoveryFile.length() == target.length()
                        }.getOrDefault(false)
                        // A backup without a computable content hash is never kept (issue:
                        // round 22 #2); its durable record carries the owner scope, file
                        // identity and the backup's immutable hash.
                        val backupHash = if (recoveryFile != null) UploadFile.sha256OrNull(recoveryFile) else null
                        // THE EXPECTED TARGET PUBLICATION (issue: round 23 #2), as in the
                        // shared publication above.
                        val expectedPublicationHash = UploadFile.sha256OrNull(staged)
                        val expectedPublicationRecordText = readPublishedProvenance(context, staged)?.recordText(target)
                        val recoveryRecorded = recoveryFile == null || (backupHash != null &&
                            writeRecoveryRecord(
                                context,
                                recoveryFile,
                                target,
                                oldRecordText,
                                oldTargetMtime,
                                file,
                                userDrive,
                                backupHash,
                                expectedPublicationHash,
                                expectedPublicationRecordText,
                            ))
                        if (!recoveryOk || !recoveryRecorded) {
                            runCatching { recoveryFile?.delete() }
                            recoveryFile?.let { runCatching { recoveryRecordFile(context, it).delete() } }
                            runCatching { staged.delete() }
                            PublicationOutcome.FAILED
                        } else {
                            // Cancellation AFTER the backup I/O, BEFORE the first mutation
                            // (issue: round 21 #22): the backup copy can be long; a cancelled
                            // publication must not proceed from it to the proof invalidation
                            // and the rename.
                            coroutineContext.ensureActive()
                            deletePublishedProvenance(context, target)
                            when {
                                staged.renameTo(target) &&
                                    movePublishedProvenance(
                                        context,
                                        staged,
                                        target,
                                        sharedWithMe = userDrive.sharedWithMe,
                                        storageKind = destinationStorageKind(context, file, target, userDrive),
                                    ) -> {
                                    runCatching { recoveryFile?.delete() }
                                    recoveryFile?.let { runCatching { recoveryRecordFile(context, it).delete() } }
                                    // TYPED UNRESOLVED (issue: round 24 #19/#20): an
                                    // unestablished generation is PUBLISHED_UNRESOLVED —
                                    // never an unconditional PUBLISHED the preview could
                                    // report as 100 or SAF accept as a proven generation.
                                    if (generationResolved) PublicationOutcome.PUBLISHED
                                    else PublicationOutcome.PUBLISHED_UNRESOLVED
                                }
                                // A FAILED RENAME NEVER DELETES THE OLD TARGET (issue: round 19
                                // #11): the rename touched nothing — the retained bytes stay, and
                                // their proof is rolled back with them. The staged file's
                                // presence distinguishes this from a rename that succeeded and
                                // then failed its proof move.
                                staged.exists() -> {
                                    if (oldRecordText != null) {
                                        runCatching { provenanceFileFor(context, target).writeText(oldRecordText) }
                                    }
                                    runCatching { recoveryFile?.delete() }
                                    recoveryFile?.let { runCatching { recoveryRecordFile(context, it).delete() } }
                                    SentryLog.e(TAG, "Failed to publish staged file to ${target.path}")
                                    PublicationOutcome.FAILED
                                }
                                else -> {
                                    // The rename replaced the old bytes and the proof move
                                    // failed — RESTORE the recoverable old bytes and their
                                    // proof (issue: round 20 #10), VERIFIED before the recovery
                                    // state is disposed (issue: round 21 #9); with nothing
                                    // recoverable (fresh publication) fail closed.
                                    val restored = recoveryFile != null && restoreRecoveryCopy(
                                        context = context,
                                        recoveryFile = recoveryFile,
                                        target = target,
                                        recordFile = recoveryRecordFile(context, recoveryFile),
                                        oldRecordText = oldRecordText,
                                        oldTargetMtimeMillis = oldTargetMtime,
                                    )
                                    if (restored) {
                                        // THE ROLLBACK SUPERSEDES THE FAILED PUBLICATION'S
                                        // PROOF (issue: round 24 #5), as in the coordinated
                                        // publication: the verified restore owns the target's
                                        // proof again — the relocation record is disposed.
                                        runCatching { proofRelocationRecordFile(context, target).delete() }
                                    }
                                    if (!restored && recoveryFile == null) {
                                        runCatching { target.delete() }
                                    }
                                    if (!restored) {
                                        SentryLog.e(TAG, "Failed to publish staged file to ${target.path}")
                                    }
                                    PublicationOutcome.FAILED
                                }
                            }
                        }
                    }
                }.onFailure { exception ->
                    if (exception is CancellationException) throw exception
                    SentryLog.e(TAG, "Staged cache publication failed for ${target.path}", exception)
                }.getOrDefault(PublicationOutcome.FAILED)
            }
            } // THE MONITOR-HELD WRITER BOUNDARY (issue: round 28 #11)
        } finally {
            // Staging cleanup on EVERY path, cancellation included (issue: round 18 #23),
            // INCLUDING the staged provenance record (issue: round 19 #11): a prepared
            // record whose bytes were never published must not linger in the registry.
            runCatching { if (staged.exists()) staged.delete() }
            runCatching { deletePublishedProvenance(context, staged) }
        }

        /**
         * Coordinated final replacement for UI-driven re-downloads (preview, download dialog)
         * (issue: round 8 #2): the response body is written to unique staging storage and only
         * the final replacement of the destination goes through publication. For an
         * offline-owned destination the shared coordinated publication re-runs ownership,
         * revision, intent and cancellation checks under the per-file locks — a preflight check
         * made before the request started (or the snapshot it took) can never authorize
         * deleting a revision an editor saved to the path in the meantime. A plain cache
         * destination has no ownership to protect, but the staged copy still makes the
         * replacement an atomic rename, so a cancelled or failed download can no longer leave
         * partial bytes — or delete existing bytes — at the destination. The destination is
         * never touched before the staged copy is complete.
         */
        /**
         * THE NO-SPIN UNRESOLVED REDRIVE GATE (issue: round 26 #20): the download endpoint
         * carries no revision token and no identity header is parsed anywhere in the client,
         * so a stable download can only ever be published EXPLICITLY UNRESOLVED (version-0
         * record) — no supported response-bound validation path exists (metadata-only
         * authority stays withdrawn). When the target already holds an explicitly unresolved
         * publication that CERTIFIES its bytes for this identity AND the row's CURRENT remote
         * generation equals the generation the record was published for, a fresh transfer
         * adds NO evidence: the identical bytes are not re-downloaded; the durable unresolved
         * state is retained and the attempt defers. Evidence that re-justifies the transfer:
         * an absent/mismatched/non-certifying record, a metadata advance in the fresh
         * details, or an indeterminate details answer (the re-drive runs and re-establishes
         * the state). This gate NEVER reports a verified completion — it only prevents the
         * identical-bytes transfer spin.
         */
        internal suspend fun unresolvedPublicationAlreadyCoversCurrentGeneration(
            context: Context,
            file: File,
            target: IOFile,
            userDrive: UserDrive,
            okHttpClient: OkHttpClient,
        ): Boolean = runCatching {
            val proof = readPublishedProvenance(context, target) ?: return@runCatching false
            if (proof.version != 0) return@runCatching false // a PROVEN record is not the spin state
            if (!(proof.identifies(file, userDrive) && proof.certifiesBytes(target))) {
                return@runCatching false
            }
            val fresh = ApiRepository.getFileDetails(
                file = File(id = file.id, driveId = userDrive.driveId),
                okHttpClient = okHttpClient,
            )
            val row = fresh.data?.takeIf { fresh.isSuccess() && it.id == file.id }
                ?: return@runCatching false // indeterminate: allow the re-drive
            // THE PERSISTED ATTEMPTED OBSERVATION DECIDES (issue: round 28 #13): the
            // complete durable remote observation — version, NULLABLE size, and the three
            // timestamps — is read from the observation sidecar written beside the record
            // (an absent sidecar means the attempt predates it: another transfer is
            // allowed). The record's own version stays zero by design (the unresolved
            // marker); the ATTEMPTED version now survives it, so a known version-number
            // advance with equal timestamps and size is no longer suppressed. This
            // evidence decides only whether another attempt can gain anything — it is
            // never a byte-generation proof.
            val observation = runCatching {
                IOFile(provenanceFileFor(context, target).path + ".obs").readText().trim().split("|")
            }.getOrNull()?.takeIf { it.size >= 7 && it[0] == "1" }
                ?: return@runCatching false
            // THE OBSERVATION'S LOCATION MUST BE THIS DESTINATION (issue: round 28 #13
            // follow-up): a sidecar re-keyed by a relocation or a record at a reused path
            // is not this destination's attempt evidence.
            val attemptedLocation = observation.drop(6).joinToString("|")
            if (attemptedLocation != target.absolutePath) return@runCatching false
            val attemptedVersion = observation[1].toIntOrNull() ?: return@runCatching false
            val attemptedSize = observation[2].toLongOrNull()
            val attemptedRevisedAt = observation[3].toLongOrNull() ?: return@runCatching false
            val attemptedLastModifiedAt = observation[4].toLongOrNull() ?: return@runCatching false
            val attemptedUpdatedAt = observation[5].toLongOrNull() ?: return@runCatching false
            row.revisedAt == attemptedRevisedAt &&
                row.lastModifiedAt == attemptedLastModifiedAt &&
                row.updatedAt == attemptedUpdatedAt &&
                row.size == attemptedSize &&
                (row.version?.number ?: 0) == attemptedVersion
        }.getOrDefault(false)

        /**
         * The post-download details re-fetch (OUTSIDE every lock) that the worker's and the
         * direct callers' generation binding runs before any publication (issues: round 22
         * #23, round 23 #22, round 24 #19/#20). An indeterminate answer (transport failure,
         * error response, identity mismatch, unestablishable comparison) is [GenerationEstablishment.FAILED],
         * never a pass; the both-unknown coarse tie is [GenerationEstablishment.COARSE_UNRESOLVED].
         */
        internal suspend fun reestablishDownloadedGeneration(
            file: File,
            userDrive: UserDrive,
            okHttpClient: OkHttpClient,
        ): GenerationEstablishment = runCatching {
            val postDownload = ApiRepository.getFileDetails(
                file = File(id = file.id, driveId = userDrive.driveId),
                okHttpClient = okHttpClient,
            )
            postDownload.data
                ?.takeIf { postDownload.isSuccess() && it.id == file.id }
                ?.let { fresh ->
                    val freshVersion = fresh.version?.number ?: 0
                    val claimedVersion = file.version?.number ?: 0
                    when {
                        // NO KNOWN-GENERATION PROOF FROM POST-DETAILS ALONE (issue: round 25
                        // #19): a positive-version match between the metadata capture and the
                        // later details response is still metadata equality — the download
                        // route carries no revision token, so the bytes' generation remains
                        // unknowable (a stale same-size byte response followed by matching
                        // current details must not receive a known proof). Published with an
                        // explicitly UNRESOLVED (version-0) proof like every other
                        // metadata-only comparison; ESTABLISHED is reserved for a
                        // response-bound identity or version-bound route.
                        freshVersion > 0 && claimedVersion > 0 &&
                            freshVersion == claimedVersion && fresh.revisedAt == file.revisedAt ->
                            GenerationEstablishment.COARSE_UNRESOLVED
                        freshVersion <= 0 && claimedVersion <= 0 &&
                            fresh.revisedAt == file.revisedAt &&
                            fresh.lastModifiedAt == file.lastModifiedAt &&
                            fresh.size == file.size ->
                            // Both sides version-less: the coarse tie is the only evidence —
                            // published, but with an explicitly UNRESOLVED (version-0) proof.
                            GenerationEstablishment.COARSE_UNRESOLVED
                        // KNOWN CLAIMED, UNKNOWN FRESH (issue: round 23 #22): a versioned
                        // claim meeting a version-less details response establishes nothing.
                        else -> GenerationEstablishment.FAILED
                    }
                } ?: GenerationEstablishment.FAILED
        }.getOrDefault(GenerationEstablishment.FAILED)

        internal suspend fun publishDownloadedCopy(
            context: Context,
            file: File,
            target: IOFile,
            userDrive: UserDrive,
            isInheritedDownload: Boolean,
            coordinated: Boolean,
            writeStaged: suspend (IOFile) -> Unit,
        ): PublicationOutcome {
            // EXPLICIT outcome return paths (round-16 compile correction): both branches are
            // total and returned explicitly — no expression-position inference over a mixed
            // if/else.
            return if (!coordinated) {
                // Plain cache publication is still SERIALIZED per file (issue: round 16 #12):
                // the rename and the provenance sidecar write are one coordinated section, so
                // two publications can no longer interleave into "R1 bytes carrying R2
                // provenance". The sidecar travels WITH the staged bytes: it is written next
                // to the staged file before the rename, the destination's old provenance is
                // invalidated BEFORE the bytes are replaced, and a provenance rename failure
                // FAILS CLOSED — the new bytes are discarded rather than left at the
                // destination without (or with stale) provenance.
                // ASYNC STAGING OUTSIDE COORDINATION (issue: round 17 #5): the suspending
                // writer (which consumes a network response body) runs WITHOUT holding any
                // ReentrantLock — a slow transfer must never retain the file lock while an
                // offline operation holds the global intent monitor waiting for it. Only the
                // SYNCHRONOUS final publication (timestamp, provenance record, atomic rename)
                // holds the file lock; the provenance record travels in the registry keyed by
                // the staged path (issue: round 17 #1) and is re-keyed after the rename, and
                // a record-commit failure fails closed.
                val staged = IOFile(target.parentFile, "${target.name}.${UUID.randomUUID()}.part")
                try {
                    val stagedComplete = runCatching { writeStaged(staged) }
                        .onFailure { exception ->
                            SentryLog.e(TAG, "Staged cache transfer failed for ${target.path}", exception)
                        }.isSuccess
                    if (!stagedComplete) {
                        return PublicationOutcome.FAILED
                    }
                    // ONE CLEANUP LIFETIME (issue: round 24 #21): staging, client
                    // acquisition, validation and publication run inside the try — a throw
                    // or cancellation between them can no longer leave the staged bytes (or
                    // a prepared record) behind; the finally cleans up every unpublished
                    // artifact while cancellation propagates.
                    // TYPED GENERATION BINDING, EVERY DIRECT CALLER (issues: round 23 #22,
                    // round 24 #19/#20): the preview, dialog and SAF publications
                    // previously published current-route response bytes under the EARLIER
                    // metadata capture's version. An ESTABLISHED generation may issue a
                    // known proof; a COARSE_UNRESOLVED one (both sides version-less, or the
                    // public-share routes that cannot re-establish the generation at all)
                    // publishes with an explicitly UNRESOLVED version-0 proof and an
                    // unresolved outcome; FAILED discards the staged bytes and fails the
                    // transfer — nothing is published, no intent/marker is consumed, and no
                    // known-version proof is issued from an unbound response.
                    val establishment = if (file.isPublicShared()) {
                        GenerationEstablishment.COARSE_UNRESOLVED
                    } else {
                        reestablishDownloadedGeneration(
                            file = file,
                            userDrive = userDrive,
                            okHttpClient = AccountUtils.getHttpClient(userDrive.userId),
                        )
                    }
                    when (establishment) {
                        GenerationEstablishment.FAILED -> PublicationOutcome.FAILED
                        else -> publishStagedCacheCopy(
                            context, file, target, userDrive, staged,
                            generationResolved = establishment == GenerationEstablishment.ESTABLISHED,
                        )
                    }
                } finally {
                    // Staging cleanup on EVERY path, cancellation included (issues: round
                    // 18 #23, round 24 #21), INCLUDING the staged provenance record: a
                    // prepared record whose bytes were never published must not linger.
                    runCatching { if (staged.exists()) staged.delete() }
                    runCatching { deletePublishedProvenance(context, staged) }
                }
            } else {
                val appContext = Realm.getApplicationContext()
                if (appContext == null) {
                    SentryLog.e(TAG, "No application context: refusing uncoordinated offline publication")
                    return PublicationOutcome.FAILED
                }
                val staged = newStagedFile(appContext, file)
                try {
                    runCatching {
                        writeStaged(staged)
                        // TYPED GENERATION BINDING, EVERY DIRECT CALLER (issues: round 23
                        // #22, round 24 #19/#20), as in the plain-cache branch: FAILED fails
                        // the transfer (no intent/marker consumed, no known-version proof
                        // from an unbound response); COARSE_UNRESOLVED — including the
                        // public-share routes that cannot re-establish the generation —
                        // publishes with an explicitly UNRESOLVED version-0 proof and an
                        // unresolved outcome.
                        val establishment = if (file.isPublicShared()) {
                            GenerationEstablishment.COARSE_UNRESOLVED
                        } else {
                            reestablishDownloadedGeneration(
                                file = file,
                                userDrive = userDrive,
                                okHttpClient = AccountUtils.getHttpClient(userDrive.userId),
                            )
                        }
                        when {
                            establishment == GenerationEstablishment.FAILED ->
                                error("Downloaded generation could not be established for file ${file.id}")
                            else -> publishStagedOfflineFile(
                                userDrive, isInheritedDownload, file, target, staged,
                                explicitUnresolvedGeneration =
                                    establishment == GenerationEstablishment.COARSE_UNRESOLVED,
                            )
                        }
                    }.onFailure { exception ->
                        SentryLog.e(TAG, "Staged offline publication failed for ${target.path}", exception)
                    }.getOrDefault(PublicationOutcome.FAILED)
                } finally {
                    // ONE CLEANUP LIFETIME (issue: round 24 #21): the cleanup runs in the
                    // finally — a cancellation escaping writeStaged, the client acquisition
                    // or the establishment can no longer skip it. Staging cleanup on EVERY
                    // path (issue: round 18 #23), INCLUDING the staged provenance record
                    // (issue: round 19 #11).
                    runCatching { if (staged.exists()) staged.delete() }
                    runCatching { deletePublishedProvenance(appContext, staged) }
                }
            }
        }


        fun getFailedDownloadWorkerOffline(context: Context) = WorkManager.getInstance(context).getWorkInfosLiveData(
            WorkQuery.Builder
                .fromTags(arrayListOf(BulkDownloadWorker::class.java.toString(), DownloadWorker.TAG))
                .addStates(arrayListOf(WorkInfo.State.FAILED))
                .build()
        )

        suspend fun isBulkDownloadWorkerRunning(context: Context): Boolean {
            // One work chain per root (issue #1902): match the shared class tag instead of a
            // single unique name.
            val workQuery = WorkQuery.Builder
                .fromTags(arrayListOf(BulkDownloadWorker::class.java.toString()))
                .addStates(arrayListOf(WorkInfo.State.RUNNING, WorkInfo.State.ENQUEUED, WorkInfo.State.BLOCKED))
                .build()
            val workInfoList = WorkManager.getInstance(context).getWorkInfos(workQuery).await()

            return workInfoList.any { it.state == WorkInfo.State.RUNNING }
        }

        fun createDownloadNotification(context: Context, id: UUID, title: String): NotificationCompat.Builder {
            val cancelPendingIntent = WorkManager.getInstance(context).createCancelPendingIntent(id)
            val cancelAction = NotificationCompat.Action(
                /* icon = */ null,
                /* title = */ context.getString(R.string.buttonCancel),
                /* intent = */ cancelPendingIntent
            )
            return context.downloadProgressNotification().apply {
                setOngoing(true)
                setContentTitle(title)
                addAction(cancelAction)
            }
        }

        fun downloadFileResponse(
            fileUrl: String,
            okHttpClient: OkHttpClient = AuthHttpClientProvider.authOkHttpClient,
            downloadInterceptor: Interceptor? = null
        ): Response {
            @OptIn(ManualAuthorizationRequired::class)
            val request = Request.Builder().url(fileUrl).headers(HttpUtils.getHeaders(contentType = null)).get().build()

            return okHttpClient.newBuilder().apply {
                downloadInterceptor?.let { interceptor -> addInterceptor(interceptor) }
            }.build().newCall(request).execute()
        }

        suspend fun downloadFileResponseAsync(
            fileUrl: String,
            okHttpClient: OkHttpClient = AuthHttpClientProvider.authOkHttpClient,
            downloadInterceptor: Interceptor? = null
        ): Response {
            @OptIn(ManualAuthorizationRequired::class)
            val request = Request.Builder().url(fileUrl).headers(HttpUtils.getHeaders(contentType = null)).get().build()

            return okHttpClient.newBuilder().apply {
                downloadInterceptor?.let { interceptor -> addInterceptor(interceptor) }
            }.build().newCall(request).await()
        }

        suspend fun saveRemoteData(
            tag: String,
            response: Response,
            outputFile: java.io.File? = null,
            outputStream: ParcelFileDescriptor.AutoCloseOutputStream? = null,
        ): Boolean {
            SentryLog.d(tag, "Save remote data to ${outputFile?.path}")
            return response.body.byteStream().buffered().use { input ->
                if (outputFile?.parentFile?.exists() == false) outputFile.parentFile?.mkdirs()
                if (outputFile?.exists() == false) outputFile.createNewFile()
                val stream = outputStream ?: outputFile?.outputStream()
                stream?.use { output ->
                    input.copyToCancellable(output)
                }
            } != null
        }

        fun downloadProgressInterceptor(
            getMostRecentLastUpdate: (() -> Long)? = null,
            onLastUpdateChange: ((lastUpdateValue: Long) -> Unit)? = null,
            onProgress: (progress: Int) -> Unit
        ) = Interceptor { chain: Interceptor.Chain ->
            val originalResponse = chain.proceed(chain.request())

            originalResponse.newBuilder()
                .body(ProgressResponseBody(originalResponse.body!!, object : ProgressResponseBody.ProgressListener {
                    override fun update(bytesRead: Long, contentLength: Long, done: Boolean) {
                        val currentSystemTimeMillis = System.currentTimeMillis()
                        val lastUpdate = getMostRecentLastUpdate?.invoke() ?: 0L
                        if (currentSystemTimeMillis - lastUpdate > MAX_INTERVAL_BETWEEN_PROGRESS_UPDATE_MS) {
                            onLastUpdateChange?.invoke(currentSystemTimeMillis)
                            // Transport progress never reaches 100 (issue: round 10 #9): the
                            // raw body read completes while publication may still be waiting
                            // for a lock, failing, or returning invalidated — dialogs treat
                            // 100 as completion and schedule their callbacks on it. 100 is
                            // reserved for confirmed usable-destination completion, emitted
                            // by each flow only after its publication outcome succeeds.
                            val progress = ((bytesRead.toFloat() / contentLength.toFloat() * 100F).toInt()).coerceAtMost(99)
                            onProgress.invoke(progress)
                        }
                    }
                })).build()
        }
    }
}

class RemoteFileException(data: String) : Exception(data)
