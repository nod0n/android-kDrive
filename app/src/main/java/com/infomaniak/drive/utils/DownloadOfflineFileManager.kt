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
import android.os.ParcelFileDescriptor
import androidx.annotation.VisibleForTesting
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
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.services.BaseDownloadWorker
import com.infomaniak.drive.data.services.BulkDownloadWorker
import com.infomaniak.drive.data.services.DownloadWorker
import com.infomaniak.drive.utils.MediaUtils.isMedia
import com.infomaniak.drive.utils.NotificationUtils.downloadProgressNotification
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.invoke
import kotlinx.coroutines.withContext
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import java.util.UUID

class DownloadOfflineFileManager(
    private val userDrive: UserDrive,
    private val filesCount: Int,
    private val downloadWorker: BaseDownloadWorker,
    private val notificationManagerCompat: NotificationManagerCompat
) {
    private var lastDownloadedFile: IOFile? = null
    private var currentFile: File? = null
    private var filesDownloaded = 0

    /**
     * The with=hash identity captured from the RAW details response, before the row refresh
     * (whose preservation rules keep the previously accepted identity). This is the identity
     * of the body the transfer is about to fetch, and only the publication seam may record it.
     */
    private var acceptedServerHash: String? = null

    private var lastUpdateProgressMillis = System.currentTimeMillis()

    /**
     * The file ids whose cache-move chain ended in a REFUSED publication (the destination
     * holds bytes the row does not own, or the publication was refused/declined after an
     * intact evidence chain): nothing was published and the row's queued intent stands, so
     * the bulk finish must not clear those rows' pending markers (the marker is the only
     * record that the work is still owed). Every completed transfer keeps the existing
     * baseline behavior. Read-only view: only the chain itself records outcomes.
     */
    private val refusedPublicationIds: MutableSet<Int> = java.util.concurrent.ConcurrentHashMap.newKeySet()

    val refusedPublicationFileIds: Set<Int> get() = refusedPublicationIds

    /**
     * The row's validation-gate state: live offline intent and the root contract's claim at
     * one read. The claim is [OfflineFolderSyncUtils.isRootOwnedCopy] — live subtree coverage
     * OR recorded root ownership ([File.offlineOwnedByRoot]) surviving a cached tree update
     * that moved the row out before this worker ran.
     */
    private data class GateState(val liveIntent: Boolean, val rootOwned: Boolean)

    suspend fun execute(
        context: Context,
        file: File,
        onProgress: (progress: Int, fileId: Int) -> Unit,
    ): ListenableWorker.Result {

        // Stale-work gate FIRST, before any filesystem effect: this worker only ever
        // downloads for live offline intent. If the current row carries none — the root was
        // unmarked, the pin withdrawn, a release applied — any queued work (initial bulk
        // chain, retry, a download enqueued before an unmark) is stale: no cache move, no
        // delete-then-transfer, no resurrection of a preserved copy.
        // ROOT OWNERSHIP IS CAPTURED HERE, at the validation gate: a real remote move (or
        // unknown parent metadata) during the details refresh below rewrites the row's
        // parent, and a cached tree update can move the row out of the root's subtree before
        // this worker even runs — in both cases coverage recomputed from the parent chain
        // silently vanishes, while the recorded ownership provenance survives. Queued
        // root-owned work keeps the root contract (the staged preservation path), never the
        // destructive baseline. Direct pins suppress the recorded claim and keep their
        // baseline behavior unchanged.
        val gate = OfflineFolderSyncUtils.certifiedFreshRealm(userDrive).use { realm ->
            FileController.getFileProxyById(file.id, customRealm = realm)?.let { row ->
                GateState(
                    liveIntent = OfflineFolderSyncUtils.hasLiveOfflineIntent(row, realm, userDrive),
                    rootOwned = OfflineFolderSyncUtils.isRootOwnedCopy(row, realm, userDrive),
                )
            }
        }
        if (gate == null || !gate.liveIntent) {
            SentryLog.d(TAG, "Stale offline download skipped for ${file.id}: no live offline intent")
            return ListenableWorker.Result.success()
        }

        val file = getFileFromRemote(context, file.id, userDrive)
        currentFile = file
        val cacheFile = file.getCacheFile(context, userDrive)
        val offlineFile = file.getOfflineFile(context, userDrive.userId)
        if (offlineFile == null) {
            SentryLog.e(TAG, "offline must not be null")
            return ListenableWorker.Result.failure()
        }

        // Root ownership is checked FIRST: the cache-move and intact branches below would
        // replace existing bytes, and a covered copy's lifecycle belongs to the root
        // contract — a stale queued download (initial bulk work, retry, or a download
        // enqueued before an unmark) must never reach them. The root maintenance pass
        // refreshes covered copies through the staged path instead. Ownership honored here is
        // the UNION of the refresh-time coverage walk and the gate-time capture (which
        // includes the recorded provenance): the gate capture is what survives a real remote
        // move that rewrote the row's parent mid-flight, and the provenance is what survives
        // a cached tree update that moved the row out before this worker ran.
        val coveredByRoot = OfflineFolderSyncUtils.certifiedFreshRealm(userDrive).use { realm ->
            OfflineFolderSyncUtils.isRootCoveredCopy(file, realm, userDrive)
        } || gate.rootOwned
        if (offlineFile.exists() && coveredByRoot) {
            OfflineFolderSyncUtils.noteExistingRootCopy(context, userDrive, file.id)
            // The preserved bytes are NOT blessed with the new remote identity: no accepted
            // transfer happened here, so no publication evidence may be recorded.
            filesDownloaded += 1
            lastDownloadedFile = offlineFile
            return ListenableWorker.Result.success()
        }

        when (moveCacheFileIfIntact(context, file, cacheFile, offlineFile)) {
            CacheMoveOutcome.MOVED -> return ListenableWorker.Result.success()
            // R1 (FHNW-caf1): the authority was declined inside the move seam itself — the
            // cache bytes are preserved where they are, nothing was published.
            CacheMoveOutcome.OWNERSHIP_STALE -> return ListenableWorker.Result.success()
            // A refusal where the row's queued intent still stands: record it centrally so
            // the bulk finish keeps the pending marker (nothing was published; the work is
            // still owed). Withdrawn intents are NOT recorded here — the marker clear must
            // never resurrect withdrawn work.
            CacheMoveOutcome.PUBLICATION_REFUSED -> {
                refusedPublicationIds.add(file.id)
                return ListenableWorker.Result.success()
            }
            CacheMoveOutcome.NOT_INTACT -> Unit // continue below
        }

        if (file.isOfflineAndIntact(offlineFile)) {
            // We can have this case for example when we try to put a lot of files at once in offline mode
            // and for some reason, the worker is cancelled after a long time, the worker is restarted.
            // Metadata equality is NOT proof of equal content: the existing bytes are preserved
            // untouched and no publication evidence is recorded for them — the root maintenance
            // pass verifies copies against the accepted basis.
            filesDownloaded += 1
            lastDownloadedFile = offlineFile
            return ListenableWorker.Result.success()
        }

        if (coveredByRoot) {
            // A covered copy never reaches the destructive path below (delete the bytes at
            // the final path, then stream a replacement in place): its initial download
            // publishes through the staged path instead — a cancellation discards owned
            // staging, and a withdrawal mid-transfer skips the atomic rename entirely
            // instead of deleting bytes at the final path after the fact. The captured
            // details identity describes the body this transfer fetches. Notification
            // progress parity is not kept here (the maintenance pass has none either); the
            // bulk chain's onFinish finalizes pending markers and the notification. The
            // gate-captured coverage keeps a root-owned copy here even when a real remote
            // move rewrote its parent during the details refresh, and the recorded
            // provenance keeps one whose row moved out before this worker ran: an edited or
            // unknown copy is then preserved by the staged decision, never destroyed.
            val outcome = runCatching {
                OfflineFolderSyncUtils.stagedRedownload(context, userDrive, file, establishedServerHash = acceptedServerHash)
            }.cancellable().onFailure { exception ->
                SentryLog.e(TAG, "Staged covered download failed for ${file.id}", exception)
            }.getOrDefault(OfflineFolderSyncUtils.StagedPublication.FAILED)
            return when (outcome) {
                OfflineFolderSyncUtils.StagedPublication.FAILED -> ListenableWorker.Result.failure()
                OfflineFolderSyncUtils.StagedPublication.PUBLISHED,
                OfflineFolderSyncUtils.StagedPublication.ADOPTED_METADATA_ONLY -> {
                    fileDownloaded(context, file.id)
                    ListenableWorker.Result.success()
                }
                OfflineFolderSyncUtils.StagedPublication.INTENT_WITHDRAWN,
                OfflineFolderSyncUtils.StagedPublication.PRESERVED_EXISTING -> {
                    filesDownloaded += 1
                    ListenableWorker.Result.success()
                }
            }
        }

        onProgress(0, file.id)

        // RESUME-9b3b item 1 (reviewer-corrected): the fresh authorization and the DELETES
        // share ONE authority hold — the former shape computed staleOrRootOwned under the
        // hold and deleted AFTER it released, which is guard-then-effect again: a marking
        // writer committing between the hold's end and the deletes still destroyed bytes.
        // The deletes are local IO (no network, no whole-file copy, no awaited coroutine),
        // so the hold stays IO-sized; the transfer starts after it releases.
        val staleOrRootOwned = OfflineCommitAuthority.withCommitAuthority {
            OfflineFolderSyncUtils.certifiedFreshRealm(userDrive).use { realm ->
                val row = FileController.getFileProxyById(file.id, customRealm = realm)
                when {
                    row == null -> true // the selection is stale: nothing to act on, nothing to delete
                    !OfflineFolderSyncUtils.hasLiveOfflineIntent(row, realm, userDrive) -> true // withdrawn since the gate
                    OfflineFolderSyncUtils.isRootOwnedCopy(row, realm, userDrive) -> true // the root contract owns the copy: preserve, no isOffline flip
                    else -> {
                        if (offlineFile.exists()) offlineFile.delete()
                        if (cacheFile.exists()) cacheFile.delete()
                        false // fresh baseline selection: the cleanup ran inside this hold
                    }
                }
            }
        }
        if (staleOrRootOwned) {
            filesDownloaded += 1
            lastDownloadedFile = offlineFile
            return ListenableWorker.Result.success()
        }

        lastDownloadedFile = offlineFile

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

    fun cleanLastDownloadedFile() {
        val file = currentFile ?: return
        // The worker's cleanup may only dispose of bytes from its own unaccepted transfer.
        // With no live offline intent left (unmark, release, unpin), the copy at the offline
        // path belongs to the user contract — preserved edited/unknown bytes — or was already
        // cleaned by the publication seam: the worker must not delete anything then, even
        // when a bulk chain's onFinish cleared the pending marker first.
        // The root contract's claim is read from the LIVE row and includes the recorded
        // ownership provenance: a refresh that moved the row out of the root's subtree
        // (unknown parent metadata, a real move) must not hand this chain's finish-cleanup
        // the authority to delete the preserved edited bytes it just kept
        // ([File.isIntactFile] compares length only, so a different-length edit reaches the
        // delete branch exactly when the ownership protection is missing).
        // FHNW-33dd item 4 (ADDENDUM A3): the guard read and the destructive effect now
        // share ONE authority hold — a root acquisition (or an intent withdrawal) between
        // the read and the delete can no longer act on newly protected or already-ended
        // bytes; the reads are local, so the hold stays IO-sized.
        OfflineCommitAuthority.withCommitAuthority {
            val liveState = OfflineFolderSyncUtils.certifiedFreshRealm(userDrive).use { realm ->
                FileController.getFileProxyById(file.id, customRealm = realm)?.let { row ->
                    OfflineFolderSyncUtils.hasLiveOfflineIntent(row, realm, userDrive) to
                        OfflineFolderSyncUtils.isRootOwnedCopy(row, realm, userDrive)
                }
            }
            if (liveState?.first != true) return@withCommitAuthority
            // A root-owned copy is never cleaned up here: its lifecycle belongs to the root
            // contract, which releases coverage without deleting bytes.
            if (liveState.second) return@withCommitAuthority
            lastDownloadedFile?.let {
                if (it.exists() && currentFile?.isIntactFile(it) == false) it.delete()
            }
        }
    }

    /** Outcome of the cache-move entry (R1, FHNW-caf1): the publication is the seam's
     *  atomic rename INSIDE the commit authority's hold; every refusal preserves the cache
     *  bytes where they are. PUBLICATION_REFUSED marks a decline where the row's own
     *  queued intent STILL STANDS (the destination was not the row's, or the publication
     *  was refused/declined after an intact evidence chain) — nothing was published and
     *  the work is still owed, so the bulk finish must not clear the pending marker.
     *  OWNERSHIP_STALE marks a decline where the intent itself is gone or moot (a
     *  withdrawn intent, a vanished row, a stale root contract, a competing mutation that
     *  owns the row now): skipping the marker clear there must never resurrect
     *  withdrawn work. */
    private enum class CacheMoveOutcome { MOVED, NOT_INTACT, PUBLICATION_REFUSED, OWNERSHIP_STALE }

    private suspend fun moveCacheFileIfIntact(
        context: Context,
        file: File,
        cacheFile: IOFile,
        offlineFile: IOFile,
    ): CacheMoveOutcome = withContext(Dispatchers.IO) {
        if (file.isObsoleteOrNotIntact(cacheFile)) return@withContext CacheMoveOutcome.NOT_INTACT

        // The mutation generation is captured BEFORE the staging copy, under an OBSERVATION
        // acquisition (an evidence capture never advances the generation — RESUME-962c item
        // 1): any participating mutation that commits while the copy runs (a competing
        // publication completing, an unmark, a tree move) advances it, and the hold below
        // refuses on the change — the pre-effect refusal covers the WHOLE copy span, not
        // just the in-hold window.
        val generationAtStart = OfflineCommitAuthority.withCommitAuthorityForEvidence {
            OfflineCommitAuthority.currentGenerationUnderAuthority()
        }

        // STAGING SHAPE (RESUME-b528-2212 item 1): the cache bytes are copied to a staging
        // temp in the offline directory OUTSIDE the commit authority — a full byte copy
        // never runs inside a hold (UI-thread writers wait on it).
        val parentDir = offlineFile.parentFile
        if (parentDir == null || (!parentDir.exists() && !parentDir.mkdirs())) {
            return@withContext CacheMoveOutcome.NOT_INTACT
        }
        val staging = java.io.File.createTempFile("offline_staging_", null, parentDir)
        try {
            cacheFile.copyTo(staging, overwrite = true)
            // Test seam (FHNW-1189 A2/E4): a regression can complete a REAL unmark or land
            // a real publication right here — after the copy, before the hold — and prove
            // the publication refuses BEFORE its first filesystem effect. Production never
            // sets it.
            preCacheMovePublicationGate?.invoke()

            // Digest evidence for the record, computed OUTSIDE the hold (of the staged
            // bytes — the rename makes them identical to the published ones).
            val stagedLength = staging.length()
            val stagedDigest = OfflineFolderSyncUtils.computeDigestForEvidence(staging)

            // DESTINATION CONTENT EVIDENCE (RESUME-962c item 4), also computed OUTSIDE the
            // hold: when the destination is occupied, the in-hold classification below must
            // prove its BYTES — not just their stat — match the row's accepted publication
            // before rename(2) may replace them. A pre-existing same-stat in-place edit
            // (original length and mtime) is invisible to every stat comparison.
            val destinationDigest = if (offlineFile.exists()) {
                OfflineFolderSyncUtils.computeDigestForEvidence(offlineFile)
            } else {
                null
            }

            // Set by the in-hold refusals where the row's queued intent still stands (the
            // destination was not the row's, or the publication was refused after an intact
            // evidence chain): the central PUBLICATION_REFUSED decision below.
            var publicationRefused = false
            val published = OfflineCommitAuthority.withCommitAuthority {
                // EVIDENCE CERTIFICATION (generation) FIRST: a participating mutation
                // committed while the staging copy ran (FHNW-1189 A2 schedule 2: a
                // competing publication completing mid-copy) — whatever it wrote at the
                // destination owns it now; this stale cache body must not replace it.
                if (OfflineCommitAuthority.currentGenerationUnderAuthority() != generationAtStart) {
                    SentryLog.w(TAG, "App state changed during the cache copy for ${file.id}; cache bytes preserved, nothing published")
                    return@withCommitAuthority null
                }
                // PRE-EFFECT AUTHORITY GATE (FHNW-1189 A2 schedule 1): the row must EXIST
                // and carry FULL live offline intent, and the root contract's reach must
                // not have ended — a row fully unmarked by a coordinated writer reads
                // offlineOwnedByRoot=false and a vanished row read false before; both
                // schedules now decline BEFORE the rename instead of being discovered by
                // the record afterwards (the forbidden move-then-delete repair).
                OfflineFolderSyncUtils.certifiedFreshRealm(userDrive).use { realm ->
                    val row = FileController.getFileProxyById(file.id, customRealm = realm)
                    when {
                        row == null -> {
                            SentryLog.w(TAG, "Row vanished before the cache move for ${file.id}; cache bytes preserved, nothing published")
                            return@withCommitAuthority null
                        }
                        !OfflineFolderSyncUtils.hasLiveOfflineIntent(row, realm, userDrive) -> {
                            SentryLog.w(TAG, "Offline intent withdrawn before the cache move for ${file.id}; cache bytes preserved, nothing published")
                            return@withCommitAuthority null
                        }
                        row.offlineOwnedByRoot && !row.isDirectlyPinned &&
                            !OfflineFolderSyncUtils.isRootCoveredCopy(row, realm, userDrive) -> {
                            SentryLog.w(TAG, "Root ownership stale before the cache move for ${file.id}; cache bytes preserved, nothing published")
                            return@withCommitAuthority null
                        }
                    }

                    // DESTINATION AUTHORITY: rename(2) replaces silently, so the
                    // destination is judged before the effect. An occupied destination is
                    // refused UNLESS it is provably THIS row's own intact accepted
                    // publication: the recorded path, a matching basis stat, AND a content
                    // proof — the destination's digest (computed outside the hold) must
                    // equal the row's recorded offlineContentDigest (RESUME-962c item 4).
                    // Stat equality alone cannot distinguish the accepted copy from a
                    // pre-existing same-stat in-place edit, and a missing digest (row or
                    // destination) cannot prove replaceable bytes either: every such case
                    // is conservatively refused — an edited, unknown, stray or competing
                    // publication's bytes are NEVER replaced (both sides preserved; no
                    // move-then-delete repair).
                    if (offlineFile.exists()) {
                        val basisIntact = row.offlineBasisModifiedAt > 0L &&
                            row.offlinePublishedPath == offlineFile.path &&
                            offlineFile.lastModified().div(1_000L) == row.offlineBasisModifiedAt &&
                            (row.offlineBasisSize == null || offlineFile.length() == row.offlineBasisSize) &&
                            row.offlineContentDigest != null &&
                            destinationDigest == row.offlineContentDigest
                        if (!basisIntact) {
                            SentryLog.w(TAG, "The cache move's destination holds bytes this row does not own for ${file.id}; both sides preserved")
                            publicationRefused = true
                            return@withCommitAuthority null
                        }
                    }

                    // STAGED-COPY EVIDENCE: the staged bytes must still be exactly what the
                    // digest described, and the digest must exist — every record-decline
                    // condition is checked BEFORE the first effect, never discovered after.
                    if (stagedDigest == null || !staging.exists() || staging.length() != stagedLength) {
                        SentryLog.w(TAG, "The staged cache copy's evidence is unusable for ${file.id}; cache bytes preserved, nothing published")
                        publicationRefused = true
                        return@withCommitAuthority null
                    }

                    // The publication mtime is set and CHECKED on the STAGING file BEFORE
                    // the rename: a refused mtime used to be ignored AFTER the replacement,
                    // publishing bytes whose mtime could never match the recorded basis
                    // (frozen as locally edited forever).
                    if (!staging.setLastModified(file.getLastModifiedInMilliSecond())) {
                        SentryLog.w(TAG, "Staging timestamp refused for ${file.id}; cache bytes preserved, nothing published")
                        publicationRefused = true
                        return@withCommitAuthority null
                    }

                    if (!staging.renameTo(offlineFile)) {
                        SentryLog.w(TAG, "Cache-move publication refused for ${file.id}; cache bytes preserved")
                        publicationRefused = true
                        return@withCommitAuthority null
                    }
                    // The moved bytes come from the cache, not from this transfer: their
                    // content is NOT proven to match the current remote identity (metadata
                    // equality is not proof of equal content), so the seam records the basis
                    // WITHOUT the server hash — the next maintenance pass then performs one
                    // verified body fetch and completes it. Reentrant-safe: the record joins
                    // this same hold.
                    OfflineFolderSyncUtils.recordAcceptedPublicationInHold(
                        userDrive = userDrive,
                        remoteFile = file,
                        offlineFile = offlineFile,
                        acceptedServerHash = null,
                        acceptedDigest = stagedDigest,
                        acceptedLength = stagedLength,
                    )
                }
            }

            if (published == OfflineFolderSyncUtils.PublicationOutcome.PUBLISHED) {
                finishCompleted(file, offlineFile, context, published)
                lastDownloadedFile = offlineFile
                cacheFile.delete()
                return@withContext CacheMoveOutcome.MOVED
            }
            if (published != null) {
                // The record declined (defense-in-depth: a coordinated writer cannot commit
                // inside the hold — this branch only fires for a bypassing mutation). The
                // refused publication's bytes at the offline path are unwanted and
                // unrecorded; the CACHE bytes are PRESERVED (a claimed refusal never
                // destroys them).
                finishCompleted(file, offlineFile, context, published)
            }
            // The central outcome decision: a refusal where the row's queued intent still
            // stands (destination not the row's, evidence/timestamp/rename refused, record
            // declined) preserves the pending marker at the bulk finish; every other
            // decline (withdrawn intent, vanished row, stale ownership, a competing
            // mutation) leaves the marker clear to the finish as before.
            return@withContext if (publicationRefused || published != null) {
                CacheMoveOutcome.PUBLICATION_REFUSED
            } else {
                CacheMoveOutcome.OWNERSHIP_STALE
            }
        } finally {
            if (staging.exists()) staging.delete()
        }
    }

    private suspend fun getFileFromRemote(
        context: Context,
        fileId: Int,
        userDrive: UserDrive = UserDrive(),
    ): File {
        // with=hash is requested on the details call that drives the download, so the
        // publication seam can record the server content identity at no extra request. The
        // hash is captured from the RAW response: the row refresh below preserves the
        // previously accepted identity by design.
        val fileDetails = Dispatchers.IO {
            ApiRepository.getFileDetailsWithHash(File(id = fileId, driveId = userDrive.driveId))
        }
        val remoteFile = fileDetails.data
        val file = if (fileDetails.isSuccess() && remoteFile != null) {
            acceptedServerHash = remoteFile.serverContentHash
            FileController.getRealmInstance(userDrive).use { realm ->
                FileController.updateExistingFile(newFile = remoteFile, realm = realm)
            }
            remoteFile
        } else {
            if (fileDetails.error?.exception is NetworkException) throw UploadTask.NetworkException()

            val translateError = fileDetails.translateError()
            val responseGsonType = object : TypeToken<ApiResponse<File>>() {}.type
            val translatedErrorText = context.getString(translateError)
            val responseJson = ApiController.gson.toJson(fileDetails, responseGsonType)
            throw RemoteFileException("$responseJson $translatedErrorText")
        }

        return file
    }

    private suspend fun startOfflineDownload(
        context: Context,
        file: File,
        offlineFile: java.io.File,
        onProgress: (progress: Int, fileId: Int) -> Unit,
    ): ListenableWorker.Result = withContext(Dispatchers.IO) {

        val okHttpClient = AccountUtils.getHttpClient(userDrive.userId, null)

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

        // RESUME-9b3b item 5 (reviewer-corrected): the body used to stream DIRECTLY to the
        // final path, so a root acquired AFTER the pre-transfer hold while the stream was
        // pending was clobbered by the in-place write and its publication recorded over the
        // root contract's row. STAGED FINAL PUBLICATION: the network work stays OUTSIDE the
        // authority (the body streams to a staging temp in the offline directory), and ONE
        // short hold re-derives the CURRENT contract at publication from a fresh in-hold
        // read — an acquisition during the stream is visible there and the staged body is
        // discarded instead of published.
        val parentDir = offlineFile.parentFile
        if (parentDir == null || (!parentDir.exists() && !parentDir.mkdirs())) {
            return@withContext ListenableWorker.Result.failure()
        }
        val staging = try {
            java.io.File.createTempFile("offline_stream_staging_", null, parentDir)
        } catch (stagingFailure: java.io.IOException) {
            SentryLog.e(TAG, "Could not create the download staging file for ${file.id}", stagingFailure)
            return@withContext ListenableWorker.Result.failure()
        }
        try {
            runCatching {
                val remoteDataHasBeenSaved = saveRemoteData(downloadWorker.workerTag(), response, staging)
                if (!remoteDataHasBeenSaved || !staging.exists()) {
                    return@runCatching ListenableWorker.Result.failure()
                }

                // Digest + timestamp evidence computed OUTSIDE the hold (a hold never
                // digests). The staged bytes carry the remote mtime BEFORE the rename, so
                // the recorded basis never describes an mtime the bytes do not carry.
                val stagedLength = staging.length()
                val stagedDigest = OfflineFolderSyncUtils.computeDigestForEvidence(staging)
                staging.setLastModified(file.getLastModifiedInMilliSecond())

                when (publishStagedDownload(file, offlineFile, staging, stagedDigest, stagedLength)) {
                    StreamPublication.PUBLISHED -> {
                        onProgress(100, file.id)
                        finishCompleted(file, offlineFile, context, OfflineFolderSyncUtils.PublicationOutcome.PUBLISHED)
                        ListenableWorker.Result.success()
                    }
                    // The intent was withdrawn (or the row vanished) during the transfer:
                    // the staged bytes are unwanted and are discarded WITHOUT ever touching
                    // the final path — no unrecorded copy lingers anywhere, and the unmark's
                    // post-condition (copy gone) was already restored by the in-hold cleanup.
                    StreamPublication.CONTRACT_ENDED -> ListenableWorker.Result.success()
                    // A root acquisition landed after the pre-transfer hold, while the
                    // stream was pending: the copy's lifecycle belongs to the root contract
                    // now — the staged body is discarded, the final path stands untouched,
                    // and no accepted basis is recorded over the root contract's row.
                    StreamPublication.ROOT_OWNED_NOW -> {
                        filesDownloaded += 1
                        ListenableWorker.Result.success()
                    }
                    StreamPublication.FAILED -> ListenableWorker.Result.failure()
                }
            }.cancellable().getOrElse { throw it }
        } finally {
            // Every refused or failed transfer discards ITS OWN staged bytes; the final
            // path is never cleaned up here (it was cleaned inside the pre-transfer hold,
            // and bytes published there since by another writer are not this transfer's).
            if (staging.exists()) staging.delete()
        }
    }

    /** Outcome of the staged download's final publication. */
    private enum class StreamPublication { PUBLISHED, CONTRACT_ENDED, ROOT_OWNED_NOW, FAILED }

    /**
     * The staged transfer's final publication: ONE authority hold re-derives the current
     * contract from a fresh in-hold read, then either publishes with one atomic rename plus
     * the accepted-basis record ([OfflineFolderSyncUtils.recordAcceptedPublicationInHold] —
     * the digest evidence was computed outside), or leaves the staged bytes to the finally
     * cleanup without touching the final path. Local IO only — the network and the
     * whole-file digest stayed outside the hold. The record's own withdrawn outcome (a
     * bypassing mutation; the hold excludes every coordinated writer) restores the unmark
     * post-condition on THESE just-published bytes, exactly as the former post-record
     * cleanup did.
     */
    private fun publishStagedDownload(
        file: File,
        offlineFile: java.io.File,
        staging: java.io.File,
        stagedDigest: String?,
        stagedLength: Long,
    ): StreamPublication = OfflineCommitAuthority.withCommitAuthority {
        OfflineFolderSyncUtils.certifiedFreshRealm(userDrive).use { realm ->
            val row = FileController.getFileProxyById(file.id, customRealm = realm)
            when {
                row == null -> StreamPublication.CONTRACT_ENDED
                !OfflineFolderSyncUtils.hasLiveOfflineIntent(row, realm, userDrive) -> StreamPublication.CONTRACT_ENDED
                OfflineFolderSyncUtils.isRootOwnedCopy(row, realm, userDrive) -> StreamPublication.ROOT_OWNED_NOW
                else -> {
                    if (!staging.renameTo(offlineFile)) return@withCommitAuthority StreamPublication.FAILED
                    when (OfflineFolderSyncUtils.recordAcceptedPublicationInHold(
                        userDrive = userDrive,
                        remoteFile = file,
                        offlineFile = offlineFile,
                        acceptedServerHash = acceptedServerHash,
                        acceptedDigest = stagedDigest,
                        acceptedLength = stagedLength,
                    )) {
                        OfflineFolderSyncUtils.PublicationOutcome.PUBLISHED,
                        OfflineFolderSyncUtils.PublicationOutcome.NOT_RECORDED,
                        -> StreamPublication.PUBLISHED // published; a failed digest stays unrecorded (UNKNOWN), preserved by maintenance
                        OfflineFolderSyncUtils.PublicationOutcome.INTENT_WITHDRAWN -> {
                            if (offlineFile.exists()) offlineFile.delete()
                            StreamPublication.CONTRACT_ENDED
                        }
                    }
                }
            }
        }
    }

    /**
     * The post-record continuation, shared by the direct-download and cache-move
     * publication paths: on a withdrawn record the just-written bytes are unwanted and
     * unrecorded and are removed (the cache-move path keeps its cache bytes — a claimed
     * refusal never destroys them); an accepted publication is scanned into MediaStore.
     */
    private fun finishCompleted(
        file: File,
        offlineFile: java.io.File,
        context: Context,
        outcome: OfflineFolderSyncUtils.PublicationOutcome,
    ) {
        if (outcome == OfflineFolderSyncUtils.PublicationOutcome.INTENT_WITHDRAWN) {
            runCatching { if (offlineFile.exists()) offlineFile.delete() }
                .onFailure { SentryLog.e(TAG, "Could not clean up the withdrawn offline copy of ${file.id}", it) }
        }
        if (file.isMedia()) MediaUtils.scanFile(context, offlineFile)
        fileDownloaded(context, file.id)
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
        /**
         * Test seam (FHNW-1189 A2/E4): invoked after the staging copy, before the
         * publication hold — a regression completes a real withdrawal or lands a real
         * publication in exactly that span. Production never sets it.
         */
        @VisibleForTesting
        internal var preCacheMovePublicationGate: (() -> Unit)? = null

        private const val MAX_INTERVAL_BETWEEN_PROGRESS_UPDATE_MS = 1000L
        private val TAG = DownloadOfflineFileManager::class.java.simpleName

        fun getFailedDownloadWorkerOffline(context: Context) = WorkManager.getInstance(context).getWorkInfosLiveData(
            WorkQuery.Builder
                .fromUniqueWorkNames(arrayListOf(BulkDownloadWorker.TAG, DownloadWorker.TAG))
                .addStates(arrayListOf(WorkInfo.State.FAILED))
                .build()
        )

        suspend fun isBulkDownloadWorkerRunning(context: Context): Boolean {
            val workQuery = WorkQuery.Builder
                .fromUniqueWorkNames(arrayListOf(BulkDownloadWorker.TAG))
                .addStates(arrayListOf(WorkInfo.State.RUNNING, WorkInfo.State.ENQUEUED, WorkInfo.State.BLOCKED))
                .build()
            val workInfoList = WorkManager.getInstance(context).getWorkInfos(workQuery).await()

            return workInfoList.isNotEmpty() && workInfoList.first().state == WorkInfo.State.RUNNING
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
                            val progress = (bytesRead.toFloat() / contentLength.toFloat() * 100F).toInt()
                            onProgress.invoke(progress)
                        }
                    }
                })).build()
        }
    }
}

class RemoteFileException(data: String) : Exception(data)
