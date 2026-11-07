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
package com.infomaniak.drive.data.sync

import android.content.Context
import android.os.Build
import android.provider.MediaStore
import com.infomaniak.core.network.models.exceptions.NetworkException
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.PhotoPairing
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.AccountUtils
import io.realm.Realm
import io.realm.RealmConfiguration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.withContext
import java.util.Date

/**
 * Orchestrates photo trash propagation (opt-in) on top of [PhotoTrashRules]. Only pairings
 * recorded from an authoritative upload response participate; only trash (never permanent
 * deletion) is propagated; every destructive action is preceded by fresh checks on BOTH
 * sides, and uncertainty is never treated as deletion.
 *
 * The local state read (MediaStore) and the cloud->device consent flow are Android-bound;
 * the local->remote pass runs at the end of the existing upload worker pass.
 */
object PhotoTrashController {

    private const val TAG = "PhotoTrashController"

    /** Pairings whose cloud->device trash just became pending user consent (UI shows the dialog). */
    val pendingConsents = MutableSharedFlow<PhotoPairing>(extraBufferCapacity = 4)

    /** All pairings of the current user, detached. */
    fun getPairings(customRealmConfig: RealmConfiguration? = null): List<PhotoPairing> {
        val block: (Realm) -> List<PhotoPairing> = { realm ->
            realm.where(PhotoPairing::class.java)
                .equalTo(PhotoPairing::userId.name, AccountUtils.currentUserId)
                .findAll()
                .map { realm.copyFromRealm(it, 0) }
        }
        return customRealmConfig?.let { Realm.getInstance(it).use(block) } ?: UploadFile.getRealmInstance().use(block)
    }

    fun getPairing(uri: String, customRealmConfig: RealmConfiguration? = null): PhotoPairing? {
        return getPairings(customRealmConfig).firstOrNull { it.uri == uri }
    }

    /**
     * Records the authoritative pairing of a completed upload: the finish-session response's
     * created server file bound to the uploaded local source. Bases are bound from the
     * ACTUAL upload and ONE MediaStore observation at pairing time — this is the activation
     * baseline's correct point (a trash after pairing is a transition, never a baseline),
     * and it keeps every later local comparison same-producer (a fallback producer's
     * timestamp precision used to latch conflicts for unchanged content). A later re-read
     * never re-blesses changed content: the recorded digest is re-verified only at the
     * destructive boundary. A pairing for a reused URI is replaced (the newest upload
     * identity wins); unknown evidence is recorded as null and preserves.
     *
     * [uploadedContentDigest] is the digest of the bytes the upload actually transferred,
     * captured by [com.infomaniak.drive.data.api.UploadTask] while the bytes were read for
     * the transfer (the direct-upload stream; the empty-body identity for empty uploads).
     * When the transfer produced no digest (chunked/resumed uploads read PARALLEL per-chunk
     * streams whose digests cannot be composed into a whole-body identity), the pairing
     * binds ONLY through equivalent validated server-to-source evidence: the created file's
     * server hash is over the bytes the server assembled from the upload, so a fresh local
     * read that MATCHES it establishes the source-to-server identity — a mismatch (the
     * source changed since the transfer, e.g. a same-metadata replacement before pairing)
     * records null and preserves. There is NO unconditional post-upload sample: a sample
     * alone cannot tell uploaded content from replaced content.
     * [remoteFile]'s server identity (hash/size/lastModifiedAt when the response established
     * them) becomes the REMOTE basis.
     */
    fun recordPairing(
        context: Context,
        uploadFile: UploadFile,
        remoteFile: File?,
        uploadedContentDigest: String?,
        customRealmConfig: RealmConfiguration? = null,
    ) {
        val remoteFileId = remoteFile?.id?.takeIf { it > 0 } ?: return
        val local = queryLocalMedia(context, uploadFile.uri)
        val baseline = when {
            // An ABSENT or unreadable row is UNKNOWN — it never starts as a
            // present-and-not-trashed baseline (a trashed-then-vanished item or an
            // unmounted volume would be misclassified as a fresh post-pairing
            // transition). It stays pending; a later capture re-observes.
            local == null || !local.exists -> null
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.R -> local.isTrashed
            else -> false // trash state is not observable below API 30; the feature is inert there
        }
        val localDigest = when {
            uploadedContentDigest != null -> uploadedContentDigest
            // Server-to-source validation (chunked/resumed/any transfer without a stream
            // digest): the server identity is parsed STRICTLY (only the documented
            // "xxh3:" + 16-hex form is supported — an unknown algorithm preserves), then
            // ONE single-stream pass over the source computes BOTH receipts (the server
            // algorithm's digest AND the local sha-256 receipt) from the same snapshot.
            // The binding lands only when the same-pass server-algorithm digest matches:
            // two separate reads could validate A and record a replaced B.
            remoteFile?.serverContentHash != null ->
                when (val parsed = PhotoTrashServerHash.parse(remoteFile.serverContentHash)) {
                    is PhotoTrashServerHash.Parsed.Supported ->
                        digestContentValidatedByServerHash(context, uploadFile.uri, parsed.canonical)
                    is PhotoTrashServerHash.Parsed.Absent, is PhotoTrashServerHash.Parsed.Unsupported -> null
                }
            else -> null
        }
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, uploadFile.uri)
                    .findFirst()
                    ?.deleteFromRealm()
                mutableRealm.createObject(PhotoPairing::class.java, uploadFile.uri)?.apply {
                    userId = uploadFile.userId
                    driveId = uploadFile.driveId
                    this.remoteFileId = remoteFileId
                    fileName = uploadFile.fileName
                    localSizeAtPairing = local?.takeIf { it.exists }?.size ?: uploadFile.fileSize
                    localModifiedAtPairing = local?.takeIf { it.exists }?.lastModified ?: uploadFile.getLastModified()
                    localContentDigest = localDigest
                    remoteContentHash = remoteFile?.serverContentHash
                    baselineTrashed = baseline
                    lastObservedTrashed = baseline ?: false
                    pairedAt = Date()
                    state = PhotoPairing.State.ACTIVE.name
                }
            }
        }
    }

    /** Drops pairings and any pending destructive state when the option is turned off. */
    fun removeAllPairings() {
        UploadFile.getRealmInstance().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java).findAll().deleteAllFromRealm()
            }
        }
    }

    /**
     * Captures a pairing's activation baseline from the CURRENT MediaStore trashed-state —
     * the retry/recovery path for rows left pending (their pairing-time observation was
     * unavailable) and for pre-existing pairings at opt-in. An observation that arrives only
     * LATE cannot tell a pre-activation trash from a post-activation delete: a TRUE baseline
     * captured here is therefore recorded as a visible [PhotoPairing.State.INCONCLUSIVE]
     * state, never silently relabeled as history. An unavailable observation stays pending
     * and is retried on a later pass. Never call this on a blocking UI thread.
     */
    fun captureActivationBaseline(context: Context, pairing: PhotoPairing, customRealmConfig: RealmConfiguration? = null): Boolean {
        val local = queryLocalMedia(context, pairing.uri)
        val baseline = when {
            // An ABSENT or unreadable row is UNKNOWN: the baseline is reset to PENDING
            // (never assumed not-trashed) so a later capture re-observes when the row is
            // readable — an old populated baseline must not survive as eligible through
            // an observation that could not actually see the item.
            local == null || !local.exists -> null
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.R -> local.isTrashed
            else -> false // trash state is not observable below API 30; baseline: not trashed
        }
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, pairing.uri)
                    .findFirst()?.let { row ->
                        row.baselineTrashed = baseline
                        if (baseline != null) {
                            row.lastObservedTrashed = baseline
                            if (baseline && row.state == PhotoPairing.State.ACTIVE.name) {
                                row.state = PhotoPairing.State.INCONCLUSIVE.name
                            }
                        }
                    }
            }
        }
        return baseline ?: false
    }

    /**
     * Activation point for the opt-in (the settings save runs it BEFORE the enabled flag is
     * written, on its existing IO context — a pass can only start once the flag is on, so
     * no pass can race this capture into propagating against a stale baseline): captures the
     * real MediaStore trashed-state for EVERY pairing of the current user, INCLUDING rows
     * whose pairing-time baseline predates this activation. An item trashed while the
     * option was off must not reclassify as a new post-activation transition: its late-TRUE
     * capture at activation is a visible [PhotoPairing.State.INCONCLUSIVE], never silent
     * history and never propagation. Only THIS activation moment re-baselines populated
     * rows; the regular pass still only fills null baselines. No blocking IO on the
     * settings UI thread.
     */
    fun captureActivationBaselines(context: Context, customRealmConfig: RealmConfiguration? = null) {
        getPairings(customRealmConfig).forEach { pairing ->
            captureActivationBaseline(context, pairing, customRealmConfig)
        }
    }

    /**
     * One local pass over the current user's pairings: captures pending baselines, then
     * decides and executes local->remote transitions, and doubles as the cloud->device
     * catch-up. Returns the number of propagated trashes.
     */
    suspend fun runLocalReconciliation(context: Context, customRealmConfig: RealmConfiguration? = null): Int = withContext(Dispatchers.IO) {
        if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse()) return@withContext 0
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return@withContext 0

        var propagated = 0
        val okHttpClient = AccountUtils.getHttpClient(AccountUtils.currentUserId, null)
        getPairings(customRealmConfig).forEach { pairing ->
            ensureActive()
            ensurePairingBaseline(context, pairing, customRealmConfig)

            val local = queryLocalMedia(context, pairing.uri)
            val remote = queryRemoteState(pairing, okHttpClient)
            val decision = PhotoTrashRules.decideLocalPropagation(
                baselineTrashed = pairing.baselineTrashed,
                lastObservedTrashed = pairing.lastObservedTrashed,
                lastLocalTrashReconciledAt = pairing.lastLocalTrashReconciledAt,
                local = local,
                remote = remote,
                pairingSize = pairing.localSizeAtPairing,
                pairingModifiedAt = pairing.localModifiedAtPairing,
                localDigestBound = pairing.localContentDigest != null,
                remoteBasisHash = pairing.remoteContentHash,
            )

            when (decision) {
                is PhotoTrashRules.LocalDecision.PropagateTrash -> {
                    // Destructive boundary: the recorded upload digest (when known) is
                    // re-verified against the CURRENT local bytes — the only local re-read
                    // that exists, so a changed source is caught here or never. A mismatch
                    // is a conflict, never an authority; an unreadable or unbound source is
                    // INCONCLUSIVE — unknown evidence preserves, it never authorizes.
                    when (verifyLocalDigestAtBoundary(context, pairing)) {
                        DigestBoundary.MISMATCH -> markConflict(pairing.uri, PhotoPairing.State.CONFLICT_LOCAL_CHANGED, customRealmConfig)
                        DigestBoundary.UNREADABLE, DigestBoundary.NOT_BOUND -> markInconclusive(pairing.uri, customRealmConfig)
                        DigestBoundary.VERIFIED -> {
                            // FINAL boundary recheck, immediately before the destructive
                            // call: the pass captured its row list and ran network/digest
                            // IO since its top-of-pass gate — the option could have been
                            // switched off or the account switched DURING the pass. An
                            // unauthorized pass stops WITHOUT marking any state and
                            // WITHOUT calling the API; the next authorized pass re-decides.
                            val stillAuthorized = UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled == true &&
                                pairing.userId == AccountUtils.currentUserId
                            val remoteFile = remote.file
                            when {
                                !stillAuthorized -> return@withContext propagated
                                remoteFile == null -> markInconclusive(pairing.uri, customRealmConfig)
                                ApiRepository.deleteFile(remoteFile).isSuccess() -> {
                                    markLocalTrashReconciled(pairing.uri, observedTrashed = true, customRealmConfig = customRealmConfig)
                                    propagated++
                                }
                                else -> markInconclusive(pairing.uri, customRealmConfig)
                            }
                        }
                    }
                }
                is PhotoTrashRules.LocalDecision.RecordObservedTrashed -> markLocalTrashReconciled(pairing.uri, observedTrashed = true, customRealmConfig = customRealmConfig)
                is PhotoTrashRules.LocalDecision.MarkRestored -> markLocalTrashReconciled(pairing.uri, observedTrashed = false, customRealmConfig = customRealmConfig)
                is PhotoTrashRules.LocalDecision.Conflict -> markConflict(pairing.uri, decision.state, customRealmConfig)
                is PhotoTrashRules.LocalDecision.Inconclusive -> markInconclusive(pairing.uri, customRealmConfig)
                is PhotoTrashRules.LocalDecision.None -> Unit
            }

            // The same pass doubles as the cloud->device catch-up. A remote trash that no
            // MQTT event delivered is armed here for consent; and because the app only ever
            // RECEIVES FILE_TRASH events (a restore has no event), a pending request or a
            // stored denial EXPIRES here, on the pass's own observation that the remote is
            // no longer trashed.
            if (remote.reachable) {
                if (!remote.isTrashed) {
                    if (pairing.remoteTrashPendingSince > 0L || pairing.state == PhotoPairing.State.DENIED.name) {
                        clearRemoteTrashPending(pairing.uri, customRealmConfig)
                    }
                } else if (pairing.remoteTrashPendingSince == 0L) {
                    val remoteDecision = PhotoTrashRules.decideRemotePropagation(
                        pendingSince = pairing.remoteTrashPendingSince,
                        denied = pairing.state == PhotoPairing.State.DENIED.name,
                        local = local,
                        remote = remote,
                        pairingSize = pairing.localSizeAtPairing,
                        pairingModifiedAt = pairing.localModifiedAtPairing,
                        localDigestBound = pairing.localContentDigest != null,
                        remoteBasisHash = pairing.remoteContentHash,
                    )
                    if (remoteDecision is PhotoTrashRules.RemoteDecision.RequestConsent) {
                        armRemoteTrashPending(pairing, customRealmConfig)?.let { armed -> pendingConsents.tryEmit(armed) }
                    }
                }
            }
        }
        propagated
    }

    /**
     * Cloud->device side: called for a remote trash event (MQTT or catch-up). Verifies the
     * current remote state, checks the local content binding and either arms the pending
     * consent state or clears a stale one. The consent dialog itself is UI: it is launched
     * only through [pollNextPendingConsent]'s launch-time revalidation, never directly from
     * an event.
     */
    suspend fun onRemoteTrashEvent(
        context: Context,
        remoteFileId: Int,
        driveId: Int,
        customRealmConfig: RealmConfiguration? = null,
    ): PhotoPairing? {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return null
        val pairing = getPairings(customRealmConfig).firstOrNull {
            it.remoteFileId == remoteFileId && it.driveId == driveId && it.userId == AccountUtils.currentUserId
        } ?: return null
        if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse()) return null

        val okHttpClient = runCatching { AccountUtils.getHttpClient(AccountUtils.currentUserId, null) }.getOrNull()
        val remote = okHttpClient?.let { queryRemoteState(pairing, it) } ?: return null
        val local = queryLocalMedia(context, pairing.uri)

        return when (val decision = PhotoTrashRules.decideRemotePropagation(
            pendingSince = pairing.remoteTrashPendingSince,
            denied = pairing.state == PhotoPairing.State.DENIED.name,
            local = local,
            remote = remote,
            pairingSize = pairing.localSizeAtPairing,
            pairingModifiedAt = pairing.localModifiedAtPairing,
            localDigestBound = pairing.localContentDigest != null,
            remoteBasisHash = pairing.remoteContentHash,
        )) {
            is PhotoTrashRules.RemoteDecision.RequestConsent -> armRemoteTrashPending(pairing, customRealmConfig)
            is PhotoTrashRules.RemoteDecision.ClearPending -> {
                clearRemoteTrashPending(pairing.uri, customRealmConfig)
                null
            }
            is PhotoTrashRules.RemoteDecision.Conflict -> {
                markConflict(pairing.uri, decision.state, customRealmConfig)
                null
            }
            is PhotoTrashRules.RemoteDecision.Inconclusive, is PhotoTrashRules.RemoteDecision.None -> null
        }
    }

    /**
     * THE SCOPED OPERATION IDENTITY of one cloud->device consent request: the pairing
     * (uri + owner account + drive + remote file) plus the consent stamp of the exact
     * outstanding request. The identity is captured at the poll, carried through the
     * pre-launch revalidation and persisted across activity recreation, and a dialog
     * result applies only when the LIVE row still matches it in full — never to a newer
     * re-arm, a replaced pairing, or another account's row.
     */
    data class ConsentRequest(
        val uri: String,
        val userId: Int,
        val driveId: Int,
        val remoteFileId: Int,
        val pendingSince: Long,
    )

    /**
     * The pre-launch revalidation of a captured [ConsentRequest] against the LIVE row and
     * gates, closing the APPLICATION-side window between a poll return and the dialog
     * launch (a switch/disable that happens during this check is caught; the platform's
     * own approval-time mutation race stays unclosable and disclosed).
     */
    fun isConsentStillPending(request: ConsentRequest, customRealmConfig: RealmConfiguration? = null): Boolean {
        if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse()) return false
        if (request.userId != AccountUtils.currentUserId) return false
        return withPairingRealm(customRealmConfig) { realm ->
            realm.where(PhotoPairing::class.java)
                .equalTo(PhotoPairing::uri.name, request.uri)
                .findFirst()?.let { row ->
                    row.userId == request.userId &&
                        row.driveId == request.driveId &&
                        row.remoteFileId == request.remoteFileId &&
                        row.remoteTrashPendingSince == request.pendingSince &&
                        row.state != PhotoPairing.State.DENIED.name
                } ?: false
        }
    }

    /**
     * Records the user's consent result for a pending cloud->device trash. The result is
     * keyed to the SAME [ConsentRequest]: it applies only when the live row still matches
     * the request's full identity (account, drive, remote file, consent stamp) AND belongs
     * to the current account — a duplicate delivery, a result that raced a re-arm, a
     * replaced pairing, or one from a switched account is dropped, never applied to a
     * newer request. A grant re-activates the pairing; a DECLINE records the denial so the
     * same remote trash is not re-asked on every pass — the denial expires when the remote
     * is observed restored (the maintenance pass or [pollNextPendingConsent] clears the
     * pending state and resets a DENIED row to ACTIVE) or the local content changes (a
     * conflict state re-evaluates the pairing).
     */
    fun onRemoteTrashConsentResult(request: ConsentRequest, granted: Boolean, customRealmConfig: RealmConfiguration? = null) {
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, request.uri)
                    .findFirst()?.let { row ->
                        if (row.userId != AccountUtils.currentUserId) return@let
                        if (row.userId != request.userId ||
                            row.driveId != request.driveId ||
                            row.remoteFileId != request.remoteFileId ||
                            row.remoteTrashPendingSince != request.pendingSince
                        ) {
                            return@let
                        }
                        row.remoteTrashPendingSince = 0L
                        if (granted) {
                            row.lastRemoteTrashAppliedAt = System.currentTimeMillis()
                            row.state = PhotoPairing.State.ACTIVE.name
                        } else {
                            row.state = PhotoPairing.State.DENIED.name
                        }
                    }
            }
        }
    }

    /**
     * The durable serialized consent drain: the EXISTING pending row state
     * ([PhotoPairing.remoteTrashPendingSince]) IS the queue, so armed requests survive
     * process death and are re-surfaced on every app entry without any in-memory collector.
     * Returns the OLDEST armed request that is STILL valid right now, as its full-identity
     * [ConsentRequest] — fully revalidated at
     * launch time (opt-in still on, fresh remote state still trashed with an unchanged bound
     * identity, local content present with an UNCHANGED digest — the destructive boundary
     * check of the cloud->device direction). The rules are re-run AS IF arming now
     * (pendingSince = 0): the outstanding stamp is already this request's identity, and its
     * validity is decided from fresh evidence only. Every other outcome is resolved in
     * place (a remote restore expires the pending state and any denial; a changed local
     * source is a conflict; unreadable or unbound content is inconclusive; a local item
     * already trashed makes the request moot) and the next row is polled, so a drain hands
     * the caller at most ONE full-identity [ConsentRequest] and a stale request can never
     * reach the user. An unreachable remote is transient: the row stays armed for a later
     * entry. null = nothing to ask right now.
     *
     * THE UNCLOSEABLE APPROVAL RACE (disclosed, not closable app-side): arming a consent is
     * a permission grant — Android applies the MediaStore trash at the moment the user
     * APPROVES the system dialog, with no app pre-apply callback. The checks above are the
     * last app-controlled ones; content replaced after them (same metadata) would be
     * trashed by the approval itself. No lock or recheck can close a system-dialog window;
     * the residual window is bounded only by this fresh digest check at launch.
     */
    suspend fun pollNextPendingConsent(context: Context, customRealmConfig: RealmConfiguration? = null): ConsentRequest? =
        withContext(Dispatchers.IO) {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return@withContext null
            if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse()) return@withContext null

            val okHttpClient = runCatching { AccountUtils.getHttpClient(AccountUtils.currentUserId, null) }.getOrNull()
            val armedRows = getPairings(customRealmConfig)
                .filter { it.remoteTrashPendingSince > 0L && it.state != PhotoPairing.State.DENIED.name }
                .sortedBy { it.remoteTrashPendingSince }
            for (pairing in armedRows) {
                ensureActive()
                // LIVE recheck per row: the opt-in/account gate ran once before the IO
                // block, and this row's revalidation involves network + digest IO — the
                // option or account could have changed in between. A changed gate ends
                // the drain: nothing is returned, nothing is marked.
                if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse()) return@withContext null
                if (pairing.userId != AccountUtils.currentUserId) return@withContext null
                // Revalidation evaluates the request AS IF arming it now (pendingSince = 0):
                // the rules' "consent already outstanding -> None" guard exists for arming
                // paths, not for validating an outstanding one. An unreachable remote is a
                // TRANSIENT condition: the request stays armed and the next row is tried —
                // a network failure never rewrites a row's state.
                val remote = okHttpClient?.let { queryRemoteState(pairing, it) }
                if (remote?.reachable != true) continue
                val local = queryLocalMedia(context, pairing.uri)
                when (val decision = PhotoTrashRules.decideRemotePropagation(
                    pendingSince = 0L,
                    denied = false,
                    local = local,
                    remote = remote,
                    pairingSize = pairing.localSizeAtPairing,
                    pairingModifiedAt = pairing.localModifiedAtPairing,
                    localDigestBound = pairing.localContentDigest != null,
                    remoteBasisHash = pairing.remoteContentHash,
                )) {
                    is PhotoTrashRules.RemoteDecision.RequestConsent -> when (verifyLocalDigestAtBoundary(context, pairing)) {
                        DigestBoundary.MISMATCH -> {
                            markConflict(pairing.uri, PhotoPairing.State.CONFLICT_LOCAL_CHANGED, customRealmConfig)
                            clearRemoteTrashPending(pairing.uri, customRealmConfig)
                        }
                        DigestBoundary.UNREADABLE, DigestBoundary.NOT_BOUND -> {
                            markInconclusive(pairing.uri, customRealmConfig)
                            clearRemoteTrashPending(pairing.uri, customRealmConfig)
                        }
                        DigestBoundary.VERIFIED -> {
                            // FINAL gate at the hand-out point: the live checks ran before
                            // this row's remote/digest IO — the option or account could
                            // have changed DURING it. The request is handed out only when
                            // the gate still holds now; otherwise the drain ends with
                            // nothing surfaced and the row's state untouched.
                            if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse() ||
                                pairing.userId != AccountUtils.currentUserId
                            ) {
                                return@withContext null
                            }
                            return@withContext ConsentRequest(
                                uri = pairing.uri,
                                userId = pairing.userId,
                                driveId = pairing.driveId,
                                remoteFileId = pairing.remoteFileId,
                                pendingSince = pairing.remoteTrashPendingSince,
                            )
                        }
                    }
                    is PhotoTrashRules.RemoteDecision.ClearPending -> clearRemoteTrashPending(pairing.uri, customRealmConfig)
                    is PhotoTrashRules.RemoteDecision.Conflict -> {
                        markConflict(pairing.uri, decision.state, customRealmConfig)
                        clearRemoteTrashPending(pairing.uri, customRealmConfig)
                    }
                    is PhotoTrashRules.RemoteDecision.Inconclusive -> {
                        markInconclusive(pairing.uri, customRealmConfig)
                        clearRemoteTrashPending(pairing.uri, customRealmConfig)
                    }
                    is PhotoTrashRules.RemoteDecision.None ->
                        // With the as-if-arming inputs the only None case is a local item
                        // already trashed: both sides are consistent, the request is moot.
                        clearRemoteTrashPending(pairing.uri, customRealmConfig)
                }
            }
            null
        }

    //region Realm state helpers

    /**
     * Runs [block] on a pairing realm. An injected [customRealmConfig] is opened and closed
     * HERE, on the calling thread, for the duration of ONE helper call only — a Realm
     * instance is thread-confined and must never be held across a suspension point (the
     * reconciliation pass runs on Dispatchers.IO and suspends for its network calls, so a
     * long-lived instance handed in from another thread would be an illegal cross-thread
     * access, and even one opened inside the pass could resume on a different IO worker).
     */
    private inline fun <T> withPairingRealm(customRealmConfig: RealmConfiguration?, block: (Realm) -> T): T =
        customRealmConfig?.let { Realm.getInstance(it).use(block) } ?: UploadFile.getRealmInstance().use(block)

    /** Persists the pending consent state and returns the (detached) pairing with the fresh stamp. */
    private fun armRemoteTrashPending(pairing: PhotoPairing, customRealmConfig: RealmConfiguration? = null): PhotoPairing? {
        val now = System.currentTimeMillis()
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, pairing.uri)
                    .findFirst()?.remoteTrashPendingSince = now
            }
        }
        pairing.remoteTrashPendingSince = now
        return pairing
    }

    private fun ensurePairingBaseline(context: Context, pairing: PhotoPairing, customRealmConfig: RealmConfiguration? = null) {
        if (pairing.baselineTrashed != null) return
        captureActivationBaseline(context, pairing, customRealmConfig)
    }

    private fun markLocalTrashReconciled(uri: String, observedTrashed: Boolean, customRealmConfig: RealmConfiguration? = null) {
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, uri)
                    .findFirst()?.let { row ->
                        row.lastObservedTrashed = observedTrashed
                        if (observedTrashed) row.lastLocalTrashReconciledAt = System.currentTimeMillis()
                        else row.lastLocalTrashReconciledAt = 0L
                        row.state = PhotoPairing.State.ACTIVE.name
                    }
            }
        }
    }

    private fun markConflict(uri: String, state: PhotoPairing.State, customRealmConfig: RealmConfiguration? = null) {
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, uri)
                    .findFirst()?.let { row ->
                        row.state = state.name
                        row.lastObservedTrashed = true
                    }
            }
        }
    }

    private fun markInconclusive(uri: String, customRealmConfig: RealmConfiguration? = null) {
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, uri)
                    .findFirst()?.let { row ->
                        row.state = PhotoPairing.State.INCONCLUSIVE.name
                    }
            }
        }
    }

    private fun clearRemoteTrashPending(uri: String, customRealmConfig: RealmConfiguration? = null) {
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, uri)
                    .findFirst()?.let { row ->
                        row.remoteTrashPendingSince = 0L
                        // A remote restore EXPIRES a recorded denial: the row becomes
                        // eligible again, so a later re-delete re-arms a fresh consent —
                        // a stored DENIED state is never permanent.
                        if (row.state == PhotoPairing.State.DENIED.name) {
                            row.state = PhotoPairing.State.ACTIVE.name
                        }
                    }
            }
        }
    }

    //endregion

    //region Observations

    /**
     * Reads the current MediaStore state of one local media item. Null means the read failed
     * entirely (security/illegal argument); a missing row maps to exists=false. Below API 30
     * the trashed flag is not observable: it is reported false so the pass records a
     * conservative non-trashed observation instead of inventing a deletion.
     */
    fun queryLocalMedia(context: Context, uriString: String): PhotoTrashRules.LocalObservation? {
        val uri = runCatching { android.net.Uri.parse(uriString) }.getOrNull() ?: return null
        return runCatching {
            val projection = buildList {
                add(MediaStore.MediaColumns.SIZE)
                add(MediaStore.MediaColumns.DATE_MODIFIED)
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) add(MediaStore.MediaColumns.IS_TRASHED)
            }.toTypedArray()
            context.contentResolver.query(uri, projection, null, null, null)?.use { cursor ->
                if (!cursor.moveToFirst()) return PhotoTrashRules.LocalObservation(exists = false, isTrashed = false)
                val size = runCatching { cursor.getLong(0) }.getOrNull()
                val lastModified = runCatching { cursor.getLong(1) * 1_000L }.getOrNull()
                val trashed = Build.VERSION.SDK_INT >= Build.VERSION_CODES.R &&
                    runCatching { cursor.getInt(2) == 1 }.getOrDefault(false)
                PhotoTrashRules.LocalObservation(exists = true, isTrashed = trashed, size = size, lastModified = lastModified)
            }
        }.onFailure { exception ->
            // The exception class is folded into the message: the plain SentryLog.d overload
            // carries no extras.
            SentryLog.d(TAG, "Local media query failed for $uriString (treated as inconclusive): ${exception.javaClass.simpleName}")
        }.getOrNull()
    }

    private fun queryRemoteState(pairing: PhotoPairing, okHttpClient: okhttp3.OkHttpClient): PhotoTrashRules.RemoteObservation {
        val response = runCatching {
            ApiRepository.getFileDetailsWithHash(
                file = File(id = pairing.remoteFileId, driveId = pairing.driveId),
                okHttpClient = okHttpClient,
            )
        }.onFailure { exception ->
            if (exception !is NetworkException) SentryLog.e(TAG, "Remote details failed", exception)
        }.getOrNull()
        val file = response?.takeIf { it.isSuccess() }?.data
        return when {
            file == null -> PhotoTrashRules.RemoteObservation(reachable = false, isTrashed = false)
            else -> PhotoTrashRules.RemoteObservation(
                reachable = true,
                isTrashed = file.isTrashed(),
                size = file.size,
                lastModifiedAt = file.lastModifiedAt,
                serverHash = file.serverContentHash,
                file = file,
            )
        }
    }

    //endregion

    //region Content binding

    /** Result of the destructive-boundary re-verification of the local source. */
    private enum class DigestBoundary {
        /** The current bytes equal the recorded upload digest. */
        VERIFIED,
        /** The current bytes diverge from the recorded upload digest: a conflict, never an authority. */
        MISMATCH,
        /** The current bytes could not be read: inconclusive. */
        UNREADABLE,
        /** No digest was recorded at pairing: metadata binding only (disclosed). */
        NOT_BOUND,
    }

    /**
     * The ONLY re-read of the local source that exists: at the destructive boundary, the
     * pairing's recorded upload digest is compared to the CURRENT bytes. A digest sampled or
     * computed anywhere else could bless changed content; here a changed source is caught or
     * the binding stays unknown ([DigestBoundary.NOT_BOUND]).
     */
    private suspend fun verifyLocalDigestAtBoundary(context: Context, pairing: PhotoPairing): DigestBoundary = withContext(Dispatchers.IO) {
        val recorded = pairing.localContentDigest ?: return@withContext DigestBoundary.NOT_BOUND
        val current = digestContent(context, pairing.uri) ?: return@withContext DigestBoundary.UNREADABLE
        if (current == recorded) DigestBoundary.VERIFIED else DigestBoundary.MISMATCH
    }

    /** sha-256 of the CURRENT local content behind [uriString]; null when unreadable. */
    private fun digestContent(context: Context, uriString: String): String? = runCatching {
        val uri = android.net.Uri.parse(uriString)
        context.contentResolver.openInputStream(uri)?.use { input ->
            val digest = java.security.MessageDigest.getInstance("SHA-256")
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            while (true) {
                val read = input.read(buffer)
                if (read < 0) break
                digest.update(buffer, 0, read)
            }
            digest.digest().joinToString("") { "%02x".format(it) }
        }
    }.onFailure { exception ->
        SentryLog.w(TAG, "Local content digest failed for $uriString (binding unavailable)", exception)
    }.getOrNull()

    /**
     * ONE single-stream pass over the source computing BOTH receipts from the same
     * snapshot: the server-algorithm (XXH3-64) digest and the local sha-256 receipt. The
     * sha-256 receipt is returned ONLY when the same-pass XXH3 digest equals
     * [serverHashCanonical] — the server-to-source identity proven against the exact
     * bytes the receipt describes. A mismatch, an unreadable source, or a mid-read
     * failure records nothing (null = unknown preserves).
     */
    private fun digestContentValidatedByServerHash(context: Context, uriString: String, serverHashCanonical: String): String? = runCatching {
        val uri = android.net.Uri.parse(uriString)
        context.contentResolver.openInputStream(uri)?.use { input ->
            // Every byte the XXH3 pass reads also updates the sha-256 receipt: both
            // digests see ONE read sequence, so a mid-pass source change can neither
            // split the two receipts nor validate one and record the other.
            val localDigest = java.security.MessageDigest.getInstance("SHA-256")
            val dualDigestSource = object : java.io.FilterInputStream(input) {
                override fun read(b: ByteArray, off: Int, len: Int): Int {
                    val read = super.read(b, off, len)
                    if (read > 0) localDigest.update(b, off, read)
                    return read
                }

                override fun read(): Int {
                    val read = super.read()
                    if (read >= 0) localDigest.update(read.toByte())
                    return read
                }
            }
            val serverAlgorithmDigest = PhotoTrashServerHash.hashStreaming(dualDigestSource)
            val localReceipt = localDigest.digest().joinToString("") { "%02x".format(it) }
            localReceipt.takeIf { serverAlgorithmDigest == serverHashCanonical }
        }
    }.onFailure { exception ->
        SentryLog.w(TAG, "Server-to-source validation read failed for $uriString (binding unavailable)", exception)
    }.getOrNull()

    //endregion

    private fun Boolean?.orFalse() = this == true
}
