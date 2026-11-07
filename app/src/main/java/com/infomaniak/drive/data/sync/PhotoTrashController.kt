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
    fun getPairings(customRealm: Realm? = null): List<PhotoPairing> {
        val block: (Realm) -> List<PhotoPairing> = { realm ->
            realm.where(PhotoPairing::class.java)
                .equalTo(PhotoPairing::userId.name, AccountUtils.currentUserId)
                .findAll()
                .map { realm.copyFromRealm(it, 0) }
        }
        return customRealm?.let(block) ?: UploadFile.getRealmInstance().use(block)
    }

    fun getPairing(uri: String, customRealm: Realm? = null): PhotoPairing? {
        return getPairings(customRealm).firstOrNull { it.uri == uri }
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
     * [uploadedContentDigest] is the digest of the bytes the upload actually transferred
     * (the direct-upload stream). When it is unavailable (chunked or empty uploads), ONE
     * pairing-time sample is taken — the window between the transfer and this sample is a
     * disclosed residual race. [remoteFile]'s server identity (hash/size/lastModifiedAt
     * when the response established them) becomes the REMOTE basis.
     */
    fun recordPairing(
        context: Context,
        uploadFile: UploadFile,
        remoteFile: File?,
        uploadedContentDigest: String?,
        customRealm: Realm? = null,
    ) {
        val remoteFileId = remoteFile?.id?.takeIf { it > 0 } ?: return
        val local = queryLocalMedia(context, uploadFile.uri)
        val baseline = when {
            local == null -> null // unavailable: stays pending; a later capture re-observes
            !local.exists -> false // an absent row starts as the present-and-not-trashed baseline
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.R -> local.isTrashed
            else -> false // trash state is not observable below API 30; the feature is inert there
        }
        val localDigest = uploadedContentDigest ?: local?.takeIf { it.exists }?.let { digestContent(context, uploadFile.uri) }
        withPairingRealm(customRealm) { realm ->
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
    fun captureActivationBaseline(context: Context, pairing: PhotoPairing, customRealm: Realm? = null): Boolean {
        val local = queryLocalMedia(context, pairing.uri)
        val baseline = when {
            local == null -> return false // unavailable: the baseline stays pending
            !local.exists -> false
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.R -> local.isTrashed
            else -> false // trash state is not observable below API 30; baseline: not trashed
        }
        withPairingRealm(customRealm) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, pairing.uri)
                    .findFirst()?.let { row ->
                        row.baselineTrashed = baseline
                        row.lastObservedTrashed = baseline
                        if (baseline && row.state == PhotoPairing.State.ACTIVE.name) {
                            row.state = PhotoPairing.State.INCONCLUSIVE.name
                        }
                    }
            }
        }
        return baseline
    }

    /**
     * Activation point for the opt-in (the settings save runs it on its existing IO
     * context): captures the real MediaStore trashed-state for every pairing still missing
     * a baseline. Upload-created pairings capture theirs at pairing time; this is the
     * recovery path for rows left pending. No blocking IO on the settings UI thread.
     */
    fun capturePendingActivationBaselines(context: Context, customRealm: Realm? = null) {
        getPairings(customRealm).filter { it.baselineTrashed == null }.forEach { pairing ->
            captureActivationBaseline(context, pairing, customRealm)
        }
    }

    /**
     * One local pass over the current user's pairings: captures pending baselines, then
     * decides and executes local->remote transitions. Returns the number of propagated trashes.
     */
    suspend fun runLocalReconciliation(context: Context, customRealm: Realm? = null): Int = withContext(Dispatchers.IO) {
        if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse()) return@withContext 0
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return@withContext 0

        var propagated = 0
        val okHttpClient = AccountUtils.getHttpClient(AccountUtils.currentUserId, null)
        getPairings(customRealm).forEach { pairing ->
            ensureActive()
            ensurePairingBaseline(context, pairing, customRealm)

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
                        DigestBoundary.MISMATCH -> markConflict(pairing.uri, PhotoPairing.State.CONFLICT_LOCAL_CHANGED, customRealm)
                        DigestBoundary.UNREADABLE, DigestBoundary.NOT_BOUND -> markInconclusive(pairing.uri, customRealm)
                        DigestBoundary.VERIFIED -> {
                            val remoteFile = remote.file
                            if (remoteFile != null && ApiRepository.deleteFile(remoteFile).isSuccess()) {
                                markLocalTrashReconciled(pairing.uri, observedTrashed = true, customRealm = customRealm)
                                propagated++
                            } else {
                                markInconclusive(pairing.uri, customRealm)
                            }
                        }
                    }
                }
                is PhotoTrashRules.LocalDecision.RecordObservedTrashed -> markLocalTrashReconciled(pairing.uri, observedTrashed = true, customRealm = customRealm)
                is PhotoTrashRules.LocalDecision.MarkRestored -> markLocalTrashReconciled(pairing.uri, observedTrashed = false, customRealm = customRealm)
                is PhotoTrashRules.LocalDecision.Conflict -> markConflict(pairing.uri, decision.state, customRealm)
                is PhotoTrashRules.LocalDecision.Inconclusive -> markInconclusive(pairing.uri, customRealm)
                is PhotoTrashRules.LocalDecision.None -> Unit
            }

            // The same pass doubles as the cloud->device catch-up: a remote trash that no
            // MQTT event delivered is armed here for consent.
            if (pairing.remoteTrashPendingSince == 0L && remote.reachable && remote.isTrashed) {
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
                    armRemoteTrashPending(pairing, customRealm)?.let { armed -> pendingConsents.tryEmit(armed) }
                }
            }
        }
        propagated
    }

    /**
     * Cloud->device side: called for a remote trash event (MQTT or catch-up). Verifies the
     * current remote state, checks the local content binding and either arms the pending
     * consent state or clears a stale one. The consent dialog itself is UI.
     */
    suspend fun onRemoteTrashEvent(context: Context, remoteFileId: Int, driveId: Int, customRealm: Realm? = null): PhotoPairing? {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return null
        val pairing = getPairings(customRealm).firstOrNull {
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
            is PhotoTrashRules.RemoteDecision.RequestConsent -> armRemoteTrashPending(pairing, customRealm)
            is PhotoTrashRules.RemoteDecision.ClearPending -> {
                clearRemoteTrashPending(pairing.uri, customRealm)
                null
            }
            is PhotoTrashRules.RemoteDecision.Conflict -> {
                markConflict(pairing.uri, decision.state, customRealm)
                null
            }
            is PhotoTrashRules.RemoteDecision.Inconclusive, is PhotoTrashRules.RemoteDecision.None -> null
        }
    }

    /**
     * Records the user's consent result for a pending cloud->device trash. A grant re-activates
     * the pairing; a DECLINE records the denial so the same remote trash is not re-asked on
     * every pass — the denial expires when the remote is restored (ClearPending resets a
     * DENIED row to ACTIVE) or the local content changes (a conflict state re-evaluates the
     * pairing). [customRealm] keeps the write on the caller's request identity.
     */
    fun onRemoteTrashConsentResult(uri: String, granted: Boolean, customRealm: Realm? = null) {
        withPairingRealm(customRealm) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, uri)
                    .findFirst()?.let { row ->
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

    //region Realm state helpers

    private inline fun <T> withPairingRealm(customRealm: Realm?, block: (Realm) -> T): T =
        customRealm?.let(block) ?: UploadFile.getRealmInstance().use(block)

    /** Persists the pending consent state and returns the (detached) pairing with the fresh stamp. */
    private fun armRemoteTrashPending(pairing: PhotoPairing, customRealm: Realm? = null): PhotoPairing? {
        val now = System.currentTimeMillis()
        withPairingRealm(customRealm) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, pairing.uri)
                    .findFirst()?.remoteTrashPendingSince = now
            }
        }
        pairing.remoteTrashPendingSince = now
        return pairing
    }

    private fun ensurePairingBaseline(context: Context, pairing: PhotoPairing, customRealm: Realm? = null) {
        if (pairing.baselineTrashed != null) return
        captureActivationBaseline(context, pairing, customRealm)
    }

    private fun markLocalTrashReconciled(uri: String, observedTrashed: Boolean, customRealm: Realm? = null) {
        withPairingRealm(customRealm) { realm ->
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

    private fun markConflict(uri: String, state: PhotoPairing.State, customRealm: Realm? = null) {
        withPairingRealm(customRealm) { realm ->
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

    private fun markInconclusive(uri: String, customRealm: Realm? = null) {
        withPairingRealm(customRealm) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, uri)
                    .findFirst()?.let { row ->
                        row.state = PhotoPairing.State.INCONCLUSIVE.name
                    }
            }
        }
    }

    private fun clearRemoteTrashPending(uri: String, customRealm: Realm? = null) {
        withPairingRealm(customRealm) { realm ->
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

    //endregion

    private fun Boolean?.orFalse() = this == true
}
