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
     * created server file bound to the uploaded local source. Called only from the upload
     * finish path; a pairing for a reused URI is replaced (the newest upload identity wins).
     */
    fun recordPairing(uploadFile: UploadFile, remoteFileId: Int) {
        if (remoteFileId <= 0) return
        UploadFile.getRealmInstance().use { realm ->
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
                    localSizeAtPairing = uploadFile.fileSize
                    localModifiedAtPairing = uploadFile.getLastModified()
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

    /** Captures the activation baseline for one pairing: the current MediaStore trashed-state. */
    fun captureActivationBaseline(context: Context, pairing: PhotoPairing): Boolean {
        val local = queryLocalMedia(context, pairing.uri)
        val baseline = when {
            local == null || !local.exists -> false // absent rows start as present-and-not-trashed baseline
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.R -> local.isTrashed
            else -> false // trash state is not observable below API 30; baseline: not trashed
        }
        UploadFile.getRealmInstance().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, pairing.uri)
                    .findFirst()?.let { row ->
                        row.baselineTrashed = baseline
                        row.lastObservedTrashed = baseline
                    }
            }
        }
        return baseline
    }

    /**
     * One local pass over the current user's pairings: captures pending baselines, then
     * decides and executes local->remote transitions. Returns the number of propagated trashes.
     */
    suspend fun runLocalReconciliation(context: Context): Int = withContext(Dispatchers.IO) {
        if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse()) return@withContext 0
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return@withContext 0

        var propagated = 0
        val okHttpClient = AccountUtils.getHttpClient(AccountUtils.currentUserId, null)
        getPairings().forEach { pairing ->
            ensureActive()
            ensurePairingBaseline(context, pairing)

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
            )

            when (decision) {
                is PhotoTrashRules.LocalDecision.PropagateTrash -> {
                    val remoteFile = remote.file
                    if (remoteFile != null && ApiRepository.deleteFile(remoteFile).isSuccess()) {
                        markLocalTrashReconciled(pairing.uri, observedTrashed = true)
                        propagated++
                    } else {
                        markInconclusive(pairing.uri)
                    }
                }
                is PhotoTrashRules.LocalDecision.RecordObservedTrashed -> markLocalTrashReconciled(pairing.uri, observedTrashed = true)
                is PhotoTrashRules.LocalDecision.MarkRestored -> markLocalTrashReconciled(pairing.uri, observedTrashed = false)
                is PhotoTrashRules.LocalDecision.Conflict -> markConflict(pairing.uri, decision.state)
                is PhotoTrashRules.LocalDecision.Inconclusive -> markInconclusive(pairing.uri)
                is PhotoTrashRules.LocalDecision.None -> Unit
            }

            // The same pass doubles as the cloud->device catch-up: a remote trash that no
            // MQTT event delivered is armed here for consent.
            if (pairing.remoteTrashPendingSince == 0L && remote.reachable && remote.isTrashed) {
                val remoteDecision = PhotoTrashRules.decideRemotePropagation(
                    pendingSince = pairing.remoteTrashPendingSince,
                    local = local,
                    remote = remote,
                    pairingSize = pairing.localSizeAtPairing,
                    pairingModifiedAt = pairing.localModifiedAtPairing,
                )
                if (remoteDecision is PhotoTrashRules.RemoteDecision.RequestConsent) {
                    armRemoteTrashPending(pairing)?.let { armed -> pendingConsents.tryEmit(armed) }
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
    suspend fun onRemoteTrashEvent(context: Context, remoteFileId: Int, driveId: Int): PhotoPairing? {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return null
        val pairing = getPairings().firstOrNull {
            it.remoteFileId == remoteFileId && it.driveId == driveId && it.userId == AccountUtils.currentUserId
        } ?: return null
        if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse()) return null

        val okHttpClient = runCatching { AccountUtils.getHttpClient(AccountUtils.currentUserId, null) }.getOrNull()
        val remote = okHttpClient?.let { queryRemoteState(pairing, it) } ?: return null
        val local = queryLocalMedia(context, pairing.uri)

        return when (val decision = PhotoTrashRules.decideRemotePropagation(
            pendingSince = pairing.remoteTrashPendingSince,
            local = local,
            remote = remote,
            pairingSize = pairing.localSizeAtPairing,
            pairingModifiedAt = pairing.localModifiedAtPairing,
        )) {
            is PhotoTrashRules.RemoteDecision.RequestConsent -> armRemoteTrashPending(pairing)
            is PhotoTrashRules.RemoteDecision.ClearPending -> {
                clearRemoteTrashPending(pairing.uri)
                null
            }
            is PhotoTrashRules.RemoteDecision.Conflict -> {
                markConflict(pairing.uri, decision.state)
                null
            }
            is PhotoTrashRules.RemoteDecision.Inconclusive, is PhotoTrashRules.RemoteDecision.None -> null
        }
    }

    /** Records the user's consent result for a pending cloud->device trash. */
    fun onRemoteTrashConsentResult(uri: String, granted: Boolean) {
        UploadFile.getRealmInstance().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, uri)
                    .findFirst()?.let { row ->
                        row.remoteTrashPendingSince = 0L
                        if (granted) row.lastRemoteTrashAppliedAt = System.currentTimeMillis()
                    }
            }
        }
    }

    //region Realm state helpers

    /** Persists the pending consent state and returns the (detached) pairing with the fresh stamp. */
    private fun armRemoteTrashPending(pairing: PhotoPairing): PhotoPairing? {
        val now = System.currentTimeMillis()
        UploadFile.getRealmInstance().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, pairing.uri)
                    .findFirst()?.remoteTrashPendingSince = now
            }
        }
        pairing.remoteTrashPendingSince = now
        return pairing
    }

    private fun ensurePairingBaseline(context: Context, pairing: PhotoPairing) {
        if (pairing.baselineTrashed != null) return
        captureActivationBaseline(context, pairing)
    }

    private fun markLocalTrashReconciled(uri: String, observedTrashed: Boolean) {
        UploadFile.getRealmInstance().use { realm ->
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

    private fun markConflict(uri: String, state: PhotoPairing.State) {
        UploadFile.getRealmInstance().use { realm ->
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

    private fun markInconclusive(uri: String) {
        UploadFile.getRealmInstance().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, uri)
                    .findFirst()?.let { row ->
                        row.state = PhotoPairing.State.INCONCLUSIVE.name
                    }
            }
        }
    }

    private fun clearRemoteTrashPending(uri: String) {
        UploadFile.getRealmInstance().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, uri)
                    .findFirst()?.remoteTrashPendingSince = 0L
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
                file = file,
            )
        }
    }

    //endregion

    private fun Boolean?.orFalse() = this == true
}
