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

import com.infomaniak.drive.data.models.PhotoPairing

/**
 * Pure decision model for photo trash propagation, shared by production and tests. Transitions
 * derive from durable pairing state and current observations only: uncertainty never counts as
 * deletion, conflicts never propagate, and only trash (never permanent deletion) is decided.
 */
object PhotoTrashRules {

    /** Observation of one local media item in this pass; null/absent fields mean unreadable. */
    data class LocalObservation(
        val exists: Boolean,
        val isTrashed: Boolean,
        val size: Long? = null,
        val lastModified: Long? = null, // ms
    )

    /** Fresh server details of the paired file; [reachable] false when the API call failed. */
    data class RemoteObservation(
        val reachable: Boolean,
        val isTrashed: Boolean,
        val size: Long? = null,
        val lastModifiedAt: Long? = null, // s
        val serverHash: String? = null, // the with=hash identity, when this response established one
        val file: com.infomaniak.drive.data.models.File? = null, // the detailed remote file, when reachable
    )

    sealed interface LocalDecision {
        /** Nothing to do. */
        data object None : LocalDecision

        /** Observation was impossible or ambiguous: never proof of deletion. */
        data object Inconclusive : LocalDecision

        /** A new local-trash transition: propagate to the server (caller verifies again before calling the API). */
        data object PropagateTrash : LocalDecision

        /** The remote file is already trashed: record the observation without an API call. */
        data object RecordObservedTrashed : LocalDecision

        /** The local item came back from trash: clear the reconciled marker (re-arm the pair). */
        data object MarkRestored : LocalDecision

        /** Content diverged on one side: record and skip. */
        data class Conflict(val state: PhotoPairing.State) : LocalDecision
    }

    /**
     * Decision for one pairing in one local pass. A pairing without a captured activation
     * baseline is never eligible for a destructive action.
     *
     * Each observation is compared to ITS OWN producer's basis: the local side against the
     * pairing-time local basis ([pairingSize]/[pairingModifiedAt], bound from the same
     * MediaStore source the pass reads), the remote side against the recorded remote
     * content hash ([remoteBasisHash], bound at pairing from the created file's identity).
     * The recorded local digest itself is NOT checked here — it is re-verified against the
     * current bytes at the destructive boundary only (on IO, by the caller), so a mid-pass
     * re-read can never bless changed content as the uploaded body.
     *
     * CONTENT IDENTITY IS REQUIRED ON BOTH SIDES, and there is NO metadata-only fallback:
     * an unbound local digest or an unbound/unprovable remote hash is unknown evidence —
     * it preserves ([Inconclusive]), it never authorizes the destructive action. Without
     * hash support the feature stays observe-only (a documented limitation, like offline
     * folders without hash support).
     *
     * @param local null when the row/volume could not be resolved at all
     */
    fun decideLocalPropagation(
        baselineTrashed: Boolean?,
        lastObservedTrashed: Boolean,
        lastLocalTrashReconciledAt: Long,
        local: LocalObservation?,
        remote: RemoteObservation,
        pairingSize: Long,
        pairingModifiedAt: Long,
        localDigestBound: Boolean,
        remoteBasisHash: String?,
    ): LocalDecision = when {
        baselineTrashed == null -> LocalDecision.None // baseline pending: observe nothing destructive
        local == null || !local.exists -> LocalDecision.Inconclusive
        !local.isTrashed -> if (lastObservedTrashed) LocalDecision.MarkRestored else LocalDecision.None
        lastObservedTrashed -> LocalDecision.None // transition already handled (restored state re-observed later)
        else -> when {
            lastLocalTrashReconciledAt > 0L -> LocalDecision.None // idempotency: already propagated
            !remote.reachable -> LocalDecision.Inconclusive
            remote.isTrashed -> LocalDecision.RecordObservedTrashed
            // A destructive action is only ever taken on a VERIFIABLE content binding: a row
            // whose size or last-modified cannot be read is inconclusive, never deletable.
            local.size == null || local.lastModified == null -> LocalDecision.Inconclusive
            // Pairing metadata from an UNOBSERVABLE row is unbound (the -1 sentinel): the
            // metadata comparison is skipped entirely — content identity rides the content
            // binding, which the destructive boundary re-verifies against the current bytes.
            !metadataBound(pairingSize, pairingModifiedAt) -> when {
                !localDigestBound -> LocalDecision.Inconclusive
                remoteBasisHash == null || remote.serverHash == null -> LocalDecision.Inconclusive
                remote.serverHash != remoteBasisHash -> LocalDecision.Conflict(PhotoPairing.State.CONFLICT_REMOTE_CHANGED)
                else -> LocalDecision.PropagateTrash
            }
            local.size != pairingSize -> LocalDecision.Conflict(PhotoPairing.State.CONFLICT_LOCAL_CHANGED)
            local.lastModified != pairingModifiedAt -> LocalDecision.Conflict(PhotoPairing.State.CONFLICT_LOCAL_CHANGED)
            // Unbound content evidence is unknown evidence: it preserves, never authorizes.
            !localDigestBound -> LocalDecision.Inconclusive
            remoteBasisHash == null || remote.serverHash == null -> LocalDecision.Inconclusive
            remote.serverHash != remoteBasisHash -> LocalDecision.Conflict(PhotoPairing.State.CONFLICT_REMOTE_CHANGED)
            else -> LocalDecision.PropagateTrash
        }
    }

    sealed interface RemoteDecision {
        data object None : RemoteDecision
        data object Inconclusive : RemoteDecision
        data object ClearPending : RemoteDecision
        data object RequestConsent : RemoteDecision
        data class Conflict(val state: PhotoPairing.State) : RemoteDecision
    }

    /**
     * Decision for one cloud-side event (MQTT or batched catch-up). The caller must have
     * verified the current remote state before asking for consent; a stale pending request is
     * cleared when the remote is no longer trashed. A pairing the user already declined
     * ([denied]) is not re-armed on every pass: without a remote trash timestamp a decline
     * can only expire when the remote is restored or the local content changes.
     *
     * Arming a consent dialog is a permission grant: Android performs the trash at APPROVAL
     * without an app pre-grant callback, so the last app-controlled check is the launch.
     * Both content identities must therefore be BOUND and MATCHING before arming — the
     * trashed remote item must still be the uploaded item ([remoteBasisHash] vs the fresh
     * hash), and the local content must be digest-bound ([localDigestBound]) and matching
     * the pairing basis. Unbound evidence is unknown evidence: it stays inconclusive, never
     * a consent (no metadata-only fallback).
     */
    fun decideRemotePropagation(
        pendingSince: Long,
        denied: Boolean,
        local: LocalObservation?,
        remote: RemoteObservation,
        pairingSize: Long,
        pairingModifiedAt: Long,
        localDigestBound: Boolean,
        remoteBasisHash: String?,
    ): RemoteDecision = when {
        !remote.reachable -> if (pendingSince > 0L) RemoteDecision.None else RemoteDecision.Inconclusive
        !remote.isTrashed -> RemoteDecision.ClearPending // restored remotely (or stale event): re-arm
        pendingSince > 0L -> RemoteDecision.None // consent already outstanding
        remoteBasisHash == null || remote.serverHash == null -> RemoteDecision.Inconclusive // unbound remote content
        remote.serverHash != remoteBasisHash -> RemoteDecision.Conflict(PhotoPairing.State.CONFLICT_REMOTE_CHANGED)
        local == null || !local.exists -> RemoteDecision.Inconclusive
        local.isTrashed -> RemoteDecision.None // both sides trashed: consistent, nothing to consent to
        denied -> RemoteDecision.None // the user declined this trash; respect it until it expires
        !localDigestBound -> RemoteDecision.Inconclusive // unbound local content: the dialog must never arm
        // Arming a consent dialog is an action too: without a readable content binding the
        // pairing cannot be told apart from replaced content, so it stays inconclusive.
        local.size == null || local.lastModified == null -> RemoteDecision.Inconclusive
        // Pairing metadata from an UNOBSERVABLE row is unbound (the -1 sentinel): no
        // metadata comparison — the launch-time digest check still guards the content.
        !metadataBound(pairingSize, pairingModifiedAt) -> RemoteDecision.RequestConsent
        local.size != pairingSize -> RemoteDecision.Conflict(PhotoPairing.State.CONFLICT_LOCAL_CHANGED)
        local.lastModified != pairingModifiedAt -> RemoteDecision.Conflict(PhotoPairing.State.CONFLICT_LOCAL_CHANGED)
        else -> RemoteDecision.RequestConsent
    }

    /** Pairing metadata below zero is the UNOBSERVABLE-ROW sentinel, never a comparable basis. */
    private fun metadataBound(pairingSize: Long, pairingModifiedAt: Long): Boolean =
        pairingSize >= 0L && pairingModifiedAt >= 0L
}
