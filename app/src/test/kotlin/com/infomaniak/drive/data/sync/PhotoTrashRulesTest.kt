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
import com.infomaniak.drive.data.sync.PhotoTrashRules.LocalDecision
import com.infomaniak.drive.data.sync.PhotoTrashRules.LocalObservation
import com.infomaniak.drive.data.sync.PhotoTrashRules.RemoteDecision
import com.infomaniak.drive.data.sync.PhotoTrashRules.RemoteObservation
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * Decision matrix for photo trash propagation: uncertainty never counts as deletion, conflicts
 * never propagate, a missing activation baseline gates everything, idempotency prevents
 * duplicate server calls, and — since content binding — NO destructive action (trash or
 * consent) is ever authorized without bound and matching content identity on BOTH sides:
 * there is no metadata-only fallback for an unbound digest or a hash-less remote response.
 */
class PhotoTrashRulesTest {

    private val pairingSize = 100L
    private val pairingModifiedAt = 1_000L
    private val basisHash = "pairing-hash"

    private val unchangedLocal = LocalObservation(exists = true, isTrashed = false, size = 100L, lastModified = 1_000L)
    private val trashedLocal = unchangedLocal.copy(isTrashed = true)

    /** The fresh remote response matching the pairing basis (bound content identity). */
    private val unchangedRemote = RemoteObservation(reachable = true, isTrashed = false, size = 100L, lastModifiedAt = 1L, serverHash = basisHash)
    private val unreachableRemote = RemoteObservation(reachable = false, isTrashed = false)

    private fun decideLocal(
        local: LocalObservation? = trashedLocal,
        baselineTrashed: Boolean? = false,
        lastObservedTrashed: Boolean = false,
        lastLocalTrashReconciledAt: Long = 0L,
        remote: RemoteObservation = unchangedRemote,
        localDigestBound: Boolean = true,
        remoteBasisHash: String? = basisHash,
    ): LocalDecision = PhotoTrashRules.decideLocalPropagation(
        baselineTrashed = baselineTrashed,
        lastObservedTrashed = lastObservedTrashed,
        lastLocalTrashReconciledAt = lastLocalTrashReconciledAt,
        local = local,
        remote = remote,
        pairingSize = pairingSize,
        pairingModifiedAt = pairingModifiedAt,
        localDigestBound = localDigestBound,
        remoteBasisHash = remoteBasisHash,
    )

    private fun decideRemote(
        local: LocalObservation? = unchangedLocal,
        pendingSince: Long = 0L,
        denied: Boolean = false,
        remote: RemoteObservation = unchangedRemote.copy(isTrashed = true),
        localDigestBound: Boolean = true,
        remoteBasisHash: String? = basisHash,
    ): RemoteDecision = PhotoTrashRules.decideRemotePropagation(
        pendingSince = pendingSince,
        denied = denied,
        local = local,
        remote = remote,
        pairingSize = pairingSize,
        pairingModifiedAt = pairingModifiedAt,
        localDigestBound = localDigestBound,
        remoteBasisHash = remoteBasisHash,
    )

    //region Activation baseline gating

    @Test
    fun `a pairing without a captured baseline never propagates`() {
        assertEquals(LocalDecision.None, decideLocal(baselineTrashed = null))
    }

    @Test
    fun `a file already trashed at activation never propagates its baseline state`() {
        // Baseline captured as trashed -> lastObservedTrashed=true -> no transition
        assertEquals(LocalDecision.None, decideLocal(baselineTrashed = true, lastObservedTrashed = true))
    }

    //endregion

    //region Local -> remote transitions

    @Test
    fun `a post-activation local trash propagates`() {
        assertEquals(LocalDecision.PropagateTrash, decideLocal())
    }

    @Test
    fun `an already propagated trash is not sent twice (idempotency)`() {
        assertEquals(LocalDecision.None, decideLocal(lastObservedTrashed = true, lastLocalTrashReconciledAt = 42L))
    }

    @Test
    fun `a remote already trashed is recorded without an API call`() {
        assertEquals(LocalDecision.RecordObservedTrashed, decideLocal(remote = unchangedRemote.copy(isTrashed = true)))
    }

    @Test
    fun `a restore re-arms the pair by clearing the reconciled marker`() {
        assertEquals(LocalDecision.MarkRestored, decideLocal(local = unchangedLocal, lastObservedTrashed = true, lastLocalTrashReconciledAt = 42L))
    }

    @Test
    fun `a missing or unresolvable local row is inconclusive, never proof of deletion`() {
        listOf(
            null,
            LocalObservation(exists = false, isTrashed = false),
        ).forEach { local ->
            assertEquals(LocalDecision.Inconclusive, decideLocal(local = local), "local=$local")
        }
    }

    @Test
    fun `an unreadable but present and not-trashed row needs no action`() {
        // Nothing is due for a present, not-trashed item: unreadability only gates
        // DESTRUCTIVE transitions, so the pass records None instead of poisoning the
        // state machine with INCONCLUSIVE (which would block legitimate re-arms).
        assertEquals(
            LocalDecision.None,
            decideLocal(local = LocalObservation(exists = true, isTrashed = false, size = null, lastModified = null)),
        )
    }

    @Test
    fun `an unreadable local row that reports trashed is inconclusive, never propagated`() {
        // Without a readable content binding the pairing cannot be verified: uncertainty
        // must never become a deletion.
        assertEquals(
            LocalDecision.Inconclusive,
            decideLocal(local = LocalObservation(exists = true, isTrashed = true, size = null, lastModified = null)),
        )
    }

    @Test
    fun `an unreadable local row never arms a remote-trash consent either`() {
        assertEquals(
            RemoteDecision.Inconclusive,
            decideRemote(local = LocalObservation(exists = true, isTrashed = false, size = null, lastModified = null)),
        )
    }

    @Test
    fun `a transient remote failure while local is trashed is inconclusive`() {
        assertEquals(LocalDecision.Inconclusive, decideLocal(remote = unreachableRemote))
    }

    @Test
    fun `a locally edited photo never propagates trash under the old pairing`() {
        assertEquals(LocalDecision.Conflict(PhotoPairing.State.CONFLICT_LOCAL_CHANGED), decideLocal(local = trashedLocal.copy(size = 999L)))
    }

    @Test
    fun `a remotely replaced body never receives local trash propagation`() {
        // The fresh remote hash no longer matches the identity bound at pairing: a different
        // body took the item's place. (Size equality alone is deliberately NOT evidence.)
        assertEquals(
            LocalDecision.Conflict(PhotoPairing.State.CONFLICT_REMOTE_CHANGED),
            decideLocal(remote = unchangedRemote.copy(serverHash = "other-hash")),
        )
    }

    //endregion

    //region Content binding (no metadata-only fallback)

    @Test
    fun `an unbound upload digest never authorizes trash`() {
        // The pairing's local digest was never captured (sample failed, chunked upload
        // without stream binding): unknown evidence preserves — never a destructive action.
        assertEquals(LocalDecision.Inconclusive, decideLocal(localDigestBound = false))
    }

    @Test
    fun `an unbound remote basis never authorizes trash`() {
        // The finish-session response established no content hash: the remote identity is
        // unknown, so the pairing can never be told apart from replaced content.
        assertEquals(LocalDecision.Inconclusive, decideLocal(remoteBasisHash = null))
    }

    @Test
    fun `a fresh remote response without a hash never authorizes trash`() {
        assertEquals(LocalDecision.Inconclusive, decideLocal(remote = unchangedRemote.copy(serverHash = null)))
    }

    @Test
    fun `an unbound upload digest never arms a consent dialog`() {
        assertEquals(RemoteDecision.Inconclusive, decideRemote(localDigestBound = false))
    }

    @Test
    fun `an unbound remote basis never arms a consent dialog`() {
        assertEquals(RemoteDecision.Inconclusive, decideRemote(remoteBasisHash = null))
    }

    @Test
    fun `a fresh remote response without a hash never arms a consent dialog`() {
        assertEquals(RemoteDecision.Inconclusive, decideRemote(remote = unchangedRemote.copy(isTrashed = true, serverHash = null)))
    }

    @Test
    fun `a replaced remote body never arms a consent dialog`() {
        assertEquals(
            RemoteDecision.Conflict(PhotoPairing.State.CONFLICT_REMOTE_CHANGED),
            decideRemote(remote = unchangedRemote.copy(isTrashed = true, serverHash = "other-hash")),
        )
    }

    @Test
    fun `metadata equality alone never substitutes for the content binding`() {
        // Same size, same mtime, but NO bound content identity on either side: exactly the
        // same-metadata URI-replacement case the old size/mtime rules blessed.
        assertEquals(
            LocalDecision.Inconclusive,
            decideLocal(localDigestBound = false, remoteBasisHash = null, remote = unchangedRemote.copy(serverHash = null)),
        )
        assertEquals(
            RemoteDecision.Inconclusive,
            decideRemote(localDigestBound = false, remoteBasisHash = null, remote = unchangedRemote.copy(isTrashed = true, serverHash = null)),
        )
    }

    //endregion

    //region Remote -> local (consent)

    @Test
    fun `a remote trash arms exactly one pending consent`() {
        assertEquals(RemoteDecision.RequestConsent, decideRemote())
        assertEquals(RemoteDecision.None, decideRemote(pendingSince = 42L))
    }

    @Test
    fun `a remote restore clears a stale pending consent`() {
        assertEquals(RemoteDecision.ClearPending, decideRemote(remote = unchangedRemote.copy(isTrashed = false)))
    }

    @Test
    fun `a locally edited photo is not trashed on the device under the old pairing`() {
        assertEquals(
            RemoteDecision.Conflict(PhotoPairing.State.CONFLICT_LOCAL_CHANGED),
            decideRemote(local = unchangedLocal.copy(lastModified = 5_000L)),
        )
    }

    @Test
    fun `an unreachable remote with no pending state is inconclusive`() {
        assertEquals(RemoteDecision.Inconclusive, decideRemote(remote = unreachableRemote))
    }

    @Test
    fun `a missing local row cannot be trashed on the device`() {
        assertEquals(RemoteDecision.Inconclusive, decideRemote(local = LocalObservation(exists = false, isTrashed = false)))
    }

    @Test
    fun `a pairing whose local item is already trashed never arms a device-trash consent`() {
        // Both sides trashed is a CONSISTENT state: asking the user to trash an item that is
        // already in the local trash would be a nonsense dialog on every pass.
        assertEquals(RemoteDecision.None, decideRemote(local = trashedLocal))
    }

    @Test
    fun `a declined consent is not re-armed on every pass`() {
        // Regression: after a denial the remote stays trashed and the local copy unchanged —
        // indistinguishable from a fresh remote trash without a denial memory, which used to
        // re-open the consent dialog endlessly.
        assertEquals(RemoteDecision.None, decideRemote(denied = true))
    }

    @Test
    fun `a denial expires when the remote is restored, so a future trash re-arms`() {
        val restored = decideRemote(denied = true, remote = unchangedRemote.copy(isTrashed = false))
        assertEquals(RemoteDecision.ClearPending, restored)

        val reTrashed = decideRemote(denied = false) // ClearPending expired the denial in the controller
        assertEquals(RemoteDecision.RequestConsent, reTrashed)
    }

    @Test
    fun `unbound pairing metadata never latches a conflict and rides the content binding`() {
        // An ABSENT-row pairing binds the -1 sentinel metadata: when the row later becomes
        // visible, the different-producer values must not latch a false CONFLICT_LOCAL_CHANGED
        // — the metadata is not consulted, and the destructive authority stays the content
        // binding (which the boundary re-verifies against the current bytes).
        val visibleRow = LocalObservation(exists = true, isTrashed = true, size = 123L, lastModified = 4_567L)
        assertEquals(
            LocalDecision.PropagateTrash,
            PhotoTrashRules.decideLocalPropagation(
                baselineTrashed = false,
                lastObservedTrashed = false,
                lastLocalTrashReconciledAt = 0L,
                local = visibleRow,
                remote = unchangedRemote,
                pairingSize = -1L,
                pairingModifiedAt = -1L,
                localDigestBound = true,
                remoteBasisHash = basisHash,
            ),
            "unchanged-content evidence rides the content binding alone",
        )

        assertEquals(
            LocalDecision.Inconclusive,
            PhotoTrashRules.decideLocalPropagation(
                baselineTrashed = false,
                lastObservedTrashed = false,
                lastLocalTrashReconciledAt = 0L,
                local = visibleRow,
                remote = unchangedRemote,
                pairingSize = -1L,
                pairingModifiedAt = -1L,
                localDigestBound = false,
                remoteBasisHash = basisHash,
            ),
            "without a content binding the unbound-metadata pairing preserves",
        )

        assertEquals(
            RemoteDecision.RequestConsent,
            PhotoTrashRules.decideRemotePropagation(
                pendingSince = 0L,
                denied = false,
                local = visibleRow.copy(isTrashed = false),
                remote = unchangedRemote.copy(isTrashed = true),
                pairingSize = -1L,
                pairingModifiedAt = -1L,
                localDigestBound = true,
                remoteBasisHash = basisHash,
            ),
            "the consent side arms on unbound metadata too; the launch-time digest still guards",
        )
    }

    //endregion
}
