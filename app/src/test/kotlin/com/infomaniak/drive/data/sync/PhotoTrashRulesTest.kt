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
 * never propagate, a missing activation baseline gates everything, and idempotency prevents
 * duplicate server calls.
 */
class PhotoTrashRulesTest {

    private val unchangedLocal = LocalObservation(exists = true, isTrashed = false, size = 100L, lastModified = 1_000L)
    private val trashedLocal = unchangedLocal.copy(isTrashed = true)
    private val unchangedRemote = RemoteObservation(reachable = true, isTrashed = false, size = 100L, lastModifiedAt = 1L)
    private val unreachableRemote = RemoteObservation(reachable = false, isTrashed = false)
    private val pairingSize = 100L
    private val pairingModifiedAt = 1_000L

    //region Activation baseline gating

    @Test
    fun `a pairing without a captured baseline never propagates`() {
        val decision = PhotoTrashRules.decideLocalPropagation(
            baselineTrashed = null,
            lastObservedTrashed = false,
            lastLocalTrashReconciledAt = 0L,
            local = trashedLocal,
            remote = unchangedRemote,
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(LocalDecision.None, decision)
    }

    @Test
    fun `a file already trashed at activation never propagates its baseline state`() {
        // Baseline captured as trashed -> lastObservedTrashed=true -> no transition
        val decision = PhotoTrashRules.decideLocalPropagation(
            baselineTrashed = true,
            lastObservedTrashed = true,
            lastLocalTrashReconciledAt = 0L,
            local = trashedLocal,
            remote = unchangedRemote,
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(LocalDecision.None, decision)
    }

    //endregion

    //region Local -> remote transitions

    @Test
    fun `a post-activation local trash propagates`() {
        val decision = PhotoTrashRules.decideLocalPropagation(
            baselineTrashed = false,
            lastObservedTrashed = false,
            lastLocalTrashReconciledAt = 0L,
            local = trashedLocal,
            remote = unchangedRemote,
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(LocalDecision.PropagateTrash, decision)
    }

    @Test
    fun `an already propagated trash is not sent twice (idempotency)`() {
        val decision = PhotoTrashRules.decideLocalPropagation(
            baselineTrashed = false,
            lastObservedTrashed = true,
            lastLocalTrashReconciledAt = 42L,
            local = trashedLocal,
            remote = unchangedRemote,
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(LocalDecision.None, decision)
    }

    @Test
    fun `a remote already trashed is recorded without an API call`() {
        val decision = PhotoTrashRules.decideLocalPropagation(
            baselineTrashed = false,
            lastObservedTrashed = false,
            lastLocalTrashReconciledAt = 0L,
            local = trashedLocal,
            remote = unchangedRemote.copy(isTrashed = true),
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(LocalDecision.RecordObservedTrashed, decision)
    }

    @Test
    fun `a restore re-arms the pair by clearing the reconciled marker`() {
        val decision = PhotoTrashRules.decideLocalPropagation(
            baselineTrashed = false,
            lastObservedTrashed = true,
            lastLocalTrashReconciledAt = 42L,
            local = unchangedLocal,
            remote = unchangedRemote,
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(LocalDecision.MarkRestored, decision)
    }

    @Test
    fun `a missing or unresolvable local row is inconclusive, never proof of deletion`() {
        listOf(
            null,
            LocalObservation(exists = false, isTrashed = false),
        ).forEach { local ->
            val decision = PhotoTrashRules.decideLocalPropagation(
                baselineTrashed = false,
                lastObservedTrashed = false,
                lastLocalTrashReconciledAt = 0L,
                local = local,
                remote = unchangedRemote,
                pairingSize = pairingSize,
                pairingModifiedAt = pairingModifiedAt,
            )
            assertEquals(LocalDecision.Inconclusive, decision, "local=$local")
        }
    }

    @Test
    fun `an unreadable but present and not-trashed row needs no action`() {
        // Nothing is due for a present, not-trashed item: unreadability only gates
        // DESTRUCTIVE transitions, so the pass records None instead of poisoning the
        // state machine with INCONCLUSIVE (which would block legitimate re-arms).
        val decision = PhotoTrashRules.decideLocalPropagation(
            baselineTrashed = false,
            lastObservedTrashed = false,
            lastLocalTrashReconciledAt = 0L,
            local = LocalObservation(exists = true, isTrashed = false, size = null, lastModified = null),
            remote = unchangedRemote,
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(LocalDecision.None, decision)
    }

    @Test
    fun `an unreadable local row that reports trashed is inconclusive, never propagated`() {
        // Without a readable content binding the pairing cannot be verified: uncertainty
        // must never become a deletion.
        val decision = PhotoTrashRules.decideLocalPropagation(
            baselineTrashed = false,
            lastObservedTrashed = false,
            lastLocalTrashReconciledAt = 0L,
            local = LocalObservation(exists = true, isTrashed = true, size = null, lastModified = null),
            remote = unchangedRemote,
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(LocalDecision.Inconclusive, decision)
    }

    @Test
    fun `an unreadable local row never arms a remote-trash consent either`() {
        val decision = PhotoTrashRules.decideRemotePropagation(
            pendingSince = 0L,
            local = LocalObservation(exists = true, isTrashed = false, size = null, lastModified = null),
            remote = unchangedRemote.copy(isTrashed = true),
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(RemoteDecision.Inconclusive, decision)
    }

    @Test
    fun `a transient remote failure while local is trashed is inconclusive`() {
        val decision = PhotoTrashRules.decideLocalPropagation(
            baselineTrashed = false,
            lastObservedTrashed = false,
            lastLocalTrashReconciledAt = 0L,
            local = trashedLocal,
            remote = unreachableRemote,
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(LocalDecision.Inconclusive, decision)
    }

    @Test
    fun `a locally edited photo never propagates trash under the old pairing`() {
        val decision = PhotoTrashRules.decideLocalPropagation(
            baselineTrashed = false,
            lastObservedTrashed = false,
            lastLocalTrashReconciledAt = 0L,
            local = trashedLocal.copy(size = 999L),
            remote = unchangedRemote,
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(LocalDecision.Conflict(PhotoPairing.State.CONFLICT_LOCAL_CHANGED), decision)
    }

    @Test
    fun `a remotely edited photo (other client) never receives local trash propagation`() {
        val decision = PhotoTrashRules.decideLocalPropagation(
            baselineTrashed = false,
            lastObservedTrashed = false,
            lastLocalTrashReconciledAt = 0L,
            local = trashedLocal,
            remote = unchangedRemote.copy(size = 999L),
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(LocalDecision.Conflict(PhotoPairing.State.CONFLICT_REMOTE_CHANGED), decision)
    }

    //endregion

    //region Remote -> local (consent)

    @Test
    fun `a remote trash arms exactly one pending consent`() {
        val first = PhotoTrashRules.decideRemotePropagation(
            pendingSince = 0L,
            local = unchangedLocal,
            remote = unchangedRemote.copy(isTrashed = true),
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(RemoteDecision.RequestConsent, first)

        val second = PhotoTrashRules.decideRemotePropagation(
            pendingSince = 42L,
            local = unchangedLocal,
            remote = unchangedRemote.copy(isTrashed = true),
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(RemoteDecision.None, second)
    }

    @Test
    fun `a remote restore clears a stale pending consent`() {
        val decision = PhotoTrashRules.decideRemotePropagation(
            pendingSince = 42L,
            local = unchangedLocal,
            remote = unchangedRemote.copy(isTrashed = false),
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(RemoteDecision.ClearPending, decision)
    }

    @Test
    fun `a locally edited photo is not trashed on the device under the old pairing`() {
        val decision = PhotoTrashRules.decideRemotePropagation(
            pendingSince = 0L,
            local = unchangedLocal.copy(lastModified = 5_000L),
            remote = unchangedRemote.copy(isTrashed = true),
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(RemoteDecision.Conflict(PhotoPairing.State.CONFLICT_LOCAL_CHANGED), decision)
    }

    @Test
    fun `an unreachable remote with no pending state is inconclusive`() {
        val decision = PhotoTrashRules.decideRemotePropagation(
            pendingSince = 0L,
            local = unchangedLocal,
            remote = unreachableRemote,
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(RemoteDecision.Inconclusive, decision)
    }

    @Test
    fun `a missing local row cannot be trashed on the device`() {
        val decision = PhotoTrashRules.decideRemotePropagation(
            pendingSince = 0L,
            local = LocalObservation(exists = false, isTrashed = false),
            remote = unchangedRemote.copy(isTrashed = true),
            pairingSize = pairingSize,
            pairingModifiedAt = pairingModifiedAt,
        )
        assertEquals(RemoteDecision.Inconclusive, decision)
    }

    //endregion
}
