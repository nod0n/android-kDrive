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
package com.infomaniak.drive.ui

import com.infomaniak.drive.data.sync.PhotoTrashController
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * The Main-confined consent dialog state machine and the final Main-side launch gate
 * ([PhotoTrashConsentUiQueue]) — the F1/F2/F3 corrections' decision core, runnable on the
 * JVM. The ACTIVITY wiring around it (ActivityResult delivery, saved-instance-state bundles,
 * onStart drains, the real recreation path) executes only on a device and stays a declared
 * device case.
 */
class PhotoTrashConsentUiQueueTest {

    private fun request(stamp: Long = 1_000L) = PhotoTrashController.ConsentRequest(
        uri = "content://fixture/queue",
        userId = 951,
        driveId = 751,
        remoteFileId = 900_501,
        pendingSince = stamp,
        pairedAt = 0L,
        activationEpoch = 3L,
        bindingGeneration = 7L,
    )

    private fun snapshot(
        userId: Int = 951,
        epoch: Long = 3L,
        optIn: Boolean = true,
    ) = PhotoTrashController.ActivationSnapshot(userId = userId, activationEpoch = epoch, optIn = optIn, revision = 1L)

    @Test
    fun `a drain holds the guard exactly until it ends`() {
        val queue = PhotoTrashConsentUiQueue()
        assertTrue(queue.canBeginDrain())

        queue.onDrainBegan()
        assertFalse(queue.canBeginDrain(), "a concurrent wake must not start a second drain")

        queue.onDrainEndedWithNothing()
        assertTrue(queue.canBeginDrain(), "the guard releases when the drain surfaced nothing")
    }

    @Test
    fun `a surfaced request validates outside the drain guard and is never the saved state`() {
        val queue = PhotoTrashConsentUiQueue()
        queue.onDrainBegan()
        val surfaced = request()
        queue.onDrainBegan()
        queue.onDrainEndedWithRequest(surfaced)
        assertEquals(surfaced, queue.validatingRequest, "the drain handed the request to validation")
        assertFalse(queue.canBeginDrain(), "a validating request must not let a second drain start")
        assertNull(queue.requestForSavedState(), "a VALIDATING request is never persisted: no dialog was ever shown")

        queue.onValidationFailed()
        assertTrue(queue.canBeginDrain(), "a failed validation releases the guard without stranding it")
        assertNull(queue.validatingRequest)
        assertNull(queue.requestForSavedState(), "a failed validation leaves nothing to persist")
    }

    @Test
    fun `a launched request is the saved state and its result is delivered exactly once`() {
        val queue = PhotoTrashConsentUiQueue()
        val shown = request(stamp = 2_000L)
        queue.onValidationLaunched(shown)

        assertEquals(shown, queue.requestForSavedState(), "only a LAUNCHED request persists across recreation")
        assertNull(queue.validatingRequest)
        assertFalse(queue.canBeginDrain(), "a launched dialog blocks all further drains")

        assertEquals(shown, queue.onResultDelivered(), "the result is attributed to exactly the shown request")
        assertNull(queue.onResultDelivered(), "a duplicate result delivery finds nothing")
        assertNull(queue.requestForSavedState())
        assertTrue(queue.canBeginDrain(), "the consumed result releases the guard")
    }

    @Test
    fun `a validation failure never erases a launched request`() {
        val queue = PhotoTrashConsentUiQueue()
        val shown = request(stamp = 3_000L)
        queue.onValidationLaunched(shown)

        queue.onValidationFailed()

        assertEquals(shown, queue.requestForSavedState(), "the launched dialog identity survives unrelated validation failures")
        assertEquals(shown, queue.onResultDelivered())
    }

    @Test
    fun `a restored launched request suppresses drains until its result arrives`() {
        val queue = PhotoTrashConsentUiQueue()
        val restored = request(stamp = 4_000L)
        queue.onLaunchedRequestRestored(restored)

        assertFalse(queue.canBeginDrain(), "the single-outstanding guard holds across recreation")
        assertEquals(restored, queue.onResultDelivered(), "the recreated activity still attributes the result")
        assertTrue(queue.canBeginDrain())
    }

    @Test
    fun `the final launch gate fails closed on every missing condition`() {
        val req = request()

        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = snapshot(), activationWatermark = 1L, bindingGenerationCurrent = 7L, lifecycleStarted = false), "a stopped lifecycle shows no dialog")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 952, activationSnapshot = snapshot(), activationWatermark = 1L, bindingGenerationCurrent = 7L, lifecycleStarted = true), "another account's request never launches")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = null, activationWatermark = 1L, bindingGenerationCurrent = 7L, lifecycleStarted = true), "unknown activation evidence fails closed")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = snapshot(optIn = false), activationWatermark = 1L, bindingGenerationCurrent = 7L, lifecycleStarted = true), "an opted-out account never launches")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = snapshot(userId = 952), activationWatermark = 1L, bindingGenerationCurrent = 7L, lifecycleStarted = true), "an authority scoped to another user fails closed")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = snapshot(epoch = 4L), activationWatermark = 1L, bindingGenerationCurrent = 7L, lifecycleStarted = true), "a newer activation cycle invalidates the request even at the same enabled value")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = snapshot(), activationWatermark = 1L, bindingGenerationCurrent = 8L, lifecycleStarted = true), "a replaced binding after the IO validation is caught at launch")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = snapshot(), activationWatermark = 2L, bindingGenerationCurrent = 7L, lifecycleStarted = true), "a mirror older than the latest commit (the commit-to-publish window) fails closed")
        assertTrue(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = snapshot(), activationWatermark = 1L, bindingGenerationCurrent = 7L, lifecycleStarted = true))
    }

    /**
     * The activation authority and its revision counter are process-wide singletons, and
     * earlier tests publish explicit revisions. Advancing the counter here guarantees a
     * base STRICTLY above everything any earlier test used, so each test's sequence is
     * deterministic whatever ran before (the counter is monotonic; anything below it loses).
     */
    private fun revisionBase(): Long {
        var last = 0L
        repeat(20) { last = PhotoTrashController.assignActivationRevision() }
        // Leave the world QUIESCENT (assigned counter == commit watermark): the certified
        // read-slot tests below need beginActivationRead() to succeed from a deterministic
        // fresh base whatever earlier tests did to the singleton state.
        PhotoTrashController.noteActivationCommitted(last)
        return last
    }

    @Test
    fun `a stale reader snapshot cannot overwrite a newer authoritative write`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = false, revision = base + 2L)

        // A reader that captured its permit BEFORE that write and observed the old enabled
        // state publishes after it: the newer write must survive.
        PhotoTrashController.noteActivationObserved(permit = base + 1L, userId = 951, activationEpoch = 0L, optIn = true)

        val authority = PhotoTrashController.currentActivationSnapshot
        assertEquals(false, authority?.optIn, "a getter snapshot is never an authority over a newer write")
        assertEquals(1L, authority?.activationEpoch)
    }

    @Test
    fun `a late publish of an earlier commit loses to a later commit's publish`() {
        val base = revisionBase()
        // Writer B (the LATER commit, disabled) publishes first; writer A (the EARLIER
        // commit, enabled) is paused and publishes afterwards. Revisions are assigned at
        // COMMIT time, so A's late publish must lose: Main must never see a state older
        // than the latest commit.
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = false, revision = base + 6L)
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = true, revision = base + 5L)

        val authority = PhotoTrashController.currentActivationSnapshot
        assertEquals(false, authority?.optIn, "the later commit's publish stays authoritative")
        assertEquals(base + 6L, authority?.revision, "the stored revision never rewinds to an earlier commit")
    }

    @Test
    fun `a stale invalidating candidate never rewinds the authority`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 2L, optIn = true, revision = base + 2L)

        // The exact rewind sequence: a stale fail-closed observation, then an older
        // enabled state — both must lose against the monotonic high-water mark, else the
        // stale true/epoch1 pair could re-authorize an older request.
        PhotoTrashController.noteActivationObserved(permit = base + 1L, userId = 951, activationEpoch = 0L, optIn = false)
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(true, this?.optIn)
            assertEquals(base + 2L, this?.revision)
        }
        PhotoTrashController.noteActivationObserved(permit = base + 1L, userId = 951, activationEpoch = 1L, optIn = true)
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(base + 2L, this?.revision)
            assertEquals(2L, this?.activationEpoch)
        }
    }

    @Test
    fun `absence published by the write seam at its commit revision is authoritative fail-closed`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 2L, optIn = true, revision = base + 6L)

        // The settings row is GONE (logout, restore wipe): the DELETION is a write seam —
        // its commit-assigned revision outranks the enabled state. Absence never preserves
        // another observation's true, and it is itself revision-ordered (a stale absence
        // observation loses — see the rewind test above).
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 0L, optIn = false, revision = base + 7L)

        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(false, this?.optIn)
            assertEquals(base + 7L, this?.revision)
        }
    }

    @Test
    fun `a reader starting after a write's commit outranks the still-pending publish`() {
        val base = revisionBase()
        // A legitimately published (earlier-commit) enabled state, and the counter advanced
        // past it (two later commits nobody mirrored) so the disable below commits strictly
        // after it.
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = true, revision = base + 1L)
        repeat(2) { PhotoTrashController.assignActivationRevision() }

        // The disable commits (revision assigned INSIDE its transaction, watermark raised
        // at the commit) but has not published yet; a reader capturing its read slot NOW is
        // quiescent AT the committed revision — the committed-to-publish visibility gap
        // cannot keep showing the pre-commit enabled state.
        val writerRevision = PhotoTrashController.assignActivationRevision()
        PhotoTrashController.noteActivationCommitted(writerRevision)
        val permit = PhotoTrashController.beginActivationRead()
        assertEquals(writerRevision, permit, "the watermark leads the publish, so the committed revision is readable")

        // The reader reads the COMMITTED (disabled) row and publishes it first:
        PhotoTrashController.noteActivationObserved(permit = permit!!, userId = 951, activationEpoch = 0L, optIn = false)
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(false, this?.optIn, "the post-commit observation outranks the pending pre-commit publish")
            assertEquals(writerRevision, this?.revision)
        }

        // The writer's own pending publish lands (equal revision, same committed content:
        // first publisher wins, content identical).
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 0L, optIn = false, revision = writerRevision)
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(false, this?.optIn)
            assertEquals(writerRevision, this?.revision)
        }
    }

    @Test
    fun `a read begun while a write is in flight is never certifiable`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = true, revision = base)

        // The writer has ASSIGNED its revision but not committed: the counter leads the
        // watermark. A reader starting now gets NO certifiable slot — the row it is about
        // to read is the PRE-commit one, and publishing it at the writer's own revision
        // would block that writer's publish forever (the pre-commit poisoning schedule).
        val inFlight = PhotoTrashController.assignActivationRevision()
        assertNull(PhotoTrashController.beginActivationRead(), "no certifiable read slot while a write is in flight")
        assertFalse(PhotoTrashController.isActivationReadCertified(base), "an observation from an uncertified slot is never publishable")

        // The mirror keeps its last certified value; nothing about the in-flight write
        // leaks into it either way.
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(true, this?.optIn)
            assertEquals(base, this?.revision)
        }

        // Once the write commits, certifiable slots resume — at the committed revision.
        PhotoTrashController.noteActivationCommitted(inFlight)
        assertEquals(inFlight, PhotoTrashController.beginActivationRead())
    }

    @Test
    fun `an observation whose read a commit overtook is never published`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = false, revision = base)

        // A certified slot opens (quiescent), then the writer commits DURING the read:
        val permit = PhotoTrashController.beginActivationRead()
        val writeRevision = PhotoTrashController.assignActivationRevision()
        PhotoTrashController.noteActivationCommitted(writeRevision)
        assertFalse(PhotoTrashController.isActivationReadCertified(permit), "a commit during the read uncertifies the slot")

        // The uncertified observation is skipped — the mirror keeps the pre-read state:
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(base, this?.revision)
        }

        // The writer's own publish supplies the truth:
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = true, revision = writeRevision)
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(true, this?.optIn)
            assertEquals(writeRevision, this?.revision)
        }
    }

    @Test
    fun `a certified read closes a commit-to-publish gap and the gate accepts it`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = false, revision = base)

        // The writer commits an enable but has not published: the watermark leads the mirror.
        val writeRevision = PhotoTrashController.assignActivationRevision()
        PhotoTrashController.noteActivationCommitted(writeRevision)

        // A reader begins (quiescent at the committed revision), reads the committed ENABLED
        // row, and publishes it under its permit:
        val permit = PhotoTrashController.beginActivationRead()
        assertEquals(writeRevision, permit)
        PhotoTrashController.noteActivationObserved(permit = permit!!, userId = 951, activationEpoch = 1L, optIn = true)
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(true, this?.optIn, "a certified read publishes exactly the committed state it observed")
            assertEquals(writeRevision, this?.revision)
        }

        // The Main launch gate accepts the now-fresh mirror:
        assertTrue(
            PhotoTrashConsentUiQueue.launchAllowedNow(
                request().copy(activationEpoch = 1L),
                currentUserId = 951,
                activationSnapshot = PhotoTrashController.currentActivationSnapshot,
                activationWatermark = PhotoTrashController.activationCommitWatermark(),
                bindingGenerationCurrent = 7L,
                lifecycleStarted = true,
            ),
        )

        // The writer's own late publish lands at the equal revision and changes nothing.
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = true, revision = writeRevision)
        assertEquals(writeRevision, PhotoTrashController.currentActivationSnapshot?.revision)
    }

    @Test
    fun `the gate refuses the mirror the moment a disable commits and reopens at its publish`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 2L, optIn = true, revision = base)
        val req = request().copy(activationEpoch = 2L)

        fun gate() = PhotoTrashConsentUiQueue.launchAllowedNow(
            request = req,
            currentUserId = 951,
            activationSnapshot = PhotoTrashController.currentActivationSnapshot,
            activationWatermark = PhotoTrashController.activationCommitWatermark(),
            bindingGenerationCurrent = 7L,
            lifecycleStarted = true,
        )

        assertTrue(gate(), "the published mirror is fresh at its commit watermark")

        // In-flight disable: assigned but NOT committed. The last committed state stays
        // launchable — an uncommitted write is not yet truth.
        val writeRevision = PhotoTrashController.assignActivationRevision()
        assertTrue(gate())

        // The disable COMMITS: the watermark leads the still-stale mirror; the gate refuses.
        // (This is the commit-to-publish window that the counter-based freshness check
        // could not see — the counter was already bumped inside the transaction.)
        PhotoTrashController.noteActivationCommitted(writeRevision)
        assertFalse(gate(), "a committed disable refuses the stale enabled mirror before its publish")

        // The publish (or any certified read) closes the gap:
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 2L, optIn = false, revision = writeRevision)
        assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn)
        assertFalse(gate(), "now refused on the opt-in itself, not on staleness")
    }
}
