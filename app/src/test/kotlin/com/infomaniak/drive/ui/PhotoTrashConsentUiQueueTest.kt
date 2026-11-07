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
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

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
        // The LIVE registry generation for this uri (the witness captures the same value).
        bindingGeneration = PhotoTrashController.bindingGeneration("content://fixture/queue"),
    )

    private fun snapshot(
        userId: Int = 951,
        epoch: Long = 3L,
        optIn: Boolean = true,
    ) = PhotoTrashController.ActivationSnapshot(userId = userId, activationEpoch = epoch, optIn = optIn, revision = 1L)

    /** A witness as the controller would capture it, with overridable parts for the table. */
    private fun witness(
        userId: Int = 951,
        epoch: Long = 3L,
        optIn: Boolean = true,
        settled: Boolean = true,
        bindingGeneration: Long = PhotoTrashController.bindingGeneration("content://fixture/queue"),
        bindingSettled: Boolean = true,
    ) = PhotoTrashController.LaunchWitness(
        activationSnapshot = snapshot(userId = userId, epoch = epoch, optIn = optIn),
        activationSettled = settled,
        bindingGeneration = bindingGeneration,
        bindingSettled = bindingSettled,
    )

    /** The gate exactly as production evaluates it: ONE fresh coherent capture, in-section. */
    private fun gate(request: PhotoTrashController.ConsentRequest): Boolean =
        PhotoTrashConsentUiQueue.launchAllowedNow(
            request = request,
            currentUserId = 951,
            activationWitness = PhotoTrashController.captureLaunchWitness(request.uri),
            lifecycleStarted = true,
        )

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
        val otherGeneration = PhotoTrashController.bindingGeneration(req.uri) + 1L

        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationWitness = witness(), lifecycleStarted = false), "a stopped lifecycle shows no dialog")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 952, activationWitness = witness(), lifecycleStarted = true), "another account's request never launches")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationWitness = null, lifecycleStarted = true), "unknown activation evidence fails closed")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationWitness = witness(optIn = false), lifecycleStarted = true), "an opted-out account never launches")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationWitness = witness(userId = 952), lifecycleStarted = true), "an authority scoped to another user fails closed")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationWitness = witness(epoch = 4L), lifecycleStarted = true), "a newer activation cycle invalidates the request even at the same enabled value")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationWitness = witness(bindingGeneration = otherGeneration), lifecycleStarted = true), "a replaced binding after the IO validation is caught at launch")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationWitness = witness(settled = false), lifecycleStarted = true), "an unsettled or incoherent activation capture fails closed")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationWitness = witness(bindingSettled = false), lifecycleStarted = true), "a binding write in flight or moved during the capture fails closed")
        assertTrue(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationWitness = witness(), lifecycleStarted = true))
    }

    /**
     * The activation authority and its revision counter are process-wide singletons, and
     * earlier tests publish explicit revisions. Advancing the counter here guarantees a
     * base STRICTLY above everything any earlier test used, so each test's sequence is
     * deterministic whatever ran before (the counter is monotonic; anything below it loses).
     */
    private fun revisionBase(): Long {
        var last = 0L
        repeat(20) {
            last = PhotoTrashController.beginActivationWrite()
            PhotoTrashController.endActivationWrite()
        }
        // Every write is settled: the world is quiescent and the certified read-slot tests
        // below start from a deterministic fresh base whatever earlier tests did.
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
    fun `a read begun while a write is unsettled is never certifiable`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = true, revision = base)

        // The writer has BEGUN (marked unsettled, revision assigned) but not settled: the
        // authority is unavailable — to the launch gate AND to certified reads. A reader
        // starting now gets NO certifiable slot: the row it is about to read is the
        // PRE-commit one, and publishing it at the writer's own revision would block that
        // writer's publish forever (the pre-commit poisoning schedule).
        val writeRevision = PhotoTrashController.beginActivationWrite()
        assertEquals(1, PhotoTrashController.activationWritesUnsettled())
        assertNull(PhotoTrashController.beginActivationRead(), "no certifiable read slot while a write is unsettled")
        assertFalse(PhotoTrashController.isActivationReadCertified(base), "an observation from an uncertified slot is never publishable")

        // The mirror keeps its last certified value; nothing about the unsettled write
        // leaks into it either way.
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(true, this?.optIn)
            assertEquals(base, this?.revision)
        }

        // The write publishes its committed authority and settles: certifiable slots
        // resume at the settled revision, and the gate reopens.
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = false, revision = writeRevision)
        PhotoTrashController.endActivationWrite()
        assertEquals(0, PhotoTrashController.activationWritesUnsettled())
        assertEquals(writeRevision, PhotoTrashController.beginActivationRead())
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(false, this?.optIn)
            assertEquals(writeRevision, this?.revision)
        }
    }

    @Test
    fun `an observation whose read a write overtook is never published`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = false, revision = base)

        // A certified slot opens (quiescent), then a write BEGINS during the read:
        val permit = PhotoTrashController.beginActivationRead()
        val writeRevision = PhotoTrashController.beginActivationWrite()
        assertFalse(PhotoTrashController.isActivationReadCertified(permit), "a write begun during the read uncertifies the slot")

        // The uncertified observation is skipped — the mirror keeps the pre-read state:
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(base, this?.revision)
        }

        // The writer commits, publishes its authority, and settles — the truth lands:
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = true, revision = writeRevision)
        PhotoTrashController.endActivationWrite()
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(true, this?.optIn)
            assertEquals(writeRevision, this?.revision)
        }
    }

    @Test
    fun `a certified read publishes exactly what it read and a settled gate accepts it`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = true, revision = base)

        // A quiescent slot opens, reads the committed row, and finishes certified: the
        // observation carries the same revision the writer published — equal revision,
        // identical content, first publisher wins (a certified read never flips content at
        // a revision it did not read).
        val permit = PhotoTrashController.beginActivationRead()
        assertTrue(PhotoTrashController.isActivationReadCertified(permit))
        PhotoTrashController.noteActivationObserved(permit = permit!!, userId = 951, activationEpoch = 1L, optIn = true)
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(true, this?.optIn)
            assertEquals(permit, this?.revision, "the certified observation stands at its own revision")
        }

        // The Main launch gate accepts the settled mirror (one coherent capture):
        val req = request().copy(activationEpoch = 1L)
        assertTrue(gate(req))
        // And refuses the very same mirror while any write is unsettled — a FRESH capture
        // reads the begin (the earlier witness value is by design never reused):
        PhotoTrashController.beginActivationWrite()
        assertFalse(gate(req), "an unsettled write refuses the launch even with a fresh mirror")
        PhotoTrashController.endActivationWrite()
    }

    @Test
    fun `the gate refuses from the moment a write begins until its publish lands`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 2L, optIn = true, revision = base)
        val req = request().copy(activationEpoch = 2L)

        assertTrue(gate(req), "the settled published mirror launches")

        // The write BEGINS (in its transaction, before it can commit): refused — this is
        // the whole [begin -> publish] span, covering both the open-transaction and the
        // commit-before-publish intervals.
        val writeRevision = PhotoTrashController.beginActivationWrite()
        assertFalse(gate(req), "an unsettled write is unavailable to destructive launch from before it can commit")
        assertFalse(PhotoTrashController.isActivationReadCertified(base), "reads are uncertified for the same span")

        // The commit lands and the publish follows; only then does the gate reopen:
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 2L, optIn = false, revision = writeRevision)
        PhotoTrashController.endActivationWrite()
        assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn)
        assertFalse(gate(req), "now refused on the opt-in itself, not on settledness")
    }

    @Test
    fun `an abandoned write is incoherent until a certified read rebuilds the mirror`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 2L, optIn = true, revision = base)
        val req = request().copy(activationEpoch = 2L)

        // A write begins and is ABANDONED (rollback/exception): it settles without a
        // publish — the last published mirror's CONTENT is still the committed truth.
        PhotoTrashController.beginActivationWrite()
        assertFalse(gate(req))
        assertNull(PhotoTrashController.beginActivationRead())
        PhotoTrashController.endActivationWrite()

        // The counter now LEADS the mirror: the authority is INCOHERENT. A rollback and a
        // post-commit publication failure are indistinguishable without a rebuild, and
        // neither may be trusted — the gate stays closed even though the mirror "looks"
        // committed. No revision is stranded; certified reads resume immediately.
        with(PhotoTrashController.captureLaunchWitness(req.uri)) {
            assertFalse(activationSettled, "the counter leading the mirror is an incoherent capture")
            assertTrue(activationSnapshot?.optIn == true, "the mirror content is unchanged; settledness is what failed")
        }
        assertFalse(gate(req), "the launch gate refuses an incoherent authority until it is rebuilt")
        assertEquals(0, PhotoTrashController.activationWritesUnsettled())
        assertTrue(PhotoTrashController.isActivationReadCertified(PhotoTrashController.beginActivationRead()))
        with(PhotoTrashController.currentActivationSnapshot) {
            assertEquals(true, this?.optIn)
            assertEquals(base, this?.revision)
        }

        // The rebuild is ANY certified read (the drains perform them constantly): it
        // republishes the committed state at the CURRENT counter, and coherence — and the
        // gate — return without another write.
        val permit = PhotoTrashController.beginActivationRead()
        PhotoTrashController.noteActivationObserved(permit = permit!!, userId = 951, activationEpoch = 2L, optIn = true)
        with(PhotoTrashController.captureLaunchWitness(req.uri)) {
            assertTrue(activationSettled, "a certified read rebuilds coherence")
        }
        assertTrue(gate(req), "the rebuilt mirror launches again")
    }

    @Test
    fun `a complete disable landing before the section capture is read coherently by it`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 2L, optIn = true, revision = base)
        val req = request().copy(activationEpoch = 2L)

        // The between-arguments schedule, at the authority: Main captures the enabled
        // snapshot, a writer performs its ENTIRE disable, Main evaluates. Two independent
        // atomic reads would pair the stale enabled snapshot with settled=true; the ONE
        // coherent capture reads whatever landed before it — the disable is in it.
        val staleArgumentRead = PhotoTrashController.currentActivationSnapshot
        val writeRevision = PhotoTrashController.beginActivationWrite()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 2L, optIn = false, revision = writeRevision)
        PhotoTrashController.endActivationWrite()

        assertEquals(true, staleArgumentRead?.optIn, "the old-style argument read is stale by construction")
        assertFalse(gate(req), "the gate refuses the completed disable on its content — never a stale enabled snapshot")
        with(PhotoTrashController.captureLaunchWitness(req.uri)) {
            assertTrue(activationSettled, "the capture is coherent (the write settled before it)")
            assertEquals(false, activationSnapshot?.optIn, "and it read the DISABLED state")
        }
    }

    /** Bounded condition wait whose result the CALLER asserts; observes a thread's verifiable position. */
    private fun awaitTrue(timeoutMs: Long = 5_000L, condition: () -> Boolean): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (condition()) return true
            Thread.sleep(10)
        }
        return condition()
    }

    @Test
    fun `a launch section excludes a writer's begin until it ends`() {
        val base = revisionBase()
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 2L, optIn = true, revision = base)
        val req = request().copy(activationEpoch = 2L)
        val settleNow = CountDownLatch(1)
        var writer: Thread? = null

        assertTrue(PhotoTrashController.tryBeginLaunchSection(), "Main takes the launch section")
        assertFalse(PhotoTrashController.tryBeginLaunchSection(), "a second section is refused (fail-closed)")
        try {
            // The section's witness is coherent and the gate accepts — the launch decision:
            with(PhotoTrashController.captureLaunchWitness(req.uri)) {
                assertTrue(activationSettled, "the section's capture is coherent")
            }
            assertTrue(gate(req))

            // A writer arriving NOW cannot begin: it waits at its begin point under the
            // section's monitor. POSITIVE contention evidence first (the writer verifiably
            // sits inside beginActivationWrite), so the bounded absence below measures the
            // guard rather than a possibly non-started thread (SOFTCHECK-1924 #4).
            val writerBegan = CountDownLatch(1)
            val w = Thread {
                PhotoTrashController.beginActivationWrite()
                writerBegan.countDown()
                settleNow.await(15, TimeUnit.SECONDS)
                PhotoTrashController.endActivationWrite()
            }
            writer = w
            w.start()
            assertTrue(
                awaitTrue {
                    w.state == Thread.State.WAITING &&
                        // The JVM variant transform renames methods with an $app suffix
                        // (seen in the diagnostic run r33b): match the base name.
                        w.stackTrace.any { it.methodName.startsWith("beginActivationWrite") }
                },
                "the writer is verifiably blocked inside beginActivationWrite (positive guard-contention evidence); " +
                    "final state=${w.state} alive=${w.isAlive} began=${writerBegan.count} " +
                    "stack=${w.stackTrace.take(8).joinToString(" | ") { "${it.className.substringAfterLast('.')}.${it.methodName}" }}",
            )
            assertFalse(writerBegan.await(500, TimeUnit.MILLISECONDS), "no writer may begin while a launch section is active")
            assertEquals(0, PhotoTrashController.activationWritesUnsettled(), "the witness stays valid: nothing began mid-section")

            // LAUNCH WINS: the section ends after the launch completed; the writer then
            // begins — observing a request already issued — and its disable publishes.
            PhotoTrashController.endLaunchSection()
            assertTrue(writerBegan.await(15, TimeUnit.SECONDS), "the writer proceeds once the section ends")
            assertEquals(1, PhotoTrashController.activationWritesUnsettled(), "the writer's begin landed after the launch section")
            assertFalse(PhotoTrashController.captureLaunchWitness(req.uri).activationSettled, "the section is gone; the writer's begin is visible to the next capture")

            PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 2L, optIn = false, revision = base + 1L)
            settleNow.countDown()
            writer.join(15_000)
            assertFalse(writer.isAlive, "the writer settled")
            assertEquals(0, PhotoTrashController.activationWritesUnsettled())
            assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn, "the disable landed after the launch decision consumed the pre-disable witness")
            assertFalse(gate(req), "the NEXT gate evaluation reads the disabled state")
        } finally {
            // Never leak the section or an unsettled write into other tests, whatever failed.
            PhotoTrashController.endLaunchSection()
            settleNow.countDown()
            writer?.join(15_000)
        }
    }
}
