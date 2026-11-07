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

        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = snapshot(), bindingGenerationCurrent = 7L, lifecycleStarted = false), "a stopped lifecycle shows no dialog")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 952, activationSnapshot = snapshot(), bindingGenerationCurrent = 7L, lifecycleStarted = true), "another account's request never launches")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = null, bindingGenerationCurrent = 7L, lifecycleStarted = true), "unknown activation evidence fails closed")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = snapshot(optIn = false), bindingGenerationCurrent = 7L, lifecycleStarted = true), "an opted-out account never launches")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = snapshot(userId = 952), bindingGenerationCurrent = 7L, lifecycleStarted = true), "an authority scoped to another user fails closed")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = snapshot(epoch = 4L), bindingGenerationCurrent = 7L, lifecycleStarted = true), "a newer activation cycle invalidates the request even at the same enabled value")
        assertFalse(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = snapshot(), bindingGenerationCurrent = 8L, lifecycleStarted = true), "a replaced binding after the IO validation is caught at launch")
        assertTrue(PhotoTrashConsentUiQueue.launchAllowedNow(req, currentUserId = 951, activationSnapshot = snapshot(), bindingGenerationCurrent = 7L, lifecycleStarted = true))
    }

    @Test
    fun `a stale reader snapshot cannot overwrite a newer authoritative write`() {
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = false)

        // A reader that captured its revision BEFORE that write and observed the old
        // enabled state publishes after it: the newer write must survive.
        PhotoTrashController.noteActivationObserved(permit = 0L, userId = 951, activationEpoch = 0L, optIn = true)

        val authority = PhotoTrashController.currentActivationSnapshot
        assertEquals(false, authority?.optIn, "a getter snapshot is never an authority over a newer write")
        assertEquals(1L, authority?.activationEpoch)
    }

    @Test
    fun `a missing-row observation fails closed over a stale enabled state`() {
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 2L, optIn = true)

        // The settings row is GONE (logout, restore wipe): a reader observing absence must
        // invalidate the enabled state, not preserve it, whatever its revision was.
        PhotoTrashController.noteActivationObserved(permit = 0L, userId = 951, activationEpoch = 0L, optIn = false)

        assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn, "missing data fails closed; it never preserves another observation's true")
    }

    @Test
    fun `a write landing during a read outranks the read and the read publishes under its permit`() {
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 1L, optIn = true)
        val permit = PhotoTrashController.activationReadPermit()

        // The write lands while the reader sits in its DB read; the reader then publishes
        // its older observation under the pre-read permit: rejected.
        PhotoTrashController.noteActivationWritten(userId = 951, activationEpoch = 2L, optIn = false)
        PhotoTrashController.noteActivationObserved(permit = permit, userId = 951, activationEpoch = 1L, optIn = true)

        val authority = PhotoTrashController.currentActivationSnapshot
        assertEquals(false, authority?.optIn, "the mid-read write stays authoritative")
        assertEquals(2L, authority?.activationEpoch)

        // With NO write landing during the read, the reader publishes normally.
        val freshPermit = PhotoTrashController.activationReadPermit()
        PhotoTrashController.noteActivationObserved(permit = freshPermit, userId = 951, activationEpoch = 2L, optIn = false)
        assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn)
    }
}
