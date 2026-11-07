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

import androidx.test.core.app.ApplicationProvider
import com.infomaniak.drive.data.models.SyncSettings
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.ui.PhotoTrashConsentUiQueue
import com.infomaniak.drive.utils.RealmModules
import io.mockk.every
import io.mockk.mockkObject
import io.mockk.unmockkAll
import io.realm.Realm
import io.realm.RealmConfiguration
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.Date
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicReference

/**
 * THE REAL SETTINGS SEAM under the unsettled-writes protocol and the launch section
 * (RESUME-9118-1853, DEVICE-2d4d-TRIAGE-1958): a REAL [UploadFile.setAppSyncSettings] on a
 * REAL fixture realm, held at its PRODUCTION protocol points — parked right after its
 * unsettled begin mark inside the open transaction (before the mutation), and parked after
 * its commit but before its publish/settle — plus a publication failure injected AT the
 * production publish seam after a real commit (the abandoned-write rule covers a post-commit
 * publication failure exactly like a rollback: neither may be trusted without a certified
 * read), the REAL [UploadFile.getAppSyncSettings] publishing only certified reads while a
 * write is unsettled, and both launch/write orders through the production seam against the
 * Main launch section.
 *
 * HARNESS DISCIPLINE (device-evidenced fixes):
 *  - NO Realm spy: every device case that drove the real seam through a MockK spy of
 *    [Realm] died with ArrayIndexOutOfBoundsException(length=2, index=2) inside the real
 *    write, and the spy's shallow field-copy leaks the native instance (all five teardowns
 *    reported an open realm). The 2d4d run is the recorded evidence. Real realms only, and
 *    interleavings are established by parking at the PRODUCTION protocol seams
 *    ([PhotoTrashController.beginActivationWrite] via callOriginal — the real mark lands
 *    inside the real transaction — and [PhotoTrashController.noteActivationWritten]).
 *  - every Realm is opened, used AND closed on the thread that touches it; the writer
 *    closes its own instance even on failure, and a close failure is attached as a
 *    SUPPRESSED exception to the primary failure — cleanup never hides it;
 *  - the full writer exception STACK is preserved and surfaced (not just its toString);
 *  - each case gets its OWN fixture realm (no shared accumulating state, no JUnit-order
 *    dependence), cleanup DELETES each fixture and REPORTS a leak (an open instance makes
 *    deleteRealm throw) instead of swallowing it;
 *  - writers are released and joined in finally even when an assertion fails.
 *
 * SCOPE HONESTY: the launch sections here are helper-driven (the controller's
 * tryBeginLaunchSection/capture/gate plus the REAL settings seam) — they do NOT exercise
 * MainActivity's ActivityResult/lifecycle wiring or any system dialog; that wiring remains a
 * declared device case. The in-transaction ROLLBACK is exercised for REAL through a
 * supported, narrow seam: the generated SyncSettings marshal writes its primitive columns
 * BEFORE calling lastSync.getTime(), so an ordinary Date subclass faulting there lands
 * INSIDE the production executeTransaction after real column writes — no Realm spying and
 * no production hook. Fully disposable and account-free: random fixture files in the
 * instrumentation target's cacheDir, synthetic rows only — NO owner data, NO account, NO
 * network.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PhotoTrashActivationSeamTest {

    private val context = ApplicationProvider.getApplicationContext<android.content.Context>()
    private val fixtureConfigs = mutableListOf<RealmConfiguration>()

    @BeforeAll
    fun initRealm() {
        Realm.init(context)
    }

    /** A fresh per-case disposable fixture: unique file, registered for leak-reporting cleanup. */
    private fun freshConfig(): RealmConfiguration {
        val config = RealmConfiguration.Builder()
            .schemaVersion(UploadMigration.DB_VERSION)
            .modules(RealmModules.SyncFilesModule())
            .name("photoTrashActivationSeam-${UUID.randomUUID()}")
            .directory(context.cacheDir)
            .build()
        fixtureConfigs.add(config)
        return config
    }

    @AfterEach
    fun tearDown() {
        unmockkAll()
        // Leak report, not a silent swallow: deleteRealm throws while an instance is open.
        val configs = ArrayList(fixtureConfigs)
        fixtureConfigs.clear()
        configs.forEach { config ->
            try {
                Realm.deleteRealm(config)
            } catch (leak: Throwable) {
                throw IllegalStateException("fixture realm leaked (an instance was still open): ${config.realmFileName}", leak)
            }
        }
    }

    /**
     * Seeds the case's settings row through the REAL settings write seam. The seam's
     * publication advances the process-global mirror to THIS fixture's state at a NEW
     * revision; a direct Realm seed + warm read would publish at an UNCHANGED revision,
     * which the strict high-water rejects — the mirror would keep the previous case's
     * user/opt-in despite the new fixture (SOFTCHECK-1924 #3). Deterministic per case and
     * JUnit-order independent.
     */
    private fun seedSettings(config: RealmConfiguration, userId: Int, driveId: Int, enabled: Boolean) {
        Realm.getInstance(config).use { realm ->
            UploadFile.setAppSyncSettings(
                SyncSettings(userId = userId, driveId = driveId, photoTrashSyncEnabled = enabled),
                realm,
            )
        }
    }

    /**
     * Bounded condition wait whose result the CALLER asserts; used for observing another
     * thread's verifiable position, never as a scheduling gate.
     */
    private fun awaitTrue(timeoutMs: Long = 5_000L, condition: () -> Boolean): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (condition()) return true
            Thread.sleep(10)
        }
        return condition()
    }

    /**
     * POSITIVE guard-contention evidence (SOFTCHECK-1924 #4): the writer is verifiably
     * BLOCKED INSIDE [PhotoTrashController.beginActivationWrite] — its thread is WAITING and
     * the blocked frame is on its stack — so the launch section (the monitor it waits on) is
     * what prevents progress, not scheduler timing. A later bounded absence window is then
     * meaningful; without this evidence a non-started writer would pass it too. No MockK
     * interception is active on this path: the frame is the REAL production method (method
     * names may carry a variant-transform suffix, $app on the JVM run — match the base name).
     */
    private fun awaitWriterContendingAtBegin(writer: Thread) {
        assertTrue(
            awaitTrue {
                writer.state == Thread.State.WAITING &&
                    writer.stackTrace.any { it.methodName.startsWith("beginActivationWrite") }
            },
            "the real writer is verifiably blocked inside beginActivationWrite (positive guard-contention evidence)",
        )
    }

    private class Park(val opened: CountDownLatch, val release: CountDownLatch) {
        fun reach() {
            opened.countDown()
        }

        fun hold() {
            check(release.await(15, TimeUnit.SECONDS)) { "the harness never released the parked writer" }
        }
    }

    /**
     * Full exception evidence for an assertion message: type, message and STACK — never only
     * the toString, which hid the ArrayIndexOutOfBoundsException's origin in the 2d4d run.
     */
    private fun failureText(failure: AtomicReference<Throwable?>): String =
        failure.get()?.let { throwable -> "${throwable::class.java.name}: ${throwable.message}\n${throwable.stackTraceToString()}" }
            ?: "null"

    /**
     * Runs ONE real [UploadFile.setAppSyncSettings] on its OWN writer thread against ITS OWN
     * REAL fixture realm (no spy; opened, used AND closed on that thread — Realm instances
     * are thread-confined). Any failure surfaces through [failure] WITH its stack; a close
     * failure is suppressed onto the primary instead of hiding it. [onDone] runs after the
     * seam returned (or threw).
     */
    private fun writerThread(
        settings: SyncSettings,
        config: RealmConfiguration,
        failure: AtomicReference<Throwable?>,
        onDone: (() -> Unit)? = null,
    ): Thread = Thread {
        val realm = Realm.getInstance(config)
        try {
            UploadFile.setAppSyncSettings(settings, realm)
        } catch (primary: Throwable) {
            failure.set(primary)
        } finally {
            try {
                realm.close()
            } catch (closeFailure: Throwable) {
                (failure.get() ?: IllegalStateException("the writer failed while closing its realm").also { failure.set(it) })
                    .addSuppressed(closeFailure)
            }
            onDone?.invoke()
        }
    }

    /**
     * Park INSIDE the open transaction, immediately AFTER the production begin mark
     * ([PhotoTrashController.beginActivationWrite] is called by the seam inside the
     * transaction, before its mutation): callOriginal keeps the REAL mark (the unsettled
     * counter and the revision bump), then the writer holds — the transaction is begun and
     * uncommitted, the mark unsettled, nothing mutated yet. A stub left installed after its
     * park's release latch opened passes later writers through harmlessly (reach is a no-op
     * countDown; hold returns immediately), so a second phase needs no unstubbing.
     */
    private fun parkAtActivationBegin(park: Park) {
        mockkObject(PhotoTrashController)
        every { PhotoTrashController.beginActivationWrite() } answers {
            val revision = callOriginal()
            park.reach()
            park.hold()
            revision
        }
    }

    /** Park AFTER the commit but before the seam's publish/settle (the real publish runs after the hold). */
    private fun parkAtPublication(park: Park) {
        mockkObject(PhotoTrashController)
        every {
            PhotoTrashController.noteActivationWritten(any(), any(), any(), any())
        } answers {
            park.reach()
            park.hold()
            callOriginal()
        }
    }

    /**
     * Failure injected AT the production publish seam, after the transaction committed for
     * real: the seam's finally settles the ABANDONED write. This is the post-commit
     * publication-failure variant of the abandoned-write rule (RESUME-9118-1853 D) — the
     * in-transaction rollback variant stays JVM-covered (see the class kDoc).
     */
    private fun failAtPublication() {
        mockkObject(PhotoTrashController)
        every {
            PhotoTrashController.noteActivationWritten(any(), any(), any(), any())
        } throws IllegalStateException("injected publication failure after the real commit")
    }

    private fun joinedWithoutFailure(thread: Thread, failure: AtomicReference<Throwable?>) {
        thread.join(15_000)
        assertFalse(thread.isAlive, "the writer thread finished")
        assertNull(failure.get(), "the real write ran cleanly: ${failureText(failure)}")
    }

    private fun committedRow(config: RealmConfiguration, userId: Int): SyncSettings =
        Realm.getInstance(config).use { realm ->
            realm.copyFromRealm(realm.where(SyncSettings::class.java).equalTo("userId", userId).findFirst()!!, 0)
        }

    @Test
    @DisplayName("a real settings write is unavailable to destructive launch until it settles")
    fun aRealSettingsWriteIsUnavailableToDestructiveLaunchUntilItSettles() {
        val config = freshConfig()
        seedSettings(config, userId = 981, driveId = 771, enabled = true)
        UploadFile.getAppSyncSettings(config) // warm the mirror through the real getter
        val warmed = PhotoTrashController.currentActivationSnapshot
        assertNotNull(warmed)

        // PHASE 1: parked INSIDE the open transaction, after the real unsettled begin mark
        // (before the mutation).
        val opened1 = CountDownLatch(1)
        val release1 = CountDownLatch(1)
        val failure1 = AtomicReference<Throwable?>(null)
        parkAtActivationBegin(Park(opened1, release1))
        val writer1 = writerThread(
            SyncSettings(userId = 981, driveId = 771, photoTrashSyncEnabled = false),
            config,
            failure1,
        )
        try {
            writer1.start()
            assertTrue(opened1.await(15, TimeUnit.SECONDS), "the writer reached its production begin mark inside the open transaction")
            assertEquals(1, PhotoTrashController.activationWritesUnsettled(), "the real seam marked the write unsettled inside its transaction")
            assertEquals(warmed, PhotoTrashController.currentActivationSnapshot, "nothing publishes while a write is unsettled")
        } finally {
            release1.countDown()
            joinedWithoutFailure(writer1, failure1)
        }
        assertEquals(0, PhotoTrashController.activationWritesUnsettled(), "the publish settles the write")
        assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn, "the committed disable is published by the real seam")

        // PHASE 2: parked AFTER the real commit, before the seam's publish/settle — the
        // commit-before-publish interval at the real seam (the phase-1 begin stub passes
        // this writer through: its release latch is already open).
        val committed2 = CountDownLatch(1)
        val release2 = CountDownLatch(1)
        val failure2 = AtomicReference<Throwable?>(null)
        parkAtPublication(Park(committed2, release2))
        val writer2 = writerThread(
            SyncSettings(userId = 981, driveId = 771, photoTrashSyncEnabled = true),
            config,
            failure2,
        )
        try {
            writer2.start()
            assertTrue(committed2.await(15, TimeUnit.SECONDS), "the writer's transaction committed")
            assertTrue(committedRow(config, userId = 981).photoTrashSyncEnabled, "the re-enable HAS committed")
            assertEquals(1, PhotoTrashController.activationWritesUnsettled(), "the seam has not settled its committed write yet")
            assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn, "the committed enable is not published yet")
        } finally {
            release2.countDown()
            joinedWithoutFailure(writer2, failure2)
        }
        assertEquals(0, PhotoTrashController.activationWritesUnsettled())
        assertEquals(true, PhotoTrashController.currentActivationSnapshot?.optIn, "the publish lands before the settle completes")
    }

    @Test
    @DisplayName("a real write whose publication fails after the commit settles without stranding the authority")
    fun aFailedPublicationSettlesWithoutStrandingTheAuthority() {
        val config = freshConfig()
        seedSettings(config, userId = 982, driveId = 772, enabled = true)
        UploadFile.getAppSyncSettings(config) // warm/publish the enabled state through the real getter
        val warmed = PhotoTrashController.currentActivationSnapshot
        assertNotNull(warmed)

        failAtPublication()
        val failure = AtomicReference<Throwable?>(null)
        val writer = writerThread(
            SyncSettings(userId = 982, driveId = 772, photoTrashSyncEnabled = false),
            config,
            failure,
        )
        writer.start()
        writer.join(15_000)
        assertFalse(writer.isAlive, "the failed writer finished")
        assertTrue(failure.get() is IllegalStateException, "the injected publication failure surfaced through the real seam: ${failureText(failure)}")
        assertEquals(0, PhotoTrashController.activationWritesUnsettled(), "the abandoned write settled in its finally: nothing stranded")

        // POST-COMMIT failure (unlike a rollback): the disable COMMITTED, but the mirror was
        // never republished. The begin's revision bump leads the unpublished mirror.
        assertFalse(committedRow(config, userId = 982).photoTrashSyncEnabled, "the disable committed before the publication failed")
        assertEquals(warmed, PhotoTrashController.currentActivationSnapshot, "a failed publication republishes nothing")

        // THE AUTHORITY IS INCOHERENT (the revisions lead the mirror): the launch gate
        // refuses it. A rollback and a post-commit publication failure are indistinguishable
        // without a rebuild, and neither may be trusted.
        val witness = PhotoTrashController.captureLaunchWitness("content://fixture/seam-failure")
        assertFalse(witness.activationSettled, "the revisions leading the mirror is an incoherent capture")
        assertTrue(witness.activationSnapshot?.optIn == true, "the mirror content is unchanged; settledness is what failed")

        // Certified reads rebuild WITHOUT another write: the real getter republishes the
        // committed (disabled) state at the current revisions, and coherence returns.
        val read = UploadFile.getAppSyncSettings(config)
        assertNotNull(read)
        assertFalse(read!!.photoTrashSyncEnabled)
        assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn)
        assertTrue(
            PhotoTrashController.captureLaunchWitness("content://fixture/seam-failure").activationSettled,
            "a certified read rebuilt the authority",
        )
    }

    @Test
    @DisplayName("a real marshal fault AFTER in-transaction column writes rolls back, settles, and recovers by certified read")
    fun realRollbackAfterInTransactionWritesSettlesAndRecovers() {
        val config = freshConfig()
        seedSettings(config, userId = 983, driveId = 773, enabled = true)
        UploadFile.getAppSyncSettings(config) // warm/publish the enabled state through the real getter
        val warmed = PhotoTrashController.currentActivationSnapshot
        assertNotNull(warmed)
        val seededRow = committedRow(config, userId = 983)

        // THE NARROW SUPPORTED SEAM (no Realm spying, no production hook): the generated
        // SyncSettings marshal writes createDatedSubFolders then driveId BEFORE marshalling
        // lastSync via getTime() — so this ordinary Date subclass faults INSIDE the
        // production executeTransaction after REAL column writes. Its read of the writer's
        // own in-transaction state is the positive evidence that a real mutation was in
        // flight when the transaction cancelled.
        val inTransactionDriveId = AtomicInteger(Int.MIN_VALUE)
        val inTransactionStillEnabled = AtomicBoolean(false)
        class FaultingDate : Date() {
            override fun getTime(): Long {
                val row = Realm.getInstance(config).where(SyncSettings::class.java)
                    .equalTo(SyncSettings::userId.name, 983).findFirst()
                inTransactionDriveId.set(row?.driveId ?: Int.MIN_VALUE)
                inTransactionStillEnabled.set(row?.photoTrashSyncEnabled ?: false)
                throw IllegalStateException("injected marshal fault after real in-transaction column writes")
            }
        }
        val faultingSettings = SyncSettings(
            userId = 983,
            driveId = 773_001, // changed: the in-transaction read must see the NEW value
            photoTrashSyncEnabled = false,
        ).apply { lastSync = FaultingDate() }

        val failure = AtomicReference<Throwable?>(null)
        val writer = writerThread(faultingSettings, config, failure)
        writer.start()
        writer.join(15_000)
        assertFalse(writer.isAlive, "the faulting writer finished")
        assertTrue(failure.get() is IllegalStateException, "the marshal fault surfaced through the real seam: ${failureText(failure)}")
        assertEquals(0, PhotoTrashController.activationWritesUnsettled(), "the rolled-back write settled in its finally: nothing stranded")

        // POSITIVE in-transaction evidence: the marshal had already written the NEW driveId
        // while the row was still the seeded enabled row (photoTrashSyncEnabled is written
        // AFTER lastSync in the generated order) — a real mutation was in flight inside the
        // open transaction when the fault landed.
        assertEquals(773_001, inTransactionDriveId.get(), "the first column write was live inside the transaction")
        assertTrue(inTransactionStillEnabled.get(), "the not-yet-written column still held the committed value")

        // REAL ROLLBACK: nothing committed — the row is exactly the seeded state.
        val rolledBack = committedRow(config, userId = 983)
        assertEquals(773, rolledBack.driveId, "the in-transaction driveId write was rolled back")
        assertEquals(true, rolledBack.photoTrashSyncEnabled, "the seeded enable survived the rollback")
        assertEquals(seededRow.photoTrashActivationEpoch, rolledBack.photoTrashActivationEpoch, "the epoch never advanced")
        assertEquals(seededRow.lastSync.time, rolledBack.lastSync.time, "the faulting timestamp was never written")

        // NOTHING PUBLISHED: the mirror is unchanged, and the begin's revision bump leads it
        // — the authority is INCOHERENT, the launch gate refuses it until a certified read.
        assertEquals(warmed, PhotoTrashController.currentActivationSnapshot, "a rollback publishes nothing")
        assertFalse(
            PhotoTrashController.captureLaunchWitness("content://fixture/seam-rollback").activationSettled,
            "the revisions leading the mirror is an incoherent capture",
        )

        // Certified reads rebuild WITHOUT another write: the getter republishes the committed
        // (enabled) state and coherence returns.
        val read = UploadFile.getAppSyncSettings(config)
        assertNotNull(read)
        assertEquals(true, read!!.photoTrashSyncEnabled)
        assertEquals(
            warmed?.activationEpoch,
            PhotoTrashController.currentActivationSnapshot?.activationEpoch,
            "the committed epoch is the published truth",
        )
        assertTrue(
            PhotoTrashController.captureLaunchWitness("content://fixture/seam-rollback").activationSettled,
            "a certified read rebuilt the authority",
        )
    }

    @Test
    @DisplayName("the real settings getter publishes only certified reads while a write is unsettled")
    fun theRealSettingsGetterPublishesOnlyCertifiedReads() {
        val config = freshConfig()
        seedSettings(config, userId = 983, driveId = 773, enabled = true)
        UploadFile.getAppSyncSettings(config) // warm/publish the enabled state
        val before = PhotoTrashController.currentActivationSnapshot
        assertNotNull(before)

        // A REAL writer parks inside its open transaction, having begun (unsettled) a disable.
        val opened = CountDownLatch(1)
        val release = CountDownLatch(1)
        val failure = AtomicReference<Throwable?>(null)
        parkAtActivationBegin(Park(opened, release))
        val writer = writerThread(
            SyncSettings(userId = 983, driveId = 773, photoTrashSyncEnabled = false),
            config,
            failure,
        )
        try {
            writer.start()
            assertTrue(opened.await(15, TimeUnit.SECONDS), "the writer reached its production begin mark inside the open transaction")

            // THE SCHEDULE at the real seam: the reader reads the PRE-commit (still enabled)
            // row while the writer is unsettled — it must return that row (the caller's
            // decision evidence) and publish NOTHING at the writer's revision.
            val read = UploadFile.getAppSyncSettings(config)
            assertNotNull(read)
            assertTrue(read!!.photoTrashSyncEnabled, "the pre-commit row is what the caller sees")
            assertEquals(before, PhotoTrashController.currentActivationSnapshot, "an uncertified read never publishes")
        } finally {
            release.countDown()
            joinedWithoutFailure(writer, failure)
        }

        // After the settle, the committed state is what reads publish:
        val after = UploadFile.getAppSyncSettings(config)
        assertNotNull(after)
        assertFalse(after!!.photoTrashSyncEnabled, "the committed disable is visible to the caller")
        assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn, "the settled authority reflects the commit")
    }

    @Test
    @DisplayName("a launch section holds a real settings write at its begin; the launch wins")
    fun aLaunchSectionHoldsARealSettingsWriteAtItsBegin() {
        val config = freshConfig()
        seedSettings(config, userId = 984, driveId = 774, enabled = true)
        UploadFile.getAppSyncSettings(config) // warm/publish the enabled state
        val uri = "content://fixture/seam-launch"
        val request = PhotoTrashController.ConsentRequest(
            uri = uri,
            userId = 984,
            driveId = 774,
            remoteFileId = 900_601,
            pendingSince = 1_000L,
            pairedAt = 0L,
            activationEpoch = PhotoTrashController.currentActivationSnapshot?.activationEpoch ?: 0L,
            bindingGeneration = PhotoTrashController.bindingGeneration(uri),
        )

        assertTrue(PhotoTrashController.tryBeginLaunchSection(), "Main takes the launch section")
        val failure = AtomicReference<Throwable?>(null)
        val writerDone = CountDownLatch(1)
        var writer: Thread? = null
        try {
            // The section's witness is coherent and the gate accepts — the launch decision:
            val witness = PhotoTrashController.captureLaunchWitness(uri)
            assertTrue(witness.activationSettled, "the section's capture is coherent")
            assertTrue(PhotoTrashConsentUiQueue.launchAllowedNow(request, 984, witness, lifecycleStarted = true))

            // A REAL settings write arriving now blocks INSIDE its open transaction, at its
            // begin (beginActivationWrite waits on the launch section's monitor): no
            // mutation, no publish. NO MockK interception is active here — the writer blocks
            // in the REAL monitor wait, and the contention evidence below inspects the real
            // frame; the bounded absence then measures the guard, not a non-started thread.
            val w = writerThread(
                SyncSettings(userId = 984, driveId = 774, photoTrashSyncEnabled = false),
                config,
                failure,
                onDone = { writerDone.countDown() },
            )
            writer = w
            w.start()
            awaitWriterContendingAtBegin(w)
            assertFalse(writerDone.await(500, TimeUnit.MILLISECONDS), "no real write may begin while a launch section is active")
            assertEquals(0, PhotoTrashController.activationWritesUnsettled(), "the section's witness stays valid: nothing began")
            assertTrue(committedRow(config, userId = 984).photoTrashSyncEnabled, "the held write has not mutated the row")

            // LAUNCH WINS: the section ends after the launch completed; the writer then
            // begins, commits and publishes its disable — observing a request already issued.
            PhotoTrashController.endLaunchSection()
            assertTrue(writerDone.await(15, TimeUnit.SECONDS), "the real write proceeds once the section ends")
            joinedWithoutFailure(writer, failure)
            assertEquals(0, PhotoTrashController.activationWritesUnsettled())
            assertFalse(committedRow(config, userId = 984).photoTrashSyncEnabled, "the disable committed after the launch")
            assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn, "the disable published after the launch decision consumed the pre-disable witness")
            assertFalse(
                PhotoTrashConsentUiQueue.launchAllowedNow(
                    request,
                    984,
                    PhotoTrashController.captureLaunchWitness(uri),
                    lifecycleStarted = true,
                ),
                "the NEXT gate evaluation reads the disabled state",
            )
        } finally {
            // Never leak the section or a held writer into other cases, whatever failed.
            PhotoTrashController.endLaunchSection()
            writer?.join(15_000)
        }
    }

    @Test
    @DisplayName("a real write unsettled at the section's capture refuses the launch (write wins)")
    fun aRealWriteUnsettledAtTheSectionCaptureRefusesTheLaunch() {
        val config = freshConfig()
        seedSettings(config, userId = 985, driveId = 775, enabled = true)
        UploadFile.getAppSyncSettings(config)
        val warmed = PhotoTrashController.currentActivationSnapshot
        assertNotNull(warmed)
        val uri = "content://fixture/seam-write-wins"
        val request = PhotoTrashController.ConsentRequest(
            uri = uri,
            userId = 985,
            driveId = 775,
            remoteFileId = 900_701,
            pendingSince = 1_000L,
            pairedAt = 0L,
            activationEpoch = warmed!!.activationEpoch,
            bindingGeneration = PhotoTrashController.bindingGeneration(uri),
        )

        // A REAL writer parks inside its open transaction, having begun (unsettled) a
        // disable: the real mark landed inside the real transaction, before the mutation.
        val opened = CountDownLatch(1)
        val release = CountDownLatch(1)
        val failure = AtomicReference<Throwable?>(null)
        parkAtActivationBegin(Park(opened, release))
        val writer = writerThread(
            SyncSettings(userId = 985, driveId = 775, photoTrashSyncEnabled = false),
            config,
            failure,
        )
        try {
            writer.start()
            assertTrue(opened.await(15, TimeUnit.SECONDS), "the writer reached its production begin mark inside the open transaction")

            // WRITE WINS: the section begins after the write did; its witness reads the
            // unsettled begin and the gate refuses — the launch never pairs the pre-write
            // mirror with a settled flag.
            assertTrue(PhotoTrashController.tryBeginLaunchSection())
            try {
                val witness = PhotoTrashController.captureLaunchWitness(uri)
                assertFalse(witness.activationSettled, "the section's capture reads the unsettled real write")
                assertTrue(witness.activationSnapshot?.optIn == true, "the mirror still holds the pre-write content; settledness is what failed")
                assertFalse(
                    PhotoTrashConsentUiQueue.launchAllowedNow(request, 985, witness, lifecycleStarted = true),
                    "write wins: no launch while a real write is unsettled",
                )
            } finally {
                PhotoTrashController.endLaunchSection()
            }
        } finally {
            release.countDown()
            joinedWithoutFailure(writer, failure)
        }
        assertEquals(0, PhotoTrashController.activationWritesUnsettled())
        assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn, "the disable published after the section ended")
        assertFalse(
            PhotoTrashConsentUiQueue.launchAllowedNow(
                request,
                985,
                PhotoTrashController.captureLaunchWitness(uri),
                lifecycleStarted = true,
            ),
            "the NEXT gate evaluation reads the disabled state",
        )
    }
}
