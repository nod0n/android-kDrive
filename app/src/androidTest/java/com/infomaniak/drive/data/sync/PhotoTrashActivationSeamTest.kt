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
import io.mockk.spyk
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
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * THE REAL SETTINGS SEAM under the unsettled-writes protocol and the launch section
 * (RESUME-9118-1853): the writer schedules the JVM helper tests cannot distinguish — a REAL
 * [UploadFile.setAppSyncSettings] parked INSIDE its open transaction (begun, uncommitted)
 * and AFTER its commit but before its publish/settle — plus a failure injected AFTER the
 * transaction began (real rollback, settled authority, certified-read rebuild), the REAL
 * [UploadFile.getAppSyncSettings] publishing only certified reads under the pre-commit
 * poisoning schedule, and both launch/write orders through the production seam against the
 * Main launch section.
 *
 * Harness discipline (device-evidenced fixes):
 *  - every Realm is opened, used AND closed on the thread that touches it (thread
 *    confinement): each writer thread creates its own spied instance from its own
 *    per-case fixture config and closes it in a finally;
 *  - the park inside the transaction is NONRECURSIVE: the stub performs the transaction
 *    itself (begin -> the production lambda -> park -> commit) instead of re-entering
 *    [Realm.executeTransaction];
 *  - the injected failure happens AFTER the production lambda's begin and mutation, inside
 *    a real transaction frame that is cancelled exactly like Realm cancels a failing
 *    lambda — so the seam settles an ABANDONED write, which is the schedule that matters;
 *  - each case gets its OWN fixture realm (no shared accumulating state, no JUnit-order
 *    dependence), cleanup DELETES each fixture and REPORTS a leak (an open instance makes
 *    deleteRealm throw) instead of swallowing it;
 *  - writers are released and joined in finally even when an assertion fails.
 *
 * SCOPE HONESTY: the launch sections here are helper-driven (the controller's
 * tryBeginLaunchSection/capture/gate plus the REAL settings seam) — they do NOT exercise
 * MainActivity's ActivityResult/lifecycle wiring or any system dialog; that wiring remains a
 * declared device case. Fully disposable and account-free: random fixture files in the
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
     * meaningful; without this evidence a non-started writer would pass it too.
     */
    private fun awaitWriterContendingAtBegin(writer: Thread) {
        assertTrue(
            awaitTrue {
                writer.state == Thread.State.WAITING &&
                    // Method names may carry a variant-transform suffix ($app on the JVM
                    // run): match the base name.
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
     * Runs ONE real [UploadFile.setAppSyncSettings] on its OWN writer thread against ITS OWN
     * spied fixture realm (opened, used AND closed on that thread — Realm instances are
     * thread-confined). The [stub] installs the park; any failure surfaces through
     * [failure]; [onDone] runs after the seam returned (or threw).
     */
    private fun writerThread(
        settings: SyncSettings,
        config: RealmConfiguration,
        failure: AtomicReference<Throwable?>,
        stub: (Realm) -> Unit = {},
        onDone: (() -> Unit)? = null,
    ): Thread = Thread {
        var realm: Realm? = null
        try {
            realm = spyk(Realm.getInstance(config))
            stub(realm)
            UploadFile.setAppSyncSettings(settings, realm)
        } catch (throwable: Throwable) {
            failure.set(throwable)
        } finally {
            runCatching { realm?.close() }
            onDone?.invoke()
        }
    }

    /**
     * NONRECURSIVE park INSIDE the open transaction: the stub performs the transaction
     * itself (begin -> the production lambda -> park -> commit) instead of re-entering
     * [Realm.executeTransaction] (which would hit the same stub). A failure after the park
     * cancels the open transaction, mirroring Realm's rollback semantics.
     */
    private fun parkInsideTransaction(park: Park): (Realm) -> Unit = { spied ->
        every { spied.executeTransaction(any()) } answers {
            val original = firstArg<Realm.Transaction>()
            spied.beginTransaction()
            try {
                original.execute(spied)
                park.reach()
                park.hold()
                spied.commitTransaction()
            } catch (throwable: Throwable) {
                runCatching { if (spied.isInTransaction) spied.cancelTransaction() }
                throw throwable
            }
        }
    }

    /** Park AFTER the commit but before the seam's publish/settle (the real call, then hold). */
    private fun parkAfterCommit(park: Park): (Realm) -> Unit = { spied ->
        every { spied.executeTransaction(any()) } answers {
            callOriginal()
            park.reach()
            park.hold()
        }
    }

    /**
     * Failure injected AFTER the production lambda's begin and mutation, inside a REAL
     * transaction frame (begin -> lambda -> throw, then cancel): the seam's finally settles
     * an ABANDONED write — not a failure that preceded the begin, which would prove nothing.
     */
    private fun failAfterBeginAndMutation(): (Realm) -> Unit = { spied ->
        every { spied.executeTransaction(any()) } answers {
            val original = firstArg<Realm.Transaction>()
            spied.beginTransaction()
            try {
                original.execute(spied)
                throw IllegalStateException("injected transaction failure after begin and mutation")
            } catch (throwable: Throwable) {
                runCatching { if (spied.isInTransaction) spied.cancelTransaction() }
                throw throwable
            }
        }
    }

    private fun joinedWithoutFailure(thread: Thread, failure: AtomicReference<Throwable?>) {
        thread.join(15_000)
        assertFalse(thread.isAlive, "the writer thread finished")
        assertNull(failure.get(), "the real write ran cleanly: ${failure.get()}")
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

        // PHASE 1: parked INSIDE the open transaction (begun, uncommitted).
        val opened1 = CountDownLatch(1)
        val release1 = CountDownLatch(1)
        val failure1 = AtomicReference<Throwable?>(null)
        val writer1 = writerThread(
            SyncSettings(userId = 981, driveId = 771, photoTrashSyncEnabled = false),
            config,
            failure1,
            parkInsideTransaction(Park(opened1, release1)),
        )
        try {
            writer1.start()
            assertTrue(opened1.await(15, TimeUnit.SECONDS), "the writer reached its transaction body")
            assertEquals(1, PhotoTrashController.activationWritesUnsettled(), "the real seam marked the write unsettled inside its transaction")
            assertEquals(warmed, PhotoTrashController.currentActivationSnapshot, "nothing publishes while a write is unsettled")
        } finally {
            release1.countDown()
            joinedWithoutFailure(writer1, failure1)
        }
        assertEquals(0, PhotoTrashController.activationWritesUnsettled(), "the publish settles the write")
        assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn, "the committed disable is published by the real seam")

        // PHASE 2: parked AFTER the commit, before the seam's publish/settle — the
        // commit-before-publish interval at the real seam.
        val committed2 = CountDownLatch(1)
        val release2 = CountDownLatch(1)
        val failure2 = AtomicReference<Throwable?>(null)
        val writer2 = writerThread(
            SyncSettings(userId = 981, driveId = 771, photoTrashSyncEnabled = true),
            config,
            failure2,
            parkAfterCommit(Park(committed2, release2)),
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
    @DisplayName("a failed real write rolls back, settles and resumes certified reads without another write")
    fun aFailedRealWriteSettlesWithoutStrandingTheAuthority() {
        val config = freshConfig()
        seedSettings(config, userId = 982, driveId = 772, enabled = true)
        UploadFile.getAppSyncSettings(config) // warm/publish the enabled state through the real getter
        val warmed = PhotoTrashController.currentActivationSnapshot
        assertNotNull(warmed)

        val failure = AtomicReference<Throwable?>(null)
        val writer = writerThread(
            SyncSettings(userId = 982, driveId = 772, photoTrashSyncEnabled = false),
            config,
            failure,
            failAfterBeginAndMutation(),
        )
        writer.start()
        writer.join(15_000)
        assertFalse(writer.isAlive, "the failed writer finished")
        assertTrue(failure.get() is IllegalStateException, "the injected failure surfaced through the real seam: ${failure.get()}")
        assertEquals(0, PhotoTrashController.activationWritesUnsettled(), "the abandoned write settled in its finally: nothing stranded")

        // REAL rollback: the transaction was cancelled — the committed row is unchanged,
        // and the abandoned write published nothing.
        assertTrue(committedRow(config, userId = 982).photoTrashSyncEnabled, "the failed transaction was cancelled")
        assertEquals(warmed, PhotoTrashController.currentActivationSnapshot, "an abandoned write publishes nothing")

        // THE AUTHORITY IS INCOHERENT (the counter leads the mirror): the launch gate
        // refuses it. A rollback and a post-commit publication failure are indistinguishable
        // without a rebuild, and neither may be trusted.
        val witness = PhotoTrashController.captureLaunchWitness("content://fixture/seam-failure")
        assertFalse(witness.activationSettled, "the counter leading the mirror is an incoherent capture")
        assertTrue(witness.activationSnapshot?.optIn == true, "the mirror content is unchanged; settledness is what failed")

        // Certified reads resume WITHOUT another write: the real getter republishes the
        // committed (still-enabled) state at the current counter, and coherence returns.
        val read = UploadFile.getAppSyncSettings(config)
        assertNotNull(read)
        assertTrue(read!!.photoTrashSyncEnabled)
        assertEquals(true, PhotoTrashController.currentActivationSnapshot?.optIn)
        assertTrue(
            PhotoTrashController.captureLaunchWitness("content://fixture/seam-failure").activationSettled,
            "a certified read rebuilt the authority",
        )
    }

    @Test
    @DisplayName("the real settings getter publishes only certified reads (pre-commit poisoning schedule)")
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
        val writer = writerThread(
            SyncSettings(userId = 983, driveId = 773, photoTrashSyncEnabled = false),
            config,
            failure,
            parkInsideTransaction(Park(opened, release)),
        )
        try {
            writer.start()
            assertTrue(opened.await(15, TimeUnit.SECONDS), "the writer reached its transaction body")

            // THE POISONING SCHEDULE at the real seam: the reader reads the PRE-commit (still
            // enabled) row while the writer is unsettled — it must return that row (the
            // caller's decision evidence) and publish NOTHING at the writer's revision.
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
            // begin (beginActivationWrite waits for the section): no mutation, no publish.
            // The contention is proven POSITIVELY first (the writer verifiably sits inside
            // beginActivationWrite), so the bounded absence below measures the guard, not a
            // non-started thread.
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
            assertTrue(committedRow(config, userId = 984).photoTrashSyncEnabled, "the parked write has not mutated the row")

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
            // Never leak the section or a parked writer into other cases, whatever failed.
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

        // A REAL writer parks inside its open transaction, having begun (unsettled) a disable.
        val opened = CountDownLatch(1)
        val release = CountDownLatch(1)
        val failure = AtomicReference<Throwable?>(null)
        val writer = writerThread(
            SyncSettings(userId = 985, driveId = 775, photoTrashSyncEnabled = false),
            config,
            failure,
            parkInsideTransaction(Park(opened, release)),
        )
        try {
            writer.start()
            assertTrue(opened.await(15, TimeUnit.SECONDS), "the writer reached its transaction body")

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
