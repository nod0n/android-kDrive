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
import com.infomaniak.drive.utils.RealmModules
import io.mockk.every
import io.mockk.spyk
import io.realm.Realm
import io.realm.RealmConfiguration
import org.junit.jupiter.api.AfterAll
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
import io.mockk.unmockkAll
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * THE REAL SETTINGS SEAM under the unsettled-writes protocol (SOFTCHECK-1828): the writer
 * schedules that the JVM helper tests cannot distinguish — a REAL [UploadFile.setAppSyncSettings]
 * parked INSIDE its open transaction (begun, uncommitted) and AFTER its commit but before its
 * publish/settle — plus failed-write recovery, and the REAL [UploadFile.getAppSyncSettings]
 * publishing only certified reads under the pre-commit poisoning schedule. The park is a
 * Mockito-free spy wrapper around [Realm.executeTransaction] on a DISPOSABLE fixture realm:
 * the production seam runs unmodified (`customRealm` injection).
 *
 * Fully disposable and account-free: a random fixture file in the instrumentation target's
 * cacheDir, synthetic rows only — NO owner data, NO account, NO network.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PhotoTrashActivationSeamTest {

    private val context = ApplicationProvider.getApplicationContext<android.content.Context>()
    private val fixtureName = "photoTrashActivationSeam-${UUID.randomUUID()}"
    private val config = RealmConfiguration.Builder()
        .schemaVersion(UploadMigration.DB_VERSION)
        .modules(RealmModules.SyncFilesModule())
        .name(fixtureName)
        .directory(context.cacheDir)
        .build()

    @BeforeAll
    fun initRealm() {
        Realm.init(context)
    }

    @AfterEach
    fun tearDownMocks() {
        unmockkAll()
    }

    /** Seeds one synthetic settings row. */
    private fun seedSettings(userId: Int, driveId: Int, enabled: Boolean) {
        Realm.getInstance(config).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(SyncSettings::class.java, userId).apply {
                    this.driveId = driveId
                    photoTrashSyncEnabled = enabled
                }
            }
        }
    }

    /**
     * A spied fixture realm whose [Realm.executeTransaction] parks the CALLING seam at the
     * chosen point: INSIDE the open transaction after its body (begun, uncommitted), or
     * AFTER the commit but before the interceptor returns to the seam (committed, publish
     * and settle still pending).
     */
    private fun parkedRealm(
        parkInsideTransaction: Boolean,
        opened: CountDownLatch,
        release: CountDownLatch,
    ): Realm {
        val spied = spyk(Realm.getInstance(config))
        every { spied.executeTransaction(any()) } answers {
            if (parkInsideTransaction) {
                val original = firstArg<Realm.Transaction>()
                spied.executeTransaction { transactionRealm ->
                    original.execute(transactionRealm)
                    opened.countDown()
                    check(release.await(15, TimeUnit.SECONDS)) { "the harness never released the parked writer" }
                }
            } else {
                callOriginal()
                opened.countDown()
                check(release.await(15, TimeUnit.SECONDS)) { "the harness never released the parked writer" }
            }
        }
        return spied
    }

    private fun writerThread(
        settings: SyncSettings,
        realm: Realm,
        failure: AtomicReference<Throwable?>,
    ): Thread = Thread {
        try {
            UploadFile.setAppSyncSettings(settings, realm)
        } catch (throwable: Throwable) {
            failure.set(throwable)
        }
    }

    private fun joinedWithoutFailure(thread: Thread, failure: AtomicReference<Throwable?>) {
        thread.join(15_000)
        assertFalse(thread.isAlive, "the writer thread finished")
        assertNull(failure.get(), "the real write ran cleanly: ${failure.get()}")
    }

    @Test
    @DisplayName("a real settings write is unavailable to destructive launch until it settles")
    fun aRealSettingsWriteIsUnavailableToDestructiveLaunchUntilItSettles() {
        seedSettings(userId = 981, driveId = 771, enabled = true)
        UploadFile.getAppSyncSettings(config) // warm the mirror through the real getter
        val warmed = PhotoTrashController.currentActivationSnapshot

        // PHASE 1: parked INSIDE the open transaction (begun, uncommitted).
        val opened1 = CountDownLatch(1)
        val release1 = CountDownLatch(1)
        val failure1 = AtomicReference<Throwable?>(null)
        val writer1 = writerThread(
            SyncSettings(userId = 981, driveId = 771, photoTrashSyncEnabled = false),
            parkedRealm(parkInsideTransaction = true, opened = opened1, release = release1),
            failure1,
        )
        writer1.start()
        assertTrue(opened1.await(15, TimeUnit.SECONDS), "the writer reached its transaction body")
        assertEquals(1, PhotoTrashController.activationWritesUnsettled(), "the real seam marked the write unsettled inside its transaction")
        assertEquals(warmed, PhotoTrashController.currentActivationSnapshot, "nothing publishes while a write is unsettled")
        release1.countDown()
        joinedWithoutFailure(writer1, failure1)
        assertEquals(0, PhotoTrashController.activationWritesUnsettled(), "the publish settles the write")
        assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn, "the committed disable is published by the real seam")

        // PHASE 2: parked AFTER the commit, before the seam's publish/settle — the
        // commit-before-publish interval at the real seam.
        val committed2 = CountDownLatch(1)
        val release2 = CountDownLatch(1)
        val failure2 = AtomicReference<Throwable?>(null)
        val writer2 = writerThread(
            SyncSettings(userId = 981, driveId = 771, photoTrashSyncEnabled = true),
            parkedRealm(parkInsideTransaction = false, opened = committed2, release = release2),
            failure2,
        )
        writer2.start()
        assertTrue(committed2.await(15, TimeUnit.SECONDS), "the writer's transaction committed")
        val committedRow = Realm.getInstance(config).use { realm ->
            realm.where(SyncSettings::class.java).equalTo("userId", 981).findFirst()!!.photoTrashSyncEnabled
        }
        assertTrue(committedRow, "the re-enable HAS committed")
        assertEquals(1, PhotoTrashController.activationWritesUnsettled(), "the seam has not settled its committed write yet")
        assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn, "the committed enable is not published yet")
        release2.countDown()
        joinedWithoutFailure(writer2, failure2)
        assertEquals(0, PhotoTrashController.activationWritesUnsettled())
        assertEquals(true, PhotoTrashController.currentActivationSnapshot?.optIn, "the publish lands before the settle completes")
    }

    @Test
    @DisplayName("a failed real write settles without stranding the authority")
    fun aFailedRealWriteSettlesWithoutStrandingTheAuthority() {
        seedSettings(userId = 982, driveId = 772, enabled = true)
        val spied = spyk(Realm.getInstance(config))
        every { spied.executeTransaction(any()) } throws IllegalStateException("injected transaction failure")

        val outcome = runCatching {
            UploadFile.setAppSyncSettings(SyncSettings(userId = 982, driveId = 772, photoTrashSyncEnabled = false), spied)
        }
        assertTrue(outcome.isFailure, "the injected failure surfaced through the real seam")
        assertEquals(0, PhotoTrashController.activationWritesUnsettled(), "the abandoned write settled in its finally: nothing stranded")

        // Certified reads resume WITHOUT requiring another write, and the real getter
        // publishes the committed (still-enabled) state.
        val read = UploadFile.getAppSyncSettings(config)
        assertNotNull(read)
        assertTrue(read!!.photoTrashSyncEnabled)
        assertEquals(true, PhotoTrashController.currentActivationSnapshot?.optIn)
    }

    @Test
    @DisplayName("the real settings getter publishes only certified reads (pre-commit poisoning schedule)")
    fun theRealSettingsGetterPublishesOnlyCertifiedReads() {
        seedSettings(userId = 983, driveId = 773, enabled = true)
        UploadFile.getAppSyncSettings(config) // warm/publish the enabled state
        val before = PhotoTrashController.currentActivationSnapshot

        // A REAL writer parks inside its open transaction, having begun (unsettled) a disable.
        val opened = CountDownLatch(1)
        val release = CountDownLatch(1)
        val failure = AtomicReference<Throwable?>(null)
        val writer = writerThread(
            SyncSettings(userId = 983, driveId = 773, photoTrashSyncEnabled = false),
            parkedRealm(parkInsideTransaction = true, opened = opened, release = release),
            failure,
        )
        writer.start()
        assertTrue(opened.await(15, TimeUnit.SECONDS), "the writer reached its transaction body")

        // THE POISONING SCHEDULE at the real seam: the reader reads the PRE-commit (still
        // enabled) row while the writer is unsettled — it must return that row (the caller's
        // decision evidence) and publish NOTHING at the writer's revision.
        val read = UploadFile.getAppSyncSettings(config)
        assertNotNull(read)
        assertTrue(read!!.photoTrashSyncEnabled, "the pre-commit row is what the caller sees")
        assertEquals(before, PhotoTrashController.currentActivationSnapshot, "an uncertified read never publishes")

        release.countDown()
        joinedWithoutFailure(writer, failure)

        // After the settle, the committed state is what reads publish:
        val after = UploadFile.getAppSyncSettings(config)
        assertNotNull(after)
        assertFalse(after!!.photoTrashSyncEnabled, "the committed disable is visible to the caller")
        assertEquals(false, PhotoTrashController.currentActivationSnapshot?.optIn, "the settled authority reflects the commit")
    }

    @AfterAll
    fun cleanup() {
        // ONLY the random own-cache fixture is deleted — never any other realm, never any
        // owner data.
        runCatching { Realm.deleteRealm(config) }
    }
}
