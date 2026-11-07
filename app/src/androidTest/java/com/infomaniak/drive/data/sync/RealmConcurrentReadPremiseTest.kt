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
import com.infomaniak.drive.utils.RealmModules
import io.realm.Realm
import io.realm.RealmConfiguration
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference

/**
 * THE REALM CONCURRENCY PREMISE of the activation authority's certified-read protocol
 * (CHALLENGE-62a3 finding 2): a write transaction that is OPEN on one instance does NOT
 * block readers on another instance, and those readers observe the LAST COMMITTED state —
 * the uncommitted write is invisible until its commit. This is exactly why a reader that
 * captures its revision permit while a write is assigned-but-uncommitted would read the
 * PRE-commit row: the premise the certified-read protocol must neutralize. Demonstrated
 * here in vitro on a disposable fixture realm rather than assumed.
 *
 * Fully disposable and account-free: a random fixture file in the instrumentation target's
 * cacheDir, one synthetic row — NO owner data, NO account, NO network.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RealmConcurrentReadPremiseTest {

    private val context = ApplicationProvider.getApplicationContext<android.content.Context>()
    private val fixtureName = "realmConcurrentReadPremise-${UUID.randomUUID()}"
    private val config = RealmConfiguration.Builder()
        .schemaVersion(UploadMigration.DB_VERSION)
        .modules(RealmModules.SyncFilesModule())
        .name(fixtureName)
        .directory(context.cacheDir)
        .build()

    @Test
    @DisplayName("an open write transaction hides its state from other instances until commit")
    fun anOpenWriteTransactionHidesItsStateFromOtherInstancesUntilCommit() {
        Realm.getInstance(config).use { setup ->
            setup.executeTransaction { transactionRealm ->
                transactionRealm.createObject(SyncSettings::class.java, 977).apply { driveId = 771 }
            }
        }

        val writerOpened = CountDownLatch(1)
        val writerMayCommit = CountDownLatch(1)
        val writerDone = CountDownLatch(1)
        val writerFailure = AtomicReference<Throwable?>(null)
        val writer = Thread {
            try {
                Realm.getInstance(config).use { writerRealm ->
                    // The transaction stays OPEN until the harness releases it — the
                    // in-vitro stand-in for a writer paused between its in-transaction
                    // revision assignment and its commit.
                    writerRealm.executeTransaction { transactionRealm ->
                        transactionRealm.where(SyncSettings::class.java)
                            .equalTo("userId", 977)
                            .findFirst()?.syncVideo = true
                        writerOpened.countDown()
                        check(writerMayCommit.await(15, TimeUnit.SECONDS)) { "the premise harness never released the writer" }
                    }
                }
            } catch (throwable: Throwable) {
                writerFailure.set(throwable)
            } finally {
                writerDone.countDown()
            }
        }
        writer.start()
        try {
            assertTrue(writerOpened.await(15, TimeUnit.SECONDS), "the writer opened its transaction")

            // A DIFFERENT instance reads WHILE the write transaction is open: readers are
            // never blocked, and they see the last COMMITTED version.
            val duringWrite = Realm.getInstance(config).use { reader ->
                reader.where(SyncSettings::class.java).equalTo("userId", 977).findFirst()?.syncVideo
            }
            assertEquals(
                false,
                duringWrite,
                "an open write transaction does not publish its state to other instances' readers",
            )
        } finally {
            writerMayCommit.countDown()
        }
        assertTrue(writerDone.await(15, TimeUnit.SECONDS), "the writer finished")
        assertNull(writerFailure.get(), "the writer transaction committed cleanly: ${writerFailure.get()}")

        // The committed state becomes visible only once the commit landed.
        val afterCommit = Realm.getInstance(config).use { reader ->
            reader.where(SyncSettings::class.java).equalTo("userId", 977).findFirst()?.syncVideo
        }
        assertEquals(true, afterCommit, "the uncommitted write surfaces only after its commit")
    }

    @AfterAll
    fun cleanup() {
        // ONLY the random own-cache fixture is deleted — never any other realm, never any
        // owner data.
        runCatching { Realm.deleteRealm(config) }
    }
}
