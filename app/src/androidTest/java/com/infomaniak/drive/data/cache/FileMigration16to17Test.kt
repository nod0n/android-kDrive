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
package com.infomaniak.drive.data.cache

import androidx.test.core.app.ApplicationProvider
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.OfflineRoot
import com.infomaniak.drive.utils.RealmModules
import io.realm.DynamicRealm
import io.realm.Realm
import io.realm.RealmConfiguration
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.Date
import java.util.UUID

/**
 * THE REAL 16→17 MIGRATION FIXTURE (androidTest instrumentation — Realm Java is a native
 * engine and CANNOT open on the JVM; this is why the test lives here and not under src/test).
 *
 * THE HONEST SHAPE CORRECTION (the test-plan round's correction, implemented): a SUBSET File
 * schema cannot be reopened against the full production module, so the fixture instead
 *  (A) creates a disposable TYPED realm at the FULL current [RealmModules.LocalFilesModule]
 *      schema STAMPED schemaVersion 16 (the module's classes carry the current shape; the
 *      stamp comes from the configuration),
 *  (B) opens the SAME file as a [DynamicRealm] at 16 and removes ONLY `serverContentHash` in
 *      one transaction — the file then holds the TRUE v16 shape (every current field except
 *      the new column), verified by a schema audit before closing,
 *  (C) reopens at [FileMigration.DB_VERSION] (17) with the REAL [FileMigration] and the FULL
 *      production module — exactly what a production upgrade executes — and audits ONLY the
 *      16→17 difference: the added column exists, is nullable, reads null on EVERY migrated
 *      row (nothing invented), every pre-existing field is preserved byte-equal, and the new
 *      column is writable.
 *
 * THE FIXTURE IS FULLY DISPOSABLE AND ACCOUNT-FREE: a RANDOM file in the instrumentation
 * target's cacheDir (its own name, deleted in [cleanup]); every row is a synthetic constant —
 * NO owner data, NO account, NO network. A second File row represents the shared-with-me
 * shape honestly: shared rows live in their own realm FILE with the SAME LocalFilesModule
 * schema (File carries no shared flag), so the schema-level migration treats them identically
 * and one extra synthetic identity pins their preservation.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FileMigration16to17Test {

    private val fixtureName = "migration16to17-${UUID.randomUUID()}"
    private val fixtureDir = ApplicationProvider.getApplicationContext<android.content.Context>().cacheDir

    private val configAt16 = RealmConfiguration.Builder()
        .schemaVersion(16L)
        .modules(RealmModules.LocalFilesModule())
        .name(fixtureName)
        .directory(fixtureDir)
        .build()

    private val configAt17 = RealmConfiguration.Builder()
        .schemaVersion(FileMigration.DB_VERSION)
        .migration(FileMigration())
        .modules(RealmModules.LocalFilesModule())
        .name(fixtureName)
        .directory(fixtureDir)
        .build()

    /** THE ORDINARY ROW'S SYNTHETIC FIXTURE (no owner data — synthetic constants only). */
    private fun seedOrdinary(file: File) = file.apply {
        uid = "900001_701"
        id = 900001
        driveId = 701
        name = "fixture-ordinary.bin"
        path = "/fixture/ordinary.bin"
        lastModifiedAt = 1_700_000_000L
        revisedAt = 1_700_000_100L
        updatedAt = 1_700_000_200L
        isMarkedAsOffline = true
        isInOfflineFolder = true
        isOffline = false
        isFromUploads = false
        offlineIntentStamp = 42L
        publishedCleanBaseline = 1_700_000_000L
    }

    /** THE SHARED-SHAPE ROW (the same schema — shared-with-me rows live in their own realm
     * FILE; the migration is schema-level and treats every File row identically). */
    private fun seedShared(file: File) = file.apply {
        uid = "900002_701"
        id = 900002
        driveId = 701
        name = "fixture-shared.bin"
        path = "/fixture/shared.bin"
        lastModifiedAt = 1_700_001_000L
        revisedAt = 1_700_001_100L
        updatedAt = 1_700_001_200L
        isMarkedAsOffline = false
        isInOfflineFolder = true
        isOffline = false
        isFromUploads = false
        offlineIntentStamp = 7L
        publishedCleanBaseline = null
    }

    @Test
    fun `migration 16 to 17 adds a nullable empty serverContentHash and preserves every row`() {
        // ---- PHASE A: a disposable typed realm at the full current module shape, stamped 16.
        Realm.getInstance(configAt16).use { realm ->
            realm.executeTransaction { transactionRealm ->
                seedOrdinary(transactionRealm.createObject(File::class.java, "900001_701"))
                seedShared(transactionRealm.createObject(File::class.java, "900002_701"))
                transactionRealm.createObject(OfflineRoot::class.java, "800001_701").apply {
                    folderId = 800001
                    driveId = 701
                    userId = 901
                    name = "fixture-root"
                    path = "/fixture/root"
                    createdAt = Date(1_700_000_000_000L)
                    lastSyncedAt = Date(1_700_000_500_000L)
                    isPendingRemoval = false
                }
            }
        }

        // ---- PHASE B: the same file as a DynamicRealm at 16 — remove ONLY the new column,
        // then verify the TRUE v16 shape before closing.
        DynamicRealm.getInstance(configAt16).use { dynamic ->
            dynamic.executeTransaction { transactionRealm ->
                val fileSchema = transactionRealm.schema.get("File")
                check(fileSchema != null && fileSchema.hasField(File::serverContentHash.name)) {
                    "the phase-A module schema must carry the current shape (serverContentHash)"
                }
                fileSchema.removeField(File::serverContentHash.name)
            }
            val fileSchema = dynamic.schema.get("File")
            check(fileSchema != null)
            assertFalse(fileSchema.hasField(File::serverContentHash.name), "the true v16 shape has no hash column")
            assertTrue(fileSchema.hasField("offlineIntentStamp"))
            assertTrue(fileSchema.hasField("publishedCleanBaseline"))
            assertTrue(fileSchema.hasField("isInOfflineFolder"))
            assertTrue(fileSchema.hasField("uid"))
            assertEquals(
                2L,
                dynamic.where("File").count(),
                "both synthetic File rows must exist at the true v16 shape",
            )
        }

        // ---- PHASE C: reopen at 17 with the REAL migration + the full module; audit ONLY
        // the 16→17 difference.
        Realm.getInstance(configAt17).use { realm ->
            val fileSchema = realm.schema.get("File")
            check(fileSchema != null)
            assertTrue(fileSchema.hasField(File::serverContentHash.name), "the migration adds the column")
            assertTrue(fileSchema.isNullable(File::serverContentHash.name), "the added column is nullable")

            val ordinary = realm.where(File::class.java)
                .equalTo("uid", "900001_701")
                .findFirst()!!
            assertNull(ordinary.serverContentHash, "nothing is invented during the migration")
            assertEquals(900001, ordinary.id)
            assertEquals(701, ordinary.driveId)
            assertEquals("fixture-ordinary.bin", ordinary.name)
            assertEquals("/fixture/ordinary.bin", ordinary.path)
            assertEquals(1_700_000_000L, ordinary.lastModifiedAt)
            assertEquals(1_700_000_100L, ordinary.revisedAt)
            assertEquals(1_700_000_200L, ordinary.updatedAt)
            assertTrue(ordinary.isMarkedAsOffline)
            assertTrue(ordinary.isInOfflineFolder)
            assertFalse(ordinary.isOffline)
            assertEquals(42L, ordinary.offlineIntentStamp)
            assertEquals(1_700_000_000L, ordinary.publishedCleanBaseline)

            val shared = realm.where(File::class.java)
                .equalTo("uid", "900002_701")
                .findFirst()!!
            assertNull(shared.serverContentHash)
            assertEquals(900002, shared.id)
            assertEquals("fixture-shared.bin", shared.name)
            assertFalse(shared.isMarkedAsOffline)
            assertTrue(shared.isInOfflineFolder)
            assertEquals(7L, shared.offlineIntentStamp)
            assertNull(shared.publishedCleanBaseline)

            val root = realm.where(OfflineRoot::class.java)
                .equalTo("uid", "800001_701")
                .findFirst()!!
            assertEquals(800001, root.folderId)
            assertEquals(701, root.driveId)
            assertEquals(901, root.userId)
            assertEquals("fixture-root", root.name)
            assertEquals("/fixture/root", root.path)
            assertEquals(Date(1_700_000_000_000L), root.createdAt)
            assertEquals(Date(1_700_000_500_000L), root.lastSyncedAt)
            assertFalse(root.isPendingRemoval)

            // THE NEW COLUMN IS WRITABLE (the authorized ingestion's contract).
            realm.executeTransaction { ordinary.serverContentHash = "xxh3:0123456789abcdef" }
            assertEquals("xxh3:0123456789abcdef", ordinary.serverContentHash)
            realm.executeTransaction { ordinary.serverContentHash = null }
            assertNull(ordinary.serverContentHash)
        }
    }

    @AfterAll
    fun cleanup() {
        // ONLY the random own-cache fixture is deleted — never any other realm, never any
        // owner data.
        runCatching { Realm.deleteRealm(configAt17) }
        runCatching { Realm.deleteRealm(configAt16) }
    }
}
