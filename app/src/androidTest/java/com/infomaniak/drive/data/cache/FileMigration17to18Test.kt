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
import com.infomaniak.drive.data.models.OfflineFolderRoot
import com.infomaniak.drive.utils.RealmModules
import io.realm.DynamicRealm
import io.realm.FieldAttribute
import io.realm.Realm
import io.realm.RealmConfiguration
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.Date
import java.util.UUID

/**
 * THE REAL 17→18 MIGRATION FIXTURE (androidTest instrumentation — Realm Java is a native
 * engine and CANNOT open on the JVM; this is why the test lives here and not under src/test).
 *
 * Version 17 is the T-0079 DEVICE TEST BUILD's schema (the offline-folder development branch,
 * whose sources are the read-only kdrive-android-offline-folders checkout). Its true shape is
 * reconstructed EXACTLY from those sources, not from memory: the File table carries
 * isInOfflineFolder (Boolean REQUIRED), offlineIntentStamp (Long REQUIRED),
 * publishedCleanBaseline (Long nullable) and serverContentHash (String nullable — added by
 * that branch's own 16→17 migration), and NOT this branch's four other new fields; the root
 * registry is the dev `OfflineRoot` table (uid "{folderId}_{driveId}", name/path, createdAt,
 * lastSyncedAt nullable, isPendingRemoval). Following the T-0079 pattern:
 *  (A) create a disposable TYPED realm at the FULL current module schema STAMPED 17,
 *  (B) open the SAME file as a [DynamicRealm] at 17 and shape-correct it to the TRUE v17
 *      schema (remove this branch's four columns and table, add the dev columns, recreate the
 *      dev OfflineRoot table and seed it), every step verified before closing,
 *  (C) reopen at [FileMigration.DB_VERSION] (18) with the REAL [FileMigration] and the FULL
 *      production module — exactly what a production upgrade executes — and audit ONLY the
 *      17→18 difference: OfflineRoot rows are TRANSFORMED into OfflineFolderRoot rows (new
 *      three-part uid), the dev table is gone, the undeclared dev File fields are removed
 *      explicitly, serverContentHash is KEPT with its value, pre-existing pins are preserved
 *      as direct pins, rows are never deleted.
 *
 * THE FIXTURE IS FULLY DISPOSABLE AND ACCOUNT-FREE: a RANDOM file in the instrumentation
 * target's cacheDir; every row is a synthetic constant — NO owner data, NO account, NO network.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FileMigration17to18Test {

    private val fixtureName = "migration17to18-${UUID.randomUUID()}"
    private val fixtureDir = ApplicationProvider.getApplicationContext<android.content.Context>().cacheDir

    private val configAt17 = RealmConfiguration.Builder()
        .schemaVersion(17L)
        .modules(RealmModules.LocalFilesModule())
        .name(fixtureName)
        .directory(fixtureDir)
        .build()

    private val configAt18 = RealmConfiguration.Builder()
        .schemaVersion(FileMigration.DB_VERSION)
        .migration(FileMigration())
        .modules(RealmModules.LocalFilesModule())
        .name(fixtureName)
        .directory(fixtureDir)
        .build()

    /** This branch's columns that v17 did NOT have. */
    private val branchOnlyFileFields = listOf(
        File::isDirectlyPinned.name,
        File::offlineBasisModifiedAt.name,
        File::offlineBasisSize.name,
        File::offlineContentDigest.name,
    )

    /** The dev columns that v17 HAD and this branch's schema does not declare. */
    private val devFileFields = listOf("isInOfflineFolder", "offlineIntentStamp", "publishedCleanBaseline")

    private fun seedPinned(file: File) = file.apply {
        id = 900201
        driveId = 721
        name = "fixture-pinned-17.bin"
        path = "/fixture/pinned-17.bin"
        lastModifiedAt = 1_700_000_000L
        isOffline = true
        isMarkedAsOffline = false
        isFromUploads = false
        serverContentHash = "hash-kept-17"
    }

    private fun seedFolderCovered(file: File) = file.apply {
        id = 900202
        driveId = 721
        name = "fixture-covered-17.bin"
        path = "/fixture/covered-17.bin"
        lastModifiedAt = 1_700_001_000L
        isOffline = false
        isMarkedAsOffline = false
        isFromUploads = false
        serverContentHash = null
    }

    @Test
    @DisplayName("migration 17 to 18 transforms OfflineRoot, drops undeclared dev fields and preserves rows")
    fun migration17to18TransformsOfflineRootAndPreservesRows() {
        // ---- PHASE A: a disposable typed realm at the full current module shape, stamped 17.
        Realm.getInstance(configAt17).use { realm ->
            realm.executeTransaction { transactionRealm ->
                seedPinned(transactionRealm.createObject(File::class.java, "900201_721"))
                seedFolderCovered(transactionRealm.createObject(File::class.java, "900202_721"))
            }
        }

        // ---- PHASE B: the same file as a DynamicRealm at 17 — correct the shape to the TRUE
        // v17 schema (sources: the offline-folder development branch), then verify.
        DynamicRealm.getInstance(configAt17).use { dynamic ->
            dynamic.executeTransaction { transactionRealm ->
                val fileSchema = transactionRealm.schema.get("File")
                check(fileSchema != null)
                branchOnlyFileFields.forEach { field ->
                    check(fileSchema.hasField(field)) { "the phase-A module schema must carry $field" }
                    fileSchema.removeField(field)
                }
                check(fileSchema.hasField("serverContentHash")) { "v17 already carries serverContentHash (16→17 added it)" }

                fileSchema.addField("isInOfflineFolder", Boolean::class.java, FieldAttribute.REQUIRED)
                fileSchema.addField("offlineIntentStamp", Long::class.java, FieldAttribute.REQUIRED)
                fileSchema.addField("publishedCleanBaseline", Long::class.java)

                val covered = transactionRealm.where("File").equalTo("uid", "900202_721").findFirst()!!
                covered.setBoolean("isInOfflineFolder", true)
                covered.setLong("offlineIntentStamp", 7L)
                covered.setNull("publishedCleanBaseline")
                val pinned = transactionRealm.where("File").equalTo("uid", "900201_721").findFirst()!!
                pinned.setBoolean("isInOfflineFolder", false)
                pinned.setLong("offlineIntentStamp", 0L)
                pinned.setNull("publishedCleanBaseline")

                check(transactionRealm.schema.get("OfflineFolderRoot") != null) {
                    "the phase-A module schema must carry OfflineFolderRoot"
                }
                transactionRealm.schema.remove("OfflineFolderRoot")

                // The dev OfflineRoot table, EXACTLY as the development branch declared it.
                check(transactionRealm.schema.get("OfflineRoot") == null)
                transactionRealm.schema.create("OfflineRoot")!!.apply {
                    addField("uid", String::class.java, FieldAttribute.PRIMARY_KEY, FieldAttribute.REQUIRED)
                    addField("folderId", Int::class.java, FieldAttribute.REQUIRED)
                    addField("driveId", Int::class.java, FieldAttribute.REQUIRED)
                    addField("userId", Int::class.java, FieldAttribute.REQUIRED)
                    addField("name", String::class.java, FieldAttribute.REQUIRED)
                    addField("path", String::class.java, FieldAttribute.REQUIRED)
                    addField("createdAt", Date::class.java, FieldAttribute.REQUIRED)
                    addField("lastSyncedAt", Date::class.java)
                    addField("isPendingRemoval", Boolean::class.java, FieldAttribute.REQUIRED)
                }
                transactionRealm.createObject("OfflineRoot", "800201_721").apply {
                    setInt("folderId", 800201)
                    setInt("driveId", 721)
                    setInt("userId", 921)
                    setString("name", "fixture-root-17")
                    setString("path", "/fixture/root-17")
                    setDate("createdAt", Date(1_700_000_000_000L))
                    setDate("lastSyncedAt", Date(1_700_000_500_000L))
                    setBoolean("isPendingRemoval", false)
                }
            }

            val fileSchema = dynamic.schema.get("File")
            check(fileSchema != null)
            branchOnlyFileFields.forEach { field -> assertFalse(fileSchema.hasField(field), "the true v17 shape has no $field") }
            devFileFields.forEach { field -> assertTrue(fileSchema.hasField(field), "the true v17 shape carries $field") }
            assertFalse(dynamic.schema.contains("OfflineFolderRoot"), "the true v17 shape has no OfflineFolderRoot table")
            assertTrue(dynamic.schema.contains("OfflineRoot"), "the true v17 shape carries the dev OfflineRoot table")
            assertEquals(1L, dynamic.where("OfflineRoot").count())
            assertEquals(2L, dynamic.where("File").count())
        }

        // ---- PHASE C: reopen at 18 with the REAL migration + the full module; audit ONLY
        // the 17→18 difference.
        Realm.getInstance(configAt18).use { realm ->
            val fileSchema = realm.schema.get("File")
            check(fileSchema != null)
            assertFalse(realm.schema.contains("OfflineRoot"), "the dev table is removed by the migration")
            assertTrue(realm.schema.contains("OfflineFolderRoot"), "the registry table exists after the transform")
            devFileFields.forEach { field ->
                assertFalse(fileSchema.hasField(field), "the undeclared dev field $field is removed explicitly")
            }
            branchOnlyFileFields.forEach { field ->
                assertTrue(fileSchema.hasField(field), "the migration adds $field")
            }

            val root = realm.where(OfflineFolderRoot::class.java).findFirst()
            check(root != null)
            assertEquals(
                OfflineFolderRoot.buildUid(800201, 721, 921),
                root.uid,
                "the dev registration is transformed to the three-part uid",
            )
            assertEquals(800201, root.folderId)
            assertEquals(721, root.driveId)
            assertEquals(921, root.userId)
            assertEquals(Date(1_700_000_000_000L), root.markedAt, "createdAt carries over as markedAt")

            val pinned = realm.where(File::class.java).equalTo("uid", "900201_721").findFirst()!!
            assertTrue(pinned.isDirectlyPinned, "the pre-existing pin is preserved as a direct pin")
            assertEquals("hash-kept-17", pinned.serverContentHash, "serverContentHash is KEPT, not reset")
            assertEquals(900201, pinned.id)
            assertEquals("fixture-pinned-17.bin", pinned.name)
            assertEquals(1_700_000_000L, pinned.lastModifiedAt)
            assertTrue(pinned.isOffline)

            val covered = realm.where(File::class.java).equalTo("uid", "900202_721").findFirst()!!
            assertFalse(covered.isDirectlyPinned, "a folder-covered row is not a direct pin")
            assertNull(covered.serverContentHash)
            assertEquals(900202, covered.id)
            assertEquals("fixture-covered-17.bin", covered.name)

            // THE TRANSFORMED ROW IS WRITABLE.
            realm.executeTransaction { root.markedAt = Date(1_700_000_600_000L) }
            assertEquals(Date(1_700_000_600_000L), root.markedAt)
        }
    }

    @AfterAll
    fun cleanup() {
        // ONLY the random own-cache fixture is deleted — never any other realm, never any
        // owner data.
        runCatching { Realm.deleteRealm(configAt18) }
        runCatching { Realm.deleteRealm(configAt17) }
    }
}
