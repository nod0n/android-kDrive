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
import com.infomaniak.drive.utils.RealmModules
import io.realm.DynamicRealm
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
import java.util.UUID

/**
 * THE REAL 11→18 MIGRATION FIXTURE (androidTest instrumentation — Realm Java is a native
 * engine and CANNOT open on the JVM; this is why the test lives here and not under src/test).
 *
 * Version 11 is the PUBLIC BASELINE schema (origin/main), so the true v11 shape is exactly the
 * current [RealmModules.LocalFilesModule] classes MINUS this branch's additions: the six new
 * File fields (isDirectlyPinned, offlineBasisModifiedAt, offlineBasisSize, offlineContentDigest,
 * serverContentHash, offlinePublishedPath) and the whole OfflineFolderRoot table. Following the T-0079 pattern:
 *  (A) create a disposable TYPED realm at the FULL current module schema STAMPED 11,
 *  (B) open the SAME file as a [DynamicRealm] at 11 and remove ONLY this branch's additions —
 *      the file then holds the TRUE v11 shape, verified by a schema audit before closing,
 *  (C) reopen at [FileMigration.DB_VERSION] (18) with the REAL [FileMigration] and the FULL
 *      production module — exactly what a production upgrade executes — and audit ONLY the
 *      11→18 difference: the six columns exist with the right nullability, the registered
 *      OfflineFolderRoot table exists empty, the pre-existing pin reads back as a direct pin,
 *      nothing else was invented, every pre-existing field is preserved, and the new nullable
 *      columns are writable.
 *
 * THE FIXTURE IS FULLY DISPOSABLE AND ACCOUNT-FREE: a RANDOM file in the instrumentation
 * target's cacheDir (its own name, deleted in [cleanup]); every row is a synthetic constant —
 * NO owner data, NO account, NO network.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FileMigration11to18Test {

    private val fixtureName = "migration11to18-${UUID.randomUUID()}"
    private val fixtureDir = ApplicationProvider.getApplicationContext<android.content.Context>().cacheDir

    private val configAt11 = RealmConfiguration.Builder()
        .schemaVersion(11L)
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

    /**
     * The baseline v11 File field set: the PERSISTED schema of the current
     * [RealmModules.LocalFilesModule] classes minus this branch's six additions.
     * @Ignore properties (driveColor, currentProgress, publicShareUuid, publicShareAuthToken)
     * and non-property names (result — a hashCode() local) are schema-INVISIBLE at every
     * version and belong to NO persisted field set. CHALLENGE-62a3 finding 4 / device-62a3:
     * the first version of this list was derived from Kotlin-source names and wrongly
     * included those five — the device run proved the persisted schema omits exactly them,
     * so they are asserted absent explicitly ([neverPersistedNames]) instead of expected.
     */
    private val baselineFileFields = setOf(
        "addedAt", "categories", "children", "color", "conversion", "createdAt", "createdBy",
        "cursor", "deletedAt", "deletedBy", "driveId", "dropbox", "extensionType",
        "externalImport", "id", "isComplete", "isFavorite", "isFromUploads",
        "isMarkedAsOffline", "isOffline", "lastActionAt", "lastModifiedAt", "name", "parentId",
        "path", "responseAt", "revisedAt", "rights", "shareLink", "size", "sortedName",
        "status", "supportedBy", "type", "uid", "updatedAt", "users", "version", "versionCode",
        "visibility",
    )

    /** Names that must NEVER appear in a persisted Realm schema at any version: @Ignore
     *  properties of the current model and a Kotlin local that shares a source file. */
    private val neverPersistedNames = listOf(
        "currentProgress", "driveColor", "publicShareAuthToken", "publicShareUuid", "result",
    )

    private val newFileFields = listOf(
        File::isDirectlyPinned.name,
        File::offlineBasisModifiedAt.name,
        File::offlineBasisSize.name,
        File::offlineContentDigest.name,
        File::serverContentHash.name,
        File::offlinePublishedPath.name,
    )

    /** A baseline pin: the persistent state in the public baseline is isOffline (not the
     * transient isMarkedAsOffline pending marker the bulk worker clears on finish). */
    private fun seedPinned(file: File) = file.apply {
        id = 900101
        driveId = 711
        name = "fixture-pinned.bin"
        path = "/fixture/pinned.bin"
        lastModifiedAt = 1_700_000_000L
        isOffline = true
        isMarkedAsOffline = false
        isFromUploads = false
    }

    private fun seedOrdinary(file: File) = file.apply {
        id = 900102
        driveId = 711
        name = "fixture-ordinary.bin"
        path = "/fixture/ordinary.bin"
        lastModifiedAt = 1_700_001_000L
        isOffline = false
        isMarkedAsOffline = false
        isFromUploads = false
    }

    @Test
    @DisplayName("migration 11 to 18 adds the offline columns and table and preserves every row")
    fun migration11to18AddsOfflineColumnsAndTableAndPreservesEveryRow() {
        // ---- PHASE A: a disposable typed realm at the full current module shape, stamped 11.
        Realm.getInstance(configAt11).use { realm ->
            realm.executeTransaction { transactionRealm ->
                seedPinned(transactionRealm.createObject(File::class.java, "900101_711"))
                seedOrdinary(transactionRealm.createObject(File::class.java, "900102_711"))
            }
        }

        // ---- PHASE B: the same file as a DynamicRealm at 11 — remove ONLY this branch's
        // additions, then verify the TRUE v11 shape before closing.
        DynamicRealm.getInstance(configAt11).use { dynamic ->
            dynamic.executeTransaction { transactionRealm ->
                val fileSchema = transactionRealm.schema.get("File")
                check(fileSchema != null)
                newFileFields.forEach { field ->
                    check(fileSchema.hasField(field)) { "the phase-A module schema must carry $field" }
                    fileSchema.removeField(field)
                }
                check(transactionRealm.schema.get("OfflineFolderRoot") != null) {
                    "the phase-A module schema must carry OfflineFolderRoot"
                }
                transactionRealm.schema.remove("OfflineFolderRoot")
            }
            val fileSchema = dynamic.schema.get("File")
            check(fileSchema != null)
            newFileFields.forEach { field -> assertFalse(fileSchema.hasField(field), "the true v11 shape has no $field") }
            neverPersistedNames.forEach { name ->
                assertFalse(fileSchema.hasField(name), "$name is @Ignore/non-persisted and is schema-invisible at v11 too")
            }
            assertEquals(
                baselineFileFields,
                fileSchema.fieldNames.toSet(),
                "the corrected schema must hold the WHOLE true v11 File field set",
            )
            assertFalse(dynamic.schema.contains("OfflineFolderRoot"), "the true v11 shape has no OfflineFolderRoot table")
            assertTrue(fileSchema.hasField("isOffline"))
            assertTrue(fileSchema.hasField("isMarkedAsOffline"))
            assertTrue(fileSchema.hasField("uid"))
            assertEquals(2L, dynamic.where("File").count(), "both synthetic rows must exist at the true v11 shape")
        }

        // ---- PHASE C: reopen at 18 with the REAL migration + the full module; audit ONLY
        // the 11→18 difference.
        Realm.getInstance(configAt18).use { realm ->
            val fileSchema = realm.schema.get("File")
            check(fileSchema != null)
            assertTrue(fileSchema.hasField(File::isDirectlyPinned.name), "the migration adds the direct-pin provenance")
            assertTrue(fileSchema.hasField(File::offlineBasisModifiedAt.name), "the migration adds the basis timestamp")
            assertTrue(fileSchema.hasField(File::offlineBasisSize.name), "the migration adds the basis size")
            assertTrue(fileSchema.hasField(File::offlineContentDigest.name), "the migration adds the basis digest")
            assertTrue(fileSchema.hasField(File::serverContentHash.name), "the migration adds the server hash identity")
            assertTrue(fileSchema.hasField(File::offlinePublishedPath.name), "the migration adds the published path")
            assertTrue(fileSchema.isNullable(File::offlineBasisSize.name), "the basis size is nullable")
            assertTrue(fileSchema.isNullable(File::offlineContentDigest.name), "the digest is nullable")
            assertTrue(fileSchema.isNullable(File::serverContentHash.name), "the server hash is nullable")
            assertTrue(fileSchema.isNullable(File::offlinePublishedPath.name), "the published path is nullable")
            assertFalse(fileSchema.isNullable(File::isDirectlyPinned.name), "the direct-pin provenance is required")
            assertFalse(fileSchema.isNullable(File::offlineBasisModifiedAt.name), "the basis timestamp is required")
            assertEquals(
                baselineFileFields + newFileFields.toSet(),
                fileSchema.fieldNames.toSet(),
                "the migrated schema must hold the WHOLE v18 File field set and nothing else",
            )
            neverPersistedNames.forEach { name ->
                assertFalse(fileSchema.hasField(name), "$name stays schema-invisible after the migration too")
            }
            assertTrue(realm.schema.contains("OfflineFolderRoot"), "the migration creates the root registry")
            assertEquals(0L, realm.where(com.infomaniak.drive.data.models.OfflineFolderRoot::class.java).count())

            val pinned = realm.where(File::class.java).equalTo("uid", "900101_711").findFirst()!!
            assertTrue(pinned.isDirectlyPinned, "the baseline persistent pin is preserved as a direct pin")
            assertEquals(0L, pinned.offlineBasisModifiedAt, "no basis is invented for migrated rows")
            assertNull(pinned.offlineBasisSize)
            assertNull(pinned.offlineContentDigest)
            assertNull(pinned.serverContentHash, "nothing is invented during the migration")
            assertEquals(900101, pinned.id)
            assertEquals(711, pinned.driveId)
            assertEquals("fixture-pinned.bin", pinned.name)
            assertEquals("/fixture/pinned.bin", pinned.path)
            assertEquals(1_700_000_000L, pinned.lastModifiedAt)
            assertTrue(pinned.isOffline)
            assertFalse(pinned.isMarkedAsOffline)

            val ordinary = realm.where(File::class.java).equalTo("uid", "900102_711").findFirst()!!
            assertFalse(ordinary.isDirectlyPinned, "an unpinned row stays unpinned")
            assertEquals(900102, ordinary.id)
            assertEquals("fixture-ordinary.bin", ordinary.name)
            assertEquals(1_700_001_000L, ordinary.lastModifiedAt)
            assertFalse(ordinary.isOffline)

            // THE NEW COLUMNS ARE WRITABLE (the publication seam's contract).
            realm.executeTransaction {
                ordinary.offlineBasisModifiedAt = 1_700_001_000L
                ordinary.offlineBasisSize = 123L
                ordinary.offlineContentDigest = "digest-abc"
                ordinary.serverContentHash = "hash-abc"
            }
            assertEquals(123L, ordinary.offlineBasisSize)
            assertEquals("digest-abc", ordinary.offlineContentDigest)
            assertEquals("hash-abc", ordinary.serverContentHash)
        }
    }

    @AfterAll
    fun cleanup() {
        // ONLY the random own-cache fixture is deleted — never any other realm, never any
        // owner data.
        runCatching { Realm.deleteRealm(configAt18) }
        runCatching { Realm.deleteRealm(configAt11) }
    }
}
