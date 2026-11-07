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
import io.realm.Realm
import io.realm.RealmConfiguration
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * THE REAL 18→19 MIGRATION FIXTURE (androidTest instrumentation — Realm Java is a native
 * engine and CANNOT open on the JVM; this is why the test lives here and not under src/test).
 *
 * Version 18 is this branch's schema BEFORE the root-ownership provenance field. Following
 * the T-0079/T-0086 pattern:
 *  (A) create a disposable TYPED realm at the FULL current module schema STAMPED 18,
 *  (B) open the SAME file as a [DynamicRealm] at 18 and shape-correct it to the TRUE v18
 *      schema (remove the new column), every step verified before closing,
 *  (C) reopen at [FileMigration.DB_VERSION] (19) with the REAL [FileMigration] and the FULL
 *      production module — exactly what a production upgrade executes — and audit the
 *      18→19 difference: the provenance column is added; rows carrying an offline signal
 *      whose cached parent chain reaches a live root derive offlineOwnedByRoot = true;
 *      never-root rows (a direct pin outside every root, a plain row) derive false; NO row
 *      is deleted and every pre-existing value is kept.
 *
 * THE FIXTURE IS FULLY DISPOSABLE AND ACCOUNT-FREE: a RANDOM file in the instrumentation
 * target's cacheDir; every row is a synthetic constant — NO owner data, NO account, NO network.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class FileMigration18to19Test {

    private val fixtureName = "migration18to19-${UUID.randomUUID()}"
    private val fixtureDir = ApplicationProvider.getApplicationContext<android.content.Context>().cacheDir

    private val configAt18 = RealmConfiguration.Builder()
        .schemaVersion(18L)
        .modules(RealmModules.LocalFilesModule())
        .name(fixtureName)
        .directory(fixtureDir)
        .build()

    private val configAt19 = RealmConfiguration.Builder()
        .schemaVersion(FileMigration.DB_VERSION)
        .migration(FileMigration())
        .modules(RealmModules.LocalFilesModule())
        .name(fixtureName)
        .directory(fixtureDir)
        .build()

    // Covered chain: root(800301) > inner(800302) > {pending 900301, published 900302}.
    // Outside: a direct pin (900303) and a plain row (900304) under a parent no root covers.
    private val rootFolderId = 800301
    private val innerFolderId = 800302
    private val outsideFolderId = 800303
    private val driveId = 731
    private val userId = 931

    private fun seedPendingCovered(file: File) = file.apply {
        id = 900301
        driveId = driveId
        name = "fixture-pending-18.bin"
        path = "/fixture/pending-18.bin"
        lastModifiedAt = 1_700_000_000L
        parentId = innerFolderId
        type = "file"
        isOffline = false
        isMarkedAsOffline = true
        isFromUploads = false
        serverContentHash = null
    }

    private fun seedPublishedCovered(file: File) = file.apply {
        id = 900302
        driveId = driveId
        name = "fixture-published-18.bin"
        path = "/fixture/published-18.bin"
        lastModifiedAt = 1_700_001_000L
        parentId = innerFolderId
        type = "file"
        isOffline = true
        isMarkedAsOffline = false
        isFromUploads = false
        offlineBasisModifiedAt = 1_700_001_000L
        offlineBasisSize = 128L
        offlineContentDigest = "digest-18"
        serverContentHash = "hash-18"
    }

    private fun seedDirectPin(file: File) = file.apply {
        id = 900303
        driveId = driveId
        name = "fixture-pin-18.bin"
        path = "/fixture/outside/pin-18.bin"
        lastModifiedAt = 1_700_002_000L
        parentId = outsideFolderId
        type = "file"
        isOffline = true
        isDirectlyPinned = true
        isMarkedAsOffline = false
        isFromUploads = false
        serverContentHash = "hash-pin-18"
    }

    private fun seedPlainRow(file: File) = file.apply {
        id = 900304
        driveId = driveId
        name = "fixture-plain-18.bin"
        path = "/fixture/plain-18.bin"
        lastModifiedAt = 1_700_003_000L
        parentId = outsideFolderId
        type = "file"
        isFromUploads = false
    }

    @Test
    @DisplayName("migration 18 to 19 adds the root-ownership provenance with a data-preserving derivation")
    fun migration18to19DerivesRootOwnershipAndPreservesRows() {
        // ---- PHASE A: a disposable typed realm at the full current module shape, stamped 18.
        Realm.getInstance(configAt18).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${rootFolderId}_$driveId").apply {
                    id = rootFolderId
                    driveId = driveId
                    name = "fixture-root-18"
                    path = "/fixture/root-18"
                    type = "dir"
                }
                transactionRealm.createObject(File::class.java, "${innerFolderId}_$driveId").apply {
                    id = innerFolderId
                    driveId = driveId
                    name = "fixture-inner-18"
                    path = "/fixture/root-18/inner"
                    type = "dir"
                    parentId = rootFolderId
                }
                transactionRealm.createObject(File::class.java, "${outsideFolderId}_$driveId").apply {
                    id = outsideFolderId
                    driveId = driveId
                    name = "fixture-outside-18"
                    path = "/fixture/outside"
                    type = "dir"
                }
                seedPendingCovered(transactionRealm.createObject(File::class.java, "900301_$driveId"))
                seedPublishedCovered(transactionRealm.createObject(File::class.java, "900302_$driveId"))
                seedDirectPin(transactionRealm.createObject(File::class.java, "900303_$driveId"))
                seedPlainRow(transactionRealm.createObject(File::class.java, "900304_$driveId"))
                transactionRealm.createObject(OfflineFolderRoot::class.java, OfflineFolderRoot.buildUid(rootFolderId, driveId, userId)).apply {
                    folderId = rootFolderId
                    driveId = driveId
                    userId = userId
                }
            }
        }

        // ---- PHASE B: the same file as a DynamicRealm at 18 — correct the shape to the TRUE
        // v18 schema (the provenance column did not exist yet), then verify.
        DynamicRealm.getInstance(configAt18).use { dynamic ->
            dynamic.executeTransaction { transactionRealm ->
                val fileSchema = transactionRealm.schema.get("File")
                check(fileSchema != null)
                check(fileSchema.hasField(File::offlineOwnedByRoot.name)) {
                    "the phase-A module schema must carry the new column (the class already declares it)"
                }
                fileSchema.removeField(File::offlineOwnedByRoot.name)
            }
            val fileSchema = dynamic.schema.get("File")
            check(fileSchema != null)
            assertFalse(fileSchema.hasField(File::offlineOwnedByRoot.name), "the true v18 shape has no provenance column")
            assertEquals(7L, dynamic.where("File").count(), "three folders + four files, exactly as seeded")
            assertEquals(1L, dynamic.where("OfflineFolderRoot").count())
        }

        // ---- PHASE C: reopen at 19 with the REAL migration + the full module; audit ONLY
        // the 18→19 difference.
        Realm.getInstance(configAt19).use { realm ->
            val fileSchema = realm.schema.get("File")
            check(fileSchema != null)
            assertTrue(fileSchema.hasField(File::offlineOwnedByRoot.name), "the migration adds the provenance column")

            val pendingCovered = realm.where(File::class.java).equalTo("uid", "900301_$driveId").findFirst()!!
            assertTrue(pendingCovered.offlineOwnedByRoot, "a PENDING row inside a live root's subtree derives root ownership")
            assertTrue(pendingCovered.isMarkedAsOffline, "the pending marker is kept")

            val publishedCovered = realm.where(File::class.java).equalTo("uid", "900302_$driveId").findFirst()!!
            assertTrue(publishedCovered.offlineOwnedByRoot, "a PUBLISHED row inside a live root's subtree derives root ownership")
            assertEquals(1_700_001_000L, publishedCovered.offlineBasisModifiedAt, "the basis is kept")
            assertEquals("digest-18", publishedCovered.offlineContentDigest, "the digest is kept")
            assertEquals("hash-18", publishedCovered.serverContentHash, "the identity is kept")

            val directPin = realm.where(File::class.java).equalTo("uid", "900303_$driveId").findFirst()!!
            assertFalse(directPin.offlineOwnedByRoot, "a direct pin OUTSIDE every root derives no root ownership")
            assertTrue(directPin.isDirectlyPinned, "the pin is kept")
            assertEquals("hash-pin-18", directPin.serverContentHash, "the pin's identity is kept")

            val plainRow = realm.where(File::class.java).equalTo("uid", "900304_$driveId").findFirst()!!
            assertFalse(plainRow.offlineOwnedByRoot, "a row with no offline signal derives no root ownership")

            assertEquals(7L, realm.where(File::class.java).count(), "rows are never deleted")
            assertEquals(1L, realm.where(OfflineFolderRoot::class.java).count(), "the root registry is untouched")

            // THE MIGRATED ROWS ARE WRITABLE.
            realm.executeTransaction { publishedCovered.offlineOwnedByRoot = false }
            assertFalse(publishedCovered.offlineOwnedByRoot)
        }
    }

    @AfterAll
    fun cleanup() {
        // ONLY the random own-cache fixture is deleted — never any other realm, never any
        // owner data.
        runCatching { Realm.deleteRealm(configAt19) }
        runCatching { Realm.deleteRealm(configAt18) }
    }
}
