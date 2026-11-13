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
 *      schema (remove the new column), every step verified before closing — including the
 *      ACTUAL fixture scope fields of every seeded row (uid, driveId, parent), so a broken
 *      seed cannot pass a derivation audit it was never aimed at (RESUME-caf1 item 1: the
 *      first draft self-assigned `driveId = driveId` inside receiver apply blocks, which
 *      javap proved writes the receiver's own default back — every fixture value is now a
 *      distinct parameter name, and the scope is asserted before and after the migration),
 *  (C) reopen at [FileMigration.DB_VERSION] (20) with the REAL [FileMigration] and the FULL
 *      production module — exactly what a production upgrade executes — and audit the
 *      18→19 difference: the provenance column is added; rows carrying an offline signal
 *      whose cached parent chain reaches a live root folder of the SAME DRIVE derive
 *      offlineOwnedByRoot = true; every negative class derives false — a direct pin outside
 *      every root, a plain row, a row whose parent row is ABSENT, rows caught in a parent
 *      CYCLE (the walk must terminate), and a same-ids TWIN on a second drive with no
 *      registered root (identical folder/file ids must derive nothing there: the derivation
 *      is drive-scoped); NO row is deleted and every pre-existing value is kept.
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

    // Covered chain on the registered drive: root(800301) > inner(800302) > {pending 900301,
    // published 900302}. Negatives on the same drive: a direct pin (900303) and a plain row
    // (900304) under the outside folder (800303), an absent-parent row (900305), and a parent
    // CYCLE (900306 <-> 900307). A SECOND drive (732) mirrors the SAME folder and file ids —
    // with NO registered root — so the derivation's drive scoping is exercised by identical ids.
    private val driveRegistered = 731
    private val driveUnregistered = 732
    private val fixtureUserId = 931
    private val rootFolderId = 800301
    private val innerFolderId = 800302
    private val outsideFolderId = 800303
    private val absentParentId = 999_001

    // The seed helpers take the row scope as EXPLICITLY NAMED parameters: inside the receiver
    // apply blocks an unqualified `driveId = driveId` resolves to the receiver's own property
    // (a self-assignment javap confirmed), so every fixture value travels under a distinct name.
    private fun seedRootFolder(file: File, rowDriveId: Int) = file.apply {
        id = rootFolderId
        driveId = rowDriveId
        name = "fixture-root-18"
        path = "/fixture/root-18"
        type = "dir"
    }

    private fun seedInnerFolder(file: File, rowDriveId: Int) = file.apply {
        id = innerFolderId
        driveId = rowDriveId
        name = "fixture-inner-18"
        path = "/fixture/root-18/inner"
        type = "dir"
        parentId = rootFolderId
    }

    private fun seedOutsideFolder(file: File, rowDriveId: Int) = file.apply {
        id = outsideFolderId
        driveId = rowDriveId
        name = "fixture-outside-18"
        path = "/fixture/outside"
        type = "dir"
    }

    private fun seedPendingCovered(file: File, rowDriveId: Int) = file.apply {
        id = 900301
        driveId = rowDriveId
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

    private fun seedPublishedCovered(file: File, rowDriveId: Int) = file.apply {
        id = 900302
        driveId = rowDriveId
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

    private fun seedDirectPin(file: File, rowDriveId: Int) = file.apply {
        id = 900303
        driveId = rowDriveId
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

    private fun seedPlainRow(file: File, rowDriveId: Int) = file.apply {
        id = 900304
        driveId = rowDriveId
        name = "fixture-plain-18.bin"
        path = "/fixture/plain-18.bin"
        lastModifiedAt = 1_700_003_000L
        parentId = outsideFolderId
        type = "file"
        isFromUploads = false
    }

    private fun seedAbsentParentRow(file: File, rowDriveId: Int) = file.apply {
        id = 900305
        driveId = rowDriveId
        name = "fixture-absent-18.bin"
        path = "/fixture/absent-18.bin"
        lastModifiedAt = 1_700_004_000L
        parentId = absentParentId
        type = "file"
        isOffline = true
        isMarkedAsOffline = false
        isFromUploads = false
    }

    private fun seedCycleRow(file: File, rowDriveId: Int, ownId: Int, parentId: Int) = file.apply {
        id = ownId
        driveId = rowDriveId
        name = "fixture-cycle-$ownId-18.bin"
        path = "/fixture/cycle-$ownId-18.bin"
        lastModifiedAt = 1_700_005_000L
        this.parentId = parentId
        type = "file"
        isOffline = true
        isMarkedAsOffline = false
        isFromUploads = false
    }

    @Test
    @DisplayName("migration 18 to 19 adds the root-ownership provenance with a data-preserving, drive-scoped derivation")
    fun migration18to19DerivesRootOwnershipAndPreservesRows() {
        // ---- PHASE A: a disposable typed realm at the full current module shape, stamped 18.
        Realm.getInstance(configAt18).use { realm ->
            realm.executeTransaction { transactionRealm ->
                seedRootFolder(transactionRealm.createObject(File::class.java, "${rootFolderId}_$driveRegistered"), driveRegistered)
                seedInnerFolder(transactionRealm.createObject(File::class.java, "${innerFolderId}_$driveRegistered"), driveRegistered)
                seedOutsideFolder(transactionRealm.createObject(File::class.java, "${outsideFolderId}_$driveRegistered"), driveRegistered)
                seedPendingCovered(transactionRealm.createObject(File::class.java, "900301_$driveRegistered"), driveRegistered)
                seedPublishedCovered(transactionRealm.createObject(File::class.java, "900302_$driveRegistered"), driveRegistered)
                seedDirectPin(transactionRealm.createObject(File::class.java, "900303_$driveRegistered"), driveRegistered)
                seedPlainRow(transactionRealm.createObject(File::class.java, "900304_$driveRegistered"), driveRegistered)
                seedAbsentParentRow(transactionRealm.createObject(File::class.java, "900305_$driveRegistered"), driveRegistered)
                seedCycleRow(transactionRealm.createObject(File::class.java, "900306_$driveRegistered"), driveRegistered, ownId = 900306, parentId = 900307)
                seedCycleRow(transactionRealm.createObject(File::class.java, "900307_$driveRegistered"), driveRegistered, ownId = 900307, parentId = 900306)
                // The same-ids twin on the UNREGISTERED drive: identical folder and file ids,
                // identical parent chain — only the drive differs, and no root is registered for it.
                seedRootFolder(transactionRealm.createObject(File::class.java, "${rootFolderId}_$driveUnregistered"), driveUnregistered)
                seedInnerFolder(transactionRealm.createObject(File::class.java, "${innerFolderId}_$driveUnregistered"), driveUnregistered)
                seedPendingCovered(transactionRealm.createObject(File::class.java, "900301_$driveUnregistered"), driveUnregistered)
                // ONE root registry row, for the registered drive only.
                transactionRealm.createObject(
                    OfflineFolderRoot::class.java,
                    OfflineFolderRoot.buildUid(rootFolderId, driveRegistered, fixtureUserId),
                ).apply {
                    this.folderId = rootFolderId
                    this.driveId = driveRegistered
                    this.userId = fixtureUserId
                }
            }
        }

        // ---- PHASE B: the same file as a DynamicRealm at 18 — correct the shape to the TRUE
        // v18 schema (the provenance column did not exist yet), verify, and assert the ACTUAL
        // fixture scope of the rows the derivation will judge.
        DynamicRealm.getInstance(configAt18).use { dynamic ->
            dynamic.executeTransaction { transactionRealm ->
                val fileSchema = transactionRealm.schema.get("File")
                check(fileSchema != null)
                check(fileSchema.hasField(File::offlineOwnedByRoot.name)) {
                    "the phase-A module schema must carry the new column (the class already declares it)"
                }
                fileSchema.removeField(File::offlineOwnedByRoot.name)
                fileSchema.removeField(File::offlineNeedsAttention.name)
            }
            val fileSchema = dynamic.schema.get("File")
            check(fileSchema != null)
            assertFalse(fileSchema.hasField(File::offlineOwnedByRoot.name), "the true v18 shape has no provenance column")
            assertFalse(fileSchema.hasField(File::offlineNeedsAttention.name), "the true v18 shape has no future status column")
            assertEquals(13L, dynamic.where("File").count(), "five folders + eight files across both drives, exactly as seeded")
            assertEquals(1L, dynamic.where("OfflineFolderRoot").count())

            // The fixture scope, read back from the ACTUAL store: the two pending twins carry
            // identical ids and identical parents on DIFFERENT drives — this is what the
            // derivation's drive scoping is judged against.
            val registeredPending = dynamic.where("File").equalTo("uid", "900301_$driveRegistered").findFirst()!!
            assertEquals(driveRegistered, registeredPending.getInt("driveId"))
            assertEquals(innerFolderId, registeredPending.getInt("parentId"))
            assertEquals("file", registeredPending.getString("type"))
            assertTrue(registeredPending.getBoolean("isMarkedAsOffline"))
            val twinPending = dynamic.where("File").equalTo("uid", "900301_$driveUnregistered").findFirst()!!
            assertEquals(driveUnregistered, twinPending.getInt("driveId"))
            assertEquals(innerFolderId, twinPending.getInt("parentId"))
            assertEquals(registeredPending.getInt("id"), twinPending.getInt("id"), "the twin mirrors the registered row's id")
            val registeredRoot = dynamic.where("File").equalTo("uid", "${rootFolderId}_$driveRegistered").findFirst()!!
            assertEquals(driveRegistered, registeredRoot.getInt("driveId"))
            val twinRoot = dynamic.where("File").equalTo("uid", "${rootFolderId}_$driveUnregistered").findFirst()!!
            assertEquals(driveUnregistered, twinRoot.getInt("driveId"))
        }

        // ---- PHASE C: reopen at current20 with the REAL migration + the full module;
        // audit the 18→19 provenance and the default for the subsequent status column.
        Realm.getInstance(configAt19).use { realm ->
            val fileSchema = realm.schema.get("File")
            check(fileSchema != null)
            assertTrue(fileSchema.hasField(File::offlineOwnedByRoot.name), "the migration adds the provenance column")
            assertFalse(fileSchema.isNullable(File::offlineNeedsAttention.name))
            assertEquals(0L, realm.where(File::class.java).equalTo(File::offlineNeedsAttention.name, true).count())

            val pendingCovered = realm.where(File::class.java).equalTo("uid", "900301_$driveRegistered").findFirst()!!
            assertTrue(pendingCovered.offlineOwnedByRoot, "a PENDING row inside a live root's subtree derives root ownership")
            assertTrue(pendingCovered.isMarkedAsOffline, "the pending marker is kept")
            assertEquals(driveRegistered, pendingCovered.driveId, "the registered drive scope is kept")

            val publishedCovered = realm.where(File::class.java).equalTo("uid", "900302_$driveRegistered").findFirst()!!
            assertTrue(publishedCovered.offlineOwnedByRoot, "a PUBLISHED row inside a live root's subtree derives root ownership")
            assertEquals(1_700_001_000L, publishedCovered.offlineBasisModifiedAt, "the basis is kept")
            assertEquals("digest-18", publishedCovered.offlineContentDigest, "the digest is kept")
            assertEquals("hash-18", publishedCovered.serverContentHash, "the identity is kept")
            assertEquals(driveRegistered, publishedCovered.driveId, "the registered drive scope is kept")

            val directPin = realm.where(File::class.java).equalTo("uid", "900303_$driveRegistered").findFirst()!!
            assertFalse(directPin.offlineOwnedByRoot, "a direct pin OUTSIDE every root derives no root ownership")
            assertTrue(directPin.isDirectlyPinned, "the pin is kept")
            assertEquals("hash-pin-18", directPin.serverContentHash, "the pin's identity is kept")

            val plainRow = realm.where(File::class.java).equalTo("uid", "900304_$driveRegistered").findFirst()!!
            assertFalse(plainRow.offlineOwnedByRoot, "a row with no offline signal derives no root ownership")

            val absentParentRow = realm.where(File::class.java).equalTo("uid", "900305_$driveRegistered").findFirst()!!
            assertFalse(
                absentParentRow.offlineOwnedByRoot,
                "an offline row whose parent row is ABSENT derives nothing: the walk ends at the missing parent",
            )
            assertEquals(absentParentId, absentParentRow.parentId, "the absent parent id is kept verbatim")

            val cycleA = realm.where(File::class.java).equalTo("uid", "900306_$driveRegistered").findFirst()!!
            val cycleB = realm.where(File::class.java).equalTo("uid", "900307_$driveRegistered").findFirst()!!
            assertFalse(cycleA.offlineOwnedByRoot, "a parent CYCLE derives nothing and the walk TERMINATES (row A)")
            assertFalse(cycleB.offlineOwnedByRoot, "a parent CYCLE derives nothing and the walk TERMINATES (row B)")
            assertEquals(900307, cycleA.parentId)
            assertEquals(900306, cycleB.parentId, "the cycle links are kept verbatim")

            val twinPending = realm.where(File::class.java).equalTo("uid", "900301_$driveUnregistered").findFirst()!!
            assertFalse(
                twinPending.offlineOwnedByRoot,
                "the same-ids twin on the UNREGISTERED drive derives nothing: identical folder ids without a root of their own own no derivation",
            )
            assertEquals(driveUnregistered, twinPending.driveId, "the twin's drive scope is kept")
            assertTrue(twinPending.isMarkedAsOffline, "the twin's pending marker is kept")

            assertEquals(13L, realm.where(File::class.java).count(), "rows are never deleted")
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
