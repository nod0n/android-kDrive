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
import java.util.Date
import java.util.UUID

/**
 * THE REAL 8→11 UPLOAD MIGRATION FIXTURE (androidTest instrumentation — Realm Java is a
 * native engine and CANNOT open on the JVM; this is why the test lives here and not under
 * src/test).
 *
 * Version 8 is the PUBLIC BASELINE Sync.realm schema (origin/main), so the true v8 shape is
 * exactly the current [RealmModules.SyncFilesModule] classes MINUS this branch's additions:
 * SyncSettings.photoTrashSyncEnabled and the whole PhotoPairing table. Following the T-0079
 * pattern: (A) typed realm at the full current module STAMPED 8, (B) DynamicRealm shape
 * correction to the TRUE v8 schema (remove only this branch's additions, verified by audit),
 * (C) reopen at [UploadMigration.DB_VERSION] (11) with the REAL [UploadMigration] and the
 * FULL production module — exactly what a production upgrade executes — and audit ONLY the
 * 8→11 difference: the opt-in column exists (REQUIRED, reads false: nothing is auto-enabled),
 * the pairing table exists empty, pending and completed upload rows are preserved byte-equal,
 * and the pairing table is writable.
 *
 * THE FIXTURE IS FULLY DISPOSABLE AND ACCOUNT-FREE: a RANDOM file in the instrumentation
 * target's cacheDir; every row is a synthetic constant — NO owner data, NO account, NO network.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UploadMigration8to11Test {

    private val fixtureName = "uploadMigration8to11-${UUID.randomUUID()}"
    private val fixtureDir = ApplicationProvider.getApplicationContext<android.content.Context>().cacheDir

    private val configAt8 = RealmConfiguration.Builder()
        .schemaVersion(8L)
        .modules(RealmModules.SyncFilesModule())
        .name(fixtureName)
        .directory(fixtureDir)
        .build()

    private val configAt11 = RealmConfiguration.Builder()
        .schemaVersion(UploadMigration.DB_VERSION)
        .migration(UploadMigration())
        .modules(RealmModules.SyncFilesModule())
        .name(fixtureName)
        .directory(fixtureDir)
        .build()

    private fun seedPending(uploadFile: UploadFile) = uploadFile.apply {
        uri = "content://fixture/pending-8"
        driveId = 731
        userId = 931
        fileName = "fixture-pending-8.jpg"
        fileSize = 4_242L
        fileModifiedAt = Date(1_700_000_000_000L)
        remoteFolder = 42
        type = UploadFile.Type.SYNC.name
    }

    private fun seedCompleted(uploadFile: UploadFile) = uploadFile.apply {
        uri = "content://fixture/completed-8"
        driveId = 731
        userId = 931
        fileName = "fixture-completed-8.jpg"
        fileSize = 1_111L
        fileModifiedAt = Date(1_700_001_000_000L)
        remoteFolder = 42
        type = UploadFile.Type.SYNC.name
        uploadAt = Date(1_700_001_500_000L)
    }

    @Test
    @DisplayName("migration 8 to 11 adds the opt-in flag and the empty pairing table and preserves rows")
    fun migration8to11AddsOptInAndPairingTableAndPreservesRows() {
        // ---- PHASE A: a disposable typed realm at the full current module shape, stamped 8.
        Realm.getInstance(configAt8).use { realm ->
            realm.executeTransaction { transactionRealm ->
                seedPending(transactionRealm.createObject(UploadFile::class.java, seedPending(UploadFile()).uri))
                seedCompleted(transactionRealm.createObject(UploadFile::class.java, seedCompleted(UploadFile()).uri))
                transactionRealm.createObject(SyncSettings::class.java, 931).apply {
                    driveId = 731
                    syncFolder = 42
                    syncVideo = true
                    lastSync = Date(1_700_000_900_000L)
                }
            }
        }

        // ---- PHASE B: the same file as a DynamicRealm at 8 — remove ONLY this branch's
        // additions, then verify the TRUE v8 shape before closing.
        DynamicRealm.getInstance(configAt8).use { dynamic ->
            dynamic.executeTransaction { transactionRealm ->
                val settingsSchema = transactionRealm.schema.get("SyncSettings")
                check(settingsSchema != null && settingsSchema.hasField("photoTrashSyncEnabled")) {
                    "the phase-A module schema must carry photoTrashSyncEnabled"
                }
                settingsSchema.removeField("photoTrashSyncEnabled")
                check(transactionRealm.schema.get("PhotoPairing") != null) {
                    "the phase-A module schema must carry PhotoPairing"
                }
                transactionRealm.schema.remove("PhotoPairing")
            }
            val settingsSchema = dynamic.schema.get("SyncSettings")
            check(settingsSchema != null)
            assertFalse(settingsSchema.hasField("photoTrashSyncEnabled"), "the true v8 shape has no opt-in column")
            assertFalse(dynamic.schema.contains("PhotoPairing"), "the true v8 shape has no pairing table")
            assertEquals(2L, dynamic.where("UploadFile").count())
            assertEquals(1L, dynamic.where("SyncSettings").count())
        }

        // ---- PHASE C: reopen at 11 with the REAL migration + the full module; audit ONLY
        // the 8→11 difference.
        Realm.getInstance(configAt11).use { realm ->
            val settingsSchema = realm.schema.get("SyncSettings")
            check(settingsSchema != null)
            assertTrue(settingsSchema.hasField(SyncSettings::photoTrashSyncEnabled.name), "the migration adds the opt-in column")
            assertTrue(realm.schema.contains("PhotoPairing"), "the migration creates the pairing table")
            assertEquals(0L, realm.where(com.infomaniak.drive.data.models.PhotoPairing::class.java).count())

            val settings = realm.where(SyncSettings::class.java).equalTo("userId", 931).findFirst()!!
            assertFalse(settings.photoTrashSyncEnabled, "the opt-in is NEVER auto-enabled by the migration")
            assertEquals(731, settings.driveId)
            assertEquals(42, settings.syncFolder)
            assertTrue(settings.syncVideo)
            assertEquals(Date(1_700_000_900_000L), settings.lastSync)

            val pending = realm.where(UploadFile::class.java).equalTo("uri", "content://fixture/pending-8").findFirst()!!
            assertEquals(731, pending.driveId)
            assertEquals("fixture-pending-8.jpg", pending.fileName)
            assertEquals(4_242L, pending.fileSize)
            assertEquals(42, pending.remoteFolder)
            assertEquals(UploadFile.Type.SYNC.name, pending.type)
            assertNull(pending.uploadAt)

            val completed = realm.where(UploadFile::class.java).equalTo("uri", "content://fixture/completed-8").findFirst()!!
            assertEquals(Date(1_700_001_500_000L), completed.uploadAt)
            assertEquals(1_111L, completed.fileSize)

            // THE PAIRING TABLE IS WRITABLE (the recordPairing seam's contract).
            // The baselineTrashed column is genuinely NULLABLE: a pairing whose activation
            // baseline has not been observed yet must read back as null.
            val pairingSchema = realm.schema.get("PhotoPairing")!!
            assertTrue(pairingSchema.isNullable("baselineTrashed"), "the activation baseline column is nullable")
            assertTrue(pairingSchema.isNullable("pairedAt"), "pairedAt is nullable")
            assertTrue(pairingSchema.isRequired("remoteFileId"), "remoteFileId is REQUIRED")
            realm.executeTransaction {
                realm.createObject(com.infomaniak.drive.data.models.PhotoPairing::class.java, "content://fixture/pairing")
            }
            val pendingPairing = realm.where(com.infomaniak.drive.data.models.PhotoPairing::class.java)
                .equalTo("uri", "content://fixture/pairing").findFirst()!!
            assertEquals(null, pendingPairing.baselineTrashed, "a pairing with no observed baseline reads back null")
            assertEquals(1L, realm.where(com.infomaniak.drive.data.models.PhotoPairing::class.java).count())
        }
    }

    @AfterAll
    fun cleanup() {
        // ONLY the random own-cache fixture is deleted — never any other realm, never any
        // owner data.
        runCatching { Realm.deleteRealm(configAt11) }
        runCatching { Realm.deleteRealm(configAt8) }
    }
}
