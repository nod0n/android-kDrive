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
import com.infomaniak.drive.data.models.MediaFolder
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

/** Real schema-7 fixture: remove all post-7 fields before applying the production chain.
 * Run only through UploadMigrationInstrumentation (self-targeted, synthetic sandbox).
 * Native execution is required; compiling this fixture is not migration acceptance. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UploadMigration7to13Test {

    private val fixtureName = "uploadMigration7to13-${UUID.randomUUID()}"
    private val fixtureDir = ApplicationProvider.getApplicationContext<android.content.Context>().cacheDir

    private val configAt7 = RealmConfiguration.Builder()
        .schemaVersion(7L)
        .modules(RealmModules.SyncFilesModule())
        .name(fixtureName)
        .directory(fixtureDir)
        .build()

    private val configAtCurrent = RealmConfiguration.Builder()
        .schemaVersion(UploadMigration.DB_VERSION)
        .migration(UploadMigration())
        .modules(RealmModules.SyncFilesModule())
        .name(fixtureName)
        .directory(fixtureDir)
        .build()

    private fun seedPending(uploadFile: UploadFile) = uploadFile.apply {
        // NO uri here: the primary key belongs to createObject exactly once — assigning it
        // on the ALREADY-MANAGED object afterwards is a RealmException (device-62a3).
        driveId = 731
        userId = 931
        fileName = "fixture-pending-7.jpg"
        fileSize = 4_242L
        fileModifiedAt = Date(1_700_000_000_000L)
        remoteFolder = 42
        type = UploadFile.Type.SYNC.name
    }

    private fun seedCompleted(uploadFile: UploadFile) = uploadFile.apply {
        driveId = 731
        userId = 931
        fileName = "fixture-completed-7.jpg"
        fileSize = 1_111L
        fileModifiedAt = Date(1_700_001_000_000L)
        remoteFolder = 42
        type = UploadFile.Type.SYNC.name
        uploadAt = Date(1_700_001_500_000L)
    }

    @Test
    @DisplayName("migration 7 to 13 adds the opt-in and epoch columns and the empty pairing table and preserves rows")
    fun migration7to13PreservesRowsAndValidatesCurrentSchema() {
        // ---- PHASE A: a disposable typed realm at the full current module shape, stamped 7.
        Realm.getInstance(configAt7).use { realm ->
            realm.executeTransaction { transactionRealm ->
                seedPending(transactionRealm.createObject(UploadFile::class.java, "content://fixture/pending-7"))
                seedCompleted(transactionRealm.createObject(UploadFile::class.java, "content://fixture/completed-7"))
                transactionRealm.createObject(MediaFolder::class.java, 71L).apply {
                    name = "synthetic bucket"; path = "/synthetic/bucket"; isSynced = true
                }
                transactionRealm.createObject(SyncSettings::class.java, 931).apply {
                    driveId = 731
                    syncFolder = 42
                    syncVideo = true
                    lastSync = Date(1_700_000_900_000L)
                }
            }
        }

        // ---- PHASE B: remove the v8 Wi-Fi field and this branch's post-8 additions,
        // then verify the genuine schema-7 shape before closing.
        DynamicRealm.getInstance(configAt7).use { dynamic ->
            dynamic.executeTransaction { transactionRealm ->
                val settingsSchema = transactionRealm.schema.get("SyncSettings")
                check(settingsSchema != null && settingsSchema.hasField("photoTrashSyncEnabled")) {
                    "the phase-A module schema must carry photoTrashSyncEnabled"
                }
                settingsSchema.removeField("photoTrashSyncEnabled")
                check(settingsSchema.hasField("onlyWifiSyncMedia"))
                settingsSchema.removeField("onlyWifiSyncMedia")
                // The v13 epoch column is part of TODAY'S typed module too: the true v7
                // shape predates it, so the fixture must remove it as well — otherwise the
                // real 12→13 migration's unconditional addField hits "field already
                // exists" (device-62a3 evidence).
                check(settingsSchema.hasField(SyncSettings::photoTrashActivationEpoch.name)) {
                    "the phase-A module schema must carry photoTrashActivationEpoch"
                }
                settingsSchema.removeField(SyncSettings::photoTrashActivationEpoch.name)
                check(transactionRealm.schema.get("PhotoPairing") != null) {
                    "the phase-A module schema must carry PhotoPairing"
                }
                transactionRealm.schema.remove("PhotoPairing")
            }
            val settingsSchema = dynamic.schema.get("SyncSettings")
            check(settingsSchema != null)
            assertFalse(settingsSchema.hasField("onlyWifiSyncMedia"))
            assertFalse(settingsSchema.hasField("photoTrashSyncEnabled"), "the true v7 shape has no opt-in column")
            assertFalse(
                settingsSchema.hasField(SyncSettings::photoTrashActivationEpoch.name),
                "the true v7 shape has no activation-epoch column",
            )
            assertFalse(dynamic.schema.contains("PhotoPairing"), "the true v7 shape has no pairing table")
            assertEquals(2L, dynamic.where("UploadFile").count())
            assertEquals(1L, dynamic.where("SyncSettings").count())
            assertEquals(1L, dynamic.where("MediaFolder").count())
        }

        // ---- PHASE C: reopen at 13 with the REAL migration + the full module; audit ONLY
        // the 7→13 difference.
        Realm.getInstance(configAtCurrent).use { realm ->
            val settingsSchema = realm.schema.get("SyncSettings")
            check(settingsSchema != null)
            assertTrue(settingsSchema.hasField(SyncSettings::photoTrashSyncEnabled.name), "the migration adds the opt-in column")
            assertTrue(settingsSchema.hasField(SyncSettings::photoTrashActivationEpoch.name), "the migration adds the activation-epoch column (v13)")
            assertTrue(realm.schema.contains("PhotoPairing"), "the migration creates the pairing table")
            assertEquals(0L, realm.where(com.infomaniak.drive.data.models.PhotoPairing::class.java).count())

            val settings = realm.where(SyncSettings::class.java).equalTo("userId", 931).findFirst()!!
            assertFalse(settings.onlyWifiSyncMedia)
            val bucket = realm.where(MediaFolder::class.java).equalTo("id", 71L).findFirst()!!
            assertEquals("synthetic bucket", bucket.name)
            assertEquals("/synthetic/bucket", bucket.path)
            assertTrue(bucket.isSynced)
            assertFalse(settings.photoTrashSyncEnabled, "the opt-in is NEVER auto-enabled by the migration")
            assertEquals(0L, settings.photoTrashActivationEpoch, "migrated rows start in the never-activated generation")
            assertEquals(731, settings.driveId)
            assertEquals(42, settings.syncFolder)
            assertTrue(settings.syncVideo)
            assertEquals(Date(1_700_000_900_000L), settings.lastSync)

            val pending = realm.where(UploadFile::class.java).equalTo("uri", "content://fixture/pending-7").findFirst()!!
            assertEquals(731, pending.driveId)
            assertEquals("fixture-pending-7.jpg", pending.fileName)
            assertEquals(4_242L, pending.fileSize)
            assertEquals(42, pending.remoteFolder)
            assertEquals(UploadFile.Type.SYNC.name, pending.type)
            assertNull(pending.uploadAt)

            val completed = realm.where(UploadFile::class.java).equalTo("uri", "content://fixture/completed-7").findFirst()!!
            assertEquals(Date(1_700_001_500_000L), completed.uploadAt)
            assertEquals(1_111L, completed.fileSize)

            // THE PAIRING TABLE IS WRITABLE (the recordPairing seam's contract).
            // The baselineTrashed column is genuinely NULLABLE: a pairing whose activation
            // baseline has not been observed yet must read back as null.
            val pairingSchema = realm.schema.get("PhotoPairing")!!
            val allFields = setOf("uri", "userId", "driveId", "remoteFileId", "fileName", "localSizeAtPairing",
                "localModifiedAtPairing", "localContentDigest", "remoteContentHash", "pairedAt", "activationEpoch",
                "baselineTrashed", "lastObservedTrashed", "lastLocalTrashReconciledAt", "remoteTrashPendingSince",
                "lastRemoteTrashAppliedAt", "state")
            assertEquals(allFields, pairingSchema.fieldNames)
            assertTrue(pairingSchema.isPrimaryKey("uri"))
            val nullable = setOf("baselineTrashed", "pairedAt", "localContentDigest", "remoteContentHash")
            allFields.filter { it != "uri" }.forEach { field ->
                assertEquals(field in nullable, pairingSchema.isNullable(field), "nullability: $field")
            }
            assertTrue(pairingSchema.isRequired("activationEpoch"))
            assertTrue(pairingSchema.isNullable("baselineTrashed"), "the activation baseline column is nullable")
            assertTrue(pairingSchema.isNullable("pairedAt"), "pairedAt is nullable")
            assertTrue(pairingSchema.isRequired("remoteFileId"), "remoteFileId is REQUIRED")
            // v12 content binding: both columns are genuinely NULLABLE — unknown evidence
            // preserves, and an unbound pairing is never eligible for a destructive action.
            assertTrue(pairingSchema.isNullable("localContentDigest"), "the uploaded-source digest column is nullable")
            assertTrue(pairingSchema.isNullable("remoteContentHash"), "the remote basis hash column is nullable")
            realm.executeTransaction {
                realm.createObject(com.infomaniak.drive.data.models.PhotoPairing::class.java, "content://fixture/pairing")
            }
            val pendingPairing = realm.where(com.infomaniak.drive.data.models.PhotoPairing::class.java)
                .equalTo("uri", "content://fixture/pairing").findFirst()!!
            assertEquals(null, pendingPairing.baselineTrashed, "a pairing with no observed baseline reads back null")
            assertNull(pendingPairing.localContentDigest, "an unbound upload digest reads back null")
            assertNull(pendingPairing.remoteContentHash, "an unbound remote basis reads back null")
            assertEquals(0L, pendingPairing.activationEpoch)
            assertFalse(pendingPairing.lastObservedTrashed)
            assertEquals(0L, pendingPairing.lastLocalTrashReconciledAt)
            assertEquals(0L, pendingPairing.remoteTrashPendingSince)
            assertEquals(0L, pendingPairing.lastRemoteTrashAppliedAt)
            realm.executeTransaction {
                pendingPairing.apply {
                    userId = 931; driveId = 731; remoteFileId = 41; fileName = "synthetic.jpg"
                    localSizeAtPairing = 51; localModifiedAtPairing = 52
                    localContentDigest = "synthetic-digest"; remoteContentHash = "synthetic-hash"
                    pairedAt = Date(53); activationEpoch = 54; baselineTrashed = true; lastObservedTrashed = true
                    lastLocalTrashReconciledAt = 55; remoteTrashPendingSince = 56; lastRemoteTrashAppliedAt = 57
                    state = "DENIED"
                }
            }
            assertEquals(931, pendingPairing.userId); assertEquals(731, pendingPairing.driveId)
            assertEquals(41, pendingPairing.remoteFileId); assertEquals("synthetic.jpg", pendingPairing.fileName)
            assertEquals(51L, pendingPairing.localSizeAtPairing); assertEquals(52L, pendingPairing.localModifiedAtPairing)
            assertEquals("synthetic-digest", pendingPairing.localContentDigest); assertEquals("synthetic-hash", pendingPairing.remoteContentHash)
            assertEquals(Date(53), pendingPairing.pairedAt); assertEquals(54L, pendingPairing.activationEpoch)
            assertEquals(true, pendingPairing.baselineTrashed); assertTrue(pendingPairing.lastObservedTrashed)
            assertEquals(55L, pendingPairing.lastLocalTrashReconciledAt); assertEquals(56L, pendingPairing.remoteTrashPendingSince)
            assertEquals(57L, pendingPairing.lastRemoteTrashAppliedAt); assertEquals("DENIED", pendingPairing.state)
            assertEquals(1L, realm.where(com.infomaniak.drive.data.models.PhotoPairing::class.java).count())
        }
    }

    @AfterAll
    fun cleanup() {
        // ONLY the random own-cache fixture is deleted — never any other realm, never any
        // owner data.
        runCatching { Realm.deleteRealm(configAtCurrent) }
        runCatching { Realm.deleteRealm(configAt7) }
    }
}
