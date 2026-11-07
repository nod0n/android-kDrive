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
import io.realm.FieldAttribute
import io.realm.Realm
import io.realm.RealmConfiguration
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.Date
import java.util.UUID

/**
 * THE REAL 10→11 UPLOAD MIGRATION FIXTURE (androidTest instrumentation — Realm Java is a
 * native engine and CANNOT open on the JVM; this is why the test lives here and not under
 * src/test).
 *
 * Version 10 is the T-0079 DEVICE TEST BUILD's Sync.realm schema (the offline-folder
 * development branch, whose sources are the read-only kdrive-android-offline-folders
 * checkout). Its true shape is reconstructed EXACTLY from those sources: UploadFile carries
 * originalFileId (Int REQUIRED), reconciliationDone (Boolean REQUIRED) and contentHash (String
 * nullable) — fields this branch's schema does not declare — and, as at v8, neither
 * SyncSettings.photoTrashSyncEnabled nor a PhotoPairing table exists. Following the T-0079
 * pattern: (A) typed realm at the full current module STAMPED 10, (B) DynamicRealm shape
 * correction to the TRUE v10 schema (remove this branch's additions, add the dev columns, seed
 * one row with dev values), verified by audit, (C) reopen at [UploadMigration.DB_VERSION]
 * (11) with the REAL [UploadMigration] and the FULL production module — exactly what a
 * production upgrade executes — and audit ONLY the 10→11 difference: the undeclared dev fields
 * are removed EXPLICITLY (never implicitly — the migration names each one), rows and pending
 * uploads are preserved, the opt-in reads false, and the pairing table is writable.
 *
 * THE FIXTURE IS FULLY DISPOSABLE AND ACCOUNT-FREE: a RANDOM file in the instrumentation
 * target's cacheDir; every row is a synthetic constant — NO owner data, NO account, NO network.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class UploadMigration10to11Test {

    private val fixtureName = "uploadMigration10to11-${UUID.randomUUID()}"
    private val fixtureDir = ApplicationProvider.getApplicationContext<android.content.Context>().cacheDir

    private val configAt10 = RealmConfiguration.Builder()
        .schemaVersion(10L)
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

    /** The dev columns that v10 HAD and this branch's schema does not declare. */
    private val devUploadFields = listOf("originalFileId", "reconciliationDone", "contentHash")

    @Test
    @DisplayName("migration 10 to 11 removes the dev fields explicitly and preserves pending uploads")
    fun migration10to11RemovesDevFieldsExplicitlyAndPreservesRows() {
        // ---- PHASE A: a disposable typed realm at the full current module shape, stamped 10.
        Realm.getInstance(configAt10).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(UploadFile::class.java, "content://fixture/pending-10").apply {
                    driveId = 741
                    userId = 941
                    fileName = "fixture-pending-10.jpg"
                    fileSize = 999L
                    fileModifiedAt = Date(1_700_000_000_000L)
                    remoteFolder = 43
                    type = UploadFile.Type.SYNC.name
                }
                transactionRealm.createObject(SyncSettings::class.java, 941).apply {
                    driveId = 741
                    syncFolder = 43
                    lastSync = Date(1_700_000_900_000L)
                }
            }
        }

        // ---- PHASE B: the same file as a DynamicRealm at 10 — correct the shape to the TRUE
        // v10 schema (sources: the offline-folder development branch), then verify.
        DynamicRealm.getInstance(configAt10).use { dynamic ->
            dynamic.executeTransaction { transactionRealm ->
                val uploadSchema = transactionRealm.schema.get("UploadFile")
                check(uploadSchema != null)
                val settingsSchema = transactionRealm.schema.get("SyncSettings")
                check(settingsSchema != null && settingsSchema.hasField("photoTrashSyncEnabled"))
                settingsSchema.removeField("photoTrashSyncEnabled")
                check(transactionRealm.schema.get("PhotoPairing") != null)
                transactionRealm.schema.remove("PhotoPairing")

                uploadSchema.addField("originalFileId", Int::class.java, FieldAttribute.REQUIRED)
                uploadSchema.addField("reconciliationDone", Boolean::class.java, FieldAttribute.REQUIRED)
                uploadSchema.addField("contentHash", String::class.java)
                val pending = transactionRealm.where("UploadFile").equalTo("uri", "content://fixture/pending-10").findFirst()!!
                pending.setInt("originalFileId", 555)
                pending.setBoolean("reconciliationDone", false)
                pending.setString("contentHash", "sha-256:deadbeef")
            }

            val uploadSchema = dynamic.schema.get("UploadFile")
            check(uploadSchema != null)
            devUploadFields.forEach { field -> assertTrue(uploadSchema.hasField(field), "the true v10 shape carries $field") }
            assertFalse(uploadSchema.hasField("photoTrashSyncEnabled"))
            assertFalse(dynamic.schema.contains("PhotoPairing"), "the true v10 shape has no pairing table")
            assertEquals(1L, dynamic.where("UploadFile").count())
        }

        // ---- PHASE C: reopen at 11 with the REAL migration + the full module; audit ONLY
        // the 10→11 difference.
        Realm.getInstance(configAt11).use { realm ->
            val uploadSchema = realm.schema.get("UploadFile")
            check(uploadSchema != null)
            devUploadFields.forEach { field ->
                assertFalse(uploadSchema.hasField(field), "the undeclared dev field $field is removed explicitly")
            }
            val settingsSchemaAfter = realm.schema.get("SyncSettings")
            check(settingsSchemaAfter != null)
            assertTrue(settingsSchemaAfter.hasField(SyncSettings::photoTrashSyncEnabled.name), "the migration adds the opt-in column")
            assertTrue(realm.schema.contains("PhotoPairing"))

            val pending = realm.where(UploadFile::class.java).equalTo("uri", "content://fixture/pending-10").findFirst()!!
            assertEquals(741, pending.driveId)
            assertEquals("fixture-pending-10.jpg", pending.fileName)
            assertEquals(999L, pending.fileSize)
            assertEquals(43, pending.remoteFolder)
            assertEquals(Date(1_700_000_000_000L), pending.fileModifiedAt)

            val settings = realm.where(SyncSettings::class.java).equalTo("userId", 941).findFirst()!!
            assertFalse(settings.photoTrashSyncEnabled, "the opt-in is NEVER auto-enabled by the migration")
            assertEquals(741, settings.driveId)

            // THE PAIRING TABLE IS WRITABLE (the recordPairing seam's contract).
            realm.executeTransaction {
                realm.createObject(com.infomaniak.drive.data.models.PhotoPairing::class.java, "content://fixture/pairing-10")
            }
            assertEquals(1L, realm.where(com.infomaniak.drive.data.models.PhotoPairing::class.java).count())
        }
    }

    @AfterAll
    fun cleanup() {
        // ONLY the random own-cache fixture is deleted — never any other realm, never any
        // owner data.
        runCatching { Realm.deleteRealm(configAt11) }
        runCatching { Realm.deleteRealm(configAt10) }
    }
}
