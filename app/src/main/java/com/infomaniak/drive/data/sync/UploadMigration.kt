/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2022-2024 Infomaniak Network SA
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

import com.infomaniak.drive.data.models.MediaFolder
import com.infomaniak.drive.data.models.PhotoPairing
import com.infomaniak.drive.data.models.SyncSettings
import com.infomaniak.drive.data.models.UploadFile
import io.realm.DynamicRealm
import io.realm.FieldAttribute
import io.realm.RealmMigration
import java.net.URLEncoder
import java.util.Date

class UploadMigration : RealmMigration {

    override fun migrate(realm: DynamicRealm, oldVersion: Long, newVersion: Long) {
        var oldVersionTemp = oldVersion

        // DynamicRealm exposes an editable schema
        val schema = realm.schema

        //region Migrate to version 1: Create table MediaFolder and remove some fields
        if (oldVersionTemp == 0L) {
            // Add MediaFolder table
            schema.create(MediaFolder::class.java.simpleName)!!
                .addField(MediaFolder::id.name, Long::class.java, FieldAttribute.PRIMARY_KEY)
                .addField(MediaFolder::name.name, String::class.java, FieldAttribute.REQUIRED)
                .addField(MediaFolder::isSynced.name, Boolean::class.java, FieldAttribute.REQUIRED)
            // Remove some fields in SyncSettings
            schema.get(SyncSettings::class.java.simpleName)!!
                .removeField("syncPicture")
                .removeField("syncScreenshot")

            oldVersionTemp++
        }
        //endregion

        //region Migrate to version 2: Add new fields in UploadFile and SyncSettings table
        if (oldVersionTemp == 1L) {
            schema.get(UploadFile::class.java.simpleName)!!
                .addField(UploadFile::remoteSubFolder.name, String::class.java)
            schema.get(SyncSettings::class.java.simpleName)!!
                .addField(SyncSettings::createDatedSubFolders.name, Boolean::class.java, FieldAttribute.REQUIRED)

            oldVersionTemp++
        }
        //endregion

        //region Migrate to version 3: Add new field in SyncSetting table
        if (oldVersionTemp == 2L) {
            schema.get(SyncSettings::class.java.simpleName)!!
                .addField(SyncSettings::deleteAfterSync.name, Boolean::class.java, FieldAttribute.REQUIRED)

            oldVersionTemp++
        }
        //endregion

        //region Migrate to version 4: Update fields with api v2
        if (oldVersionTemp == 3L) {
            schema.get(UploadFile::class.java.simpleName)!!
                .addField(UploadFile::uploadToken.name, String::class.java)
                .removeField("identifier")

            oldVersionTemp++
        }
        //endregion

        //region Migrate to version 5: Migrate remoteSubFolder with api v2
        if (oldVersionTemp == 4L) {
            realm.where(UploadFile::class.java.simpleName)
                .equalTo(UploadFile::type.name, UploadFile.Type.SYNC.name)
                .findAll()?.onEach { uploadFile ->
                    val fileName = uploadFile.getString("fileName")
                    val encodedName = URLEncoder.encode(fileName, "UTF-8")

                    uploadFile.getString("remoteSubFolder")?.let { subFolder ->
                        uploadFile.setString("remoteSubFolder", subFolder.substringBeforeLast(encodedName))
                    }
                }

            oldVersionTemp++
        }
        //endregion

        //region Migrate to version 6: Api v3 - Add uploadHost
        if (oldVersionTemp == 5L) {
            schema.get(UploadFile::class.java.simpleName)!!
                .addField(UploadFile::uploadHost.name, String::class.java)
            oldVersionTemp++
        }
        //endregion

        //region Migrate to version 7: Add path to MediaFolder
        if (oldVersionTemp == 6L) {
            schema[MediaFolder::class.java.simpleName]?.addField("path", String::class.java, FieldAttribute.REQUIRED)
            oldVersionTemp++
        }
        //endregion

        if (oldVersionTemp < 8L) {
            schema.get(SyncSettings::class.java.simpleName)
                ?.addField("onlyWifiSyncMedia", Boolean::class.java, FieldAttribute.REQUIRED)
        }

        // Migrate to version 11: photo trash propagation opt-in and pairing table.
        // Versions 9/10 only ever existed on development branches with different meanings
        // (photo-deletion / offline-folder experiments); the one actually installed source
        // above 8 is the T-0079 device test build at version 10. That schema's undeclared
        // development fields are removed explicitly (never implicitly); rows and pending
        // uploads are preserved, nothing is deleted.
        if (oldVersionTemp in 8L..10L) {
            schema.get(SyncSettings::class.java.simpleName)
                ?.addField(SyncSettings::photoTrashSyncEnabled.name, Boolean::class.java, FieldAttribute.REQUIRED)

            if (schema.get(PhotoPairing::class.java.simpleName) == null) {
                schema.create(PhotoPairing::class.java.simpleName)!!.apply {
                    addField(PhotoPairing::uri.name, String::class.java, FieldAttribute.REQUIRED)
                    addPrimaryKey(PhotoPairing::uri.name)
                    addField(PhotoPairing::userId.name, Int::class.java, FieldAttribute.REQUIRED)
                    addField(PhotoPairing::driveId.name, Int::class.java, FieldAttribute.REQUIRED)
                    addField(PhotoPairing::remoteFileId.name, Int::class.java, FieldAttribute.REQUIRED)
                    addField(PhotoPairing::fileName.name, String::class.java, FieldAttribute.REQUIRED)
                    addField(PhotoPairing::localSizeAtPairing.name, Long::class.java, FieldAttribute.REQUIRED)
                    addField(PhotoPairing::localModifiedAtPairing.name, Long::class.java, FieldAttribute.REQUIRED)
                    addField(PhotoPairing::pairedAt.name, Date::class.java)
                    // The model declares Boolean? (baseline pending until actually observed):
                    // the column must be BOXED/nullable, never a REQUIRED primitive, or the
                    // migrated schema would not validate against the typed module.
                    addField(PhotoPairing::baselineTrashed.name, Boolean::class.javaObjectType)
                    addField(PhotoPairing::lastObservedTrashed.name, Boolean::class.java, FieldAttribute.REQUIRED)
                    addField(PhotoPairing::lastLocalTrashReconciledAt.name, Long::class.java, FieldAttribute.REQUIRED)
                    addField(PhotoPairing::remoteTrashPendingSince.name, Long::class.java, FieldAttribute.REQUIRED)
                    addField(PhotoPairing::lastRemoteTrashAppliedAt.name, Long::class.java, FieldAttribute.REQUIRED)
                    addField(PhotoPairing::state.name, String::class.java, FieldAttribute.REQUIRED)
                }
            }

            if (oldVersionTemp == 10L) {
                schema.get(UploadFile::class.java.simpleName)?.apply {
                    if (hasField("originalFileId")) removeField("originalFileId")
                    if (hasField("reconciliationDone")) removeField("reconciliationDone")
                    if (hasField("contentHash")) removeField("contentHash")
                }
            }

            oldVersionTemp = 11L
        }
        //endregion

        //region Migrate to version 12: photo content binding (TRIAGE-02 item 2).
        // The recorded upload id proves which remote ITEM was created, not that the content
        // still equals the uploaded source. These nullable columns bind the smallest real
        // content evidence to the actual successful upload: the digest of the uploaded
        // source bytes (direct-upload stream, else the same-pass receipt validated against
        // the created file's server hash — no post-upload sample exists) and the created
        // file's server content hash as the REMOTE basis. Both are nullable: unknown
        // evidence preserves — an unbound pairing is never eligible for a destructive
        // action on either side (no metadata-only fallback).
        if (oldVersionTemp == 11L) {
            schema.get(PhotoPairing::class.java.simpleName)!!.apply {
                addField(PhotoPairing::localContentDigest.name, String::class.java)
                addField(PhotoPairing::remoteContentHash.name, String::class.java)
            }

            oldVersionTemp = 12L
        }
        //endregion

    }

    companion object {
        const val DB_VERSION = 12L // Must be bumped when the schema changes
    }
}
