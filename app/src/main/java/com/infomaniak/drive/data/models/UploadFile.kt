/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2022-2026 Infomaniak Network SA
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
package com.infomaniak.drive.data.models

import android.content.ContentResolver
import android.content.Context
import android.net.Uri
import android.os.Build.VERSION.SDK_INT
import android.provider.DocumentsContract
import android.provider.MediaStore
import androidx.core.net.toFile
import androidx.core.net.toUri
import com.infomaniak.core.common.utils.SECONDS_IN_A_DAY
import com.infomaniak.core.common.utils.format
import com.infomaniak.core.network.models.exceptions.NetworkException
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.UploadTask
import com.infomaniak.drive.data.api.UploadTask.Companion.ConflictOption
import com.infomaniak.drive.data.cache.DriveInfosController
import com.infomaniak.drive.data.sync.PhotoTrashController
import com.infomaniak.drive.data.sync.UploadMigration
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.IOFile
import com.infomaniak.drive.utils.RealmModules
import io.realm.Realm
import io.realm.RealmConfiguration
import io.realm.RealmObject
import io.realm.RealmQuery
import io.realm.RealmResults
import io.realm.annotations.Ignore
import io.realm.annotations.PrimaryKey
import io.realm.kotlin.oneOf
import io.realm.kotlin.toFlow
import com.infomaniak.core.sentry.SentryLog
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.util.Date
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

open class UploadFile(
    @PrimaryKey var uri: String = "",
    var deletedAt: Date? = null,
    var driveId: Int = -1,
    var fileCreatedAt: Date? = null,
    var fileModifiedAt: Date = Date(),
    var fileName: String = "",
    var fileSize: Long = 0L,
    var uploadToken: String? = null,
    var remoteFolder: Int = -1,
    var remoteSubFolder: String? = null,
    var type: String = Type.SYNC.name,
    var uploadAt: Date? = null,
    var uploadHost: String? = null,
    var userId: Int = -1,
) : RealmObject() {

    @delegate:Ignore
    val okHttpClient: OkHttpClient by lazy { runBlocking { AccountUtils.getHttpClient(userId = userId, timeout = 120) } }

    fun isSchemeFile() = getUriObject().scheme.equals(ContentResolver.SCHEME_FILE)

    fun createSubFolder(parent: String, createDatedSubFolders: Boolean) {
        remoteSubFolder = parent + if (createDatedSubFolders) "/${Date(getLastModified()).format("yyyy/MM")}" else ""
    }

    fun store(coroutineContext: CoroutineContext = EmptyCoroutineContext, customRealm: Realm? = null) {
        var realm: Realm? = null
        try {
            realm = customRealm ?: getRealmInstance()
            realm.executeTransaction { mutableRealm ->
                mutableRealm.insertOrUpdate(this)
                coroutineContext.ensureActive()
            }
        } finally {
            if (customRealm == null) realm?.close()
        }
    }

    fun isSync() = type == Type.SYNC.name

    fun isSyncOffline() = type == Type.SYNC_OFFLINE.name

    private fun isCloudStorage() = type == Type.CLOUD_STORAGE.name

    fun replaceOnConflict() = isSync() || isSyncOffline() || isCloudStorage()

    fun uploadConflictOption() = if (replaceOnConflict()) ConflictOption.VERSION else ConflictOption.RENAME

    fun resetUploadToken() {
        getRealmInstance().use { realm ->
            uploadFileByUriQuery(realm, uri).findFirst()?.apply {
                realm.executeTransaction {
                    uploadToken = null
                    uploadHost = null
                }
            }
        }
    }

    fun getUriObject() = uri.toUri()

    fun getOriginalUri(context: Context): Uri {
        val uriObject = getUriObject()
        return when {
            DocumentsContract.isDocumentUri(context, uriObject) -> uriObject
            SDK_INT >= 29 -> MediaStore.setRequireOriginal(uriObject)
            else -> uriObject
        }
    }

    fun updateFileSize(newFileSize: Long) {
        getRealmInstance().use { realm ->
            uploadFileByUriQuery(realm, uri).findFirst()?.let { uploadFile ->
                realm.executeTransaction { uploadFile.fileSize = newFileSize }
            }
        }
        fileSize = newFileSize
    }

    fun updateUploadToken(newUploadToken: String, uploadHost: String) {
        getRealmInstance().use { realm ->
            uploadFileByUriQuery(realm, uri).findFirst()?.let { uploadFile ->
                realm.executeTransaction {
                    uploadFile.uploadToken = newUploadToken
                    uploadFile.uploadHost = uploadHost
                }
            }
        }
        uploadToken = newUploadToken
    }

    fun deleteIfExists(keepFile: Boolean = false, customRealm: Realm? = null) {
        when (customRealm) {
            null -> getRealmInstance().use { deleteIfExistsInternal(keepFile = keepFile, it) }
            else -> deleteIfExistsInternal(keepFile = keepFile, customRealm)
        }
    }

    fun getLastModified(): Long {
        val currentTimeMillis = System.currentTimeMillis()
        val lastModifiedAt = if (fileModifiedAt.time > currentTimeMillis) currentTimeMillis else fileModifiedAt.time
        return lastModifiedAt
    }

    private fun deleteIfExistsInternal(keepFile: Boolean = false, realm: Realm) {
        val uploadFileProxy = uploadFileByUriQuery(realm, uri).findFirst() ?: return
        // Cancel session if exists
        uploadFileProxy.uploadToken?.let { uploadToken ->
            val response = ApiRepository.cancelSession(uploadFileProxy.driveId, uploadToken, okHttpClient)
            if (response.error?.exception is NetworkException) throw UploadTask.NetworkException()
        }
        // Delete in realm
        realm.executeTransaction {
            if (uploadFileProxy.isValid) {
                if (keepFile) uploadFileProxy.deletedAt = Date() else uploadFileProxy.deleteFromRealm()
            }
        }
    }

    enum class Type {
        SYNC, UPLOAD, SHARED_FILE, SYNC_OFFLINE, CLOUD_STORAGE
    }

    companion object {
        private const val TAG = "UploadFile"
        private const val DB_NAME = "Sync.realm"
        private const val ONE_DAY = SECONDS_IN_A_DAY * 1_000L
        private var realmConfiguration: RealmConfiguration = RealmConfiguration.Builder().name(DB_NAME)
            .schemaVersion(UploadMigration.DB_VERSION)
            .modules(RealmModules.SyncFilesModule())
            .migration(UploadMigration())
            .build()

        private inline val Realm.uploadTable get() = where(UploadFile::class.java)

        fun getRealmInstance(): Realm {
            try {
                return Realm.getInstance(realmConfiguration)
            } catch (exception: Exception) {
                // Never delete the database on open failure (the former realm-java#7706
                // workaround wiped Sync.realm here): rows, pending uploads and settings must
                // survive a transient or schema problem.
                SentryLog.e(TAG, "Failed to open Sync.realm; keeping the database intact", exception)
                throw exception
            }
        }

        private fun uploadFileByUriQuery(realm: Realm, uri: String): RealmQuery<UploadFile> {
            return realm.uploadTable.equalTo(UploadFile::uri.name, uri)
        }

        private fun pendingUploadsQuery(
            realm: Realm,
            folderId: Int? = null,
            onlyCurrentUser: Boolean = false,
            driveIds: Array<Int>? = null
        ): RealmQuery<UploadFile> {
            return realm.uploadTable.apply {
                folderId?.let { equalTo(UploadFile::remoteFolder.name, it) }
                if (onlyCurrentUser) equalTo(UploadFile::userId.name, AccountUtils.currentUserId)
                driveIds?.let { oneOf(UploadFile::driveId.name, it) }
                isNull(UploadFile::uploadAt.name)
                isNull(UploadFile::deletedAt.name)
            }
        }

        private fun allPendingFoldersQuery(realm: Realm): RealmQuery<UploadFile> {
            return pendingUploadsQuery(realm, onlyCurrentUser = true, driveIds = currentDriveAndSharedWithMeIds())
                .distinct(UploadFile::remoteFolder.name)
        }

        private fun currentDriveAndSharedWithMeIds(): Array<Int> {
            val sharedWithMeIds = DriveInfosController.getDrives(AccountUtils.currentUserId, sharedWithMe = true).map { it.id }
            return arrayOf(AccountUtils.currentDriveId, *sharedWithMeIds.toTypedArray())
        }

        fun getAllPendingUploads(customRealm: Realm? = null): ArrayList<UploadFile> {
            val block: (Realm) -> ArrayList<UploadFile> = { realm ->
                realm.refresh() // TODO: (Realm kotlin) - Remove when we update to Realm Kotlin
                val results = pendingUploadsQuery(realm).findAll()
                val priorityUploadFiles = results.where().notEqualTo(UploadFile::type.name, Type.SYNC.name).findAll()
                val syncUploadFiles = results.where().equalTo(UploadFile::type.name, Type.SYNC.name).findAll()
                arrayListOf(
                    *realm.copyFromRealm(priorityUploadFiles, 0).toTypedArray(),
                    *realm.copyFromRealm(syncUploadFiles, 0).toTypedArray()
                )
            }

            return customRealm?.let(block) ?: getRealmInstance().use(block)
        }

        fun getAllPendingUploadsWithoutPriorityCount(realm: Realm): Long {
            return pendingUploadsQuery(realm).count()
        }

        fun getAllPendingPriorityFilesCount(): Long {
            return getRealmInstance().use { pendingUploadsQuery(it).notEqualTo(UploadFile::type.name, Type.SYNC.name).count() }
        }

        fun getAllPendingFolders(realm: Realm): RealmResults<UploadFile>? {
            return allPendingFoldersQuery(realm).findAll()
        }

        fun getAllPendingFoldersCount(realm: Realm): Long {
            return allPendingFoldersQuery(realm).count()
        }

        fun getCurrentUserPendingUploads(realm: Realm, folderId: Int): RealmResults<UploadFile>? {
            return pendingUploadsQuery(realm, folderId, true).findAll()
        }

        fun getAllPendingUploadsCount(customRealm: Realm? = null): Int {
            val block: (Realm) -> Int = { realm ->
                realm.refresh() // TODO: (Realm kotlin) - Remove when we update to Realm Kotlin
                pendingUploadsQuery(realm).count().toInt()
            }

            return customRealm?.let(block) ?: getRealmInstance().use(block)
        }

        fun getCurrentUserPendingUploadsCount(folderId: Int? = null): Int {
            return getRealmInstance().use { realm ->
                realm.refresh() // TODO: (Realm kotlin) - Remove when we update to Realm Kotlin
                pendingUploadsQuery(realm, folderId, true, driveIds = currentDriveAndSharedWithMeIds()).count().toInt()
            }
        }

        fun getCurrentUserPendingUploadFile(folderId: Int? = null): RealmResults<UploadFile> {
            return pendingUploadsQuery(
                realm = getRealmInstance(),
                folderId = folderId,
                onlyCurrentUser = true,
                driveIds = currentDriveAndSharedWithMeIds()
            ).findAllAsync()
        }

        fun getCurrentUserPendingUploadFilesCount(folderId: Int?): Flow<Int> {
            return getCurrentUserPendingUploadFile(folderId).toFlow().map(Collection<*>::count)
        }

        fun getAllUploadedFiles(type: String = Type.SYNC.name): List<UploadFile>? = getRealmInstance().use { realm ->
            realm.uploadTable
                .equalTo(UploadFile::type.name, type)
                .isNull(UploadFile::deletedAt.name)
                .isNotNull(UploadFile::uploadAt.name)
                .findAll()?.map { realm.copyFromRealm(it, 0) }
        }

        fun uploadFinished(uri: Uri) {
            getRealmInstance().use { realm ->
                uploadFileByUriQuery(realm, uri.toString()).findFirst()?.let { uploadFile ->
                    realm.executeTransaction {
                        uploadFile.uploadToken = null
                        uploadFile.uploadHost = null
                        uploadFile.uploadAt = Date()
                    }
                }
            }
        }

        fun getLastDate(context: Context): Date {
            val date: Date? = getRealmInstance().use { realm ->
                realm.where(SyncSettings::class.java).findFirst()?.lastSync
            }

            return if (date == null) {
                Date(IOFile(context.filesDir, DB_NAME).lastModified())
            } else {
                val now = System.currentTimeMillis()
                // If you change time zone
                if (date.time > now) Date(now - ONE_DAY) else date
            }
        }

        fun canUpload(uri: Uri, lastModified: Date, customRealm: Realm? = null): Boolean {
            val block: (Realm) -> Boolean = { realm ->
                uploadFileByUriQuery(realm, uri.toString())
                    .equalTo(UploadFile::fileModifiedAt.name, lastModified)
                    .findFirst() == null
            }
            return customRealm?.let(block) ?: getRealmInstance().use(block)
        }

        fun deleteAll(uploadFiles: List<UploadFile>) {
            getRealmInstance().use {
                it.executeTransaction { realm ->
                    uploadFiles.forEach { uploadFile ->
                        deleteFromRealm(realm, uploadFile.uri)
                    }
                }
            }
        }

        fun deleteAllFromUris(uris: List<Uri>) {
            getRealmInstance().use {
                it.executeTransaction { mutableRealm ->
                    uris.forEach { uri ->
                        deleteFromRealm(mutableRealm, uri.toString())
                    }
                }
            }
        }

        private fun deleteFromRealm(realm: Realm, uploadFileUri: String) {
            uploadFileByUriQuery(realm, uploadFileUri).findFirst()?.let { uploadFileRealm ->
                // Don't delete definitively if it's a sync
                if (uploadFileRealm.type == Type.SYNC.name) {
                    uploadFileRealm.deletedAt = Date()
                } else {
                    // Delete definitively
                    val uri = uploadFileRealm.getUriObject()
                    if (uri.scheme.equals(ContentResolver.SCHEME_FILE) && !uploadFileRealm.isSyncOffline()) {
                        uri.toFile().apply { if (exists()) delete() }
                    }
                    uploadFileRealm.deleteFromRealm()
                }
            }
        }

        fun cancelAllPendingFilesSessions(folderId: Int) {
            getRealmInstance().use { realm ->
                realm.uploadTable
                    .equalTo(UploadFile::remoteFolder.name, folderId)
                    .isNull(UploadFile::uploadAt.name)
                    .isNotNull(UploadFile::uploadToken.name)
                    .findAll()?.onEach { uploadFileProxy ->
                        with(uploadFileProxy) {
                            ApiRepository.cancelSession(driveId, uploadToken!!, okHttpClient)
                        }
                    }
            }
        }

        fun deleteAll(folderId: Int?, permanently: Boolean = false) {
            getRealmInstance().use {
                it.executeTransaction { realm ->
                    // Delete all data files for all uploads with scheme FILE
                    realm.uploadTable
                        .apply { folderId?.let { equalTo(UploadFile::remoteFolder.name, folderId) } }
                        .beginsWith(UploadFile::uri.name, ContentResolver.SCHEME_FILE)
                        .findAll().forEach { uploadFile ->
                            if (!uploadFile.isSyncOffline()) uploadFile.getUriObject().toFile().apply { if (exists()) delete() }
                        }

                    if (permanently) {
                        realm.uploadTable
                            .apply { folderId?.let { equalTo(UploadFile::remoteFolder.name, folderId) } }
                            .isNull(UploadFile::uploadAt.name)
                            .findAll().deleteAllFromRealm()
                    } else {
                        // Delete all uploads with type SYNC
                        pendingUploadsQuery(realm, folderId)
                            .equalTo(UploadFile::type.name, Type.SYNC.name)
                            .isNull(UploadFile::uploadAt.name)
                            .findAll().forEach { uploadFile -> uploadFile.deletedAt = Date() }

                        // Delete all uploads without type SYNC
                        realm.uploadTable
                            .apply { folderId?.let { equalTo(UploadFile::remoteFolder.name, folderId) } }
                            .notEqualTo(UploadFile::type.name, Type.SYNC.name)
                            .findAll().deleteAllFromRealm()
                    }
                }
            }
        }

        fun deleteAllSyncFile(realm: Realm? = null) {
            val block: (Realm) -> Unit = { realm ->
                val transaction: (Realm) -> Unit = {
                    it.uploadTable
                        .equalTo(UploadFile::type.name, Type.SYNC.name)
                        .findAll()?.deleteAllFromRealm()
                }
                if (realm.isInTransaction) transaction(realm) else realm.executeTransaction(transaction)
            }
            realm?.let(block) ?: getRealmInstance().use(block)
        }

        fun removeAppSyncSettings() {
            var writeRevision = 0L
            var began = false
            try {
                getRealmInstance().use { realm ->
                    realm.executeTransaction {
                        // Commit-ordered revision, marked UNSETTLED before any mutation:
                        // from this instant — before the write can commit — the authority is
                        // unavailable to destructive launch until the publish below lands
                        // (or the write is abandoned and settled without one).
                        writeRevision = PhotoTrashController.beginActivationWrite()
                        began = true
                        it.where(SyncSettings::class.java).findFirst()?.deleteFromRealm()
                    }
                }
                // A write seam: the deletion is authoritative fail-closed evidence (absence
                // of the opt-in), published at its commit-assigned revision — BEFORE
                // settling: the publish must lead the settle, never trail it.
                PhotoTrashController.noteActivationWritten(
                    userId = AccountUtils.currentUserId,
                    activationEpoch = 0L,
                    optIn = false,
                    revision = writeRevision,
                )
            } finally {
                // Settles the write whatever happened: after the publish on success; on a
                // rollback/exception without one — the last published mirror remains the
                // committed truth and no assigned revision is stranded (certified reads
                // resume immediately, without requiring another write).
                if (began) PhotoTrashController.endActivationWrite()
            }
        }

        fun setAppSyncSettings(syncSettings: SyncSettings, customRealm: Realm? = null) {
            // The activation epoch is a per-user MONOTONIC generation owned by THIS seam:
            // every not-enabled -> enabled transition increments it (the live row, not a
            // detached snapshot, is the authority), any other write keeps the live value.
            // A stale writer object can therefore never rewind it, and every writer path
            // (settings UI, backup restore, upload-worker refresh) increments coherently.
            var activationEpoch = syncSettings.photoTrashActivationEpoch
            var writeRevision = 0L
            var began = false
            try {
                val block: (Realm) -> Unit = { realm ->
                    realm.executeTransaction { mutableRealm ->
                        val existing = mutableRealm.where(SyncSettings::class.java)
                            .equalTo(SyncSettings::userId.name, syncSettings.userId)
                            .findFirst()
                        activationEpoch = when {
                            existing == null -> if (syncSettings.photoTrashSyncEnabled) 1L else 0L
                            !existing.photoTrashSyncEnabled && syncSettings.photoTrashSyncEnabled ->
                                existing.photoTrashActivationEpoch + 1L
                            else -> maxOf(existing.photoTrashActivationEpoch, syncSettings.photoTrashActivationEpoch)
                        }
                        syncSettings.photoTrashActivationEpoch = activationEpoch
                        // Commit-ordered revision, marked UNSETTLED before any mutation (see
                        // removeAppSyncSettings).
                        writeRevision = PhotoTrashController.beginActivationWrite()
                        began = true
                        mutableRealm.insertOrUpdate(syncSettings)
                    }
                }
                customRealm?.let(block) ?: getRealmInstance().use(block)
                // The single settings write seam refreshes the Main-readable activation
                // authority under its commit-assigned revision — BEFORE settling: the
                // publish must lead the settle, never trail it.
                PhotoTrashController.noteActivationWritten(
                    userId = syncSettings.userId,
                    activationEpoch = activationEpoch,
                    optIn = syncSettings.photoTrashSyncEnabled,
                    revision = writeRevision,
                )
            } finally {
                // Settles the write whatever happened (see removeAppSyncSettings).
                if (began) PhotoTrashController.endActivationWrite()
            }
        }

        fun getAppSyncSettings(customRealmConfig: RealmConfiguration? = null): SyncSettings? {
            // A reader publishes an observation ONLY from a CERTIFIED read slot: BEGIN
            // before the realm opens (null while an activation write is unsettled — the row
            // about to be read may predate that write's commit), ENFORCE a fresh snapshot
            // (realm.refresh(): a long-lived thread-local instance may otherwise pin an old
            // version, and matching process counters alone would not prove a freshly read
            // committed database version), then FINISH-check AFTER the row read and publish
            // only if neither the revision counter nor the unsettled marker moved. An
            // uncertified read still RETURNS the row it read (the caller's decision
            // evidence); it only skips the mirror publication, so a pre-commit row can
            // never be published at its writer's own revision and block that writer's real
            // publish (see PhotoTrashController's revision protocol).
            val readPermit = PhotoTrashController.beginActivationRead()
            return (customRealmConfig?.let { Realm.getInstance(it) } ?: getRealmInstance()).use { realm ->
                if (readPermit != null) realm.refresh()
                val realmObject = realm.where(SyncSettings::class.java).findFirst()
                // FINISH of the certified read: evaluated AFTER the row read, never before.
                val certifiedPermit = readPermit?.takeIf { PhotoTrashController.isActivationReadCertified(it) }
                if (realmObject == null) {
                    // A missing row is authoritative fail-closed evidence: absence never
                    // preserves another observation's enabled state.
                    certifiedPermit?.let { certified ->
                        PhotoTrashController.noteActivationObserved(
                            permit = certified,
                            userId = AccountUtils.currentUserId,
                            activationEpoch = 0L,
                            optIn = false,
                        )
                    }
                    null
                } else {
                    certifiedPermit?.let { certified ->
                        PhotoTrashController.noteActivationObserved(
                            permit = certified,
                            userId = realmObject.userId,
                            activationEpoch = realmObject.photoTrashActivationEpoch,
                            optIn = realmObject.photoTrashSyncEnabled,
                        )
                    }
                    realm.copyFromRealm(realmObject, 0)
                }
            }
        }
    }
}
