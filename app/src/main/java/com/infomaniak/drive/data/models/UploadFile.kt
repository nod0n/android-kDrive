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
import com.infomaniak.drive.data.sync.UploadMigration
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.IOFile
import com.infomaniak.drive.utils.RealmModules
import io.realm.Realm
import java.util.UUID
import io.realm.RealmConfiguration
import io.realm.RealmObject
import io.realm.RealmQuery
import io.realm.RealmResults
import io.realm.annotations.Ignore
import io.realm.annotations.PrimaryKey
import io.realm.kotlin.oneOf
import io.realm.kotlin.toFlow
import io.sentry.Sentry
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
        private const val DB_NAME = "Sync.realm"
        private const val ONE_DAY = SECONDS_IN_A_DAY * 1_000L

        /** Durable (filesDir) folder holding per-revision offline folder upload snapshots. */
        const val OFFLINE_SNAPSHOTS_FOLDER = "offline_snapshots"
        private var realmConfiguration: RealmConfiguration = RealmConfiguration.Builder().name(DB_NAME)
            .schemaVersion(UploadMigration.DB_VERSION)
            .modules(RealmModules.SyncFilesModule())
            .migration(UploadMigration())
            .build()

        private inline val Realm.uploadTable get() = where(UploadFile::class.java)

        fun getRealmInstance(): Realm {
            return runCatching {
                Realm.getInstance(realmConfiguration)
            }.getOrElse { exception ->
                // Temporary fix, Issue https://github.com/realm/realm-java/issues/7706
                Sentry.captureException(exception)
                // Delete the database because it could be corrupted
                Realm.deleteRealm(realmConfiguration)
                // Create a new database
                Realm.getInstance(realmConfiguration)
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
                    val isOwnedSnapshot = isDurableOfflineSnapshot(uploadFile.uri)
                    realm.executeTransaction {
                        uploadFile.uploadToken = null
                        uploadFile.uploadHost = null
                        uploadFile.uploadAt = Date()
                    }
                    if (isOwnedSnapshot) {
                        // The snapshot was the only copy of this revision: its upload completed,
                        // so the owned file can go (issue #1902).
                        runCatching { IOFile(uri.path ?: "").takeIf { it.exists() }?.delete() }
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

        /**
         * Whether an upload of [uri] is still pending (not yet uploaded, not deleted). Used by the
         * offline folder root removal to keep the local copy backing an offline edit until it has
         * been sent to the server.
         */
        fun hasPendingUpload(uri: String, customRealm: Realm? = null): Boolean {
            val block: (Realm) -> Boolean = { realm ->
                realm.uploadTable
                    .equalTo(UploadFile::uri.name, uri)
                    .isNull(UploadFile::uploadAt.name)
                    .isNull(UploadFile::deletedAt.name)
                    .count() > 0
            }
            return customRealm?.let(block) ?: getRealmInstance().use(block)
        }

        /**
         * Whether an upload record already exists for this exact content revision (identified by
         * file name, target folder, drive, user and modification date). Distinct from
         * [hasPendingUpload]: the uri of an offline folder snapshot changes per revision, so the
         * identity check must not rely on the uri alone.
         */
        fun hasUploadRecordFor(
            fileName: String,
            remoteFolder: Int,
            driveId: Int,
            userId: Int,
            fileModifiedAt: Date,
            pendingOnly: Boolean,
            customRealm: Realm? = null,
        ): Boolean {
            val block: (Realm) -> Boolean = { realm ->
                realm.uploadTable
                    .equalTo(UploadFile::fileName.name, fileName)
                    .equalTo(UploadFile::remoteFolder.name, remoteFolder)
                    .equalTo(UploadFile::driveId.name, driveId)
                    .equalTo(UploadFile::userId.name, userId)
                    .equalTo(UploadFile::fileModifiedAt.name, fileModifiedAt)
                    .isNull(UploadFile::deletedAt.name)
                    .apply { if (pendingOnly) isNull(UploadFile::uploadAt.name) }
                    .count() > 0
            }
            return customRealm?.let(block) ?: getRealmInstance().use(block)
        }

        /**
         * Allocates a unique, durable snapshot file for one upload revision of an offline copy.
         * Unlike the shared upload cache (hash-named, evictable), each snapshot is a distinct
         * file under filesDir, so a later revision can never overwrite a still-pending one.
         */
        fun createDurableOfflineSnapshot(
            context: Context,
            source: IOFile,
            fileModifiedAt: Date,
            fileId: Int,
            driveId: Int,
        ): Uri? {
            return runCatching {
                val folder = IOFile(context.filesDir, OFFLINE_SNAPSHOTS_FOLDER).apply { if (!exists()) mkdirs() }
                val snapshot = IOFile(folder, "offline_${fileId}_${driveId}_${UUID.randomUUID()}")
                source.copyTo(snapshot, overwrite = true)
                snapshot.setLastModified(fileModifiedAt.time)
                snapshot.toUri()
            }.getOrNull()
        }

        /**
         * Whether [uri] is a file whose CANONICAL parent is exactly the app's durable offline
         * snapshots directory. Ownership is never inferred from a substring: an ordinary offline
         * file inside a user-created folder named like the snapshot directory must not match
         * (issue: round 3 #5).
         */
        fun isDurableOfflineSnapshot(uri: String): Boolean {
            val context = Realm.getApplicationContext() ?: return false
            return runCatching {
                val snapshotsDir = IOFile(context.filesDir, OFFLINE_SNAPSHOTS_FOLDER).canonicalFile
                val path = uri.toUri().path ?: return@runCatching false
                IOFile(path).canonicalFile.parentFile == snapshotsDir
            }.getOrDefault(false)
        }

        /**
         * Disposes a verified owned snapshot together with its upload record; ordinary offline
         * sources are never touched here (issue: round 3 #14).
         */
        private fun deleteOwnedSnapshotFile(uploadFile: UploadFile) {
            val uri = uploadFile.getUriObject()
            if (isDurableOfflineSnapshot(uri.toString())) {
                runCatching { uri.toFile().apply { if (exists()) delete() } }
            }
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
                    if (uploadFileRealm.isSyncOffline()) {
                        // Owned durable snapshots go with their record; ordinary offline sources
                        // keep their bytes, which back offline availability (issue: round 3 #14).
                        deleteOwnedSnapshotFile(uploadFileRealm)
                    } else if (uri.scheme.equals(ContentResolver.SCHEME_FILE)) {
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
                            if (!uploadFile.isSyncOffline()) {
                                uploadFile.getUriObject().toFile().apply { if (exists()) delete() }
                            } else {
                                // Owned durable snapshots are disposed with their record
                                // (issue: round 3 #14).
                                deleteOwnedSnapshotFile(uploadFile)
                            }
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
            getRealmInstance().use { realm ->
                realm.executeTransaction {
                    it.where(SyncSettings::class.java).findFirst()?.deleteFromRealm()
                }
            }
        }

        fun setAppSyncSettings(syncSettings: SyncSettings, customRealm: Realm? = null) {
            val block: (Realm) -> Unit = { realm ->
                realm.executeTransaction { it.insertOrUpdate(syncSettings) }
            }
            customRealm?.let(block) ?: getRealmInstance().use(block)
        }

        fun getAppSyncSettings(): SyncSettings? {
            return getRealmInstance().use { realm ->
                val realmObject = realm.where(SyncSettings::class.java).findFirst()
                realmObject?.let {
                    realm.copyFromRealm(it, 0)
                }
            }
        }
    }
}
