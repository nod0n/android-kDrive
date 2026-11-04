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
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.UploadTask.Companion.ConflictOption
import com.infomaniak.drive.data.cache.DriveInfosController
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.sync.UploadMigration
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.IOFile
import com.infomaniak.drive.utils.OfflineFileCoordination
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
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import java.util.Date
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

/**
 * Process-wide cancellation signal for an EXECUTING upload, keyed by the record's EXACT uri
 * (issue: round 9 #9): a record stopped in the UI — token-less, so only its row was
 * soft-deleted — publishes here, and the executing transfer observes the signal and aborts
 * its HTTP call, including direct uploads and session creation. Retained snapshot bytes and
 * ownership are untouched: the cancellation paths already soft-delete the record, and the
 * aborted transfer never reaches a completion marking. The signal is cleared when the same
 * uri is registered again ([UploadFile.store]), because that is a new user intent.
 */
object UploadCancellationSignal {
    private val cancelledUris: MutableSet<String> = java.util.concurrent.ConcurrentHashMap.newKeySet()
    private val activeJobs: java.util.concurrent.ConcurrentHashMap<String, kotlinx.coroutines.Job> =
        java.util.concurrent.ConcurrentHashMap()

    fun cancel(uri: String) {
        cancelledUris.add(uri)
        // IMMEDIATE executing-task cancellation (issue: round 10 #10): the polling watcher
        // alone is bounded by its interval, and a blocking request could complete before it
        // even starts observing — a registered executing job is cancelled directly.
        activeJobs.remove(uri)?.cancel()
    }

    fun isCancelled(uri: String): Boolean = cancelledUris.contains(uri)

    fun clear(uri: String) {
        cancelledUris.remove(uri)
        activeJobs.remove(uri)
    }

    fun registerJob(uri: String, job: kotlinx.coroutines.Job) {
        activeJobs[uri] = job
        if (cancelledUris.contains(uri)) cancel(uri)
    }

    fun unregisterJob(uri: String) {
        activeJobs.remove(uri)
    }

    /** Thrown into the racing upload by [awaitCancellation]; NOT a coroutine cancellation. */
    class UploadStoppedByUserException(uri: String) :
        IllegalStateException("Upload stopped by user for $uri")

    /**
     * Racer for the executing transfer: suspends until this exact uri's signal fires, then
     * aborts the racing upload — the still-running HTTP coroutine is cancelled and ktor
     * aborts the underlying request.
     */
    suspend fun awaitCancellation(uri: String): Nothing = coroutineScope {
        while (!isCancelled(uri)) {
            ensureActive()
            delay(CHECK_INTERVAL_MILLIS)
        }
        throw UploadStoppedByUserException(uri)
    }

    private const val CHECK_INTERVAL_MILLIS = 200L
}

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
            // A (re-)registration is a NEW user intent for this exact uri: any stale stop
            // signal from a previously cancelled record must not abort the new transfer
            // (issue: round 9 #9).
            UploadCancellationSignal.clear(uri)
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
        val capturedToken = uploadFileProxy.uploadToken
        val capturedDriveId = uploadFileProxy.driveId
        val capturedClient = uploadFileProxy.okHttpClient
        // The record's removal publishes the exact-URI stop signal (issue: round 9 #9), and
        // the DURABLE local stop commits BEFORE any network call (issue: round 10 #11): a
        // remote-cancellation failure can no longer leave a pending record that uploads
        // again after a process restart.
        UploadCancellationSignal.cancel(uri)
        val unsentOfflineEdit = uploadFileProxy.isSyncOffline() && uploadFileProxy.uploadAt == null
        realm.executeTransaction {
            if (uploadFileProxy.isValid) {
                if (keepFile || unsentOfflineEdit) uploadFileProxy.deletedAt = Date() else uploadFileProxy.deleteFromRealm()
            }
        }
        // Best-effort remote cancellation with the CAPTURED session identity, AFTER the
        // durable stop: the local cancellation stands regardless of the network outcome.
        capturedToken?.let { token ->
            runCatching {
                ApiRepository.cancelSession(capturedDriveId, token, capturedClient)
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
                    // Cancellation propagation by the exact identity (issue: round 8 #11): a
                    // record cancelled while its transfer was executing — its soft delete
                    // landed after the session was created — is NOT un-cancelled by the late
                    // completion; it stays the retained owner of its unsent snapshot bytes.
                    if (uploadFile.deletedAt != null) return@let
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
                    if (uploadFile.type == Type.SYNC_OFFLINE.name) {
                        // The completed snapshot owns its ORIGINAL revision's provenance
                        // (issue: round 16 #14): a successfully uploaded exact snapshot must
                        // advance the original copy's clean baseline, or later walks keep
                        // classifying the uploaded bytes as an unsent edit and suppress
                        // downloading newer remote content.
                        reconcileOriginalCopyCleanBaseline(uploadFile)
                    }
                }
            }
        }

        /**
         * Advances the ORIGINAL copy's clean provenance to the revision its completed upload
         * carried (issue: round 16 #14), under the intent/file coordination. The advance
         * happens ONLY when the live row still holds the snapshot's identity AND the bytes at
         * its current path still ARE the uploaded revision (same modification date) — a newer
         * intervening edit is never relabeled as clean, and a renamed/moved row keeps its own
         * provenance story.
         */
        private fun reconcileOriginalCopyCleanBaseline(record: UploadFile) {
            val context = Realm.getApplicationContext() ?: return
            val userDrive = UserDrive(userId = record.userId, driveId = record.driveId)
            // The lock key is derived from an uncoordinated identity lookup; the DECISION is
            // made on the fresh row re-read inside the coordination.
            val candidateId = FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(File::class.java)
                    .equalTo(File::name.name, record.fileName)
                    .equalTo(File::parentId.name, record.remoteFolder)
                    .findFirst()?.id
            } ?: return
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withFileLock(userDrive, candidateId) {
                    FileController.getRealmInstance(userDrive).use { realm ->
                        runCatching { realm.refresh() }
                        val liveRow = realm.where(File::class.java)
                            .equalTo(File::id.name, candidateId)
                            .findFirst() ?: return@withFileLock
                        // Identity must still match the completed snapshot.
                        if (liveRow.name != record.fileName || liveRow.parentId != record.remoteFolder) return@withFileLock
                        val original = liveRow.getOfflineFile(context, userDrive.userId) ?: return@withFileLock
                        // Only the EXACT uploaded revision is advanced: the bytes must still be
                        // the ones the snapshot was created from — never a newer edit.
                        if (!original.exists() || original.lastModified() != record.fileModifiedAt.time) {
                            return@withFileLock
                        }
                        val uploadedBaselineSeconds = record.fileModifiedAt.time / 1_000L
                        if ((liveRow.publishedCleanBaseline ?: 0L) >= uploadedBaselineSeconds) return@withFileLock
                        realm.executeTransaction { liveRow.publishedCleanBaseline = uploadedBaselineSeconds }
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
         * Whether [uri] is still owned by an UNSENT offline upload record, whether currently
         * pending OR retained after a failure/cancellation (soft-deleted with [deletedAt]).
         * Such a record means these bytes are the only copy of an unsent edit: that is content
         * ownership, independent of the upload's scheduling state (issue: round 6 #1) — a
         * soft-deleted unsent record must keep protecting its source exactly like a pending
         * one, even when the file row's clean baseline was advanced at registration.
         */
        fun hasRetainedUnsentUpload(uri: String, customRealm: Realm? = null): Boolean {
            val block: (Realm) -> Boolean = { realm ->
                realm.uploadTable
                    .equalTo(UploadFile::uri.name, uri)
                    .equalTo(UploadFile::type.name, Type.SYNC_OFFLINE.name)
                    .isNull(UploadFile::uploadAt.name)
                    .count() > 0
            }
            return customRealm?.let(block) ?: getRealmInstance().use(block)
        }

        /**
         * READ-ONLY ownership check for the ORIGINAL revision at a destination (issue: round
         * 15 #7): durable offline revisions are registered under unique SNAPSHOT uris, so the
         * destination-uri queries cannot see their ownership of the original revision.
         * Matches the destination identity, modification date and RECORDED size, covering
         * pending AND retained-unsent records, and creates no snapshot — a record for this
         * exact revision means the destination bytes are durably owned local content.
         */
        fun hasDurableOriginalRevisionOwnership(
            fileName: String,
            remoteFolder: Int,
            driveId: Int,
            userId: Int,
            fileModifiedAt: Date,
            fileSize: Long,
            customRealm: Realm? = null,
        ): Boolean {
            val block: (Realm) -> Boolean = { realm ->
                realm.uploadTable
                    .equalTo(UploadFile::fileName.name, fileName)
                    .equalTo(UploadFile::remoteFolder.name, remoteFolder)
                    .equalTo(UploadFile::driveId.name, driveId)
                    .equalTo(UploadFile::userId.name, userId)
                    .equalTo(UploadFile::fileModifiedAt.name, fileModifiedAt)
                    .equalTo(UploadFile::type.name, Type.SYNC_OFFLINE.name)
                    .equalTo(UploadFile::fileSize.name, fileSize)
                    .isNull(UploadFile::uploadAt.name)
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
         * Whether a CANCELLED (soft-deleted) offline upload record still exists for this exact
         * content revision (owner, destination identity and modification date — issue: round 8
         * #7). Such a record retains its snapshot bytes AND the user's cancellation decision:
         * re-registering the same revision would silently reactivate the cancelled upload, so
         * registration callers treat the revision as already-owned content instead.
         */
        fun hasCancelledUploadRecordFor(
            fileName: String,
            remoteFolder: Int,
            driveId: Int,
            userId: Int,
            fileModifiedAt: Date,
            customRealm: Realm? = null,
        ): Boolean {
            val block: (Realm) -> Boolean = { realm ->
                realm.uploadTable
                    .equalTo(UploadFile::fileName.name, fileName)
                    .equalTo(UploadFile::remoteFolder.name, remoteFolder)
                    .equalTo(UploadFile::driveId.name, driveId)
                    .equalTo(UploadFile::userId.name, userId)
                    .equalTo(UploadFile::fileModifiedAt.name, fileModifiedAt)
                    .equalTo(UploadFile::type.name, Type.SYNC_OFFLINE.name)
                    .isNotNull(UploadFile::deletedAt.name)
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
            // ANY removal of the record publishes the exact-URI stop signal (issue: round 9
            // #9): an executing transfer for this record aborts instead of completing an
            // upload the user (or a bulk close) just cancelled.
            UploadCancellationSignal.cancel(uploadFileUri)
            uploadFileByUriQuery(realm, uploadFileUri).findFirst()?.let { uploadFileRealm ->
                // Don't delete definitively if it's a sync
                if (uploadFileRealm.type == Type.SYNC.name) {
                    uploadFileRealm.deletedAt = Date()
                } else {
                    // Delete definitively
                    val uri = uploadFileRealm.getUriObject()
                    if (uploadFileRealm.isSyncOffline()) {
                        // Owned snapshots may hold the ONLY copy of an unsent offline edit:
                        // cancellation soft-deletes the record and keeps the file; disposal is
                        // reserved for confirmed successful uploads (issue: round 4 #3).
                        uploadFileRealm.deletedAt = Date()
                    } else {
                        if (uri.scheme.equals(ContentResolver.SCHEME_FILE)) {
                            uri.toFile().apply { if (exists()) delete() }
                        }
                        uploadFileRealm.deleteFromRealm()
                    }
                }
            }
        }

        /**
         * Cancels in-flight sessions for a folder, scoped by account and drive so one account's
         * cancellation cannot reach another account's snapshots in a shared folder (issue:
         * round 5 #14). The account/drive scope is REQUIRED: bulk session cancellation never
         * runs without an explicit owner, and there is no current-account fallback for a
         * missing one. Each record is cancelled against its OWN drive identity — the drive the
         * session was actually created on — and its own captured-account client.
         */
        /**
         * Cancels ONE record's in-flight session against its own drive identity and captured
         * client (issues: round 6 #9, round 8 #10): the exact captured set drives the per-record
         * cancellations, so no second, broader folder query can cancel a different set than
         * the one being removed.
         */
        fun cancelSessionOf(uploadFile: UploadFile) {
            uploadFile.uploadToken?.let { token ->
                ApiRepository.cancelSession(
                    driveId = uploadFile.driveId,
                    uploadToken = token,
                    okHttpClient = uploadFile.okHttpClient,
                )
            }
        }

        /**
         * Detached copies of the CURRENT pending records of one (owner, drive, folder) group,
         * captured ONCE for a cancellation pass (issue: round 8 #10): the same captured records
         * feed both the session cancellations and the removal, so a record registered between
         * the two operations can neither be removed without cancelling its session nor
         * cancelled by one query and removed by a different, broader one. Folder views
         * represent many records per displayed row — this returns ALL of the folder's pending
         * members, not just the representative.
         */
        fun getPendingRecordsFor(userId: Int, driveId: Int, remoteFolder: Int): List<UploadFile> {
            return getRealmInstance().use { realm ->
                realm.uploadTable
                    .equalTo(UploadFile::userId.name, userId)
                    .equalTo(UploadFile::driveId.name, driveId)
                    .equalTo(UploadFile::remoteFolder.name, remoteFolder)
                    .isNull(UploadFile::uploadAt.name)
                    .isNull(UploadFile::deletedAt.name)
                    .findAll()
                    ?.let { realm.copyFromRealm(it, 0) }
                    ?: emptyList()
            }
        }

        fun cancelAllPendingFilesSessions(folderId: Int, userId: Int, driveId: Int) {
            getRealmInstance().use { realm ->
                realm.uploadTable
                    .equalTo(UploadFile::remoteFolder.name, folderId)
                    .equalTo(UploadFile::userId.name, userId)
                    .equalTo(UploadFile::driveId.name, driveId)
                    .isNull(UploadFile::uploadAt.name)
                    .isNotNull(UploadFile::uploadToken.name)
                    .findAll()?.forEach { uploadFileProxy ->
                        // Null-safe even though the query filters isNotNull: no unsafe
                        // assertion on ownership-critical identity (round 5 compile fix).
                        uploadFileProxy.uploadToken?.let { token ->
                            ApiRepository.cancelSession(
                                driveId = uploadFileProxy.driveId,
                                uploadToken = token,
                                okHttpClient = uploadFileProxy.okHttpClient,
                            )
                        }
                    }
            }
        }

        /**
         * Bulk removal of upload records. Owned durable snapshots hold the ONLY copy of unsent
         * offline edits: they are never disposed here — not on errors, not on cancellation
         * (issue: round 4 #3). Disposal happens exclusively after a confirmed successful upload
         * ([uploadFinished]); soft-deleted records keep owning their snapshot files until then.
         * Destructive queries are scoped by user and drive in addition to the folder.
         */
        fun deleteAll(
            folderId: Int? = null,
            permanently: Boolean = false,
            userId: Int? = null,
            driveId: Int? = null,
        ) {
            getRealmInstance().use {
                it.executeTransaction { realm ->
                    fun RealmQuery<UploadFile>.scoped(): RealmQuery<UploadFile> = this.apply {
                        folderId?.let { equalTo(UploadFile::remoteFolder.name, folderId) }
                        userId?.let { equalTo(UploadFile::userId.name, it) }
                        driveId?.let { equalTo(UploadFile::driveId.name, it) }
                    }

                    // Delete data files of ordinary (non-offline) uploads with scheme FILE.
                    realm.uploadTable
                        .scoped()
                        .beginsWith(UploadFile::uri.name, ContentResolver.SCHEME_FILE)
                        .findAll().forEach { uploadFile ->
                            if (!uploadFile.isSyncOffline()) {
                                uploadFile.getUriObject().toFile().apply { if (exists()) delete() }
                            }
                            // Owned snapshots keep their files: they may be the only unsent edit.
                        }

                    if (permanently) {
                        realm.uploadTable
                            .scoped()
                            .isNull(UploadFile::uploadAt.name)
                            .findAll().forEach { uploadFile ->
                                if (uploadFile.isSyncOffline()) {
                                    // Keep the record as the durable owner of the retained
                                    // snapshot; only its pending state is cleared.
                                    uploadFile.deletedAt = Date()
                                } else {
                                    uploadFile.deleteFromRealm()
                                }
                            }
                    } else {
                        // Delete all uploads with type SYNC
                        realm.uploadTable
                            .scoped()
                            .equalTo(UploadFile::type.name, Type.SYNC.name)
                            .isNull(UploadFile::uploadAt.name)
                            .findAll().forEach { uploadFile -> uploadFile.deletedAt = Date() }

                        // Soft-delete all uploads without type SYNC, owned snapshots included:
                        // their records keep owning the retained files.
                        realm.uploadTable
                            .scoped()
                            .notEqualTo(UploadFile::type.name, Type.SYNC.name)
                            .findAll().forEach { uploadFile -> uploadFile.deletedAt = Date() }
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
