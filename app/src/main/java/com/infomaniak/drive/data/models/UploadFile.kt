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
import com.infomaniak.drive.data.cache.OfflineFolderController
import com.infomaniak.drive.data.sync.UploadMigration
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.DownloadOfflineFileManager
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
    // Stable identity of the ORIGINAL file row this snapshot was created from (issue: round
    // 17 #4): reconciliation must not depend on a name+parent first-match, which a
    // concurrent rename/move or a same-name sibling resolves to the wrong row. 0 = legacy
    // record registered before this field existed (the historical lookup is its fallback).
    var originalFileId: Int = 0,
    // Durable completion marker for the provenance reconciliation (issue: round 17 #4): the
    // snapshot is disposed only AFTER this is recorded, so a crash between the upload
    // completion and the baseline advance leaves the snapshot owned and retryable.
    var reconciliationDone: Boolean = false,
    // EXACT content identity of the snapshot's bytes (issue: round 18 #7): name, destination,
    // scope and modification date do not distinguish two same-mtime revisions — a cancelled
    // snapshot of revision A must never be treated as ownership of a later revision B that
    // merely retained A's mtime (and even size). The SHA-256 of the snapshot bytes is
    // recorded at registration and re-verified in dedup, cancellation retention, protection
    // and reconciliation; legacy records without it fall back to their retained snapshot
    // bytes' hash, and to by-identity matching only when no content proof exists.
    var contentHash: String? = null,
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
        // DURABLE completion state (issue: round 18 #8): a COMPLETED offline-upload record
        // (uploadAt set) is never removed while its provenance reconciliation is still
        // deferred — the retry query's only handle on it — nor while its owned snapshot
        // still exists (deleting the record first orphans the snapshot as unowned residue).
        // The worker's cleanup honors that state by LEAVING the record intact; the retry
        // pass concludes the reconciliation and disposes the snapshot idempotently.
        val completedOfflinePendingConclusion = uploadFileProxy.isSyncOffline() &&
            uploadFileProxy.uploadAt != null &&
            (!uploadFileProxy.reconciliationDone || durableSnapshotFile(uploadFileProxy.uri)?.exists() == true)
        if (completedOfflinePendingConclusion) {
            // Skip the cancellation publish? No — the durable local stop above already ran;
            // only the RECORD removal is deferred. A completed record blocks nothing.
            return
        }
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

        /**
         * SHA-256 content identity of [file]'s bytes, or null when it cannot be read (issue:
         * round 18 #7): modification date and size do not distinguish two same-second,
         * same-size revisions — the exact content identity does.
         */
        fun sha256OrNull(file: IOFile): String? = runCatching {
            val digest = java.security.MessageDigest.getInstance("SHA-256")
            file.inputStream().use { input ->
                val buffer = ByteArray(64 * 1024)
                while (true) {
                    val read = input.read(buffer)
                    if (read <= 0) break
                    digest.update(buffer, 0, read)
                }
            }
            digest.digest().joinToString("") { "%02x".format(it) }
        }.getOrNull()

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
                    val isSyncOffline = uploadFile.type == Type.SYNC_OFFLINE.name
                    realm.executeTransaction {
                        uploadFile.uploadToken = null
                        uploadFile.uploadHost = null
                        uploadFile.uploadAt = Date()
                    }
                    if (isSyncOffline) {
                        // Durable retryable reconciliation BEFORE disposal (issue: round 17
                        // #4): the snapshot is the last owner of the uploaded revision until
                        // the baseline advance is durably concluded. A deferred verdict
                        // (transient ownership ambiguity) keeps record + snapshot intact for
                        // [retryUnreconciledOriginals]; a concluded one is recorded in its
                        // own transaction, and only then is the snapshot disposed.
                        if (reconcileOriginalCopyCleanBaseline(uploadFile)) {
                            realm.executeTransaction { uploadFile.reconciliationDone = true }
                            if (isOwnedSnapshot) {
                                runCatching { IOFile(uri.path ?: "").takeIf { it.exists() }?.delete() }
                            }
                        }
                    } else if (isOwnedSnapshot) {
                        // The snapshot was the only copy of this revision: its upload completed,
                        // so the owned file can go (issue #1902).
                        runCatching { IOFile(uri.path ?: "").takeIf { it.exists() }?.delete() }
                    }
                }
            }
        }

        /**
         * Advances the ORIGINAL copy's clean provenance to the revision its completed upload
         * carried (issue: round 16 #14), under the intent/file coordination. The advance
         * happens ONLY when the live row still holds the snapshot's STABLE id AND the bytes at
         * its current path still ARE the uploaded revision — same modification date, recorded
         * size (exactly, zero included, issue: round 18 #7) and EXACT CONTENT (the snapshot's
         * recorded SHA-256) — a newer intervening edit is never relabeled as clean, and a
         * renamed/moved row keeps its own provenance story.
         *
         * Legacy records (originalFileId == 0) NEVER advance a baseline (issue: round 18 #7):
         * their name+parent lookup is an arbitrary row selection — a record without
         * trustworthy ownership proof must not relabel another row's bytes as published. They
         * conclude durably (the upload completed; the snapshot's retained revision is on the
         * server and is disposed), leaving the original's provenance to the ordinary
         * verification/rewrite paths.
         *
         * Returns TRUE when reconciliation CONCLUDED durably (baseline advanced, already
         * current, or a terminal no-advance such as a vanished row, a superseded revision or
         * the legacy rule above): the caller may record completion and dispose the snapshot.
         * Returns FALSE only when the verdict is DEFERRED — a transiently ambiguous or foreign
         * source ownership, or a content identity that could not be read: record and snapshot
         * stay intact for [retryUnreconciledOriginals] (issue: round 17 #4).
         */
        private fun reconcileOriginalCopyCleanBaseline(record: UploadFile): Boolean {
            val context = Realm.getApplicationContext() ?: return false
            // LEGACY rule (issue: round 18 #7): no trustworthy ownership/content proof —
            // conclude without advancing ANY row's baseline, and without selecting an
            // arbitrary name+parent match.
            if (record.originalFileId <= 0) return true
            val userDrive = UserDrive(userId = record.userId, driveId = record.driveId)
            val candidateId = record.originalFileId
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                var deferred = false
                OfflineFileCoordination.withFileLock(userDrive, candidateId) {
                    FileController.getRealmInstance(userDrive).use { realm ->
                        runCatching { realm.refresh() }
                        val liveRow = realm.where(File::class.java)
                            .equalTo(File::id.name, candidateId)
                            .findFirst() ?: return@withFileLock
                        val original = liveRow.getOfflineFile(context, userDrive.userId)
                            ?: return@withFileLock
                        if (!original.exists()) return@withFileLock
                        // EXCLUSIVE source ownership before any provenance effect (issues:
                        // round 17 #2/#4): an ambiguous or foreign path may be another file's
                        // copy at a transiently contested location — defer, never advance.
                        if (OfflineFolderController.exclusivePathOwner(realm, context, userDrive, original.path)
                            != liveRow.id
                        ) {
                            deferred = true
                            return@withFileLock
                        }
                        // Only the EXACT uploaded revision is advanced: same modification date,
                        // recorded size (zero bytes compared exactly — issue: round 18 #7) and
                        // EXACT CONTENT (issue: round 18 #7) — a same-second same-size edit is
                        // not the uploaded revision. A content identity that cannot be read
                        // defers to the retry pass instead of guessing.
                        if (original.lastModified() != record.fileModifiedAt.time ||
                            original.length() != record.fileSize
                        ) {
                            return@withFileLock
                        }
                        // EXACT CONTENT of the UPLOADED revision (issues: round 18 #7, round
                        // 19 #3): the stored hash when present; otherwise — a v9-migrated
                        // record with a positive originalFileId but no hash — the hash is
                        // derived from the OWNED IMMUTABLE SNAPSHOT (still on disk: this
                        // reconciliation runs BEFORE disposal). Without that proof the
                        // baseline is NEVER advanced: a same-size, same-mtime original B
                        // would otherwise be relabeled clean while the snapshot actually
                        // uploaded A.
                        val uploadedHash = record.contentHash
                            ?: record.uri.takeIf { it.isNotEmpty() }?.let { snapshotUri ->
                                durableSnapshotFile(snapshotUri)?.let { snapshotFile -> sha256OrNull(snapshotFile) }
                            }
                        if (uploadedHash == null) {
                            deferred = true
                            return@withFileLock
                        }
                        val actualHash = UploadFile.sha256OrNull(original)
                        if (actualHash == null) {
                            deferred = true
                            return@withFileLock
                        }
                        if (actualHash != uploadedHash) return@withFileLock
                        val uploadedBaselineSeconds = record.fileModifiedAt.time / 1_000L
                        // PROOF REPAIR (issue: round 20 #13): the baseline-already-advanced
                        // early return previously prevented subsequent proof repair — a pass
                        // that advanced the baseline but failed its provenance write could
                        // never be repaired, yet the record still concluded and the snapshot
                        // was disposed. The early return applies only when the proof is
                        // COMPLETE: a record exists whose IMMUTABLE byte identity certifies
                        // these bytes.
                        val provenanceContext = Realm.getApplicationContext()
                        val existingProof = provenanceContext?.let {
                            DownloadOfflineFileManager.readPublishedProvenance(it, original)
                        }
                        if (existingProof?.certifiesBytes(original) == true &&
                            liveRow.effectivePublishedBaseline >= uploadedBaselineSeconds
                        ) {
                            // COMPLETE SUCCESS REQUIRES THE OBSERVATION STATE TOO (review-34
                            // #7): a previous pass that wrote the proof but FAILED its
                            // observation retirement left a stale observation bound to the
                            // replaced publication — the early return must not bypass the
                            // idempotent repair. The stale observation is retired HERE (on
                            // repair retries, even when the baseline and byte proof already
                            // match); a failed deletion defers with the upload ownership
                            // retained.
                            val staleResolved = provenanceContext != null &&
                                DownloadOfflineFileManager.staleObservationResolved(
                                    provenanceContext,
                                    original,
                                    existingProof.publicationId,
                                )
                            if (staleResolved) {
                                return@withFileLock
                            }
                            deferred = true
                            return@withFileLock
                        }
                        // VERIFIED-UPLOADED-BUT-NOT-CURRENT IDENTITY (issue: round 20 #13):
                        // the record binds the BYTE IDENTITY and the VERIFIED byte baseline —
                        // the row's CURRENT version/revision fields are NOT the uploaded
                        // revision's server result (newer remote B metadata can commit while
                        // uploaded A remains local; a tied size/mtime would label A as B).
                        // The record is written WITHOUT generation claims (a detached
                        // identity row: version 0, no revisedAt/updatedAt), so it can never
                        // prove currentness by itself; the authoritative version-bearing
                        // request resolves it.
                        val provenanceWritten = provenanceContext != null && DownloadOfflineFileManager
                            .writeVerifiedRevisionProvenance(
                                context = provenanceContext,
                                target = original,
                                row = File(id = liveRow.id, driveId = liveRow.driveId).apply {
                                    parentId = liveRow.parentId
                                    name = liveRow.name
                                },
                                userDrive = userDrive,
                                verifiedLastModifiedAt = uploadedBaselineSeconds,
                                // The ALREADY-VERIFIED immutable uploaded hash (issue: round
                                // 21 #13): the writer records and revalidates against THIS
                                // identity — never a fresh sample of the mutable original,
                                // which an external editor can change between the caller's
                                // verification and the proof write.
                                verifiedContentHash = uploadedHash,
                            )
                        // DURABLE PROOF BEFORE CONCLUSION (issue: round 20 #13): a failed
                        // provenance write DEFERS — the baseline does not advance, the
                        // record does not conclude, and the snapshot stays owned for the
                        // retry (the writer's false result and exceptions are no longer
                        // ignored).
                        if (!provenanceWritten) {
                            deferred = true
                            return@withFileLock
                        }
                        // FULL PROVENANCE AND BASELINE ADVANCE TOGETHER (issue: round 19
                        // #13): the hash proof above established these bytes as the uploaded
                        // revision — the copy's provenance record is written from that
                        // VERIFIED byte baseline in the same coordinated section as the
                        // baseline advance.
                        realm.executeTransaction { liveRow.publishedCleanBaseline = uploadedBaselineSeconds }
                    }
                }
                return !deferred
            }
        }

        /**
         * Durable retry pass (issues: round 17 #4, round 18 #8): every completed offline
         * upload record whose lifecycle has not durably concluded keeps its snapshot and is
         * retried here each sync pass. A record with `reconciliationDone == false` re-runs the
         * provenance reconciliation; a CONCLUDED record whose snapshot still exists — the
         * crash window between the durable completion marker and the disposal — gets its
         * snapshot disposed IDEMPOTENTLY. The record itself is retained in every case (it
         * remains the revision's dedup and protection evidence until the ordinary sweeps
         * remove it), so a deferred record is never orphaned by an eager cleanup.
         */
        fun retryUnreconciledOriginals() {
            // CONFIRMED-COMPLETED records are processed INDEPENDENTLY of cancellation
            // status (issue: round 19 #6): cancellation and bulk deletion can soft-delete an
            // already completed offline upload — including a transfer that finished after
            // the cancellation set was captured — and the previous `deletedAt == null`
            // filter excluded that record from reconciliation and snapshot disposal
            // forever. Cancelled UNSENT snapshots (uploadAt == null) remain excluded:
            // their retention IS the cancellation decision and they are never re-enqueued.
            val completedRecords = getRealmInstance().use { realm ->
                realm.where(UploadFile::class.java)
                    .equalTo(UploadFile::type.name, Type.SYNC_OFFLINE.name)
                    .isNotNull(UploadFile::uploadAt.name)
                    .findAll()?.map { realm.copyFromRealm(it, 0) }
            } ?: return
            completedRecords.forEach { record ->
                var concluded = record.reconciliationDone
                if (!concluded && reconcileOriginalCopyCleanBaseline(record)) {
                    getRealmInstance().use { realm ->
                        val managed = uploadFileByUriQuery(realm, record.uri).findFirst() ?: return@use
                        if (!managed.reconciliationDone) {
                            realm.executeTransaction { managed.reconciliationDone = true }
                        }
                    }
                    concluded = true
                }
                if (!concluded) return@forEach
                // Idempotent disposal (issue: round 18 #8): a crash after the durable
                // completion marker but before the snapshot delete leaves the bytes unowned —
                // the retry pass re-runs the exact same ownership-verified disposal.
                durableSnapshotFile(record.uri)?.let { snapshot ->
                    runCatching { snapshot.takeIf { it.exists() }?.delete() }
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
         * Whether a candidate record provably holds [contentHash]'s bytes (issues: round 18
         * #7, round 19 #5): the record's stored identity when present, else its RETAINED
         * SNAPSHOT bytes' hash read through the CANONICAL owned-snapshot decoder (a stored
         * `file:///…` uri string is not a filesystem path — hashing `IOFile(uriString)`
         * silently misses the file and a retained cancelled legacy snapshot then failed
         * dedup, re-registering and re-uploading the cancelled revision). A record whose
         * content cannot be proven is never treated as ownership of [contentHash]'s
         * revision.
         */
        private fun recordProvesContent(record: UploadFile, contentHash: String): Boolean {
            val recorded = record.contentHash
                ?: record.uri.takeIf { it.isNotEmpty() }?.let { snapshotUri ->
                    durableSnapshotFile(snapshotUri)?.let { snapshotFile -> sha256OrNull(snapshotFile) }
                }
            return recorded == contentHash
        }

        /**
         * READ-ONLY ownership check for the ORIGINAL revision at a destination (issue: round
         * 15 #7): durable offline revisions are registered under unique SNAPSHOT uris, so the
         * destination-uri queries cannot see their ownership of the original revision.
         * Matches the destination identity, modification date and RECORDED size, covering
         * pending AND retained-unsent records, and creates no snapshot — a record for this
         * exact revision means the destination bytes are durably owned local content. The
         * caller's successful content hash is REQUIRED (issues: round 18 #7, round 21 #12):
         * a candidate must PROVE those bytes — a same-mtime, same-size record of different
         * content is not ownership, and a FAILED hash/read is unresolved, never ownership
         * (metadata alone no longer grants the durable-ownership answer; callers hash the
         * copy first and skip the query when hashing failed).
         */
        fun hasDurableOriginalRevisionOwnership(
            fileName: String,
            remoteFolder: Int,
            driveId: Int,
            userId: Int,
            fileModifiedAt: Date,
            fileSize: Long,
            contentHash: String,
            originalFileId: Int? = null,
            customRealm: Realm? = null,
        ): Boolean {
            val block: (Realm) -> Boolean = { realm ->
                val candidates = realm.uploadTable
                    .equalTo(UploadFile::fileName.name, fileName)
                    .equalTo(UploadFile::remoteFolder.name, remoteFolder)
                    .equalTo(UploadFile::driveId.name, driveId)
                    .equalTo(UploadFile::userId.name, userId)
                    .equalTo(UploadFile::fileModifiedAt.name, fileModifiedAt)
                    .equalTo(UploadFile::type.name, Type.SYNC_OFFLINE.name)
                    .equalTo(UploadFile::fileSize.name, fileSize)
                    .isNull(UploadFile::uploadAt.name)
                    .apply { if (originalFileId != null && originalFileId > 0) equalTo(UploadFile::originalFileId.name, originalFileId) }
                    .findAll()
                candidates.any { recordProvesContent(it, contentHash) }
            }
            return customRealm?.let(block) ?: getRealmInstance().use(block)
        }

        /**
         * READ-ONLY COMPLETED-UPLOAD ownership check for the ORIGINAL revision (review-38 #3):
         * [hasDurableOriginalRevisionOwnership]'s `uploadAt == null` requirement matches
         * PENDING/retained-unsent snapshots — local snapshot ownership, never an upload
         * completion. This query requires an ACTIVE COMPLETED record: [uploadAt] is set by
         * [uploadFinished] alone (the transfer's actual completion) and a cancelled record
         * (deletedAt != null) is never completion. The caller's successful content hash is
         * REQUIRED and the candidate must PROVE those bytes — the exact owner, bytes and
         * revision (identity fields plus the stable original id when known) are bound
         * together, so a completed transfer of different content grants nothing. When the
         * row is later discarded the evidence is gone and the caller must retain — no
         * predicate substitutes for the missing durable completion.
         */
        fun hasCompletedOriginalRevisionOwnership(
            fileName: String,
            remoteFolder: Int,
            driveId: Int,
            userId: Int,
            fileModifiedAt: Date,
            fileSize: Long,
            contentHash: String,
            originalFileId: Int? = null,
            customRealm: Realm? = null,
        ): Boolean {
            val block: (Realm) -> Boolean = { realm ->
                val candidates = realm.uploadTable
                    .equalTo(UploadFile::fileName.name, fileName)
                    .equalTo(UploadFile::remoteFolder.name, remoteFolder)
                    .equalTo(UploadFile::driveId.name, driveId)
                    .equalTo(UploadFile::userId.name, userId)
                    .equalTo(UploadFile::fileModifiedAt.name, fileModifiedAt)
                    .equalTo(UploadFile::type.name, Type.SYNC_OFFLINE.name)
                    .isNotNull(UploadFile::uploadAt.name)
                    .isNull(UploadFile::deletedAt.name)
                    .apply { if (originalFileId != null && originalFileId > 0) equalTo(UploadFile::originalFileId.name, originalFileId) }
                    .findAll()
                candidates.any { recordProvesContent(it, contentHash) }
            }
            return customRealm?.let(block) ?: getRealmInstance().use(block)
        }

        /**
         * Whether an upload record already exists for this exact content revision (identified by
         * file name, target folder, drive, user and modification date). Distinct from
         * [hasPendingUpload]: the uri of an offline folder snapshot changes per revision, so the
         * identity check must not rely on the uri alone. When [contentHash] is provided, a
         * candidate must also PROVE those bytes (issue: round 18 #7).
         */
        fun hasUploadRecordFor(
            fileName: String,
            remoteFolder: Int,
            driveId: Int,
            userId: Int,
            fileModifiedAt: Date,
            pendingOnly: Boolean,
            contentHash: String? = null,
            originalFileId: Int? = null,
            customRealm: Realm? = null,
        ): Boolean {
            val block: (Realm) -> Boolean = { realm ->
                val candidates = realm.uploadTable
                    .equalTo(UploadFile::fileName.name, fileName)
                    .equalTo(UploadFile::remoteFolder.name, remoteFolder)
                    .equalTo(UploadFile::driveId.name, driveId)
                    .equalTo(UploadFile::userId.name, userId)
                    .equalTo(UploadFile::fileModifiedAt.name, fileModifiedAt)
                    // OFFLINE revisions only (issue: round 19 #5): the general query
                    // previously accepted any upload type occupying the same identity.
                    .equalTo(UploadFile::type.name, Type.SYNC_OFFLINE.name)
                    .isNull(UploadFile::deletedAt.name)
                    .apply { if (pendingOnly) isNull(UploadFile::uploadAt.name) }
                    // STABLE ORIGINAL identity (issue: round 19 #5): a record for an EARLIER
                    // file occupying the same name/parent must never certify a replacement
                    // file's bytes. When the caller knows the row id, an ID-LESS legacy
                    // record (originalFileId == 0) is excluded too — it carries no
                    // trustworthy ownership and never grants another file its authority.
                    .apply { if (originalFileId != null && originalFileId > 0) equalTo(UploadFile::originalFileId.name, originalFileId) }
                    .findAll()
                if (candidates.isEmpty()) {
                    false
                } else if (contentHash == null) {
                    true
                } else {
                    candidates.any { recordProvesContent(it, contentHash) }
                }
            }
            return customRealm?.let(block) ?: getRealmInstance().use(block)
        }

        /**
         * Whether a CANCELLED (soft-deleted) offline upload record still exists for this exact
         * content revision (owner, destination identity and modification date — issue: round 8
         * #7). Such a record retains its snapshot bytes AND the user's cancellation decision:
         * re-registering the same revision would silently reactivate the cancelled upload, so
         * registration callers treat the revision as already-owned content instead. When
         * [contentHash] is provided, a candidate must PROVE those bytes (issue: round 18 #7):
         * a cancelled snapshot of revision A is NOT ownership of a later revision B that
         * merely retained A's modification date — B registers normally, and A stays cancelled.
         */
        fun hasCancelledUploadRecordFor(
            fileName: String,
            remoteFolder: Int,
            driveId: Int,
            userId: Int,
            fileModifiedAt: Date,
            contentHash: String? = null,
            originalFileId: Int? = null,
            customRealm: Realm? = null,
        ): Boolean {
            val block: (Realm) -> Boolean = { realm ->
                val candidates = realm.uploadTable
                    .equalTo(UploadFile::fileName.name, fileName)
                    .equalTo(UploadFile::remoteFolder.name, remoteFolder)
                    .equalTo(UploadFile::driveId.name, driveId)
                    .equalTo(UploadFile::userId.name, userId)
                    .equalTo(UploadFile::fileModifiedAt.name, fileModifiedAt)
                    .equalTo(UploadFile::type.name, Type.SYNC_OFFLINE.name)
                    .isNotNull(UploadFile::deletedAt.name)
                    // STABLE ORIGINAL identity (issue: round 19 #5): an ID-less legacy
                    // record never certifies another file's revision as cancelled.
                    .apply { if (originalFileId != null && originalFileId > 0) equalTo(UploadFile::originalFileId.name, originalFileId) }
                    .findAll()
                if (candidates.isEmpty()) {
                    false
                } else if (contentHash == null) {
                    true
                } else {
                    candidates.any { recordProvesContent(it, contentHash) }
                }
            }
            return customRealm?.let(block) ?: getRealmInstance().use(block)
        }

        /**
         * Whether a RETAINED CANCELLED SYNC_OFFLINE record WITHOUT a stable original identity
         * (originalFileId <= 0, pre-migration) matches [fileName]/[remoteFolder]/scope and
         * provably holds [contentHash]'s bytes (issue: round 20 #5): the ID-less
         * cancellation protection. Such a record IS the cancellation decision for that
         * revision, but its ID-less ownership must never CERTIFY ownership for a
         * replacement file — the match BLOCKS automatic re-registration pending identity
         * reconciliation only, granting ownership nowhere.
         */
        fun hasLegacyCancelledUploadRecordFor(
            fileName: String,
            remoteFolder: Int,
            driveId: Int,
            userId: Int,
            fileModifiedAt: Date,
            contentHash: String?,
            customRealm: Realm? = null,
        ): Boolean {
            val block: (Realm) -> Boolean = { realm ->
                val candidates = realm.uploadTable
                    .equalTo(UploadFile::fileName.name, fileName)
                    .equalTo(UploadFile::remoteFolder.name, remoteFolder)
                    .equalTo(UploadFile::driveId.name, driveId)
                    .equalTo(UploadFile::userId.name, userId)
                    .equalTo(UploadFile::fileModifiedAt.name, fileModifiedAt)
                    .equalTo(UploadFile::type.name, Type.SYNC_OFFLINE.name)
                    .isNotNull(UploadFile::deletedAt.name)
                    .lessThanOrEqualTo(UploadFile::originalFileId.name, 0)
                    .findAll()
                if (candidates.isEmpty()) {
                    false
                } else if (contentHash == null) {
                    // Without a content claim, an exact scope/destination/mtime match is
                    // enough to refuse automatic re-registration (conservative; no
                    // ownership granted to any row).
                    true
                } else {
                    candidates.any { recordProvesContent(it, contentHash) }
                }
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
         * The verified durable snapshot FILE of [uri], or null when the uri is not an owned
         * snapshot (issue: round 18 #8): shared by the completion disposal and the retry
         * pass's idempotent disposal so both test the exact same ownership rule.
         */
        private fun durableSnapshotFile(uri: String): IOFile? {
            if (!isDurableOfflineSnapshot(uri)) return null
            val path = uri.toUri().path ?: return null
            return IOFile(path)
        }

        /** The verified owned snapshot file of a freshly created snapshot [uri] (issue: round 19 #4). */
        fun durableSnapshotFileOf(uri: Uri): IOFile? = durableSnapshotFile(uri.toString())

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
