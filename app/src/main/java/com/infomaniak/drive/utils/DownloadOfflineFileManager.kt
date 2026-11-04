/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2024-2025 Infomaniak Network SA
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
package com.infomaniak.drive.utils

import android.content.Context
import android.net.Uri
import android.os.ParcelFileDescriptor
import androidx.concurrent.futures.await
import androidx.core.app.NotificationCompat
import androidx.core.app.NotificationManagerCompat
import androidx.work.ListenableWorker
import androidx.work.WorkInfo
import androidx.work.WorkManager
import androidx.work.WorkQuery
import com.google.gson.reflect.TypeToken
import com.infomaniak.core.auth.networking.AuthHttpClientProvider
import com.infomaniak.core.common.cancellable
import com.infomaniak.core.network.api.ApiController
import com.infomaniak.core.network.models.ApiResponse
import com.infomaniak.core.network.models.exceptions.NetworkException
import com.infomaniak.core.network.networking.HttpUtils
import com.infomaniak.core.network.networking.ManualAuthorizationRequired
import com.infomaniak.core.network.utils.ApiErrorCode.Companion.translateError
import com.infomaniak.core.network.utils.await
import com.infomaniak.core.notifications.notifyCompat
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.R
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.ApiRoutes
import com.infomaniak.drive.data.api.ProgressResponseBody
import com.infomaniak.drive.data.api.UploadTask
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.cache.OfflineFolderController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.services.BaseDownloadWorker
import com.infomaniak.drive.data.services.BulkDownloadWorker
import com.infomaniak.drive.data.services.DownloadWorker
import com.infomaniak.drive.utils.MediaUtils.deleteInMediaScan
import com.infomaniak.drive.utils.MediaUtils.isMedia
import com.infomaniak.drive.utils.NotificationUtils.downloadProgressNotification
import io.realm.Realm
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.invoke
import kotlinx.coroutines.withContext
import okhttp3.Interceptor
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.Response
import java.util.UUID

class DownloadOfflineFileManager(
    private val userDrive: UserDrive,
    private val filesCount: Int,
    private val downloadWorker: BaseDownloadWorker,
    private val notificationManagerCompat: NotificationManagerCompat,
    /**
     * True when this manager downloads files on behalf of an offline folder walk (issue #1902):
     * completion never manufactures an explicit pin, and a copy whose coverage disappeared
     * while the download was in flight is dropped instead of being kept.
     */
    private val isInheritedDownload: Boolean = false,
) {
    private var lastDownloadedFile: IOFile? = null
    private var currentFile: File? = null
    private var filesDownloaded = 0

    /**
     * Whether the most recent [execute] transfer was rejected as superseded (issue: round 9
     * #8): the bulk caller must exclude such a transfer from its completion-marker clearing
     * AND from its batch's synchronized verdict, because a newer generation owns the file's
     * pending state and its replacement is still outstanding. The manager's own result maps
     * the outcome to success (the newer transfer's bytes win, nothing failed here), which is
     * why this status must travel beside it.
     */
    var lastTransferSuperseded: Boolean = false
        private set

    private var lastUpdateProgressMillis = System.currentTimeMillis()

    suspend fun execute(
        context: Context,
        file: File,
        onProgress: (progress: Int, fileId: Int) -> Unit,
    ): ListenableWorker.Result {

        // Reset the per-transfer cleanup state BEFORE any early return (issue: round 3 #13): a
        // previous transfer's file must never be examined or deleted by this transfer's cleanup.
        currentFile = file
        lastDownloadedFile = null
        lastTransferSuperseded = false

        // Fetch first WITHOUT committing metadata, so an inherited copy at the old location can
        // be preserved (migrated, or snapshotted against its OLD baseline) before the remote
        // rename/move upsert would forget that location (issues: round 3 #7, round 7 #2). The
        // shared upsert boundary covers both moved and unchanged paths; if preservation is
        // incomplete, the transfer aborts with the OLD metadata intact: nothing is stranded,
        // and the offline sync retries (issue: round 4 #9).
        val remoteFile = fetchRemoteFileDetails(context, file.id)
        // ONE coordinated operation (issue: round 11 #2): the ROOT-INTENT monitor precedes the
        // file lock (issue: round 16 #20) — the details response can CHANGE THE LOCATION
        // (parentId), a commit that reparents rows and alters the ancestry coverage
        // resolution walks; holding only the file lock let this reparenting interleave with
        // SAF's ancestry decisions (delete D after finding no owned descendants, then this
        // phase commits F under the deleted D). Order: intent monitor → file lock →
        // Realm/FS mutation — never the monitor from inside an already-held file lock.
        val committed = synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive, remoteFile.id) {
                val commitFile = FileController.getRealmInstance(userDrive).use { realm ->
                    // The transfer's basis [file] is the request-observed state (issue: round 13
                    // #1): a tied-timestamp identity change is accepted only while the live row
                    // still matches it.
                    OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                        realm = realm,
                        remoteFiles = listOf(remoteFile),
                        userDrive = userDrive,
                        observedById = mapOf(remoteFile.id to file),
                    ).first()
                }
                // COMPLETE DESTINATION-CHAIN validation BEFORE any preservation side
                // effect (issue: round 17 #8, superseding the round-16 #20 single-parent
                // check that ran after the boundary): every ancestor of the incoming
                // metadata's destination — not just the immediate parent, whose own ancestry
                // could be missing — must currently exist, or the whole operation defers
                // WITHOUT touching source bytes; otherwise preservation could migrate or
                // snapshot/delete the old copy and only then discover the unusable
                // destination, leaving the old metadata pointing at an emptied location.
                // The same validation is re-applied to the boundary-committed identity
                // before the metadata commit.
                fun destinationChainExists(parentId: Int, realm: Realm): Boolean {
                    var currentId: Int? = parentId
                    val visited = mutableSetOf<Int>()
                    while (currentId != null && currentId > 0) {
                        if (!visited.add(currentId)) return false
                        if (FileController.getFileById(realm, currentId) == null) return false
                        currentId = FileController.getFileById(realm, currentId)?.parentId
                    }
                    return true
                }
                val prePreservationChainOk = remoteFile.parentId <= 0 ||
                    FileController.getRealmInstance(userDrive).use { realm ->
                        runCatching { realm.refresh() }
                        destinationChainExists(remoteFile.parentId, realm)
                    }
                if (!prePreservationChainOk) {
                    SentryLog.i(TAG, "Deferring refresh of file ${file.id}: its destination chain is incomplete")
                    return@withFileLock false
                }
                if (commitFile !== remoteFile) {
                    SentryLog.i(TAG, "Deferring refresh of file ${file.id}: its old offline copy is not safely preserved yet")
                    return@withFileLock false
                }
                // The SAME complete-chain validation holds THROUGH the commit (issue: round
                // 17 #8): a chain broken during preservation defers without the metadata
                // write.
                val commitChainOk = commitFile.parentId <= 0 ||
                    FileController.getRealmInstance(userDrive).use { realm ->
                        destinationChainExists(commitFile.parentId, realm)
                    }
                if (!commitChainOk) {
                    SentryLog.i(TAG, "Deferring refresh of file ${file.id}: its destination chain broke during preservation")
                    return@withFileLock false
                }

                // Conditional replacement (issue: round 10 #4): a delayed response from an older
                // fetch must not roll back a newer generation another worker already committed —
                // otherwise this transfer's later publication compares against the rolled-back row,
                // passes, and leaves bytes and metadata describing different revisions. The
                // acceptance decision reads the live row INSIDE the committing transaction
                // (issue: round 11 #5) against the request-observed basis (issue: round 13 #1).
                FileController.getRealmInstance(userDrive).use { realm ->
                    FileController.updateExistingFileIfNotOlder(newFile = commitFile, realm = realm, userDrive = userDrive, observed = file)
                }
            }
        }
        if (!committed) {
            SentryLog.i(TAG, "Deferring refresh of file ${file.id}: a newer generation was committed while the metadata response was in flight")
            return ListenableWorker.Result.failure()
        }

        val file = remoteFile
        currentFile = file
        val cacheFile = file.getCacheFile(context, userDrive)
        val offlineFile = file.getOfflineFile(context, userDrive.userId)
        if (offlineFile == null) {
            SentryLog.e(TAG, "offline must not be null")
            return ListenableWorker.Result.failure()
        }

        // POSITIVE destination ownership for the early-success paths (issue: round 15 #11):
        // pending/dirty fast paths return success without otherwise proving who owns the
        // destination — foreign or unknown ownership defers like a conflict, so the bulk
        // retains the root's incompleteness instead of completing F over another row's
        // protected bytes.
        val destinationOwner = FileController.getRealmInstance(userDrive).use { realm ->
            OfflineFolderController.destinationOwnerRow(realm, context, userDrive, file, offlineFile)
        }
        if (destinationOwner == null || destinationOwner.id != file.id) {
            lastTransferSuperseded = true
            return ListenableWorker.Result.success()
        }
        // Protect offline edits (issue #1902): never replace the bytes backing a pending upload
        // or a locally newer copy; the sync layer uploads them and a later walk re-evaluates.
        if (UploadFile.hasPendingUpload(uri = Uri.fromFile(offlineFile).toString())) {
            return ListenableWorker.Result.success()
        }
        if (offlineFile.exists() && OfflineFolderPolicy.isLocallyModified(
                offlineFileLastModifiedMillis = offlineFile.lastModified(),
                // The PUBLISHED byte baseline, not accepted server metadata (issue: round
                // 15 #6) — retained old-clean bytes are not a local edit.
                remoteLastModifiedAtSeconds = file.effectivePublishedBaseline,
            )
        ) {
            return ListenableWorker.Result.success()
        }

        if (moveCacheFileIfIntact(context, file, cacheFile, offlineFile)) return ListenableWorker.Result.success()

        if (file.isOfflineAndProvenCurrent(context, offlineFile)) {
            // We can have this case for example when we try to put a lot of files at once in offline mode
            // and for some reason, the worker is cancelled after a long time, the worker is restarted
            filesDownloaded += 1
            return ListenableWorker.Result.success()
        }

        onProgress(0, file.id)

        // The previous published copy is never deleted upfront: the fresh download is staged in
        // an app-owned directory and published by an atomic rename, so a failed transfer never
        // destroys it (issue: round 3 #2/#4).
        if (cacheFile.exists()) cacheFile.delete()
        deletePublishedProvenance(context, cacheFile)

        return if (currentFile?.rights?.canRead == false) {
            ListenableWorker.Result.failure()
        } else {
            runCatching {
                startOfflineDownload(context, file, offlineFile, onProgress)
            }.cancellable().onFailure { exception ->
                SentryLog.e(TAG, "An error has occurred", exception)
            }.getOrDefault(ListenableWorker.Result.failure())
        }
    }

    /**
     * Cleanup only ever concerns this transfer's owned staged file: it is deleted on failure and
     * renamed on success, so at cleanup time an existing staged file is always residue.
     */
    fun cleanLastDownloadedFile() {
        lastDownloadedFile?.let { staged ->
            runCatching { if (staged.exists()) staged.delete() }
        }
    }

    /**
     * Fetches the fresh remote metadata WITHOUT committing it to Realm: the caller preserves any
     * old inherited copy first, then upserts (issue: round 3 #7).
     */
    private suspend fun fetchRemoteFileDetails(
        context: Context,
        fileId: Int,
    ): File {
        // Always use the captured account's HTTP client: the walk may target another account/drive
        // than the currently active one (issue #1902).
        val fileDetails = Dispatchers.IO {
            ApiRepository.getFileDetails(
                file = File(id = fileId, driveId = userDrive.driveId),
                okHttpClient = AccountUtils.getHttpClient(userDrive.userId),
            )
        }
        val remoteFile = fileDetails.data
        return if (fileDetails.isSuccess() && remoteFile != null) {
            remoteFile
        } else {
            if (fileDetails.error?.exception is NetworkException) throw UploadTask.NetworkException()

            val translateError = fileDetails.translateError()
            val responseGsonType = object : TypeToken<ApiResponse<File>>() {}.type
            val translatedErrorText = context.getString(translateError)
            val responseJson = ApiController.gson.toJson(fileDetails, responseGsonType)
            throw RemoteFileException("$responseJson $translatedErrorText")
        }
    }

    /**
     * Promotes an intact cache file through the same staged, validated publication as network
     * downloads (issue: round 3 #3): partial promotion bytes never appear at the offline path,
     * and the final coordinated recheck runs before the atomic replace.
     */
    private suspend fun moveCacheFileIfIntact(
        context: Context,
        file: File,
        cacheFile: IOFile,
        offlineFile: IOFile,
    ): Boolean = withContext(Dispatchers.IO) {
        // ASYNC-PREPARATION / HELD-PUBLICATION split (round-16 compile correction): Kotlin
        // forbids a suspension point inside a critical section, so the SHARED provenance
        // validation THROUGH STAGING (issues: round 16 #11/#12) runs as ONE SYNCHRONOUS
        // section under the ROOT-INTENT monitor and the file lock — the cache bytes and
        // their provenance sidecar cannot change between validation and staging, and the
        // monitor-first order matches the global protocol. The staged publication is
        // SUSPEND, so it runs OUTSIDE the critical section: its own coordinated core
        // (publishStagedOfflineFile) re-acquires the SAME monitor and locks in the SAME
        // order and revalidates identity, ownership, intent and protection before its
        // rename, so no outcome check is lost.
        val staged = synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive, file.id) {
                // EXACT current generation provenance (issues: round 14 #14, round 15 #9,
                // round 16 #11): exact mtime and size do not establish WHICH generation the
                // cached bytes are — the sidecar written at cache publication must match
                // this transfer's authoritative (details-sourced) version and baseline.
                // Unavailable or mismatched provenance downloads the current revision.
                if (!cacheBytesProvenCurrent(context, cacheFile, file)) {
                    null
                } else {
                    val candidate = newStagedFile(context, file)
                    // EXPLICIT Boolean success AFTER the actual copy (round-16 compile fix #2):
                    // File.copyTo returns the destination File, so getOrDefault(false) against
                    // it inferred a File/Boolean least-upper-bound instead of a condition.
                    val copied = runCatching {
                        cacheFile.copyTo(candidate, overwrite = true)
                        true
                    }.getOrDefault(false)
                    if (copied) {
                        candidate
                    } else {
                        runCatching { if (candidate.exists()) candidate.delete() }
                        null
                    }
                }
            }
        } ?: return@withContext false

        val published = runCatching {
            publishStagedOfflineFile(context, file, offlineFile, staged)
        }.onFailure { exception ->
            SentryLog.e(TAG, "Cache promotion failed for ${offlineFile.path}", exception)
        }.getOrDefault(PublicationOutcome.FAILED)
        // Cache promotion is a transfer path too: its superseded/conflict outcomes must reach
        // the bulk caller like the network branch's (issues: round 10 #7, round 14 #13), or
        // the batch clears pending work and reports the root synchronized.
        lastTransferSuperseded =
            published == PublicationOutcome.SKIPPED_SUPERSEDED || published == PublicationOutcome.SKIPPED_CONFLICT

        runCatching { if (staged.exists()) staged.delete() }
        when (published) {
            // Completion (flag write, media scan, invalidation) already ran inside the per-file
            // lock via onPublishedLocked (issue: round 5 #2).
            PublicationOutcome.PUBLISHED -> true
            // Skipped outcomes leave the existing bytes and timestamps untouched (issue: round
            // 4 #1): only the pending marker is cleared, since there is nothing valid left to
            // download for this transfer.
            PublicationOutcome.SKIPPED_PROTECTED, PublicationOutcome.SKIPPED_INVALIDATED -> {
                filesDownloaded += 1
                clearPendingMarker(file.id)
                true
            }
            PublicationOutcome.SKIPPED_CONFLICT -> {
                // The destination holds protected content owned by another row (issue: round
                // 14 #13): deferred — the pending marker stays so the work and the root's
                // incompleteness are retained until the conflict resolves.
                true
            }
            PublicationOutcome.SKIPPED_SUPERSEDED -> {
                // A newer generation owns the file's pending state and completion (issue:
                // round 8 #9): this transfer's skip clears no marker and runs no completion.
                true
            }
            PublicationOutcome.FAILED -> false
        }
    }

    /**
     * Final coordinated publication AND completion (issue: round 5 #2): the completion flag
     * write, its exact-revision-verified invalidation and the media scan all run INSIDE the same
     * per-file lock as the rename, so an edit becoming a pending upload (or a root removal
     * retaining it) between publication and completion can neither be overwritten nor deleted.
     */
    private suspend fun publishStagedOfflineFile(
        context: Context,
        file: File,
        target: IOFile,
        staged: IOFile,
    ): PublicationOutcome = publishStagedOfflineFile(
        userDrive = userDrive,
        isInheritedDownload = isInheritedDownload,
        file = file,
        target = target,
        staged = staged,
        onPublishedLocked = { completePublishedFileLocked(context, file, target) },
    )

    /**
     * Completion core for bytes this transfer just published, executed while the per-file lock
     * is still held. The redundant timestamp write is gone: the staged file was already
     * timestamped with the remote revision before the atomic rename.
     */
    private fun completePublishedFileLocked(context: Context, file: File, target: IOFile) {
        var coverageInvalidated = false

        // All validation happens INSIDE the committing transaction (issue: round 3 #9): current
        // explicit pin and durable ACTIVE root ancestry are re-derived here; a materialized
        // inherited flag alone never authorizes retention.
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction {
                val liveFile = FileController.getFileById(realm, file.id)
                val pinned = liveFile?.isOffline == true
                val ancestorRootCovers = liveFile != null && OfflineFolderController.isInsideActiveRoot(realm, file.id)

                when {
                    isInheritedDownload && !pinned && !ancestorRootCovers -> {
                        coverageInvalidated = true
                    }
                    isInheritedDownload && ancestorRootCovers -> {
                        if (liveFile != null && liveFile.isValid) liveFile.isInOfflineFolder = true
                    }
                    else -> {
                        if (liveFile != null && liveFile.isValid) {
                            // A file covered by an offline folder keeps its inherited state: it
                            // must not become an explicit pin, otherwise removing the folder
                            // could not clean it up without stripping separately pinned files.
                            liveFile.isOffline = liveFile.isOffline || !liveFile.isInOfflineFolder
                        }
                    }
                }
            }
        }

        if (coverageInvalidated) {
            // Coverage disappeared while this transfer ran. Delete the target only if it is
            // still exactly the revision this transfer published: anything else (an edit that
            // became a pending upload, for instance) is owned by someone else now.
            val isExactPublishedRevision = target.exists() && target.lastModified() / 1_000L == file.lastModifiedAt
            if (isExactPublishedRevision) {
                deletePublishedProvenance(context, target)
                runCatching { target.delete() }
                if (file.isMedia()) file.deleteInMediaScan(context, userDrive)
            }
        } else {
            if (file.isMedia()) MediaUtils.scanFile(context, target)
        }
        fileDownloaded(context, file.id)
    }

    private suspend fun startOfflineDownload(
        context: Context,
        file: File,
        offlineFile: java.io.File,
        onProgress: (progress: Int, fileId: Int) -> Unit,
    ): ListenableWorker.Result = withContext(Dispatchers.IO) {

        val okHttpClient = AccountUtils.getHttpClient(userDrive.userId, null)

        val response = downloadFileResponseAsync(
            fileUrl = ApiRoutes.downloadFile(file),
            okHttpClient = okHttpClient,
            downloadInterceptor = downloadProgressInterceptor(
                getMostRecentLastUpdate = { lastUpdateProgressMillis },
                onLastUpdateChange = { lastUpdate -> lastUpdateProgressMillis = lastUpdate },
                onProgress = { progress ->
                    ensureActive()

                    onProgress(progress, file.id)

                    if (downloadWorker.isForOneFile()) {
                        updateDownloadNotification(
                            context = context,
                            contentTitle = file.name,
                            contentText = "%d%%".format(progress),
                            progressPercent = progress,
                        )
                    }

                    SentryLog.d(downloadWorker.workerTag(), "download $progress%")
                },
            )
        )

        if (!response.isSuccessful) return@withContext ListenableWorker.Result.failure()

        // Download into a unique, app-owned staged file (issue: round 3 #2): never a sibling of
        // the offline path (a user file could live there), never shared between concurrent
        // workers. In-progress bytes are never visible at the offline path.
        val staged = newStagedFile(context, file)
        lastDownloadedFile = staged
        makeSureFileExists(staged) ?: return@withContext ListenableWorker.Result.failure()

        runCatching {
            val remoteDataHasBeenSaved = saveRemoteData(downloadWorker.workerTag(), response, staged)

            // Validate before publishing (size when the remote metadata carries one), then hand
            // over to the final coordinated publication (intent, pending uploads, target
            // revision, atomic replace).
            val sizeIsValidated = file.size == null || file.isIntactFile(staged)
            if (remoteDataHasBeenSaved && staged.exists() && sizeIsValidated) {
                val outcome = publishStagedOfflineFile(context, file, offlineFile, staged)
                // Public superseded/conflict status for the bulk caller (issues: round 9 #8,
                // round 14 #13).
                lastTransferSuperseded =
                    outcome == PublicationOutcome.SKIPPED_SUPERSEDED || outcome == PublicationOutcome.SKIPPED_CONFLICT
                when (outcome) {
                    PublicationOutcome.FAILED -> return@withContext ListenableWorker.Result.failure()
                    // Completion (flag writes, timestamp, media scan) only runs for bytes this
                    // transfer actually published; skipped outcomes leave the existing content
                    // untouched (issue: round 4 #1).
                    PublicationOutcome.PUBLISHED -> onProgress(100, file.id)
                    PublicationOutcome.SKIPPED_PROTECTED, PublicationOutcome.SKIPPED_INVALIDATED -> {
                        clearPendingMarker(file.id)
                    }
                    PublicationOutcome.SKIPPED_CONFLICT -> {
                        // A protected destination owned by another row defers (issue: round
                        // 14 #13): the pending marker stays so the work is retained.
                    }
                    PublicationOutcome.SKIPPED_SUPERSEDED -> {
                        // A newer generation owns the file's pending state and completion
                        // (issue: round 8 #9): this transfer's skip must not clear its marker
                        // out from under the newer transfer.
                    }
                }
                ListenableWorker.Result.success()
            } else {
                runCatching { staged.delete() }
                ListenableWorker.Result.failure()
            }
        }.cancellable().getOrElse {
            runCatching { staged.delete() }
            throw it
        }
    }

    /**
     * Clears only the download-pending marker, without touching bytes, timestamps or coverage
     * flags (issue: round 4 #1).
     */
    private fun clearPendingMarker(fileId: Int) {
        FileController.markFileAsOffline(fileId = fileId, isMarkedAsOffline = false, userDrive = userDrive)
    }

    private fun makeSureFileExists(offlineFile: java.io.File): Unit? {
        val parentExists = offlineFile.parentFile?.exists()

        runCatching {
            if (parentExists == false) offlineFile.parentFile?.mkdirs()
            // This line is here to help some devices that don't succeed in automatically creating the file…
            offlineFile.createNewFile()
        }.onFailure {
            SentryLog.e(TAG, "Failed to create a new file", it) { scope ->
                scope.setExtra("does parent exist", parentExists.toString())
            }

            return null
        }

        return Unit
    }

    private fun fileDownloaded(context: Context, fileId: Int) {
        FileController.markFileAsOffline(fileId = fileId, isMarkedAsOffline = false, userDrive = userDrive)
        lastUpdateProgressMillis = System.currentTimeMillis()
        filesDownloaded += 1

        val progressPercent = (filesDownloaded * 100) / filesCount
        downloadWorker.downloadNotification()?.let { notification ->
            val notificationContentTitle = notification.titleResId?.let { titleResId ->
                context.getString(titleResId, progressPercent)
            }
            val notificationContentText = notification.contentResId?.let { contentResId ->
                context.resources.getQuantityString(
                    contentResId,
                    filesDownloaded,
                    filesDownloaded,
                    filesCount
                )
            }
            updateDownloadNotification(
                context = context,
                contentTitle = notificationContentTitle,
                contentText = notificationContentText,
                progressPercent = progressPercent
            )
        }
    }

    private fun updateDownloadNotification(
        context: Context,
        contentTitle: String?,
        contentText: String?,
        progressPercent: Int
    ) {
        val downloadNotification = downloadWorker.downloadNotification()
        downloadNotification?.notification?.apply {
            setContentTitle(contentTitle)
            setContentText(contentText)
            setProgress(100, progressPercent, false)
            notificationManagerCompat.notifyCompat(downloadNotification.id, this)
        }
    }

    companion object {
        private const val MAX_INTERVAL_BETWEEN_PROGRESS_UPDATE_MS = 1000L
        private val TAG = DownloadOfflineFileManager::class.java.simpleName

        private const val STAGING_FOLDER = "offline_staging"

        internal enum class PublicationOutcome { PUBLISHED, SKIPPED_PROTECTED, SKIPPED_CONFLICT, SKIPPED_INVALIDATED, SKIPPED_SUPERSEDED, FAILED }

        /** Suffix of the old SIBLING sidecar experiment — never written or deleted again (round 17 #1). */
        private const val CACHE_PROVENANCE_SUFFIX = ".offlinegen"

        /** Registry directory for published-generation provenance, OUTSIDE the user-file namespace (round 17 #1). */
        private const val PROVENANCE_FOLDER = "offline_provenance"

        private fun provenanceRoot(context: Context): IOFile =
            IOFile(File.getOfflineFolder(context).parentFile, PROVENANCE_FOLDER)

        /**
         * The provenance record of [publishedFile], stored OUTSIDE the user-file namespace
         * (issue: round 17 #1): the previous sibling `${name}.offlinegen` collided with real
         * user files (a synced `report.offlinegen` could be truncated by publishing `report`,
         * or deleted by clearing it, with no ownership or protection check). The record is
         * keyed by the SHA-256 of the published file's ABSOLUTE path — which encodes the
         * captured account, drive and exact published location — never by a suffix reserved
         * inside directories holding user content.
         */
        private fun provenanceFileFor(context: Context, publishedFile: IOFile): IOFile {
            val digest = java.security.MessageDigest.getInstance("SHA-256")
                .digest(publishedFile.absolutePath.toByteArray())
            val hex = digest.joinToString("") { "%02x".format(it) }
            return IOFile(IOFile(provenanceRoot(context), hex.substring(0, 2)), "$hex.gen")
        }

        /** The generation identity recorded with published bytes (issues: round 16 #12/#13, round 17 #13). */
        internal data class PublishedProvenance(
            val version: Int,
            val lastModifiedAt: Long,
            val revisedAt: Long,
            val updatedAt: Long,
        )

        /**
         * Associates PUBLISHED bytes (cache or offline destination) with the content generation
         * that produced them: exact mtime and size cannot establish the generation — two
         * same-sized revisions can share the preserved client mtime with different server
         * identities. The record names the producing version, its modification baseline AND
         * the accepted revision identity (revisedAt, updatedAt) every current-content decision
         * validates.
         */
        private fun writePublishedProvenance(context: Context, target: IOFile, file: File): Boolean = runCatching {
            provenanceFileFor(context, target).writeText(
                "${file.version?.number ?: 0}|${file.lastModifiedAt}|${file.revisedAt}|${file.updatedAt}",
            )
            true
        }.getOrDefault(false)

        internal fun readPublishedProvenance(context: Context, publishedFile: IOFile): PublishedProvenance? = runCatching {
            val parts = provenanceFileFor(context, publishedFile).readText().trim().split("|")
            require(parts.size == 4)
            PublishedProvenance(
                version = parts[0].trim().toInt(),
                lastModifiedAt = parts[1].trim().toLong(),
                revisedAt = parts[2].trim().toLong(),
                updatedAt = parts[3].trim().toLong(),
            )
        }.getOrNull()

        internal fun deletePublishedProvenance(context: Context, publishedFile: IOFile) {
            runCatching { provenanceFileFor(context, publishedFile).delete() }
        }

        /**
         * Moves the provenance record of [source]'s published bytes onto [target] (issue:
         * round 16 #12): a byte migration (move reconciliation) keeps its generation record,
         * and no orphan is left at the source path. True when there was nothing to move or
         * the move succeeded.
         */
        internal fun movePublishedProvenance(context: Context, source: IOFile, target: IOFile): Boolean {
            val provenance = readPublishedProvenance(context, source)
            deletePublishedProvenance(context, source)
            if (provenance == null) return true
            return runCatching {
                provenanceFileFor(context, target).writeText(
                    "${provenance.version}|${provenance.lastModifiedAt}|${provenance.revisedAt}|${provenance.updatedAt}",
                )
                true
            }.getOrDefault(false)
        }

        /**
         * Whether [cacheFile]'s bytes are PROVEN to be the current content generation of [file]
         * (issues: round 15 #9, round 16 #11): exact size, exact modification baseline, and the
         * generation provenance sidecar matching the row's authoritative version and baseline.
         * EVERY cache promotion — worker and UI alike — requires this; missing or mismatched
         * provenance must enqueue a fresh download instead of publishing unknown-generation
         * bytes under current metadata.
         */
        internal fun cacheBytesProvenCurrent(context: Context, cacheFile: IOFile, file: File): Boolean {
            if (!(cacheFile.exists() && file.isIntactFile(cacheFile) && (cacheFile.lastModified() / 1_000L) == file.lastModifiedAt)) {
                return false
            }
            val provenance = readPublishedProvenance(context, cacheFile)
            val transferVersion = file.version?.number ?: 0
            return transferVersion > 0 && provenance != null &&
                provenance.version == transferVersion && provenance.lastModifiedAt == file.lastModifiedAt
        }

        /**
         * App-owned staging directory on the SAME volume as the offline storage (so publication
         * is an atomic rename), but OUTSIDE it: remote file paths can never resolve here, so no
         * user content can be staged-over or mistaken for residue (issue: round 3 #2).
         */
        private fun stagingDirectory(context: Context): IOFile =
            IOFile(File.getOfflineFolder(context).parentFile, STAGING_FOLDER).apply { if (!exists()) mkdirs() }

        /**
         * A unique staged file per transfer: identical downloads by concurrent workers (e.g. an
         * ancestor and a nested root) never collide (issue: round 3 #2).
         */
        internal fun newStagedFile(context: Context, file: File): IOFile =
            IOFile(stagingDirectory(context), "${file.id}_${file.driveId}_${UUID.randomUUID()}.part")

        /**
         * Shared, coordinated publication of a staged file into its final offline location
         * (issue: round 4 #4): used by network downloads, worker cache promotion AND the UI
         * promotion, all through the shared per-file coordination so uploads, cleanup, moves and
         * completion cannot interleave with the publication.
         */
        private suspend fun publishStagedOfflineFile(
            userDrive: UserDrive,
            isInheritedDownload: Boolean,
            file: File,
            target: IOFile,
            staged: IOFile,
            onPublishedLocked: (() -> Unit)? = null,
        ): PublicationOutcome = withContext(Dispatchers.IO) {
            // ROOT-INTENT coordination for publication (issue: round 12 #8): an ancestry-
            // dependent publication must serialize with ancestry-changing writers (folder
            // moves take the monitor before their locks) — a monitored UI folder move could
            // otherwise change ancestry after this publication validates its destination,
            // because it locks the ancestor rather than this publishing leaf. The monitor is
            // acquired BEFORE the ordered file locks and retained through the destination
            // ownership read, the final supersession decision and the filesystem rename
            // inside publishLockedCore. Order: intent monitor → ascending file locks →
            // Realm/FS mutation.
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                // Destination ownership (issue: round 6 #3): the row owning the existing bytes at
                // [target] is resolved BEFORE locking, then this publication holds BOTH the
                // incoming file's and the owner's coordination locks in a stable ascending-id
                // order, so the owner's mutators (cleanup, reconciliation, its own publications)
                // are serialized with it and no lock ordering can deadlock.
                val appContext = Realm.getApplicationContext()
                val preLockOwner = appContext?.let { ctx ->
                    FileController.getRealmInstance(userDrive).use { realm ->
                        OfflineFolderController.destinationOwnerRow(realm, ctx, userDrive, file, target)
                    }
                }
                val lockedOwnerId: Int? = preLockOwner?.id

                // Both locks (incoming + established owner) in ONE ascending-id acquisition,
                // constant stack (issues: round 11 #1, round 11 #12): the same ordered protocol as
                // every graph commit and cleanup acquisition, so no lock cycle is possible, and
                // the explicit-lock loop cannot overflow the stack the way nested monitors did.
                OfflineFileCoordination.withLocksFor(userDrive, listOfNotNull(file.id, preLockOwner?.id)) {
                    publishLockedCore(
                        userDrive, isInheritedDownload, file, target, staged, onPublishedLocked, lockedOwnerId,
                    ) { coroutineContext.ensureActive() }
                }
            }
        }

        /**
         * Publication body, executed while the coordination locks of the incoming file (and of
         * the established destination owner, if any) are held. [checkCancelled] restores the
         * final cancellation check inside the locks (issue: round 7 #11): Java monitor
         * acquisition does not respond to coroutine cancellation, so a cancelled transfer
         * waiting for an ownership lock must re-check after acquiring them and immediately
         * before the publication.
         */
        private fun publishLockedCore(
            userDrive: UserDrive,
            isInheritedDownload: Boolean,
            file: File,
            target: IOFile,
            staged: IOFile,
            onPublishedLocked: (() -> Unit)?,
            lockedOwnerId: Int?,
            checkCancelled: () -> Unit,
        ): PublicationOutcome {
            checkCancelled()
            // Live intent: an inherited download requires a current pin or durable root
            // ancestry (a materialized inherited flag alone never authorizes retention); a
            // pin download accepts its own pending marker or explicit pin as the user's
            // request. The same realm read also decides whether this transfer's captured
            // identity/revision has been superseded (issues: round 8 #9, round 9 #7).
            val appContextForCheck = Realm.getApplicationContext()
            var supersededGeneration = false
            val intentOk = FileController.getRealmInstance(userDrive).use { realm ->
                val live = FileController.getFileById(realm, file.id)
                val pinned = live?.isOffline == true
                val ancestryCovers = OfflineFolderController.isInsideActiveRoot(realm, file.id)
                // Superseded rejection — full identity/generation CAS (issues: round 8 #9,
                // round 9 #7, round 10 #4): the transfer is stale when the live row holds a
                // NEWER SERVER GENERATION ([File.revisedAt] — the modification time alone is
                // not monotonic, a re-upload can move it backwards; the mtime comparison only
                // covers legacy rows whose revisedAt was never populated), OR a different
                // location identity (direct rename/move, or an ancestor-chain change that
                // moved the resolved offline path). Publishing at the old location and
                // completing against the new row would desync bytes and metadata. The check
                // and the rename form one compare-and-set: identity/revision writers take the
                // same per-file lock before their transactions (round 10 #3).
                supersededGeneration = live != null && (
                    live.revisedAt > file.revisedAt ||
                        (live.revisedAt == 0L && live.lastModifiedAt > file.lastModifiedAt) ||
                        live.parentId != file.parentId ||
                        live.name != file.name ||
                        // Accepted content-generation identity (issues: round 12 #6, round
                        // 13 #9): the committed metadata and this transfer must describe the
                        // SAME revision. UNKNOWN version data (listing rows omit the
                        // optional expansion) is not a generation difference — only two
                        // KNOWN, differing version numbers supersede; the COMPLETE content
                        // baseline (modification time AND size) decides otherwise, so a
                        // tied-timestamp same-size-differs revision still supersedes the
                        // staged bytes.
                        ((live.version?.number ?: 0) > 0 && (file.version?.number ?: 0) > 0 &&
                            live.version?.number != file.version?.number) ||
                        // Ambiguous version provenance (issue: round 14 #7): when exactly ONE
                        // side carries the authoritative version and the complete coarse
                        // baseline ties, timestamp/size equality alone cannot prove the same
                        // content generation — an inherited or omitted version must not
                        // falsely prove equality with an in-flight transfer. The publication
                        // defers (SKIPPED_SUPERSEDED) and the retry obtains the authoritative
                        // identity: details routes carry the version expansion, so the
                        // worker's refetch and the deferred markers' refresh resolve it.
                        // Both sides UNKNOWN remains the documented coarse-baseline case.
                        (((live.version?.number ?: 0) > 0) != ((file.version?.number ?: 0) > 0) &&
                            live.lastModifiedAt == file.lastModifiedAt && live.size == file.size) ||
                        live.lastModifiedAt != file.lastModifiedAt ||
                        live.size != file.size ||
                        run {
                            val livePath = appContextForCheck?.let { ctx -> live.getOfflineFile(ctx, userDrive.userId)?.path }
                            livePath == null || livePath != target.path
                        }
                    )
                if (isInheritedDownload) {
                    pinned || ancestryCovers
                } else {
                    pinned || live?.isMarkedAsOffline == true || ancestryCovers
                }
            }

            if (!intentOk) {
                runCatching { staged.delete() }
                return PublicationOutcome.SKIPPED_INVALIDATED
            }
            if (supersededGeneration) {
                runCatching { staged.delete() }
                return PublicationOutcome.SKIPPED_SUPERSEDED
            }

            // The existing copy may have been edited, registered for upload, or re-owned by
            // another row during the transfer: those bytes always win over the freshly
            // downloaded ones (issue: round 6 #3).
            if (target.exists()) {
                val appContext = Realm.getApplicationContext()
                val liveOwner = appContext?.let { ctx ->
                    FileController.getRealmInstance(userDrive).use { realm ->
                        OfflineFolderController.destinationOwnerRow(realm, ctx, userDrive, file, target)
                    }
                }

                val targetProtected = UploadFile.hasPendingUpload(uri = Uri.fromFile(target).toString()) ||
                        UploadFile.hasRetainedUnsentUpload(uri = Uri.fromFile(target).toString()) || when {
                    // Without an application context the ownership cannot be established: preserve.
                    appContext == null -> true
                    // Unknown ownership (no row claims the path — e.g. new-file/path-reuse
                    // residue): preserve, never overwrite.
                    liveOwner == null -> true
                    // The owner changed after the locks were taken: its mutators are not
                    // serialized with this publication — defer and let the next attempt lock
                    // the current owner correctly.
                    liveOwner.id != lockedOwnerId -> true
                    // Known owner: protect against ITS PUBLISHED byte baseline (issue:
                    // round 15 #6), never the incoming revision — the previous baseline
                    // declared another file's older dirty copy unprotected and overwrote
                    // its sole edit, and accepted metadata alone must not reclassify
                    // retained old-clean bytes as an edit.
                    else -> OfflineFolderPolicy.isLocallyModified(
                        offlineFileLastModifiedMillis = target.lastModified(),
                        remoteLastModifiedAtSeconds = liveOwner.effectivePublishedBaseline,
                    )
                }

                if (targetProtected) {
                    runCatching { staged.delete() }
                    // Usable protected content FOR THIS FILE vs a destination conflict
                    // (issue: round 14 #13): the protection proves F's own usable content
                    // only when the destination's owner row is F itself — its pending
                    // upload, retained unsent edit, or a local modification against its own
                    // baseline. Unknown ownership and a CHANGED owner are preservation
                    // reasons for OTHER content, not proof that F has usable local content:
                    // they defer as a conflict, with pending and root work retained.
                    // POSITIVE ownership (issue: round 15 #11): the protected content must
                    // belong to the INCOMING file — [lockedOwnerId] is the previously
                    // discovered destination owner and can equally be another row's, which
                    // would report foreign bytes as F's usable content.
                    val ownedByThisFile = liveOwner != null && liveOwner.id == file.id && liveOwner.id == lockedOwnerId
                    return if (ownedByThisFile) {
                        PublicationOutcome.SKIPPED_PROTECTED
                    } else {
                        PublicationOutcome.SKIPPED_CONFLICT
                    }
                }
            }

            staged.setLastModified(file.getLastModifiedInMilliSecond())
            // Final cancellation check immediately before the atomic replace (issue: round 7 #11).
            checkCancelled()
            // The OLD provenance is invalidated BEFORE the bytes are replaced (issue: round
            // 16 #12): a crash between the rename and the new sidecar write must never leave
            // the new bytes carrying the previous generation's provenance.
            val provenanceContext = appContextForCheck
            if (provenanceContext == null) {
                runCatching { staged.delete() }
                return PublicationOutcome.FAILED
            }
            deletePublishedProvenance(provenanceContext, target)
            if (staged.renameTo(target)) {
                // The published generation's provenance sidecar is written FAIL-CLOSED
                // (issue: round 16 #12): bytes without provenance are never accepted as the
                // current generation by the intact/promotion decisions (issue: round 16 #13),
                // so a failed write must discard the publication rather than leave
                // unproven bytes at the destination.
                if (!writePublishedProvenance(provenanceContext, target, file)) {
                    runCatching { target.delete() }
                    return PublicationOutcome.FAILED
                }
                // Record the PUBLISHED bytes' clean baseline separately from the accepted
                // server metadata (issue: round 15 #6): edit detection judges the copy
                // against the baseline its bytes were actually written under until a
                // successful replacement advances it — metadata acceptance alone must not
                // reclassify retained old-clean bytes as a local edit.
                FileController.getRealmInstance(userDrive).use { realm ->
                    realm.executeTransaction {
                        FileController.getFileById(realm, file.id)?.publishedCleanBaseline = file.lastModifiedAt
                    }
                }
                // Run the owner's completion while the per-file lock is still held
                // (issue: round 5 #2).
                onPublishedLocked?.invoke()
                return PublicationOutcome.PUBLISHED
            } else {
                SentryLog.e(TAG, "Failed to publish staged file to ${target.path}")
                runCatching { staged.delete() }
                return PublicationOutcome.FAILED
            }
        }

        /**
         * UI cache promotion (issue: round 4 #8): routes through the same protected, coordinated
         * publication as the workers. The caller sets the explicit pin BEFORE calling so the
         * intent check sees the user's request; the cache source is deleted only after a
         * confirmed publication. Returns the full [PublicationOutcome] (issue: round 13 #13):
         * a superseded or invalidated publication is NOT a success the UI may promote — the
         * caller decides what remains of the user's intent per outcome.
         */
        internal suspend fun promoteCacheToOffline(
            context: Context,
            file: File,
            cacheFile: IOFile,
            offlineFile: IOFile,
            userDrive: UserDrive,
        ): PublicationOutcome {
            // AUTHORITATIVE version resolution for ambiguous transfers (issue: round 16 #13):
            // a presentation row's unknown version against an unknown live version left only
            // the coarse baseline, and two same-second, same-size revisions passed as
            // "current". The details route carries the version expansion; resolving the
            // identity before publication turns the ambiguous comparison into a
            // known-generation one (the stale-operation risk is re-checked against the live
            // row under the publication locks anyway).
            val authoritativeFile = if ((file.version?.number ?: 0) > 0) file else {
                runCatching {
                    ApiRepository.getFileDetails(File(id = file.id, driveId = userDrive.driveId)).data
                }.getOrNull()?.takeIf { details -> details.id == file.id } ?: file
            }
            // HELD-STAGING / SUSPEND-PUBLICATION split (round-16 compile correction): the
            // SHARED provenance validation THROUGH STAGING (issues: round 16 #11/#12) runs as
            // ONE SYNCHRONOUS section under the ROOT-INTENT monitor and the file lock — every
            // cache promotion, UI included, requires the generation sidecar to match the
            // authoritative identity, and the cache bytes cannot change between validation
            // and staging. The staged publication is SUSPEND, so it runs OUTSIDE the critical
            // section and re-acquires the SAME monitor and locks in the SAME order inside its
            // coordinated core; missing or mismatched provenance fails and the caller
            // enqueues a fresh download.
            val staged = newStagedFile(context, authoritativeFile)
            val stagedOk = synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withFileLock(userDrive, file.id) {
                    // EXPLICIT Boolean success AFTER the actual copy (round-16 compile fix
                    // #2), as in moveCacheFileIfIntact: File.copyTo's File return must not
                    // merge with the failure default into a non-Boolean condition.
                    cacheBytesProvenCurrent(context, cacheFile, authoritativeFile) && runCatching {
                        cacheFile.copyTo(staged, overwrite = true)
                        true
                    }.getOrDefault(false)
                }
            }
            if (!stagedOk) {
                runCatching { if (staged.exists()) staged.delete() }
                SentryLog.i(TAG, "Cache promotion refused for ${offlineFile.path}: unproven generation")
                return PublicationOutcome.FAILED
            }
            val outcome = runCatching {
                publishStagedOfflineFile(userDrive, isInheritedDownload = false, authoritativeFile, offlineFile, staged)
            }.onFailure { exception ->
                SentryLog.e(TAG, "Cache promotion failed for ${offlineFile.path}", exception)
            }.getOrDefault(PublicationOutcome.FAILED)

            runCatching { if (staged.exists()) staged.delete() }
            if (outcome == PublicationOutcome.PUBLISHED) {
                runCatching { if (cacheFile.exists()) cacheFile.delete() }
                deletePublishedProvenance(context, cacheFile)
            }
            return outcome
        }

        /**
         * Coordinated final replacement for UI-driven re-downloads (preview, download dialog)
         * (issue: round 8 #2): the response body is written to unique staging storage and only
         * the final replacement of the destination goes through publication. For an
         * offline-owned destination the shared coordinated publication re-runs ownership,
         * revision, intent and cancellation checks under the per-file locks — a preflight check
         * made before the request started (or the snapshot it took) can never authorize
         * deleting a revision an editor saved to the path in the meantime. A plain cache
         * destination has no ownership to protect, but the staged copy still makes the
         * replacement an atomic rename, so a cancelled or failed download can no longer leave
         * partial bytes — or delete existing bytes — at the destination. The destination is
         * never touched before the staged copy is complete.
         */
        internal suspend fun publishDownloadedCopy(
            context: Context,
            file: File,
            target: IOFile,
            userDrive: UserDrive,
            isInheritedDownload: Boolean,
            coordinated: Boolean,
            writeStaged: suspend (IOFile) -> Unit,
        ): PublicationOutcome {
            // EXPLICIT outcome return paths (round-16 compile correction): both branches are
            // total and returned explicitly — no expression-position inference over a mixed
            // if/else.
            return if (!coordinated) {
                // Plain cache publication is still SERIALIZED per file (issue: round 16 #12):
                // the rename and the provenance sidecar write are one coordinated section, so
                // two publications can no longer interleave into "R1 bytes carrying R2
                // provenance". The sidecar travels WITH the staged bytes: it is written next
                // to the staged file before the rename, the destination's old provenance is
                // invalidated BEFORE the bytes are replaced, and a provenance rename failure
                // FAILS CLOSED — the new bytes are discarded rather than left at the
                // destination without (or with stale) provenance.
                // ASYNC STAGING OUTSIDE COORDINATION (issue: round 17 #5): the suspending
                // writer (which consumes a network response body) runs WITHOUT holding any
                // ReentrantLock — a slow transfer must never retain the file lock while an
                // offline operation holds the global intent monitor waiting for it. Only the
                // SYNCHRONOUS final publication (timestamp, provenance record, atomic rename)
                // holds the file lock; the provenance record travels in the registry keyed by
                // the staged path (issue: round 17 #1) and is re-keyed after the rename, and
                // a record-commit failure fails closed.
                val staged = IOFile(target.parentFile, "${target.name}.${UUID.randomUUID()}.part")
                val stagedComplete = runCatching { writeStaged(staged) }
                    .onFailure { exception ->
                        SentryLog.e(TAG, "Staged cache transfer failed for ${target.path}", exception)
                    }.isSuccess
                if (!stagedComplete) {
                    runCatching { if (staged.exists()) staged.delete() }
                    return PublicationOutcome.FAILED
                }
                val outcome = OfflineFileCoordination.withFileLock(userDrive, file.id) {
                    runCatching {
                        staged.setLastModified(file.getLastModifiedInMilliSecond())
                        writePublishedProvenance(context, staged, file)
                        deletePublishedProvenance(context, target)
                        if (staged.renameTo(target) && movePublishedProvenance(context, staged, target)) {
                            PublicationOutcome.PUBLISHED
                        } else {
                            // Fail closed: no unproven bytes at the destination.
                            runCatching { target.delete() }
                            SentryLog.e(TAG, "Failed to publish staged file to ${target.path}")
                            PublicationOutcome.FAILED
                        }
                    }.onFailure { exception ->
                        SentryLog.e(TAG, "Staged cache publication failed for ${target.path}", exception)
                    }.getOrDefault(PublicationOutcome.FAILED)
                }
                runCatching { if (staged.exists()) staged.delete() }
                outcome
            } else {
                val appContext = Realm.getApplicationContext()
                if (appContext == null) {
                    SentryLog.e(TAG, "No application context: refusing uncoordinated offline publication")
                    return PublicationOutcome.FAILED
                }
                val staged = newStagedFile(appContext, file)
                val outcome = runCatching {
                    writeStaged(staged)
                    publishStagedOfflineFile(userDrive, isInheritedDownload, file, target, staged)
                }.onFailure { exception ->
                    SentryLog.e(TAG, "Staged offline publication failed for ${target.path}", exception)
                }.getOrDefault(PublicationOutcome.FAILED)

                runCatching { if (staged.exists()) staged.delete() }
                outcome
            }
        }


        fun getFailedDownloadWorkerOffline(context: Context) = WorkManager.getInstance(context).getWorkInfosLiveData(
            WorkQuery.Builder
                .fromTags(arrayListOf(BulkDownloadWorker::class.java.toString(), DownloadWorker.TAG))
                .addStates(arrayListOf(WorkInfo.State.FAILED))
                .build()
        )

        suspend fun isBulkDownloadWorkerRunning(context: Context): Boolean {
            // One work chain per root (issue #1902): match the shared class tag instead of a
            // single unique name.
            val workQuery = WorkQuery.Builder
                .fromTags(arrayListOf(BulkDownloadWorker::class.java.toString()))
                .addStates(arrayListOf(WorkInfo.State.RUNNING, WorkInfo.State.ENQUEUED, WorkInfo.State.BLOCKED))
                .build()
            val workInfoList = WorkManager.getInstance(context).getWorkInfos(workQuery).await()

            return workInfoList.any { it.state == WorkInfo.State.RUNNING }
        }

        fun createDownloadNotification(context: Context, id: UUID, title: String): NotificationCompat.Builder {
            val cancelPendingIntent = WorkManager.getInstance(context).createCancelPendingIntent(id)
            val cancelAction = NotificationCompat.Action(
                /* icon = */ null,
                /* title = */ context.getString(R.string.buttonCancel),
                /* intent = */ cancelPendingIntent
            )
            return context.downloadProgressNotification().apply {
                setOngoing(true)
                setContentTitle(title)
                addAction(cancelAction)
            }
        }

        fun downloadFileResponse(
            fileUrl: String,
            okHttpClient: OkHttpClient = AuthHttpClientProvider.authOkHttpClient,
            downloadInterceptor: Interceptor? = null
        ): Response {
            @OptIn(ManualAuthorizationRequired::class)
            val request = Request.Builder().url(fileUrl).headers(HttpUtils.getHeaders(contentType = null)).get().build()

            return okHttpClient.newBuilder().apply {
                downloadInterceptor?.let { interceptor -> addInterceptor(interceptor) }
            }.build().newCall(request).execute()
        }

        suspend fun downloadFileResponseAsync(
            fileUrl: String,
            okHttpClient: OkHttpClient = AuthHttpClientProvider.authOkHttpClient,
            downloadInterceptor: Interceptor? = null
        ): Response {
            @OptIn(ManualAuthorizationRequired::class)
            val request = Request.Builder().url(fileUrl).headers(HttpUtils.getHeaders(contentType = null)).get().build()

            return okHttpClient.newBuilder().apply {
                downloadInterceptor?.let { interceptor -> addInterceptor(interceptor) }
            }.build().newCall(request).await()
        }

        suspend fun saveRemoteData(
            tag: String,
            response: Response,
            outputFile: java.io.File? = null,
            outputStream: ParcelFileDescriptor.AutoCloseOutputStream? = null,
        ): Boolean {
            SentryLog.d(tag, "Save remote data to ${outputFile?.path}")
            return response.body.byteStream().buffered().use { input ->
                if (outputFile?.parentFile?.exists() == false) outputFile.parentFile?.mkdirs()
                if (outputFile?.exists() == false) outputFile.createNewFile()
                val stream = outputStream ?: outputFile?.outputStream()
                stream?.use { output ->
                    input.copyToCancellable(output)
                }
            } != null
        }

        fun downloadProgressInterceptor(
            getMostRecentLastUpdate: (() -> Long)? = null,
            onLastUpdateChange: ((lastUpdateValue: Long) -> Unit)? = null,
            onProgress: (progress: Int) -> Unit
        ) = Interceptor { chain: Interceptor.Chain ->
            val originalResponse = chain.proceed(chain.request())

            originalResponse.newBuilder()
                .body(ProgressResponseBody(originalResponse.body!!, object : ProgressResponseBody.ProgressListener {
                    override fun update(bytesRead: Long, contentLength: Long, done: Boolean) {
                        val currentSystemTimeMillis = System.currentTimeMillis()
                        val lastUpdate = getMostRecentLastUpdate?.invoke() ?: 0L
                        if (currentSystemTimeMillis - lastUpdate > MAX_INTERVAL_BETWEEN_PROGRESS_UPDATE_MS) {
                            onLastUpdateChange?.invoke(currentSystemTimeMillis)
                            // Transport progress never reaches 100 (issue: round 10 #9): the
                            // raw body read completes while publication may still be waiting
                            // for a lock, failing, or returning invalidated — dialogs treat
                            // 100 as completion and schedule their callbacks on it. 100 is
                            // reserved for confirmed usable-destination completion, emitted
                            // by each flow only after its publication outcome succeeds.
                            val progress = ((bytesRead.toFloat() / contentLength.toFloat() * 100F).toInt()).coerceAtMost(99)
                            onProgress.invoke(progress)
                        }
                    }
                })).build()
        }
    }
}

class RemoteFileException(data: String) : Exception(data)
