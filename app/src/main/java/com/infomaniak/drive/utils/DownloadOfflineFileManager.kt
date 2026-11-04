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

        // Fetch first WITHOUT committing metadata, so an inherited copy at the old location can
        // be preserved (migrated, or snapshotted against its OLD baseline) before the remote
        // rename/move upsert would forget that location (issues: round 3 #7, round 7 #2). The
        // shared upsert boundary covers both moved and unchanged paths; if preservation is
        // incomplete, the transfer aborts with the OLD metadata intact: nothing is stranded,
        // and the offline sync retries (issue: round 4 #9).
        val remoteFile = fetchRemoteFileDetails(context, file.id)
        val commitFile = FileController.getRealmInstance(userDrive).use { realm ->
            OfflineFolderController.reconcileOwnedCopiesBeforeCommit(realm, listOf(remoteFile), userDrive).first()
        }
        if (commitFile !== remoteFile) {
            SentryLog.i(TAG, "Deferring refresh of file ${file.id}: its old offline copy is not safely preserved yet")
            return ListenableWorker.Result.failure()
        }

        FileController.getRealmInstance(userDrive).use { realm ->
            FileController.updateExistingFile(newFile = commitFile, realm = realm)
        }

        val file = remoteFile
        currentFile = file
        val cacheFile = file.getCacheFile(context, userDrive)
        val offlineFile = file.getOfflineFile(context, userDrive.userId)
        if (offlineFile == null) {
            SentryLog.e(TAG, "offline must not be null")
            return ListenableWorker.Result.failure()
        }

        // Protect offline edits (issue #1902): never replace the bytes backing a pending upload
        // or a locally newer copy; the sync layer uploads them and a later walk re-evaluates.
        if (UploadFile.hasPendingUpload(uri = Uri.fromFile(offlineFile).toString())) {
            return ListenableWorker.Result.success()
        }
        if (offlineFile.exists() && OfflineFolderPolicy.isLocallyModified(
                offlineFileLastModifiedMillis = offlineFile.lastModified(),
                remoteLastModifiedAtSeconds = file.lastModifiedAt,
            )
        ) {
            return ListenableWorker.Result.success()
        }

        if (moveCacheFileIfIntact(context, file, cacheFile, offlineFile)) return ListenableWorker.Result.success()

        if (file.isOfflineAndIntact(offlineFile)) {
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
        if (file.isObsoleteOrNotIntact(cacheFile)) return@withContext false

        val staged = newStagedFile(context, file)
        val published = runCatching {
            cacheFile.copyTo(staged, overwrite = true)
            publishStagedOfflineFile(context, file, offlineFile, staged)
        }.onFailure { exception ->
            SentryLog.e(TAG, "Cache promotion failed for ${offlineFile.path}", exception)
        }.getOrDefault(PublicationOutcome.FAILED)

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
                when (outcome) {
                    PublicationOutcome.FAILED -> return@withContext ListenableWorker.Result.failure()
                    // Completion (flag writes, timestamp, media scan) only runs for bytes this
                    // transfer actually published; skipped outcomes leave the existing content
                    // untouched (issue: round 4 #1).
                    PublicationOutcome.PUBLISHED -> onProgress(100, file.id)
                    PublicationOutcome.SKIPPED_PROTECTED, PublicationOutcome.SKIPPED_INVALIDATED -> {
                        clearPendingMarker(file.id)
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

        internal enum class PublicationOutcome { PUBLISHED, SKIPPED_PROTECTED, SKIPPED_INVALIDATED, FAILED }

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
            val incomingLock = OfflineFileCoordination.lockFor(userDrive, file.id)
            val ownerLock = preLockOwner?.id?.takeIf { it != file.id }?.let { OfflineFileCoordination.lockFor(userDrive, it) }
            val lockedOwnerId: Int? = preLockOwner?.id

            when {
                ownerLock == null -> synchronized(incomingLock) {
                    publishLockedCore(
                        userDrive, isInheritedDownload, file, target, staged, onPublishedLocked, lockedOwnerId,
                    ) { coroutineContext.ensureActive() }
                }
                file.id < (preLockOwner?.id ?: file.id) -> synchronized(incomingLock) {
                    synchronized(ownerLock) {
                        publishLockedCore(
                            userDrive, isInheritedDownload, file, target, staged, onPublishedLocked, lockedOwnerId,
                        ) { coroutineContext.ensureActive() }
                    }
                }
                else -> synchronized(ownerLock) {
                    synchronized(incomingLock) {
                        publishLockedCore(
                            userDrive, isInheritedDownload, file, target, staged, onPublishedLocked, lockedOwnerId,
                        ) { coroutineContext.ensureActive() }
                    }
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
            // request.
            val intentOk = FileController.getRealmInstance(userDrive).use { realm ->
                val live = FileController.getFileById(realm, file.id)
                val pinned = live?.isOffline == true
                val ancestryCovers = OfflineFolderController.isInsideActiveRoot(realm, file.id)
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
                    // Known owner: protect against ITS last clean revision, never the
                    // incoming revision — the previous baseline declared another file's
                    // older dirty copy unprotected and overwrote its sole edit.
                    else -> OfflineFolderPolicy.isLocallyModified(
                        offlineFileLastModifiedMillis = target.lastModified(),
                        remoteLastModifiedAtSeconds = liveOwner.lastModifiedAt,
                    )
                }

                if (targetProtected) {
                    runCatching { staged.delete() }
                    return PublicationOutcome.SKIPPED_PROTECTED
                }
            }

            staged.setLastModified(file.getLastModifiedInMilliSecond())
            // Final cancellation check immediately before the atomic replace (issue: round 7 #11).
            checkCancelled()
            if (staged.renameTo(target)) {
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
         * confirmed publication.
         */
        internal suspend fun promoteCacheToOffline(
            context: Context,
            file: File,
            cacheFile: IOFile,
            offlineFile: IOFile,
            userDrive: UserDrive,
        ): Boolean {
            val staged = newStagedFile(context, file)
            val outcome = runCatching {
                cacheFile.copyTo(staged, overwrite = true)
                publishStagedOfflineFile(userDrive, isInheritedDownload = false, file, offlineFile, staged)
            }.onFailure { exception ->
                SentryLog.e(TAG, "Cache promotion failed for ${offlineFile.path}", exception)
            }.getOrDefault(PublicationOutcome.FAILED)

            runCatching { if (staged.exists()) staged.delete() }
            if (outcome == PublicationOutcome.PUBLISHED) {
                runCatching { if (cacheFile.exists()) cacheFile.delete() }
            }
            return outcome != PublicationOutcome.FAILED
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
                            val progress = (bytesRead.toFloat() / contentLength.toFloat() * 100F).toInt()
                            onProgress.invoke(progress)
                        }
                    }
                })).build()
        }
    }
}

class RemoteFileException(data: String) : Exception(data)
