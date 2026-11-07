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
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.services.BaseDownloadWorker
import com.infomaniak.drive.data.services.BulkDownloadWorker
import com.infomaniak.drive.data.services.DownloadWorker
import com.infomaniak.drive.utils.MediaUtils.isMedia
import com.infomaniak.drive.utils.NotificationUtils.downloadProgressNotification
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
    private val notificationManagerCompat: NotificationManagerCompat
) {
    private var lastDownloadedFile: IOFile? = null
    private var currentFile: File? = null
    private var filesDownloaded = 0

    /**
     * The with=hash identity captured from the RAW details response, before the row refresh
     * (whose preservation rules keep the previously accepted identity). This is the identity
     * of the body the transfer is about to fetch, and only the publication seam may record it.
     */
    private var acceptedServerHash: String? = null

    private var lastUpdateProgressMillis = System.currentTimeMillis()

    /** The row's validation-gate state: live offline intent and root coverage at one read. */
    private data class GateState(val liveIntent: Boolean, val rootCovered: Boolean)

    suspend fun execute(
        context: Context,
        file: File,
        onProgress: (progress: Int, fileId: Int) -> Unit,
    ): ListenableWorker.Result {

        // Stale-work gate FIRST, before any filesystem effect: this worker only ever
        // downloads for live offline intent. If the current row carries none — the root was
        // unmarked, the pin withdrawn, a release applied — any queued work (initial bulk
        // chain, retry, a download enqueued before an unmark) is stale: no cache move, no
        // delete-then-transfer, no resurrection of a preserved copy.
        // ROOT COVERAGE IS CAPTURED HERE, at the validation gate: a real remote move (or
        // unknown parent metadata) during the details refresh below rewrites the row's
        // parent, so coverage recomputed AFTER the refresh can silently vanish — queued
        // root-owned work must then keep the root contract (the staged preservation path),
        // never fall to the destructive baseline. Direct pins have no root coverage here and
        // keep their baseline behavior unchanged.
        val gate = FileController.getRealmInstance(userDrive).use { realm ->
            FileController.getFileProxyById(file.id, customRealm = realm)?.let { row ->
                GateState(
                    liveIntent = OfflineFolderSyncUtils.hasLiveOfflineIntent(row, realm, userDrive),
                    rootCovered = OfflineFolderSyncUtils.isRootCoveredCopy(row, realm, userDrive),
                )
            }
        }
        if (gate == null || !gate.liveIntent) {
            SentryLog.d(TAG, "Stale offline download skipped for ${file.id}: no live offline intent")
            return ListenableWorker.Result.success()
        }

        val file = getFileFromRemote(context, file.id, userDrive)
        currentFile = file
        val cacheFile = file.getCacheFile(context, userDrive)
        val offlineFile = file.getOfflineFile(context, userDrive.userId)
        if (offlineFile == null) {
            SentryLog.e(TAG, "offline must not be null")
            return ListenableWorker.Result.failure()
        }

        // Root coverage is checked FIRST: the cache-move and intact branches below would
        // replace existing bytes, and a covered copy's lifecycle belongs to the root
        // contract — a stale queued download (initial bulk work, retry, or a download
        // enqueued before an unmark) must never reach them. The root maintenance pass
        // refreshes covered copies through the staged path instead. Coverage honored here is
        // the UNION of the refresh-time walk and the gate-time capture: the gate capture is
        // what survives a real remote move that rewrote the row's parent mid-flight.
        val coveredByRoot = FileController.getRealmInstance(userDrive).use { realm ->
            OfflineFolderSyncUtils.isRootCoveredCopy(file, realm, userDrive)
        } || gate.rootCovered
        if (offlineFile.exists() && coveredByRoot) {
            // The preserved bytes are NOT blessed with the new remote identity: no accepted
            // transfer happened here, so no publication evidence may be recorded.
            filesDownloaded += 1
            lastDownloadedFile = offlineFile
            return ListenableWorker.Result.success()
        }

        if (moveCacheFileIfIntact(context, file, cacheFile, offlineFile)) return ListenableWorker.Result.success()

        if (file.isOfflineAndIntact(offlineFile)) {
            // We can have this case for example when we try to put a lot of files at once in offline mode
            // and for some reason, the worker is cancelled after a long time, the worker is restarted.
            // Metadata equality is NOT proof of equal content: the existing bytes are preserved
            // untouched and no publication evidence is recorded for them — the root maintenance
            // pass verifies copies against the accepted basis.
            filesDownloaded += 1
            lastDownloadedFile = offlineFile
            return ListenableWorker.Result.success()
        }

        if (coveredByRoot) {
            // A covered copy never reaches the destructive path below (delete the bytes at
            // the final path, then stream a replacement in place): its initial download
            // publishes through the staged path instead — a cancellation discards owned
            // staging, and a withdrawal mid-transfer skips the atomic rename entirely
            // instead of deleting bytes at the final path after the fact. The captured
            // details identity describes the body this transfer fetches. Notification
            // progress parity is not kept here (the maintenance pass has none either); the
            // bulk chain's onFinish finalizes pending markers and the notification. The
            // gate-captured coverage keeps a root-owned copy here even when a real remote
            // move rewrote its parent during the details refresh: an edited or unknown
            // copy is then preserved by the staged decision, never destroyed.
            val outcome = runCatching {
                OfflineFolderSyncUtils.stagedRedownload(context, userDrive, file, establishedServerHash = acceptedServerHash)
            }.cancellable().onFailure { exception ->
                SentryLog.e(TAG, "Staged covered download failed for ${file.id}", exception)
            }.getOrDefault(OfflineFolderSyncUtils.StagedPublication.FAILED)
            return when (outcome) {
                OfflineFolderSyncUtils.StagedPublication.FAILED -> ListenableWorker.Result.failure()
                OfflineFolderSyncUtils.StagedPublication.PUBLISHED,
                OfflineFolderSyncUtils.StagedPublication.ADOPTED_METADATA_ONLY -> {
                    fileDownloaded(context, file.id)
                    ListenableWorker.Result.success()
                }
                OfflineFolderSyncUtils.StagedPublication.INTENT_WITHDRAWN,
                OfflineFolderSyncUtils.StagedPublication.PRESERVED_EXISTING -> {
                    filesDownloaded += 1
                    ListenableWorker.Result.success()
                }
            }
        }

        onProgress(0, file.id)

        if (offlineFile.exists()) offlineFile.delete()
        if (cacheFile.exists()) cacheFile.delete()

        lastDownloadedFile = offlineFile

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

    fun cleanLastDownloadedFile() {
        val file = currentFile ?: return
        // The worker's cleanup may only dispose of bytes from its own unaccepted transfer.
        // With no live offline intent left (unmark, release, unpin), the copy at the offline
        // path belongs to the user contract — preserved edited/unknown bytes — or was already
        // cleaned by the publication seam: the worker must not delete anything then, even
        // when a bulk chain's onFinish cleared the pending marker first.
        val liveIntent = FileController.getRealmInstance(userDrive).use { realm ->
            FileController.getFileProxyById(file.id, customRealm = realm)
                ?.let { OfflineFolderSyncUtils.hasLiveOfflineIntent(it, realm, userDrive) } == true
        }
        if (!liveIntent) return
        // A root-covered copy is never cleaned up here: its lifecycle belongs to the root
        // contract, which releases coverage without deleting bytes.
        val coveredByRoot = FileController.getRealmInstance(userDrive).use { realm ->
            OfflineFolderSyncUtils.isRootCoveredCopy(file, realm, userDrive)
        }
        if (coveredByRoot) return
        lastDownloadedFile?.let {
            if (it.exists() && currentFile?.isIntactFile(it) == false) it.delete()
        }
    }

    private suspend fun moveCacheFileIfIntact(
        context: Context,
        file: File,
        cacheFile: IOFile,
        offlineFile: IOFile,
    ): Boolean = withContext(Dispatchers.IO) {
        if (!file.isObsoleteOrNotIntact(cacheFile)) {
            Utils.moveCacheFileToOffline(file, cacheFile, offlineFile)
            // The moved bytes come from the cache, not from this transfer: their content is
            // NOT proven to match the current remote identity (metadata equality is not proof
            // of equal content), so the seam records the basis WITHOUT the server hash — the
            // next maintenance pass then performs one verified body fetch and completes it.
            notifyCompleted(file, offlineFile, context, acceptedServerHash = null)
            lastDownloadedFile = offlineFile
            return@withContext true
        }
        return@withContext false
    }

    private suspend fun getFileFromRemote(
        context: Context,
        fileId: Int,
        userDrive: UserDrive = UserDrive(),
    ): File {
        // with=hash is requested on the details call that drives the download, so the
        // publication seam can record the server content identity at no extra request. The
        // hash is captured from the RAW response: the row refresh below preserves the
        // previously accepted identity by design.
        val fileDetails = Dispatchers.IO {
            ApiRepository.getFileDetailsWithHash(File(id = fileId, driveId = userDrive.driveId))
        }
        val remoteFile = fileDetails.data
        val file = if (fileDetails.isSuccess() && remoteFile != null) {
            acceptedServerHash = remoteFile.serverContentHash
            FileController.getRealmInstance(userDrive).use { realm ->
                FileController.updateExistingFile(newFile = remoteFile, realm = realm)
            }
            remoteFile
        } else {
            if (fileDetails.error?.exception is NetworkException) throw UploadTask.NetworkException()

            val translateError = fileDetails.translateError()
            val responseGsonType = object : TypeToken<ApiResponse<File>>() {}.type
            val translatedErrorText = context.getString(translateError)
            val responseJson = ApiController.gson.toJson(fileDetails, responseGsonType)
            throw RemoteFileException("$responseJson $translatedErrorText")
        }

        return file
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
        makeSureFileExists(offlineFile) ?: return@withContext ListenableWorker.Result.failure()

        runCatching {
            val remoteDataHasBeenSaved = saveRemoteData(downloadWorker.workerTag(), response, offlineFile)

            if (remoteDataHasBeenSaved && offlineFile.exists()) {
                onProgress(100, file.id)
                notifyCompleted(file, offlineFile, context, acceptedServerHash)
                ListenableWorker.Result.success()
            } else {
                ListenableWorker.Result.failure()
            }
        }.cancellable().getOrElse {
            offlineFile.delete()
            throw it
        }
    }

    /**
     * The freshly accepted bytes are on disk: the publication seam records the basis (it also
     * sets [File.isOffline], under its live-intent gate — an unpin or release that happened
     * during the transfer is not overridden). When the intent was withdrawn mid-transfer, the
     * just-written bytes are unwanted and unrecorded: they are removed, so no unrecorded copy
     * lingers at the offline path and the unmark's post-condition (copy gone) is restored.
     * The captured with=hash identity describes THESE bytes, because they were just fetched
     * from the server; the cache-move path passes null (unknown identity).
     */
    private suspend fun notifyCompleted(
        file: File,
        offlineFile: java.io.File,
        context: Context,
        acceptedServerHash: String?,
    ) {
        offlineFile.setLastModified(file.getLastModifiedInMilliSecond())
        val outcome = OfflineFolderSyncUtils.recordAcceptedPublication(userDrive, file, offlineFile, acceptedServerHash)
        if (outcome == OfflineFolderSyncUtils.PublicationOutcome.INTENT_WITHDRAWN) {
            runCatching { if (offlineFile.exists()) offlineFile.delete() }
                .onFailure { SentryLog.e(TAG, "Could not clean up the withdrawn offline copy of ${file.id}", it) }
        }
        if (file.isMedia()) MediaUtils.scanFile(context, offlineFile)
        fileDownloaded(context, file.id)
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
        FileController.markFileAsOffline(fileId = fileId, isMarkedAsOffline = false)
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

        fun getFailedDownloadWorkerOffline(context: Context) = WorkManager.getInstance(context).getWorkInfosLiveData(
            WorkQuery.Builder
                .fromUniqueWorkNames(arrayListOf(BulkDownloadWorker.TAG, DownloadWorker.TAG))
                .addStates(arrayListOf(WorkInfo.State.FAILED))
                .build()
        )

        suspend fun isBulkDownloadWorkerRunning(context: Context): Boolean {
            val workQuery = WorkQuery.Builder
                .fromUniqueWorkNames(arrayListOf(BulkDownloadWorker.TAG))
                .addStates(arrayListOf(WorkInfo.State.RUNNING, WorkInfo.State.ENQUEUED, WorkInfo.State.BLOCKED))
                .build()
            val workInfoList = WorkManager.getInstance(context).getWorkInfos(workQuery).await()

            return workInfoList.isNotEmpty() && workInfoList.first().state == WorkInfo.State.RUNNING
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
