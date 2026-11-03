/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2024-2026 Infomaniak Network SA
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
package com.infomaniak.drive.data.services

import android.content.Context
import androidx.work.WorkerParameters
import androidx.work.workDataOf
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.R
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.cache.OfflineFolderController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UiSettings
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.DownloadOfflineFileManager
import com.infomaniak.drive.utils.NotificationUtils.BULK_DOWNLOAD_ID
import com.infomaniak.drive.utils.NotificationUtils.cancelNotification
import com.infomaniak.drive.utils.OfflineFolderUtils

class BulkDownloadWorker(context: Context, workerParams: WorkerParameters) : BaseDownloadWorker(context, workerParams) {

    private val folderId: Int by lazy { inputData.getInt(FOLDER_ID, 0) }
    private val recursive: Boolean by lazy { inputData.getBoolean(RECURSIVE, false) }

    /**
     * In recursive mode this is only known once the remote walk has run, so it is filled by
     * [downloadAction] before the download loop starts.
     */
    private var files: List<File> = emptyList()
    private var walkCompleted = false

    private val userDrive: UserDrive by lazy {
        UserDrive(
            userId = inputData.getInt(USER_ID, AccountUtils.currentUserId),
            driveId = inputData.getInt(DRIVE_ID, AccountUtils.currentDriveId)
        )
    }
    private val downloadOfflineFileManager by lazy {
        DownloadOfflineFileManager(
            userDrive,
            filesCount = files.size,
            downloadWorker = this,
            notificationManagerCompat
        )
    }

    override fun downloadNotification(): DownloadNotification {
        return DownloadNotification(
            titleResId = R.string.bulkDownloadNotificationTitleWithProgress,
            contentResId = R.plurals.bulkDownloadNotificationContent,
            id = BULK_DOWNLOAD_ID,
            notification = downloadProgressNotification
        )
    }

    override suspend fun downloadAction(): Result {
        if (recursive) {
            val walkResult = OfflineFolderUtils.walkAndMarkOfflineFolder(
                context = applicationContext,
                rootFolderId = folderId,
                userDrive = userDrive,
            )
            walkCompleted = walkResult.completed
            if (!walkResult.rootExists) return Result.success()

            files = walkResult.fileIds.mapNotNull { FileController.getFileByUidOrId(it, userDrive) }

            val sizeOfDownload = files.sumOf { it.size ?: 0L }
            if (!hasEnoughSpaceLeftAfterDownload(sizeOfDownload)) {
                SentryLog.i(
                    TAG,
                    "After the download of the folder, the device storage would have been below 500 MB so we cancel the download."
                )
                return Result.failure(getNotEnoughSpaceOutputData())
            }
        } else {
            files = FileController.getFolderOfflineFilesId(folderId = folderId, sortType = UiSettings(applicationContext).sortType)
                .mapNotNull { FileController.getFileByUidOrId(it, userDrive) }
        }
        return downloadFiles()
    }

    override fun onFinish() {
        FileController.markFilesAsOffline(filesId = files.map { it.id }, isMarkedAsOffline = false)
        if (recursive) {
            // The root folder itself carries the "walk in progress" marker.
            FileController.markFilesAsOffline(filesId = listOf(folderId), isMarkedAsOffline = false)
            if (walkCompleted) OfflineFolderController.updateLastSyncedAt(folderId, userDrive)
        }
        clearLastDownloadedFile()
        notifyDownloadFinished()
        notificationManagerCompat.cancel(BULK_DOWNLOAD_ID)
    }

    override fun isForOneFile() = false

    override fun workerTag() = TAG

    override val notificationTitle = context.getString(R.string.bulkDownloadNotificationTitleNoProgress)

    override val notificationId = BULK_DOWNLOAD_ID

    override fun getSizeOfDownload() = files.sumOf { it.size ?: 0L }

    private fun clearLastDownloadedFile() {
        downloadOfflineFileManager.cleanLastDownloadedFile()
    }

    private suspend fun downloadFiles(): Result {
        var result = Result.failure()

        files.forEach { file ->
            result = downloadOfflineFileManager.execute(applicationContext, file) { progress, downloadedFileId ->
                setProgressAsync(workDataOf(PROGRESS to progress, FILE_ID to downloadedFileId))
            }
        }

        if (result == Result.success()) {
            applicationContext.cancelNotification(BULK_DOWNLOAD_ID)
        }

        return result
    }

    companion object {
        const val TAG = "BulkDownloadWorker"
        const val FOLDER_ID = "folder_id"
        const val RECURSIVE = "recursive"
    }
}
