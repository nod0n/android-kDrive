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
package com.infomaniak.drive.ui.fileList

import android.content.Context
import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.infomaniak.core.common.cancellable
import com.infomaniak.core.legacy.utils.SingleLiveEvent
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.data.api.ApiRoutes
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.DownloadOfflineFileManager
import com.infomaniak.drive.utils.IOFile
import com.infomaniak.drive.utils.OfflineOpenData
import com.infomaniak.drive.utils.RootOfflineOpen
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import okhttp3.Response

class DownloadProgressViewModel internal constructor(
    private val offlineOpenData: OfflineOpenData,
    private val ioDispatcher: CoroutineDispatcher,
) : ViewModel() {
    constructor() : this(OfflineOpenData(), Dispatchers.IO)

    val downloadProgressLiveData = MutableLiveData(0)
    val localFile = SingleLiveEvent<File?>()

    fun getLocalFile(fileId: Int, userDrive: UserDrive) {
        localFile.value = FileController.getFileByUidOrId(fileId, userDrive)
    }

    fun downloadFile(context: Context, file: File, userDrive: UserDrive) = run {
        val captured = if (file.isOffline && !userDrive.sharedWithMe && !file.isPublicShared()) offlineOpenData.captureScope() else null
        viewModelScope.launch(ioDispatcher) {
            runCatching {
                val output = file.getStoredFile(context, userDrive) ?: throw java.io.IOException("Missing original path")
                val rootOpen = captured?.let { RootOfflineOpen(context, file.id, userDrive, output, offlineOpenData, it) }
                if (rootOpen?.existingCopy() != null || !file.isObsoleteOrNotIntact(output)) {
                    downloadProgressLiveData.postValue(PROGRESS_COMPLETE)
                    return@runCatching
                }
                DownloadOfflineFileManager.downloadFileResponse(
                    fileUrl = ApiRoutes.downloadFile(file),
                    downloadInterceptor = DownloadOfflineFileManager.downloadProgressInterceptor { progress ->
                        // Streaming completion is not publication completion.
                        downloadProgressLiveData.postValue(progress.coerceAtMost(PROGRESS_COMPLETE - 1))
                    }
                ).use { response ->
                    if (!response.isSuccessful) throw java.io.IOException("Original download failed")
                    if (rootOpen == null) {
                        saveData(file, output, response)
                    } else {
                        val staging = IOFile.createTempFile("open_staging_", null, output.absoluteFile.parentFile)
                        try {
                            if (!DownloadOfflineFileManager.saveRemoteData(TAG, response, staging)) throw java.io.IOException("Missing original body")
                            currentCoroutineContext().ensureActive()
                            staging.setLastModified(file.getLastModifiedInMilliSecond())
                            rootOpen.publish(staging)
                            downloadProgressLiveData.postValue(PROGRESS_COMPLETE)
                        } finally { staging.delete() }
                    }
                }
            }.cancellable().onFailure { exception ->
                if (exception !is java.io.IOException) SentryLog.e(TAG, "downloadFile failed", exception)
                downloadProgressLiveData.postValue(null)
            }
        }
    }

    private suspend fun saveData(file: File, outputFile: IOFile, response: Response) {
        if (outputFile.exists()) outputFile.delete()
        if (DownloadOfflineFileManager.saveRemoteData(TAG, response, outputFile)) {
            outputFile.setLastModified(file.getLastModifiedInMilliSecond())
            downloadProgressLiveData.postValue(PROGRESS_COMPLETE)
        }
    }

    companion object {
        const val PROGRESS_COMPLETE = 100
        private const val TAG = "DownloadProgressDialog"
    }
}
