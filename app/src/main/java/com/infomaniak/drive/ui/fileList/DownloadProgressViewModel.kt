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
import com.infomaniak.drive.data.cache.OfflineFolderController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.DownloadOfflineFileManager
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.PublicationOutcome
import com.infomaniak.drive.utils.IOFile
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.launch
import okhttp3.Response

class DownloadProgressViewModel : ViewModel() {

    val downloadProgressLiveData = MutableLiveData(0)
    val localFile = SingleLiveEvent<File?>()

    fun getLocalFile(fileId: Int, userDrive: UserDrive) {
        localFile.value = FileController.getFileByUidOrId(fileId, userDrive)
    }

    fun downloadFile(context: Context, file: File, userDrive: UserDrive) = viewModelScope.launch(Dispatchers.IO) {
        val outputFile = file.getStoredFile(context, userDrive)
        if (outputFile == null) {
            downloadProgressLiveData.postValue(null)
            return@launch
        }

        if (file.isObsoleteOrNotIntact(outputFile)) {
            if ((file.isOffline || file.isInOfflineFolder) &&
                OfflineFolderController.decisionForReplacingOwnedCopy(file, outputFile, userDrive) ==
                OfflineFolderController.OwnedCopyReplacementDecision.KEEP_LOCAL
            ) {
                // The offline-owned copy backs an unsent upload (or its snapshot failed): it is
                // the usable local content and must not be bare-deleted (issue: round 7 #3).
                downloadProgressLiveData.postValue(PROGRESS_COMPLETE)
                return@launch
            }
            runCatching {
                val apiResponse = DownloadOfflineFileManager.downloadFileResponse(
                    fileUrl = ApiRoutes.downloadFile(file),
                    downloadInterceptor = DownloadOfflineFileManager.downloadProgressInterceptor { progress ->
                        downloadProgressLiveData.postValue(progress)
                    }
                )

                if (apiResponse.isSuccessful) {
                    saveData(context, file, outputFile, apiResponse, userDrive)
                } else {
                    downloadProgressLiveData.postValue(null)
                }
            }.cancellable().onFailure { exception ->
                if (exception !is java.io.IOException) SentryLog.e(TAG, "downloadFile failed", exception)
                downloadProgressLiveData.postValue(null)
            }
        } else {
            downloadProgressLiveData.postValue(PROGRESS_COMPLETE)
        }
    }

    private suspend fun saveData(context: Context, file: File, outputFile: IOFile, response: Response, userDrive: UserDrive) {
        // The final replacement goes through the coordinated staged publication (issue:
        // round 8 #2): the preflight check above ran before the request, so it cannot
        // authorize deleting a revision saved to the offline path in the meantime — the
        // publication re-checks ownership, revision, intent and cancellation under the
        // per-file locks. An offline-owned destination never holds partial or deleted bytes
        // while the download runs.
        val outcome = DownloadOfflineFileManager.publishDownloadedCopy(
            file = file,
            target = outputFile,
            userDrive = userDrive,
            isInheritedDownload = file.isInOfflineFolder && !file.isOffline,
            coordinated = file.isOffline || file.isInOfflineFolder,
        ) { staged ->
            if (!DownloadOfflineFileManager.saveRemoteData(TAG, response, staged)) {
                throw java.io.IOException("Remote data could not be saved")
            }
        }
        // A skipped transfer never modifies the retained destination, and the dialog does not
        // report an invalidated intent (pin/root removed mid-download) or a failure as a
        // completed download (issue: round 9 #1). Supersession is completion only when the
        // CURRENT row still owns the scoped destination (issue: round 11 #11): the offline
        // path is parent+name derived, so another file can occupy the obsolete location and
        // the retained bytes would be that file's — the live row must still hold the offline
        // intent and resolve to the same path; anything else defers (null), like an
        // invalidated intent.
        downloadProgressLiveData.postValue(
            when (outcome) {
                PublicationOutcome.SKIPPED_INVALIDATED, PublicationOutcome.FAILED -> null
                PublicationOutcome.SKIPPED_SUPERSEDED -> {
                    val liveOwnership = FileController.getRealmInstance(userDrive).use { realm ->
                        FileController.getFileById(realm, file.id)?.let { live ->
                            (live.isOffline || live.isInOfflineFolder) to
                                live.getOfflineFile(context, userDrive.userId)?.path
                        }
                    }
                    if (liveOwnership?.first == true && liveOwnership.second == outputFile.path && outputFile.exists()) {
                        PROGRESS_COMPLETE
                    } else {
                        null
                    }
                }
                else -> PROGRESS_COMPLETE
            }
        )
    }

    companion object {
        const val PROGRESS_COMPLETE = 100
        private const val TAG = "DownloadProgressDialog"
    }
}
