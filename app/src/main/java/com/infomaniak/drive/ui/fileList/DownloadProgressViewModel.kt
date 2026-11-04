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
import android.net.Uri
import java.util.Date
import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.infomaniak.core.common.cancellable
import com.infomaniak.core.legacy.utils.SingleLiveEvent
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.data.api.ApiRoutes
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.cache.OfflineFolderController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.StoredOfflineCopyState
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.DownloadOfflineFileManager
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.PublicationOutcome
import com.infomaniak.drive.utils.IOFile
import com.infomaniak.drive.utils.OfflineFileCoordination
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

        // SHARED current-content validation at entry (issues: round 17 #13/#14/#15): an
        // offline-owned destination answers through its three distinct copy states —
        // PROVEN current completes, READABLE-but-unverified completes with verification
        // scheduled (legacy access preserved, never falsely current), ABSENT/STALE downloads.
        if (file.isOffline || file.isInOfflineFolder) {
            when (file.storedOfflineCopyState(context, outputFile, userDrive)) {
                StoredOfflineCopyState.PROVEN_CURRENT -> {
                    downloadProgressLiveData.postValue(PROGRESS_COMPLETE)
                    return@launch
                }
                StoredOfflineCopyState.READABLE_UNVERIFIED -> {
                    OfflineFolderController.scheduleOfflineVerification(
                        fileId = file.id,
                        userDrive = userDrive,
                        observedGeneration = outputFile.lastModified() / 1_000L,
                    )
                    downloadProgressLiveData.postValue(PROGRESS_COMPLETE)
                    return@launch
                }
                // POSITIVELY FOREIGN provenance (issue: round 19 #12): ownership precedes
                // completion — the dialog never reports 100 over another identity's bytes.
                // The download is attempted; the coordinated publication reports the
                // conflict (null progress), preserving the foreign copy.
                StoredOfflineCopyState.FOREIGN_CONFLICT -> Unit
                StoredOfflineCopyState.ABSENT_OR_STALE -> Unit
            }
        } else if (!file.isObsoleteOrNotIntact(outputFile)) {
            downloadProgressLiveData.postValue(PROGRESS_COMPLETE)
            return@launch
        }
        if (file.isObsoleteOrNotIntact(outputFile) || file.isOffline || file.isInOfflineFolder) {
            // PRESERVATION-REQUIRED IS NOT USABLE-COMPLETE (issue: round 22 #7): KEEP_LOCAL
            // certifies preservation, not usability — a pending record after the original
            // disappeared, a failed snapshot registration, or unproven historical content
            // must never complete the dialog over missing, unreadable or truncated bytes.
            // The completion validates readability and completeness against the retained
            // generation's available evidence (an unknown row size stays conservative);
            // otherwise the data is retained and the transfer defers through the
            // coordinated publication (null progress, bytes kept).
            val retainedUsable = outputFile.exists() &&
                UploadFile.sha256OrNull(outputFile) != null &&
                (file.size == null || outputFile.length() == file.size)
            if ((file.isOffline || file.isInOfflineFolder) && retainedUsable &&
                OfflineFolderController.decisionForReplacingOwnedCopy(file, outputFile, userDrive) ==
                OfflineFolderController.OwnedCopyReplacementDecision.KEEP_LOCAL
            ) {
                // The offline-owned copy backs an unsent upload (or its snapshot failed): it is
                // the usable local content and must not be bare-deleted (issue: round 7 #3).
                downloadProgressLiveData.postValue(PROGRESS_COMPLETE)
                return@launch
            }
            runCatching {
                // The DIALOG's captured account client BEFORE the request (issue: round
                // 21 #16): the publication below runs in the supplied UserDrive's scope —
                // the previous default global client authenticated the request against the
                // CURRENTLY ACTIVE account instead.
                val okHttpClient = AccountUtils.getHttpClient(userDrive.userId)
                val apiResponse = DownloadOfflineFileManager.downloadFileResponse(
                    fileUrl = ApiRoutes.downloadFile(file),
                    okHttpClient = okHttpClient,
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
            context = context,
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
                // An unresolved generation is never a completed download (issue: round 20
                // #14): the bytes were published, but no generation proof exists — the
                // dialog reports no 100 over them.
                PublicationOutcome.SKIPPED_INVALIDATED, PublicationOutcome.FAILED,
                PublicationOutcome.SKIPPED_CONFLICT, PublicationOutcome.PUBLISHED_UNRESOLVED,
                -> null
                PublicationOutcome.SKIPPED_SUPERSEDED -> {
                    // Supersession is completion only when the CURRENT row still owns the
                    // scoped destination AND that destination holds usable content (issue:
                    // round 12 #11): metadata ownership alone accepts an obsolete retained
                    // copy. Validation runs under the shared protocol (issue: round 13 #11):
                    // intent monitor → file lock → refreshed row. An EXACT current clean
                    // baseline (equality with the row's own modification time and size —
                    // "not older" accepts retained bytes newer than a backward-dated newer
                    // baseline) or a positively established protected local edit; anything
                    // else defers (null), like an invalidated intent.
                    val usable = synchronized(OfflineFileCoordination.rootIntentMonitor) {
                        OfflineFileCoordination.withFileLock(userDrive, file.id) {
                            val liveRow = FileController.getRealmInstance(userDrive).use { realm ->
                                runCatching { realm.refresh() }
                                // DEPTH-ONE detached copy (issue: round 17 #14): a depth-zero copy discards the
                                // embedded version and converts a KNOWN mismatching
                                // generation into the permissive unknown-version case.
                                FileController.getFileById(realm, file.id)?.let { live -> realm.copyFromRealm(live, 1) }
                            }
                            val livePath = liveRow?.getOfflineFile(context, userDrive.userId)?.path
                            val targetUri = Uri.fromFile(outputFile).toString()
                            liveRow != null &&
                                (liveRow.isOffline || liveRow.isInOfflineFolder) &&
                                livePath == outputFile.path &&
                                outputFile.exists() &&
                                // Exact current clean baseline, or PRE-EXISTING durable
                                // upload ownership. READ-ONLY inspection (issue: round 14
                                // #4): the mutating replacement decision would classify the
                                // old generation's clean bytes as an edit against the
                                // replacement baseline and manufacture an upload that can
                                // restore old content over the newer generation.
                                (liveRow.isOfflineAndProvenCurrent(context, outputFile, userDrive) ||
                                    UploadFile.hasPendingUpload(uri = targetUri) ||
                                    UploadFile.hasRetainedUnsentUpload(uri = targetUri) ||
                                    // Durable SNAPSHOT registrations own the original
                                    // revision under unique uris the uri queries cannot
                                    // see (issue: round 15 #7). A FAILED hash/read is
                                    // unresolved, not ownership (issue: round 21 #12).
                                    UploadFile.sha256OrNull(outputFile)?.let { currentHash ->
                                        UploadFile.hasDurableOriginalRevisionOwnership(
                                            fileName = liveRow.name,
                                            remoteFolder = liveRow.parentId,
                                            driveId = userDrive.driveId,
                                            userId = userDrive.userId,
                                            fileModifiedAt = Date(outputFile.lastModified()),
                                            fileSize = outputFile.length(),
                                            contentHash = currentHash,
                                            originalFileId = liveRow.id,
                                        )
                                    } == true)
                        }
                    }
                    if (usable) PROGRESS_COMPLETE else null
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
