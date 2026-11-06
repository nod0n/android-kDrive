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
import android.util.Log
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
import com.infomaniak.drive.utils.OfflineFileCoordination
import com.infomaniak.drive.utils.OfflineFolderUtils
import java.util.Date

class BulkDownloadWorker(context: Context, workerParams: WorkerParameters) : BaseDownloadWorker(context, workerParams) {

    private val folderId: Int by lazy { inputData.getInt(FOLDER_ID, 0) }
    private val recursive: Boolean by lazy { inputData.getBoolean(RECURSIVE, false) }

    /**
     * In recursive mode this is only known once the remote walk has run, so it is filled by
     * [downloadAction] before the download loop starts.
     */
    private var files: List<File> = emptyList()
    private var walkCompleted = false
    private var synchronizationSucceeded = false

    /**
     * The walk's result, retained for [onFinish]'s root completion (issue: round 18 #20):
     * the root marker and lastSyncedAt advance only when the root's intent generation is
     * unchanged since the walk captured it BEFORE its first request.
     */
    private var walkResult: OfflineFolderUtils.WalkResult? = null

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
            notificationManagerCompat = notificationManagerCompat,
            isInheritedDownload = recursive,
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
            val result = OfflineFolderUtils.walkAndMarkOfflineFolder(
                context = applicationContext,
                rootFolderId = folderId,
                userDrive = userDrive,
            )
            walkResult = result
            walkCompleted = result.completed
            if (!result.rootExists) {
                synchronizationSucceeded = true // the root was removed: nothing left to synchronize
                return Result.success()
            }

            // Safe partial progress (issue: round 6 #10): the files already collected by the
            // completed part of the walk are downloaded even when the overall walk is
            // incomplete (a failed folder listing or deferred moves); the root outcome stays
            // incomplete below, so the remaining folders and moves are retried without
            // advancing lastSyncedAt.
            files = result.fileIds.mapNotNull { FileController.getFileByUidOrId(it, userDrive) }
        } else {
            files = FileController.getFolderOfflineFilesId(
                folderId = folderId,
                sortType = UiSettings(applicationContext).sortType,
                userDrive = userDrive,
            ).mapNotNull { FileController.getFileByUidOrId(it, userDrive) }
        }

        // Perform the size check with the actual list in both branches: the base pre-check runs
        // before the list is loaded and would otherwise always pass.
        val sizeOfDownload = files.sumOf { it.size ?: 0L }
        if (!hasEnoughSpaceLeftAfterDownload(sizeOfDownload)) {
            SentryLog.i(
                TAG,
                "After the download of file(s), the device storage would have been below 500 MB so we cancel the download."
            )
            return Result.failure(getNotEnoughSpaceOutputData())
        }

        val downloadResult = downloadFiles()
        // Traversal completeness is a RECURSIVE-mode property (issue: round 7 #10): explicit-pin
        // batches have no walk, and gating them on walkCompleted returned retry for successful
        // batches — their APPEND_OR_REPLACE chains never completed normally and successors
        // stayed blocked behind retries.
        val traversalSucceeded = !recursive || walkCompleted
        // An incomplete walk never reports success: the root's walk-in-progress marker stays
        // and lastSyncedAt does not advance (issue: round 6 #10).
        synchronizationSucceeded = downloadResult == Result.success() && traversalSucceeded
        return if (traversalSucceeded) downloadResult else Result.retry()
    }

    override fun onFinish() {
        // FILE-MARKER clearing is NOT repeated here (issue: round 18 #20): the batch-end
        // unconditional clear erased markers belonging to NEWER work — F can finish early,
        // a newer request re-marks F pending while the batch processes another file, and
        // the old batch then cleared that marker without any generation comparison. Each
        // transfer consumes its OWN marker at completion (the manager's paths clear
        // conditionally on the row still being at the generation the transfer refreshed
        // against); a superseded transfer's marker belongs to the newer transfer and is
        // excluded upstream.
        if (recursive) {
            // The root folder itself carries the "walk in progress" marker; it is only cleared
            // for a successful synchronization so unfinished work keeps its pending state
            // (issue #1902). An explicit removal clears it through the pending-removal path.
            // ROOT COMPLETION MATCHES THE PROCESSED INTENT GENERATION (issue: round 18
            // #20), and the GENERATION CHECK + COMPLETION RUN IN ONE MONITORED FRESH
            // TRANSACTION (issue: round 19 #24): the previous check ran OUTSIDE any
            // coordination and the marker clear and the lastSyncedAt write ran as
            // SEPARATE operations — a root removal or revival could interleave between
            // the check and the effects, unmarking (or advancing) a root whose intent
            // this walk no longer matches. The check and BOTH effects now run under the
            // intent monitor and the root's file lock, on a refreshed Realm, in ONE
            // transaction; a concurrent intent writer cannot interleave.
            val intentGeneration = walkResult?.intentGeneration ?: 0L
            if (synchronizationSucceeded) {
                synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    OfflineFileCoordination.withFileLock(userDrive, folderId) {
                        FileController.getRealmInstance(userDrive).use { realm ->
                            runCatching { realm.refresh() }
                            if (!OfflineFolderController.rootIntentChangedSince(userDrive, folderId, intentGeneration)) {
                                realm.executeTransaction {
                                    val root = FileController.getFileById(realm, folderId)
                                    if (root != null && root.isValid) {
                                        if (root.isMarkedAsOffline) root.apply {
                                            isMarkedAsOffline = false
                                            bumpOfflineIntentGeneration()
                                        }
                                    }
                                    // ACTUAL MODEL IDENTITY (round-19 build fix): lastSyncedAt
                                    // lives on the DURABLE OfflineRoot record, not on the File
                                    // row — updateLastSyncedAt's own implementation reads it
                                    // through getOfflineRoot in this same realm.
                                    if (walkCompleted) {
                                        val rootRecord = OfflineFolderController.getOfflineRoot(folderId, customRealm = realm)
                                        if (rootRecord != null && rootRecord.isValid) rootRecord.lastSyncedAt = Date()
                                    }
                                }
                            } else {
                                SentryLog.i(TAG, "Root $folderId intent changed during the walk; completion deferred to the newer walk")
                            }
                        }
                    }
                }
            }
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
        if (files.isEmpty()) {
            // Completed walk with nothing to download: a successful synchronization.
            applicationContext.cancelNotification(BULK_DOWNLOAD_ID)
            return Result.success()
        }

        var allFilesSucceeded = true

        files.forEach { file ->
            // Validate effective coverage before starting each download: unmarking the root
            // must not leave this batch converting the remaining files into explicit pins. A
            // root pending removal no longer validates coverage (issue #1902).
            if (recursive && !OfflineFolderController.hasActiveOfflineRoot(folderId, userDrive)) {
                SentryLog.i(TAG, "Offline root $folderId was removed mid-batch, stopping the remaining downloads")
                allFilesSucceeded = false
                return@forEach
            }

            val result = downloadOfflineFileManager.execute(applicationContext, file) { progress, downloadedFileId ->
                setProgressAsync(workDataOf(PROGRESS to progress, FILE_ID to downloadedFileId))
            }
            if (result == Result.success()) {
                if (downloadOfflineFileManager.lastTransferSuperseded) {
                    // A superseded generation's replacement is still outstanding (issue: round
                    // 9 #8): its pending marker belongs to the newer transfer, so it is
                    // excluded from completion, and the batch must not report the root
                    // synchronized while that replacement runs.
                    // THE RESULT-GATE BREADCRUMB (the post-chain triage, 2026-11-06): the
                    // retained incompleteness is named per file — stage/status only.
                    Log.i(TAG, "stage: file ${file.id} retained the root's incompleteness (unresolved/superseded outcome)")
                    allFilesSucceeded = false
                }
                // Ordinary success (including a protected-copy skip whose own marker the
                // publication cleared under the locks — issue: round 18 #20): the transfer
                // consumed its own marker; the batch records nothing.
            } else {
                Log.i(TAG, "stage: file ${file.id} transfer returned failure")
                allFilesSucceeded = false
            }
        }

        if (allFilesSucceeded) {
            applicationContext.cancelNotification(BULK_DOWNLOAD_ID)
            return Result.success()
        }
        return Result.failure()
    }

    companion object {
        const val TAG = "BulkDownloadWorker"
        const val FOLDER_ID = "folder_id"
        const val RECURSIVE = "recursive"

        /**
         * Independently deduplicated and cancellable work identities per account, drive and root
         * (issue #1902), SEPARATE per mode (issue: round 4 #13): a recursive walk never suppresses
         * or absorbs an explicit-pin batch and vice versa, and two accounts sharing a drive never
         * share a chain.
         */
        fun walkWorkName(userDrive: UserDrive, folderId: Int) =
            "${TAG}_walk_${userDrive.userId}_${userDrive.driveId}_$folderId"

        fun pinWorkName(userDrive: UserDrive, folderId: Int) =
            "${TAG}_pin_${userDrive.userId}_${userDrive.driveId}_$folderId"
    }
}
