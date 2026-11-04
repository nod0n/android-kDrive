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
package com.infomaniak.drive.utils

import android.app.Dialog
import android.content.ActivityNotFoundException
import android.content.ComponentName
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.view.LayoutInflater
import androidx.activity.result.ActivityResultLauncher
import androidx.annotation.DrawableRes
import androidx.annotation.StringRes
import androidx.appcompat.app.AlertDialog
import androidx.core.content.ContextCompat
import androidx.core.net.toUri
import androidx.core.os.bundleOf
import androidx.core.view.isGone
import androidx.core.view.isVisible
import androidx.core.widget.doOnTextChanged
import androidx.lifecycle.liveData
import androidx.navigation.NavController
import androidx.navigation.NavOptions
import androidx.work.Constraints
import androidx.work.ExistingWorkPolicy
import androidx.work.NetworkType
import androidx.work.OneTimeWorkRequestBuilder
import androidx.work.Operation
import androidx.work.WorkManager
import androidx.work.workDataOf
import androidx.concurrent.futures.await
import com.google.android.material.dialog.MaterialAlertDialogBuilder
import com.infomaniak.core.common.utils.DownloadManagerUtils
import com.infomaniak.core.legacy.utils.showKeyboard
import com.infomaniak.core.legacy.utils.showToast
import com.infomaniak.drive.R
import com.infomaniak.drive.data.cache.DriveInfosController
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.cache.OfflineFolderController
import com.infomaniak.drive.data.models.AppSettings
import com.infomaniak.drive.data.models.BulkOperationType
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.File.Companion.getCloudAndFileUris
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.services.BaseDownloadWorker
import com.infomaniak.drive.data.services.BulkDownloadWorker
import com.infomaniak.drive.data.services.DownloadWorker
import com.infomaniak.drive.databinding.DialogDownloadProgressBinding
import com.infomaniak.drive.databinding.DialogNamePromptBinding
import com.infomaniak.drive.ui.MainViewModel
import com.infomaniak.drive.ui.MainViewModel.FileResult
import com.infomaniak.drive.ui.fileList.FolderNavigationRestrictions
import com.infomaniak.drive.ui.fileList.SelectFolderActivity
import com.infomaniak.drive.ui.fileList.SelectFolderActivityArgs
import com.infomaniak.drive.ui.fileList.multiSelect.MultiSelectFragment
import com.infomaniak.drive.ui.fileList.preview.PreviewPDFActivity
import com.infomaniak.drive.ui.fileList.preview.PreviewSliderFragmentArgs
import com.infomaniak.drive.utils.SyncUtils.uploadFolder
import com.infomaniak.drive.views.FileInfoActionsView.Companion.SINGLE_OPERATION_CUSTOM_TAG
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import java.util.Date
import kotlin.math.pow

object Utils {

    const val ROOT_ID = 1
    const val OTHER_ROOT_ID = -1

    const val INDETERMINATE_PROGRESS = -1

    fun createConfirmation(
        context: Context,
        title: String,
        message: String? = null,
        buttonText: String? = null,
        autoDismiss: Boolean = true,
        isDeletion: Boolean = false,
        onConfirmation: (dialog: Dialog) -> Unit
    ): AlertDialog {
        val style = if (isDeletion) R.style.DeleteDialogStyle else R.style.DialogStyle
        val dialog = MaterialAlertDialogBuilder(context, style)
            .setTitle(title)
            .setMessage(message)
            .setPositiveButton(buttonText ?: context.getString(R.string.buttonConfirm)) { _, _ -> }
            .setNegativeButton(R.string.buttonCancel) { _, _ -> }
            .setCancelable(false)
            .show()

        dialog.getButton(AlertDialog.BUTTON_POSITIVE).setOnClickListener {
            onConfirmation(dialog)
            if (autoDismiss) {
                dialog.dismiss()
            } else {
                it.isEnabled = false
                dialog.getButton(AlertDialog.BUTTON_NEGATIVE).isEnabled = false
            }
        }
        return dialog
    }

    fun confirmFileDeletion(
        context: Context,
        fileName: String? = null,
        fileCount: Int = 1,
        fromTrash: Boolean = false,
        onConfirmation: (dialog: Dialog) -> Unit
    ) {
        val title: Int = if (fromTrash) R.string.modalDeleteTitle else R.string.modalMoveTrashTitle
        val message: String = context.resources.getQuantityString(
            if (fromTrash) R.plurals.modalDeleteDescription
            else R.plurals.modalMoveTrashDescription,
            fileCount,
            fileName ?: fileCount
        )
        val button: Int = if (fromTrash) R.string.buttonDelete else R.string.buttonMove

        createConfirmation(
            context = context,
            title = context.getString(title),
            message = message,
            autoDismiss = false,
            isDeletion = true,
            buttonText = context.getString(button),
            onConfirmation = onConfirmation
        )
    }

    fun createPromptNameDialog(
        context: Context,
        @StringRes title: Int,
        @StringRes fieldName: Int,
        @StringRes positiveButton: Int,
        @DrawableRes iconRes: Int? = null,
        fieldValue: String? = null,
        selectedRange: Int? = null,
        onPositiveButtonClicked: (dialog: Dialog, body: String) -> Unit
    ) {
        val promptLayoutBinding = DialogNamePromptBinding.inflate(LayoutInflater.from(context))
        val nameEditText = promptLayoutBinding.nameEditText
        promptLayoutBinding.nameLayout.setHint(fieldName)

        iconRes?.let {
            promptLayoutBinding.icon.apply {
                setImageDrawable(ContextCompat.getDrawable(context, it))
                isVisible = true
            }
        }

        val dialog = MaterialAlertDialogBuilder(context, R.style.DialogStyle)
            .setTitle(title)
            .setView(promptLayoutBinding.root)
            .setPositiveButton(positiveButton) { _, _ -> }
            .setNegativeButton(R.string.buttonCancel) { _, _ -> }
            .setCancelable(false)
            .create()
            .apply {
                show()
                showKeyboard()
            }

        val buttonPositive = dialog.getButton(AlertDialog.BUTTON_POSITIVE)

        nameEditText.apply {
            doOnTextChanged { text, _, _, _ -> buttonPositive.isEnabled = text?.length != 0 }
            fieldValue?.let {
                setText(it)
                selectedRange?.let { range -> setSelection(0, range) }
                requestFocus()
            }
        }

        buttonPositive.setOnClickListener {
            onPositiveButtonClicked(dialog, nameEditText.text.toString().trim())
            it.isEnabled = false
            dialog.getButton(AlertDialog.BUTTON_NEGATIVE).isEnabled = false
        }
    }

    /**
     * Specific method which allows to popBackstack by ignoring create folder "stack" (in case of folder creation)
     */
    fun ignoreCreateFolderBackStack(navController: NavController, mustIgnore: Boolean) {
        when {
            mustIgnore -> if (!navController.popBackStack(R.id.newFolderFragment, true)) navController.popBackStack()
            else -> navController.popBackStack()
        }
    }

    fun displayFile(
        mainViewModel: MainViewModel,
        navController: NavController,
        selectedFile: File,
        fileList: List<File>,
        isSharedWithMe: Boolean = false,
    ) {
        mainViewModel.currentPreviewFileList = fileList.associateBy { it.id } as LinkedHashMap<Int, File>

        val navOptions = NavOptions.Builder()
            .setEnterAnim(R.anim.fragment_open_enter)
            .setExitAnim(R.anim.fragment_open_exit)
            .build()

        val (destinationClass, bundle) = if (selectedFile.isPublicShared()) {
            R.id.publicSharePreviewSliderFragment to null
        } else {
            val args = PreviewSliderFragmentArgs(
                fileId = selectedFile.id,
                driveId = selectedFile.driveId,
                isSharedWithMe = isSharedWithMe,
            )

            R.id.previewSliderFragment to args.toBundle()
        }

        navController.navigate(destinationClass, bundle, navOptions)
    }

    fun convertBytesToGigaBytes(bytes: Long) = (bytes / 1024.0.pow(3))
    fun convertGigaByteToBytes(gigaBytes: Double) = (gigaBytes * 1024.0.pow(3)).toLong()

    fun Context.moveFileClicked(
        disabledDestinationFolderId: Int?,
        selectFolderResultLauncher: ActivityResultLauncher<Intent>,
        mainViewModel: MainViewModel,
        navigationRestrictions: FolderNavigationRestrictions = FolderNavigationRestrictions(),
    ) {
        mainViewModel.ignoreSyncOffline = true
        Intent(this, SelectFolderActivity::class.java).apply {
            putExtras(
                SelectFolderActivityArgs(
                    userId = AccountUtils.currentUserId,
                    driveId = AccountUtils.currentDriveId,
                    folderId = disabledDestinationFolderId ?: -1,
                    disabledDestinationFolderId = disabledDestinationFolderId ?: -1,
                    navigationRestrictions = navigationRestrictions,
                    customArgs = bundleOf(
                        MultiSelectFragment.BULK_OPERATION_CUSTOM_TAG to BulkOperationType.MOVE,
                        SINGLE_OPERATION_CUSTOM_TAG to SingleOperation.MOVE.name,
                    ),
                ).toBundle(),
            )
        }.also(selectFolderResultLauncher::launch)
    }

    fun Context.duplicateFilesClicked(
        selectFolderResultLauncher: ActivityResultLauncher<Intent>,
        mainViewModel: MainViewModel,
    ) {
        Intent(this, SelectFolderActivity::class.java).apply {
            putExtras(
                SelectFolderActivityArgs(
                    userId = AccountUtils.currentUserId,
                    driveId = AccountUtils.currentDriveId,
                    folderId = mainViewModel.currentFolder.value?.id ?: -1,
                    customArgs = bundleOf(
                        MultiSelectFragment.BULK_OPERATION_CUSTOM_TAG to BulkOperationType.COPY,
                        SINGLE_OPERATION_CUSTOM_TAG to SingleOperation.COPY.name,
                    ),
                ).toBundle(),
            )
        }.also(selectFolderResultLauncher::launch)
    }

    fun Context.openWith(uri: Uri, type: String?, flags: Int) {
        startActivityFor(openWithIntentExceptkDrive(uri, type, flags))
    }

    fun Context.openWith(file: File, userDrive: UserDrive) {
        startActivityFor(openWithIntentExceptkDrive(file, userDrive))
    }

    private fun Context.startActivityFor(openWithIntent: Intent) {
        try {
            startActivity(openWithIntent)
        } catch (e: ActivityNotFoundException) {
            showToast(R.string.errorNoSupportingAppFound)
        }
    }

    fun Context.openWithIntentExceptkDrive(file: File, userDrive: UserDrive): Intent {
        val (cloudUri, uri) = file.getCloudAndFileUris(this, userDrive)
        val flags = Intent.FLAG_GRANT_READ_URI_PERMISSION or
                Intent.FLAG_GRANT_WRITE_URI_PERMISSION or
                Intent.FLAG_GRANT_PERSISTABLE_URI_PERMISSION
        return openWithIntentExceptkDrive(uri, contentResolver.getType(cloudUri), flags)
    }

    private fun Context.intentExcludingPdfReader(openWithIntent: Intent): Intent {
        val components = arrayOf(ComponentName(this, PreviewPDFActivity::class.java))
        return Intent.createChooser(openWithIntent, null).putExtra(Intent.EXTRA_EXCLUDE_COMPONENTS, components)
    }

    fun Context.openWithIntentExceptkDrive(uri: Uri, type: String?, flags: Int): Intent {
        val openWithIntent = Intent().apply {
            action = Intent.ACTION_VIEW
            this.flags = flags
            setDataAndType(uri, type)
        }

        // We only do that when we try to openWith with a PDF because we have our own PDF reader
        // Title in the chooser might not be displayed, at the discretion of the brand manufacturer
        // So we keep the ACTION_VIEW for every type of files EXCEPT PDF files
        return if (type == "application/pdf") applicationContext.intentExcludingPdfReader(openWithIntent) else openWithIntent
    }

    fun downloadAsOfflineFile(
        context: Context,
        file: File,
        userDrive: UserDrive = UserDrive(),
        isInheritedDownload: Boolean = false,
        cancelPendingWork: Boolean = true,
    ) {
        enqueueOfflineDownload(context, file, userDrive, isInheritedDownload, cancelPendingWork)
    }

    /**
     * Same enqueue as [downloadAsOfflineFile], returning the WorkManager [Operation] so a
     * caller whose cursor commit must wait for persistence (issue: round 8 #12) can await it
     * OUTSIDE any thread-confined Realm session (issue: round 9 #2): the awaiting variant was
     * suspended while the sync's Realm and managed objects stayed live across the suspension,
     * violating Realm thread affinity. The caller detaches the values it needs, closes its
     * Realm, awaits the operation, then revalidates on a fresh Realm.
     */
    fun downloadAsOfflineFileOperation(
        context: Context,
        file: File,
        userDrive: UserDrive = UserDrive(),
        isInheritedDownload: Boolean = false,
        cancelPendingWork: Boolean = true,
    ): Operation = enqueueOfflineDownload(context, file, userDrive, isInheritedDownload, cancelPendingWork)

    private fun enqueueOfflineDownload(
        context: Context,
        file: File,
        userDrive: UserDrive,
        isInheritedDownload: Boolean,
        cancelPendingWork: Boolean,
    ): Operation {
        val workManager = WorkManager.getInstance(context)

        // Cancellation belongs to EXPLICIT REPLACEMENT only (issue: round 19 #17): the
        // verification path never cancels — repeated verification calls must not keep
        // killing their own in-flight work for the file.
        // The tag carries the WORK'S CAPTURED scope (issue: round 21 #15) — the same
        // scope the input data below binds, never the currently active account.
        if (cancelPendingWork && file.isMarkedAsOffline) workManager.cancelAllWorkByTag(file.getWorkerTag(userDrive))
        val inputData = workDataOf(
            BaseDownloadWorker.FILE_ID to file.id,
            DownloadWorker.FILE_NAME to file.name,
            BaseDownloadWorker.USER_ID to userDrive.userId,
            BaseDownloadWorker.DRIVE_ID to userDrive.driveId,
            DownloadWorker.IS_INHERITED_DOWNLOAD to isInheritedDownload,
        )
        val networkType = if (AppSettings.onlyWifiSyncOffline) NetworkType.UNMETERED else NetworkType.CONNECTED
        val constraints = Constraints.Builder()
            .setRequiredNetworkType(networkType)
            .setRequiresStorageNotLow(true)
            .build()
        val downloadRequest = OneTimeWorkRequestBuilder<DownloadWorker>()
            .addTag(file.getWorkerTag(userDrive))
            .addTag(DownloadWorker.TAG)
            .setInputData(inputData)
            .setConstraints(constraints)
            .setExpeditedIfAvailable()
            .build()

        return if (cancelPendingWork) {
            // Explicit requests keep the shared draining chain (consecutive selections
            // drain instead of being silently dropped).
            workManager.enqueueUniqueWork(DownloadWorker.TAG, ExistingWorkPolicy.APPEND_OR_REPLACE, downloadRequest)
        } else {
            // VERIFICATION (issue: round 19 #17): ACCOUNT-SCOPED UNIQUE per-file work,
            // KEEP-deduplicated — a pending verification for this account+file is kept;
            // repeated scheduling neither cancels it nor piles duplicates. The request
            // still carries the shared tags for the failure/redrive queries.
            workManager.enqueueUniqueWork(
                "${DownloadWorker.TAG}_${userDrive.userId}_${userDrive.driveId}_${file.id}",
                ExistingWorkPolicy.KEEP,
                downloadRequest,
            )
        }
    }

    fun enqueueBulkDownloadWorker(
        context: Context,
        folderId: Int,
        userDrive: UserDrive = UserDrive(),
        recursive: Boolean = false,
    ) {
        val workManager = WorkManager.getInstance(context)
        val inputData = workDataOf(
            BulkDownloadWorker.FOLDER_ID to folderId,
            BaseDownloadWorker.USER_ID to userDrive.userId,
            BaseDownloadWorker.DRIVE_ID to userDrive.driveId,
            BulkDownloadWorker.RECURSIVE to recursive,
        )
        val networkType = if (AppSettings.onlyWifiSyncOffline) NetworkType.UNMETERED else NetworkType.CONNECTED
        val constraints = Constraints.Builder()
            .setRequiredNetworkType(networkType)
            .setRequiresStorageNotLow(true)
            .build()
        val downloadRequest = OneTimeWorkRequestBuilder<BulkDownloadWorker>()
            .addTag(BulkDownloadWorker::class.java.toString())
            .setInputData(inputData)
            .setConstraints(constraints)
            .setExpeditedIfAvailable()
            .build()

        // Recursive walks are KEEP-deduplicated (one pending walk per root, cancellable on
        // removal); explicit-pin batches use their own identity and APPEND so consecutive
        // selections drain instead of being silently dropped (issue: round 4 #13).
        val (uniqueName, policy) = if (recursive) {
            BulkDownloadWorker.walkWorkName(userDrive, folderId) to ExistingWorkPolicy.KEEP
        } else {
            BulkDownloadWorker.pinWorkName(userDrive, folderId) to ExistingWorkPolicy.APPEND_OR_REPLACE
        }
        workManager.enqueueUniqueWork(uniqueName, policy, downloadRequest)
    }

    /**
     * Fire-and-forget entry for [reconfigureOfflineFolderWork] running in a process-lifetime
     * scope (issue: round 6 #8): the settings dialog that requests the reconfiguration
     * dismisses itself immediately, and a dialog-owned coroutine would be cancelled before
     * the awaited WorkManager cancellation and the replacement enqueue complete — leaving the
     * original failure mode (cancelled work, nothing rescheduled) intact.
     */
    fun reconfigureOfflineFolderWorkAsync(context: Context) {
        reconfigurationScope.launch { reconfigureOfflineFolderWork(context) }
    }

    private val reconfigurationScope = CoroutineScope(SupervisorJob() + Dispatchers.IO)

    /**
     * Serializes successive reconfigurations (issue: round 7 #12): two overlapping runs would
     * otherwise interleave cancellation and re-enqueue, and the second run's cancellation could
     * destroy the chains the first run had just rebuilt.
     */
    private val reconfigurationMutex = Mutex()

    /**
     * Cancels all offline folder download chains and re-enqueues their durable work with the
     * current constraint settings (issue: round 4 #14, round 5 #17): a Wi-Fi-only change must
     * not leave a mobile-data job running, nor an unmetered job blocked — and cancellation
     * alone would lose the cancelled work, because the offline sync only re-drives roots,
     * never unfinished pin requests. Durable root intent, coverage flags, explicit pins and
     * pending markers live in Realm and are unaffected by the cancellation.
     */
    suspend fun reconfigureOfflineFolderWork(context: Context) = reconfigurationMutex.withLock {
        val workManager = WorkManager.getInstance(context)
        // Await the cancellation before re-enqueueing: a still-propagating cancel could
        // otherwise swallow the replacement chains (issue: round 5 #17). Both worker classes
        // carry offline download chains (issue: round 7 #12): the single-file
        // [DownloadWorker] chain is cancelled too — only offline downloads use that tag —
        // and unfinished single-file pins are re-driven below by their folder pin batches.
        workManager.cancelAllWorkByTag(BulkDownloadWorker::class.java.toString()).result.await()
        workManager.cancelAllWorkByTag(DownloadWorker.TAG).result.await()

        // Every durable root re-drives its walk in its own captured scope (issue: round 5 #17).
        AccountUtils.getAllUsersSync().forEach { user ->
            DriveInfosController.getDrives(userId = user.id, maintenance = false).forEach { drive ->
                val userDrive = UserDrive(userId = user.id, driveId = drive.id)
                OfflineFolderController.getOfflineRoots(userDrive).forEach { root ->
                    enqueueBulkDownloadWorker(context, root.folderId, userDrive, recursive = true)
                }
                // Unfinished pin requests: every folder holding an explicit pin or a pending
                // download marker re-drives its batch with the new constraints; intact copies
                // are skipped inside the worker (issue: round 5 #17).
                FileController.getFoldersWithOfflineIntent(userDrive).forEach { folderId ->
                    enqueueBulkDownloadWorker(context, folderId, userDrive, recursive = false)
                }
            }
        }
    }

    fun getInvalidFileNameCharacter(fileName: String): String? = DownloadManagerUtils.regexInvalidSystemChar.find(fileName)?.value

    fun copyDataToUploadCache(context: Context, file: IOFile, fileModifiedAt: Date): Uri {
        val outputFile = IOFile(context.uploadFolder, file.toUri().hashCode().toString()).apply {
            if (exists()) delete()
            setLastModified(fileModifiedAt.time)
        }
        file.copyTo(outputFile, true)
        return outputFile.toUri()
    }

    fun createProgressDialog(context: Context, title: Int): AlertDialog {
        return MaterialAlertDialogBuilder(context, R.style.DialogStyle).apply {
            setTitle(title)
            setCancelable(false)
            DialogDownloadProgressBinding.inflate(LayoutInflater.from(context)).apply {
                icon.isGone = true
                downloadProgressIndicator.isIndeterminate = true
                setView(root)
            }
        }.create()
    }

    enum class Shortcuts(val id: String) {
        UPLOAD("upload"),
        SCAN("scan"),
        SEARCH("search"),
        FEEDBACK("feedback"),
    }
}
