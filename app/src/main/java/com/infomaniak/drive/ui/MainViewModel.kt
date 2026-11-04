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
package com.infomaniak.drive.ui

import android.app.Application
import android.content.Context
import android.net.Uri
import android.provider.MediaStore
import androidx.collection.MutableIntList
import androidx.collection.mutableIntListOf
import androidx.fragment.app.FragmentActivity
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.LiveData
import androidx.lifecycle.MediatorLiveData
import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.SavedStateHandle
import androidx.lifecycle.application
import androidx.lifecycle.liveData
import androidx.lifecycle.viewModelScope
import androidx.navigation.NavController
import androidx.work.WorkInfo
import androidx.work.WorkManager
import androidx.work.WorkQuery
import com.infomaniak.core.auth.networking.AuthHttpClientProvider
import com.infomaniak.core.legacy.utils.SingleLiveEvent
import com.infomaniak.core.network.NetworkAvailability
import com.infomaniak.core.network.models.ApiResponse
import com.infomaniak.core.network.models.ApiResponseStatus
import com.infomaniak.core.network.utils.ApiErrorCode.Companion.translateError
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.MainApplication
import com.infomaniak.drive.MatomoDrive.MatomoName
import com.infomaniak.drive.MatomoDrive.trackNewElementEvent
import com.infomaniak.drive.R
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.ErrorCode
import com.infomaniak.drive.data.cache.DriveInfosController
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.cache.FolderFilesProvider
import com.infomaniak.drive.data.cache.OfflineFolderController
import com.infomaniak.drive.utils.OfflineFileCoordination
import com.infomaniak.drive.data.models.CreateFile
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.FileCategory
import com.infomaniak.drive.data.models.FileListNavigationType
import com.infomaniak.drive.data.models.MqttAction
import com.infomaniak.drive.data.models.MqttNotification
import com.infomaniak.drive.data.models.ShareableItems.FeedbackAccessResource
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.models.deeplink.DeeplinkType.DeeplinkAction
import com.infomaniak.drive.data.models.file.FileExternalImport.FileExternalImportStatus
import com.infomaniak.drive.data.models.file.SpecialFolder.Trash
import com.infomaniak.drive.data.services.BulkDownloadWorker
import com.infomaniak.drive.data.services.CopyToDriveProgressWorker
import com.infomaniak.drive.data.services.DownloadWorker
import com.infomaniak.drive.ui.addFiles.UploadFilesHelper
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.DownloadOfflineFileManager
import com.infomaniak.drive.utils.FileId
import com.infomaniak.drive.utils.IOFile
import com.infomaniak.drive.utils.MediaUtils.deleteInMediaScan
import com.infomaniak.drive.utils.MediaUtils.isMedia
import com.infomaniak.drive.utils.SyncOfflineUtils
import com.infomaniak.drive.utils.SyncUtils.isSyncScheduled
import com.infomaniak.drive.utils.SyncUtils.syncImmediately
import com.infomaniak.drive.utils.Utils
import com.infomaniak.drive.utils.find
import io.realm.Realm
import io.sentry.Breadcrumb
import io.sentry.Sentry
import io.sentry.SentryLevel
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import java.util.Date

class MainViewModel(
    appContext: Application,
    private val savedStateHandle: SavedStateHandle,
) : AndroidViewModel(appContext) {

    var selectFolderUserDrive: UserDrive? = null
    val realm: Realm by lazy {
        selectFolderUserDrive?.let {
            FileController.getRealmInstance(it)
        } ?: FileController.getRealmInstance()
    }

    private val privateFolder = MutableLiveData<File>()
    private val _currentFolder = MutableLiveData<File?>()
    private var pendingCopyToDriveImport: Pair<Int, String>? = null

    val currentFolder: LiveData<File?> = _currentFolder // Use `setCurrentFolder` and `postCurrentFolder` to set value on it

    val currentFolderOpenAddFileBottom = MutableLiveData<File>()
    var currentPreviewFileList = LinkedHashMap<Int, File>()

    val navigateFileListTo = SingleLiveEvent<FileListNavigationType>()
    val navigateDeeplink = MutableStateFlow<DeeplinkAction.Drive?>(null)

    val deleteFileFromHome = SingleLiveEvent<Boolean>()
    val refreshActivities = SingleLiveEvent<Boolean>()
    val updateOfflineFile = SingleLiveEvent<FileId>()

    val updateVisibleFiles = MutableLiveData<Boolean>()
    val isBulkDownloadRunning = MutableLiveData<Boolean>()

    val isNetworkAvailable = NetworkAvailability.isNetworkAvailable
    var hasNetwork: Boolean = true
        private set

    var mustOpenUploadShortcut: Boolean = true
        get() = savedStateHandle[SAVED_STATE_MUST_OPEN_UPLOAD_SHORTCUT_KEY] ?: field
        set(value) {
            savedStateHandle[SAVED_STATE_MUST_OPEN_UPLOAD_SHORTCUT_KEY] = value
            field = value
        }

    var ignoreSyncOffline = false

    var uploadFilesHelper: UploadFilesHelper? = null

    private var rootFilesJob: Job = Job()
    private var getFileDetailsJob = Job()
    private var syncOfflineFilesJob: Job? = null
    private var setCurrentFolderJob = Job()

    val deleteFilesFromGallery = SingleLiveEvent<List<Int>>()
    val copyToDriveResult = SingleLiveEvent<FileResult>()

    init {
        viewModelScope.launch {
            isNetworkAvailable.collect {
                onNetworkAvailabilityChanged(it)
                hasNetwork = it
            }
        }
    }

    private fun getContext() = getApplication<MainApplication>()

    fun hasEligibleDestinationDrives(file: File): Boolean {
        val userId = DriveInfosController.getDrive(driveId = file.driveId)?.userId ?: AccountUtils.currentUserId
        return DriveInfosController.hasEligibleDestinationDrives(userId)
    }

    fun setCurrentFolder(folder: File?) {
        folder?.let {
            setCurrentFolderJob.cancel()
            saveCurrentFolderId()
            uploadFilesHelper?.setParentFolder(it)
            _currentFolder.value = it
        }
    }

    fun setCurrentFolderAsRoot(): Job {
        setCurrentFolderJob.cancel()
        setCurrentFolderJob = Job()
        return viewModelScope.launch(Dispatchers.IO + setCurrentFolderJob) {
            val file = privateFolder.value ?: FileController.getPrivateFolder().also { privateFolder.postValue(it) }
            setCurrentFolderJob.ensureActive()
            _currentFolder.postValue(file)
        }
    }

    private fun postCurrentFolder(file: File?) {
        setCurrentFolderJob.cancel()
        _currentFolder.postValue(file)
    }

    fun initUploadFilesHelper(fragmentActivity: FragmentActivity, navController: NavController) {
        uploadFilesHelper = UploadFilesHelper(
            activity = fragmentActivity,
            navController = navController,
            onOpeningPicker = {
                trackNewElementEvent(MatomoName.UploadFile)
                uploadFilesHelper?.let { setParentFolder() } ?: Sentry.captureMessage("UploadFilesHelper is null. It should not!")
            },
        )
        initCurrentFolderFromRealm()
        setParentFolder()
    }

    fun loadRootFiles() {
        rootFilesJob.cancel()
        rootFilesJob = viewModelScope.launch {
            FolderFilesProvider.loadRootFiles(UserDrive(), hasNetwork)
        }
    }

    fun navigateFileListTo(navController: NavController, fileId: Int, userDrive: UserDrive, subfolderId: Int?) {
        // Clear FileListFragment stack
        navController.popBackStack(R.id.rootFilesFragment, false)

        if (fileId <= Utils.ROOT_ID) {
            if (fileId == Trash.id) {
                subfolderId?.let {
                    navigateFileListTo.postValue(FileListNavigationType.Subfolder(Trash.file, it))
                } ?: run {
                    navigateFileListTo.postValue(FileListNavigationType.Folder(Trash.file))
                }
            }
            return // Deeplinks could lead us to navigating to the true root
        }

        // Emit destination folder id
        viewModelScope.launch(Dispatchers.IO) {
            val file = FileController.getFileByUidOrId(fileId, userDrive)
                ?: FileController.getFileDetails(fileId, userDrive = userDrive)
                ?: return@launch
            navigateFileListTo.postValue(FileListNavigationType.Folder(file))
        }
    }

    fun loadCurrentFolder(folderId: Int, userDrive: UserDrive) = viewModelScope.launch(Dispatchers.IO) {
        postCurrentFolder(FileController.getFileByUidOrId(folderId, userDrive))
    }

    fun createMultiSelectMediator(): MediatorLiveData<MultiSelectMediatorState> =
        MediatorLiveData<MultiSelectMediatorState>().apply {
            value = MultiSelectMediatorState(numberOfSuccessfulActions = 0, totalOfActions = 0, errorResId = null)
        }

    fun updateMultiSelectMediator(mediator: MediatorLiveData<MultiSelectMediatorState>): (FileResult) -> Unit = { fileRequest ->
        var numberOfSuccessfulActions = mediator.value!!.numberOfSuccessfulActions
        if (fileRequest.isSuccess) numberOfSuccessfulActions++

        val totalOfActions = mediator.value!!.totalOfActions + 1
        val currentErrorResId = mediator.value!!.errorResId

        mediator.value = MultiSelectMediatorState(
            numberOfSuccessfulActions,
            totalOfActions,
            errorResId = currentErrorResId ?: fileRequest.errorResId.takeIf { !fileRequest.isSuccess },
        )
    }

    fun getFileShare(fileId: Int, userDrive: UserDrive? = null) = liveData(Dispatchers.IO) {
        val okHttpClient = getHttpClient(userDrive)
        val driveId = userDrive?.driveId ?: AccountUtils.currentDriveId
        val apiResponse = ApiRepository.getFileShare(okHttpClient, File(id = fileId, driveId = driveId))
        emit(apiResponse)
    }

    private suspend fun getHttpClient(userDrive: UserDrive?): OkHttpClient {
        return userDrive?.let { AccountUtils.getHttpClient(userDrive.userId) } ?: AuthHttpClientProvider.authOkHttpClient
    }

    fun createOffice(driveId: Int, folderId: Int, createFile: CreateFile) = liveData(Dispatchers.IO) {
        emit(ApiRepository.createOfficeFile(driveId, folderId, createFile))
    }

    fun addFileToFavorites(file: File, userDrive: UserDrive? = null, onSuccess: (() -> Unit)? = null) =
        liveData(Dispatchers.IO) {
            with(ApiRepository.postFavoriteFile(file)) {
                emit(FileResult(this.isSuccess()))

                if (isSuccess()) {
                    FileController.updateFile(file.id, userDrive = userDrive) {
                        it.isFavorite = true
                    }
                    onSuccess?.invoke()
                }
            }
        }

    fun deleteFileFromFavorites(file: File, userDrive: UserDrive? = null, onSuccess: ((File) -> Unit)? = null) =
        liveData(Dispatchers.IO) {
            with(ApiRepository.deleteFavoriteFile(file)) {
                emit(FileResult(this.isSuccess()))

                if (isSuccess()) {
                    FileController.updateFile(file.id, userDrive = userDrive) {
                        it.isFavorite = false
                        onSuccess?.invoke(it)
                    }
                }
            }
        }

    fun getFileDetails(fileId: Int, userDrive: UserDrive): LiveData<File?> {
        getFileDetailsJob.cancel()
        getFileDetailsJob = Job()
        return liveData(Dispatchers.IO + getFileDetailsJob) {
            emit(FileController.getFileDetails(fileId, userDrive))
        }
    }

    fun moveFile(
        file: File,
        newParent: File,
        isSharedWithMe: Boolean = false,
        onSuccess: ((fileId: Int) -> Unit)? = null
    ) = liveData(Dispatchers.IO) {
        // The viewed file's account scope AND the presentation scope are captured BEFORE the
        // asynchronous request (issues: round 6 #2, round 7 #8): deriving either after the
        // move could mix accounts mid-operation. Every Realm, filesystem and enqueue
        // operation below uses these captured scopes, never the active account re-resolved
        // after the request.
        val userDrive = UserDrive(userId = AccountUtils.currentUserId, driveId = file.driveId)
        val presentationScope = UserDrive()
        val apiResponse = ApiRepository.moveFile(file, newParent)
        if (apiResponse.isSuccess()) {
            // The local copy move must complete (or durably defer) before the metadata commit
            // (issue: round 6 #2): committing first would strand inherited copies — including
            // unsent edits — at the old, untracked path. The row realm stays the session's
            // current-drive Realm (shared-with-me rows live here, as upstream); the captured
            // scopes key the offline-path/preservation APIs and the shares realm.
            FileController.getRealmInstance(presentationScope).use { currentDriveRealm ->
                // ONE intent → ordered-locks → Realm operation (issues: round 15 #14, round
                // 11 #7): fresh acceptance, source/destination resolution, preservation and
                // the conditional identity replacement hold ONE section — the previous
                // shape preserved under a released lock and then committed the captured UI
                // object unconditionally, so an intervening committed identity (a newer
                // refresh that published or received an edit elsewhere) could be
                // overwritten with the older one, leaving its bytes undiscoverable. The
                // pre-HTTP [file] object is the immutable request observation.
                synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    val lockIds = listOf(file.id, file.parentId, newParent.id).filter { it > 0 }.distinct().sorted()
                    OfflineFileCoordination.withLocksFor(userDrive, lockIds) {
                        runCatching { currentDriveRealm.refresh() }
                        val freshRow = FileController.getFileById(currentDriveRealm, file.id)
                        // Fresh acceptance against the immutable observation: an intervening
                        // committed identity (a newer move or metadata) defers the local
                        // commit — the row's newer state and its bytes stay authoritative
                        // and the next listing reconciles.
                        val movedIdentity = File(id = file.id, driveId = file.driveId).apply {
                            parentId = newParent.id
                            name = file.name
                            updatedAt = file.updatedAt
                            revisedAt = file.revisedAt
                        }
                        val accepted = freshRow == null ||
                            !FileController.isStaleForAuthoritativeAction(freshRow, movedIdentity, null, observed = file)
                        var preserved = accepted
                        if (accepted) {
                            // Destination resolution in its CORRECT captured realm (issue: round 8 #5):
                            // a shared destination's row lives in the shares realm, not the
                            // current-drive realm — missing it there skipped the destination
                            // reconciliation entirely, and the source bytes were then removed under
                            // their OLD identity.
                            val folderProxy = FileController.getFileById(currentDriveRealm, newParent.id)
                                ?: if (isSharedWithMe) FileController.getRealmInstance(
                                    UserDrive(userId = presentationScope.userId, sharedWithMe = true)
                                ).use { sharesRealm ->
                                    // DETACHED copy (issue: round 9 #3): the shares Realm closes here,
                                    // and preservation reads the destination's identity afterward —
                                    // a managed row would escape its closed Realm and fail the
                                    // reconciliation. The captured account scope stays explicit.
                                    FileController.getFileById(sharesRealm, newParent.id)?.let { sharesRealm.copyFromRealm(it, 0) }
                                } else null
                            val freshSource = freshRow?.getStoredFile(getContext(), userDrive)
                            if (freshRow == null || freshRow.isFolder()) {
                                // Directory storage never goes through the single-file helper (issue:
                                // round 8 #6): a directory timestamp cannot establish that descendants
                                // are clean, and physically moving the whole subtree first would leave
                                // descendant rows pointing at paths that no longer hold their bytes —
                                // breaking per-child reconciliation. The old directory location and
                                // ownership metadata are retained until the offline walk reconciles
                                // each descendant individually.
                                preserved = true
                            } else if (freshSource != null && freshSource.exists() && folderProxy != null) {
                                preserved = moveIfOfflineFileOrDelete(freshRow, freshSource, folderProxy, userDrive, currentDriveRealm)
                            }
                            // No stored file at the old location (or destination unresolvable with
                            // nothing owned): nothing to preserve — the identity commit proceeds.
                        }

                        if (accepted && preserved) {
                            FileController.updateFile(file.parentId, currentDriveRealm) { localFolder ->
                                // Ignore expired transactions when it's suspended
                                // In case the phone is slow or in standby, the transaction can create an IllegalStateException
                                // because realm will not be available anymore, the transaction is resumed afterwards
                                // so we ignore the cases where it fails.
                                runCatching { localFolder.children.remove(file) }
                            }
                        }

                        if (isSharedWithMe) {
                        // If the selected folder is a shared folder, it is removed from the user's realm table (userId_driveId)
                        // and added to the share realm (userId_share)
                        // Explicit captured scope (issue: round 5 #6): preservation and cache
                        // deletion must not re-derive the account from the active one. The shares
                        // realm is keyed by the CAPTURED user (issue: round 7 #8).
                        if (accepted && preserved) {
                            // The committed identity carries the FRESH row's current local
                            // fields (issue: round 15 #14) — the captured UI object could be
                            // stale in fields the ordering gate cannot see (an unmark since
                            // capture) — with the complete post-move identity applied: the
                            // stale source parent/path must not survive, or uploads keep
                            // targeting the old location (issue: round 8 #5).
                            val movedChild = freshRow?.let { currentDriveRealm.copyFromRealm(it, 0) }?.apply {
                                parentId = newParent.id
                                path = ""
                            } ?: file.apply {
                                parentId = newParent.id
                                path = ""
                            }
                            FileController.removeFile(
                                fileId = file.id,
                                customRealm = currentDriveRealm,
                                userDrive = userDrive,
                            )
                            val sharedScope = UserDrive(userId = presentationScope.userId, sharedWithMe = true)
                            FileController.getRealmInstance(sharedScope).use { sharedRealm ->
                                // Every affected identity in the ACTUAL realm's captured scope
                                // (issue: round 11 #3): the parent lock used to be scope-less
                                // while the child's identity changed inside the transaction —
                                // both the destination folder and the moved child are locked
                                // here, ascending, before the transaction.
                                OfflineFileCoordination.withLocksFor(sharedScope, listOf(newParent.id, file.id)) {
                                    FileController.updateFile(newParent.id, sharedRealm, userDrive = sharedScope) { localFolder ->
                                        runCatching {
                                            localFolder.children.add(movedChild)
                                        }.onFailure(Sentry::captureException)
                                    }
                                }
                            }
                        } else {
                            // Preservation incomplete or a newer committed identity won
                            // (pending upload, protected destination, intervening commit): the
                            // row keeps its OLD identity and location in this realm; the offline
                            // sync re-processes the move until preservation succeeds (issues:
                            // round 8 #5, round 15 #14).
                            SentryLog.i("MainViewModel", "Deferred shared move of ${file.id}: its offline copy is not safely preserved yet")
                        }
                    } else if (accepted && preserved) {
                        // The committed identity is the complete post-move one (issue: round 7 #5):
                        // the stale source path must not survive the parent change, or derived
                        // offline paths keep pointing at the location the bytes left. The add
                        // commits the FRESH row's current local fields with the moved identity
                        // applied (issue: round 15 #14); the acceptance gate already rejected
                        // intervening identity changes under this section's locks.
                        val movedChild = freshRow?.let { currentDriveRealm.copyFromRealm(it, 0) }?.apply {
                            parentId = newParent.id
                            path = ""
                        } ?: file.apply {
                            parentId = newParent.id
                            path = ""
                        }
                        FileController.addChild(
                            newParent.id,
                            movedChild,
                            currentDriveRealm,
                            userDrive,
                            // This add also COMMITS the post-move identity (issue: round 14
                            // #11): the row still holds the source parent, so the fresh-row
                            // container check must not apply.
                            revalidateExistingRow = false,
                        )
                    } else {
                        // Preservation incomplete or a newer committed identity won (pending
                        // upload, protected destination, intervening commit): the row keeps its
                        // OLD identity and location; the offline sync and the shared upsert
                        // boundary re-process the move until preservation succeeds (issues:
                        // round 6 #2, round 15 #14).
                        SentryLog.i("MainViewModel", "Deferred local move of ${file.id}: its offline copy is not safely preserved yet")
                    }
                    }
                }
            }

            onSuccess?.invoke(file.id)
        }

        emit(
            FileResult(
                isSuccess = apiResponse.isSuccess(),
                errorCode = apiResponse.error?.code,
                errorResId = when {
                    apiResponse.isSuccess() -> null
                    apiResponse.error?.code == ErrorCode.LIMIT_EXCEEDED_ERROR -> R.string.errorFilesLimitExceeded
                    else -> apiResponse.translateError(defaultMessage = R.string.errorMove)
                },
            )
        )
    }

    fun renameFile(file: File, newName: String) = liveData(Dispatchers.IO) {
        // The viewed file's account scope is captured for the rename preservation and the
        // metadata write (issue: round 6 #4).
        val userDrive = UserDrive(userId = AccountUtils.currentUserId, driveId = file.driveId)
        emit(FileController.renameFile(file, newName, userDrive = userDrive))
    }

    fun updateFolderColor(file: File, color: String, userDrive: UserDrive) = liveData(Dispatchers.IO) {
        emit(FileResult(isSuccess = FileController.updateFolderColor(file, color, userDrive).isSuccess()))
    }

    fun manageCategory(categoryId: Int, files: List<File>, isAdding: Boolean) = liveData(Dispatchers.IO) {
        with(manageCategoryApiCall(files, categoryId, isAdding)) {
            data?.forEach { feedbackResource ->
                if (feedbackResource.result) {
                    FileController.updateFile(feedbackResource.id) {
                        if (isAdding) {
                            it.categories.add(FileCategory(categoryId, userId = AccountUtils.currentUserId, addedAt = Date()))
                        } else {
                            it.categories.find(categoryId)?.deleteFromRealm()
                        }
                    }
                }
            }

            emit(this)
        }
    }

    fun deleteFile(file: File, userDrive: UserDrive? = null, onSuccess: ((fileId: Int) -> Unit)? = null) =
        liveData(Dispatchers.IO) {
            with(FileController.deleteFile(file, userDrive = userDrive, context = getContext(), onSuccess = onSuccess)) {
                emit(
                    FileResult(
                        isSuccess = this.isSuccess(),
                        data = this.data,
                        errorCode = this.error?.code,
                        errorResId = this.translateError()
                    )
                )
            }
        }

    fun restoreTrashFile(file: File, newFolderId: Int? = null, onSuccess: (() -> Unit)? = null) = liveData(Dispatchers.IO) {
        val body = newFolderId?.let { mapOf("destination_directory_id" to it) }
        with(ApiRepository.postRestoreTrashFile(file, body)) {
            emit(FileResult(this.isSuccess(), errorCode = this.error?.code))
            if (isSuccess()) onSuccess?.invoke()
        }
    }

    fun deleteTrashFile(file: File, onSuccess: (() -> Unit)? = null) = liveData(Dispatchers.IO) {
        with(ApiRepository.deleteTrashFile(file)) {
            emit(FileResult(this.isSuccess()))
            if (isSuccess()) onSuccess?.invoke()
        }
    }

    fun copyFileToAnotherDrive(file: File, destinationFolder: File) = copyFileToAnotherDrive(
        fileId = file.id,
        fileName = file.name,
        sourceDriveId = file.driveId,
        destinationDriveId = destinationFolder.driveId,
        destinationFolderId = destinationFolder.id,
    )

    fun copyFileToAnotherDrive(
        fileId: Int,
        fileName: String,
        sourceDriveId: Int,
        destinationDriveId: Int,
        destinationFolderId: Int,
    ) = viewModelScope.launch(Dispatchers.IO) {
        val sourcePath = getSourceRemotePath(fileId, sourceDriveId)
        if (sourcePath.isNullOrBlank()) {
            copyToDriveResult.postValue(
                FileResult(isSuccess = false, errorResId = R.string.errorCopyToDrive, fileName = fileName)
            )
            return@launch
        }

        val apiResponse = ApiRepository.copyFileToAnotherDrive(
            sourceDriveId = sourceDriveId,
            sourcePath = sourcePath,
            destinationDriveId = destinationDriveId,
            destinationFolderId = destinationFolderId,
        )

        apiResponse.data?.firstOrNull()?.let { externalImport ->
            pendingCopyToDriveImport = externalImport.id to fileName
            val realDestFolderId = externalImport.directoryId.takeIf { it > 0 } ?: destinationFolderId
            CopyToDriveProgressWorker.scheduleWork(
                getContext(),
                externalImport.id,
                fileName,
                destinationDriveId,
                realDestFolderId
            )
        }

        copyToDriveResult.postValue(mapCopyApiResponseToFileResult(apiResponse, fileName))
    }

    private fun getSourceRemotePath(fileId: Int, sourceDriveId: Int): String? {
        val sourceDrive = DriveInfosController.getDrive(driveId = sourceDriveId)
        val userDrive = UserDrive(
            userId = sourceDrive?.userId ?: AccountUtils.currentUserId,
            driveId = sourceDriveId,
            sharedWithMe = sourceDrive?.sharedWithMe == true,
        )
        val path = FileController.getFileByUidOrId(fileId, userDrive)?.getRemotePath(userDrive) ?: return null

        return path.removePrefix(PRIVATE_FOLDER_PATH_PREFIX)
    }

    fun resolveCopyToDriveNotification(notification: MqttNotification): CopyToDriveResult? {
        val importId = notification.importId ?: return null
        val fileName = pendingCopyToDriveImport?.takeIf { it.first == importId }?.second ?: return null

        val isSuccess = when (notification.action) {
            MqttAction.EXTERNAL_IMPORT_FINISHED -> true
            MqttAction.EXTERNAL_IMPORT_ERROR, MqttAction.EXTERNAL_IMPORT_CANCELED -> false
            else -> return null
        }

        pendingCopyToDriveImport = null
        return CopyToDriveResult(isSuccess, fileName)
    }

    fun duplicateFile(
        file: File,
        destinationId: Int? = null,
        onSuccess: ((apiResponse: ApiResponse<File>) -> Unit)? = null,
    ) = liveData(Dispatchers.IO) {
        ApiRepository.duplicateFile(file, destinationId ?: Utils.ROOT_ID).let { apiResponse ->
            if (apiResponse.isSuccess()) onSuccess?.invoke(apiResponse)
            emit(
                FileResult(
                    isSuccess = apiResponse.isSuccess(),
                    data = apiResponse.data,
                    errorCode = apiResponse.error?.code,
                    errorResId = when {
                        apiResponse.isSuccess() -> null
                        apiResponse.error?.code == ErrorCode.LIMIT_EXCEEDED_ERROR -> R.string.errorFilesLimitExceeded
                        else -> apiResponse.translateError(defaultMessage = R.string.errorDuplicate)
                    },
                )
            )
        }
    }

    fun convertFile(file: File) = liveData(Dispatchers.IO) {
        emit(ApiRepository.convertFile(file))
    }

    fun cancelExternalImport(importId: Int) = liveData(Dispatchers.IO) {
        val driveId = AccountUtils.currentDriveId
        val apiResponse = ApiRepository.cancelExternalImport(driveId, importId)

        if (apiResponse.isSuccess()) {
            FileController.updateExternalImportStatus(driveId, importId, FileExternalImportStatus.CANCELING)
        }

        emit(apiResponse)
    }

    fun observeDownloadOffline(context: Context) = WorkManager.getInstance(context).getWorkInfosLiveData(
        WorkQuery.Builder
            .fromUniqueWorkNames(arrayListOf(DownloadWorker.TAG))
            .addStates(arrayListOf(WorkInfo.State.RUNNING, WorkInfo.State.SUCCEEDED))
            .build()
    )

    fun restartUploadWorkerIfNeeded() {
        viewModelScope.launch {
            if (UploadFile.getAllPendingUploadsCount() > 0 && !getContext().isSyncScheduled()) {
                getContext().syncImmediately(isAutomaticTrigger = true)
            }
        }
    }

    fun removeSelectedFilesFromOffline(files: List<File>, onSuccess: (() -> Unit)? = null) = liveData {
        // The captured account scope of each file travels through its whole unpin (issue:
        // round 16 #6): flags, coverage, preservation and deletion all key the same scope,
        // never the account that happens to be active when a later step runs.
        val capturedScopes = files.associate { file ->
            file.id to UserDrive(userId = AccountUtils.currentUserId, driveId = file.driveId)
        }
        val filesId = files.map { it.id }

        viewModelScope.launch(Dispatchers.IO) {
            filesId.forEach { fileId ->
                unpinAndConditionallyDeleteCopies(fileId, capturedScopes[fileId] ?: UserDrive())
            }
            onSuccess?.invoke()
            emit(FileResult(isSuccess = true))
        }
    }

    fun removeOfflineFile(
        file: File,
        offlineFile: IOFile,
        cacheFile: IOFile,
        userDrive: UserDrive = UserDrive(),
        onFileRemovedFromOffline: (() -> Unit)? = null,
    ) {
        // We need to call this method outside the UI thread
        viewModelScope.launch(Dispatchers.IO) {
            // The captured [offlineFile]/[cacheFile] paths are NOT trusted for the deletion
            // (issue: round 16 #6): the fresh row's current paths are resolved inside the
            // coordinated section below.
            unpinAndConditionallyDeleteCopies(
                fileId = file.id,
                userDrive = userDrive,
                isFolder = file.isFolder(),
            )
            onFileRemovedFromOffline?.invoke()
        }
    }

    /**
     * ONE intent → file-lock → fresh-Realm section for an unpin (issue: round 16 #6): the
     * flag write, the current coverage resolution, the ownership/preservation decision and
     * the conditional byte deletion all read the FRESH row under the same root-intent and
     * per-file coordination, in the captured account scope. The previous shape checked
     * coverage in one coroutine and deleted the captured paths in a later, uncoordinated
     * one — a root or pin committed in between (which could publish or receive an edit)
     * had its newly owned copy deleted by the stale unpin, and an account switch could
     * clear flags in one Realm while deleting paths from another's storage.
     */
    private fun unpinAndConditionallyDeleteCopies(
        fileId: Int,
        userDrive: UserDrive,
        isFolder: Boolean = false,
    ) {
        synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive, fileId) {
                val context = getContext()
                var deleteOfflinePath: String? = null
                var deleteCachePath: String? = null
                var mediaFile: File? = null
                FileController.getRealmInstance(userDrive).use { realm ->
                    runCatching { realm.refresh() }
                    realm.executeTransaction {
                        val liveRow = FileController.getFileById(realm, fileId) ?: return@executeTransaction
                        // The captured row's flags can be stale: the fresh row's flags and the
                        // CURRENT coverage result decide (issue #1902).
                        liveRow.isOffline = false
                        liveRow.isInOfflineFolder = OfflineFolderController.isInsideActiveRoot(realm, fileId)
                        // The pending download marker is part of the removed intent (issue:
                        // round 5 #15): a stale marker cannot resurrect the pin.
                        liveRow.isMarkedAsOffline = false
                        if (!isFolder && !liveRow.isInOfflineFolder &&
                            !OfflineFolderController.isFileCoveredByAnyRoot(fileId, userDrive, realm)
                        ) {
                            deleteCachePath = liveRow.getCacheFile(context, userDrive).path
                            val offlineFile = liveRow.getOfflineFile(context, userDrive.userId)
                            if (offlineFile != null && offlineFile.exists()) {
                                // PRESERVATION before any deletion (issue #1902): a pending or
                                // retained-unsent edit keeps its bytes; a dirty copy is snapshotted
                                // first (a failed registration keeps the copy); an uploaded or
                                // clean copy is replaceable.
                                when (OfflineFolderController.decisionForReplacingOwnedCopy(liveRow, offlineFile, userDrive)) {
                                    OfflineFolderController.OwnedCopyReplacementDecision.KEEP_LOCAL -> Unit
                                    OfflineFolderController.OwnedCopyReplacementDecision.REPLACEABLE -> {
                                        deleteOfflinePath = offlineFile.path
                                        if (liveRow.isMedia()) mediaFile = realm.copyFromRealm(liveRow, 0)
                                    }
                                }
                            }
                        }
                    }
                }
                // Conditional deletion in the SAME section, after the flags committed (issue:
                // round 16 #6): never a later, uncoordinated coroutine on captured paths.
                deleteOfflinePath?.let { path ->
                    mediaFile?.let { file -> file.deleteInMediaScan(context, userDrive) }
                    runCatching { IOFile(path).takeIf { it.exists() }?.delete() }
                }
                deleteCachePath?.let { path ->
                    runCatching { IOFile(path).takeIf { it.exists() }?.delete() }
                }
            }
        }
    }

    fun syncOfflineFiles() {
        syncOfflineFilesJob?.cancel()
        syncOfflineFilesJob = viewModelScope.launch(Dispatchers.IO) {
            SyncOfflineUtils.startSyncOffline(getContext())
        }
    }

    /**
     * Marks [file] (a folder) as an offline root (issue #1902): the durable intent is stored and
     * a recursive walk downloads all its descendants, now and on every future sync.
     */
    fun addOfflineFolderRoot(file: File, userDrive: UserDrive = UserDrive()) = viewModelScope.launch(Dispatchers.IO) {
        val rootAdded = OfflineFolderController.addOfflineRoot(file, userDrive)
        if (rootAdded) Utils.enqueueBulkDownloadWorker(getContext(), file.id, userDrive, recursive = true)
    }

    /**
     * Removes the offline root on [file]: cancels its own work chain, then clears covered files'
     * offline copies (offline cache only), while explicit pins and files still covered by
     * another root are kept. If protected copies remain, the root record stays as the durable
     * owner of the deferred cleanup until the offline sync finishes it (issue #1902).
     */
    fun removeOfflineFolderRoot(file: File, userDrive: UserDrive = UserDrive()) = viewModelScope.launch(Dispatchers.IO) {
        WorkManager.getInstance(getContext()).cancelUniqueWork(BulkDownloadWorker.walkWorkName(userDrive, file.id))
        OfflineFolderController.removeOfflineRoot(file.id, userDrive)
    }

    fun cancelSyncOfflineFiles() {
        syncOfflineFilesJob?.cancel()
    }

    // Only for API 29 and below, otherwise use MediaStore.createDeleteRequest()
    fun deleteSynchronizedFilesOnDevice(filesToDelete: List<Uri>) = viewModelScope.launch(Dispatchers.IO) {
        val fileDeleted: MutableList<Uri> = mutableListOf()
        val isIOFilesDeleted: MutableList<Boolean> = mutableListOf()
        val fileDeleteContentResolver: MutableIntList = mutableIntListOf()
        var inconsistenciesCount = 0
        val tag = "deleteSynchronizedFilesOnDevice"

        SentryLog.i(tag, "filesToDelete (size): ${filesToDelete.size}")
        filesToDelete.forEach { uri ->
            try {
                val query = getContext().contentResolver.query(uri, arrayOf(MediaStore.Images.Media.DATA), null, null, null)
                query?.use { cursor ->
                    if (cursor.moveToFirst()) {
                        var columnIndex: Int? = null
                        try {
                            columnIndex = cursor.getColumnIndexOrThrow(MediaStore.Images.Media.DATA)
                            val pathname = cursor.getString(columnIndex)

                            isIOFilesDeleted.add(IOFile(pathname).delete())
                            fileDeleteContentResolver.add(getContext().contentResolver.delete(uri, null, null))
                        } catch (_: NullPointerException) {
                            Sentry.captureException(Exception("deleteSynchronizedFilesOnDevice()")) { scope ->
                                scope.setExtra("columnIndex", columnIndex.toString())
                            }
                        } finally {
                            fileDeleted.add(uri)
                        }
                    } else {
                        // The app was killed before updating `deleteAt` in the realm db
                        inconsistenciesCount++
                        fileDeleted.add(uri)
                    }
                } ?: fileDeleted.add(uri)
            } catch (exception: SecurityException) {
                Sentry.captureException(exception)
                exception.printStackTrace()
                fileDeleted.add(uri)
            }
        }
        SentryLog.i(tag, "isIOFilesDeleted[${isIOFilesDeleted.size}]: $isIOFilesDeleted")
        SentryLog.i(tag, "fileDeleteContentResolver[${fileDeleteContentResolver.size}]: $fileDeleteContentResolver")
        SentryLog.i(tag, "fileDeleted size after increment: ${fileDeleted.size}")
        SentryLog.i(tag, "file doesn't exist on device but deleteAt isn't up to date in the realm db: $inconsistenciesCount")
        Sentry.captureMessage("End deleteSynchronizedFilesOnDevice. Nb of error $inconsistenciesCount")

        UploadFile.deleteAllFromUris(fileDeleted)
    }

    fun checkBulkDownloadStatus() = viewModelScope.launch {
        val isRunning = DownloadOfflineFileManager.isBulkDownloadWorkerRunning(getContext())
        isBulkDownloadRunning.value = isRunning
        ignoreSyncOffline = isRunning
    }

    /**
     * One awaited bulk-offline operation (issue: round 6 #7): the durable explicit-pin and
     * pending-marker transaction COMMITS before the pin batch is enqueued, so the worker can
     * never enumerate an empty selection and leave the requests idle until an unrelated user
     * action.
     */
    fun addSelectionToOffline(
        filesId: List<Int>,
        folderId: Int,
        userDrive: UserDrive = UserDrive(),
        onSuccess: (() -> Unit)? = null,
    ) = liveData(Dispatchers.IO) {
        // The captured scope flows through persistence AND enqueueing (issue: round 7 #8):
        // resolving the active account after dispatch could persist and enqueue for another
        // account than the one the selection was made in.
        FileController.markFilesAsOffline(
            filesId = filesId,
            isMarkedAsOffline = true,
            userDrive = userDrive,
            setExplicitPin = true,
        )
        Utils.enqueueBulkDownloadWorker(getApplication(), folderId, userDrive)
        onSuccess?.invoke()
        emit(FileResult(isSuccess = true))
    }

    private fun mapCopyApiResponseToFileResult(apiResponse: ApiResponse<*>, fileName: String): FileResult {
        val isSuccess = apiResponse.result == ApiResponseStatus.SUCCESS
                || apiResponse.result == ApiResponseStatus.ASYNCHRONOUS
        return FileResult(
            isSuccess = isSuccess,
            errorCode = apiResponse.error?.code,
            errorResId = if (isSuccess) null else apiResponse.translateError(defaultMessage = R.string.errorCopyToDrive),
            fileName = fileName,
        )
    }

    private suspend fun onNetworkAvailabilityChanged(isNetworkAvailable: Boolean) {
        SentryLog.d("Internet availability", if (isNetworkAvailable) "Available" else "Unavailable")
        Sentry.addBreadcrumb(Breadcrumb().apply {
            category = "Network"
            message = "Internet access is available : $isNetworkAvailable"
            level = if (isNetworkAvailable) SentryLevel.INFO else SentryLevel.WARNING
        })
        if (isNetworkAvailable) {
            AccountUtils.updateCurrentUserAndDrives(this@MainViewModel.getContext())
            restartUploadWorkerIfNeeded()
        }
    }

    /**
     * Moves [file]'s local copy to its new parent, coordinated through the shared preservation
     * routine for BOTH ownership types (issue: round 6 #2): an inherited-only copy used to
     * reach a bare delete here, losing pending edits. A plain cache copy remains deletable
     * (it is re-downloadable). Returns false when preservation is incomplete: the caller must
     * not commit the new metadata yet.
     */
    private fun moveIfOfflineFileOrDelete(
        file: File,
        ioFile: IOFile,
        newParent: File,
        userDrive: UserDrive,
        realm: Realm,
    ): Boolean {
        if (!file.isOffline && !file.isInOfflineFolder) {
            ioFile.delete()
            return true
        }
        val offlineFile = newParent.getOfflineFile(application, userDrive.userId) ?: return false
        val destinationFile = IOFile(offlineFile, file.name)
        // The snapshot registers under the POST-MOVE identity (issue: round 7 #5): the edit
        // belongs to the moved file, and registering the source parent would queue the upload
        // for the old location while the caller commits the new one. Protection queries keep
        // the source identity separately (oldIdentity* below).
        val postMoveIdentity = File(id = file.id, driveId = userDrive.driveId).apply {
            parentId = newParent.id
            name = file.name
        }
        return OfflineFolderController.reconcileMovedInheritedCopy(
            context = application,
            oldOfflineFile = ioFile,
            oldIdentityName = file.name,
            oldIdentityParentId = file.parentId,
            // The bytes' provenance baseline (issue: round 16 #2), never bare server metadata
            // that a backward-dated replacement may have moved away from the bytes.
            oldLastModifiedAt = file.publishedCleanBaseline ?: file.lastModifiedAt,
            newIdentity = postMoveIdentity,
            userDrive = userDrive,
            newOfflineTarget = destinationFile,
            realm = realm,
        )
    }

    private fun saveCurrentFolder() {
        saveCurrentFolderId()
        uploadFilesHelper?.setParentFolder(currentFolder.value!!)
    }

    private fun setParentFolder() {
        currentFolder.value?.let {
            saveCurrentFolder()
        } ?: run {
            initCurrentFolderFromRealm()
        }
    }

    private fun manageCategoryApiCall(
        files: List<File>,
        categoryId: Int,
        isAdding: Boolean,
    ): ApiResponse<List<FeedbackAccessResource<Int, Unit>>> {
        return if (isAdding) ApiRepository.addCategory(files, categoryId) else ApiRepository.removeCategory(files, categoryId)
    }

    private fun saveCurrentFolderId() {
        currentFolder.value?.let { savedStateHandle[SAVED_STATE_FOLDER_ID_KEY] = it.id }
    }

    private fun initCurrentFolderFromRealm() {
        val savedFolderId: Int? = savedStateHandle[SAVED_STATE_FOLDER_ID_KEY]
        if (currentFolder.value == null && savedFolderId != null) {
            FileController.getFileByUidOrId(savedFolderId)?.let {
                _currentFolder.value = it
                saveCurrentFolder()
            }
        }
    }

    fun switchToNextUser(onUserSwitched: () -> Unit) = viewModelScope.launch(Dispatchers.IO) {
        if (AccountUtils.getAllUsersSync().size < 2) return@launch

        AccountUtils.switchToNextUser()

        withContext(Dispatchers.Main) { onUserSwitched() }
    }

    override fun onCleared() {
        realm.close()
        super.onCleared()
    }

    data class FileResult(
        val isSuccess: Boolean,
        val errorResId: Int? = null,
        val data: Any? = null,
        val errorCode: String? = null,
        val fileName: String? = null,
    )

    data class CopyToDriveResult(val isSuccess: Boolean, val fileName: String)

    data class MultiSelectMediatorState(
        var numberOfSuccessfulActions: Int,
        var totalOfActions: Int,
        var errorResId: Int? = null,
    )

    companion object {
        const val TAG = "MainViewModel"

        private const val SAVED_STATE_FOLDER_ID_KEY = "folderId"
        private const val SAVED_STATE_MUST_OPEN_UPLOAD_SHORTCUT_KEY = "mustOpenUploadShortcut"
        private const val PRIVATE_FOLDER_PATH_PREFIX = "/Private"
    }
}
