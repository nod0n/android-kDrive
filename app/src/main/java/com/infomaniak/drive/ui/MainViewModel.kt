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
import com.infomaniak.drive.utils.OfflineFolderPolicy
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

                        // SOURCE UNLINK DELAYED UNTIL DESTINATION VERIFICATION (issue:
                        // round 21 #8): the cross-realm transfer below verifies the
                        // destination's committed graph BEFORE the source row is unlinked
                        // or removed — the previous order removed the source backlink
                        // first, so a failed verification stranded the row from both
                        // sides (the shared case unlinks inside its verified branch). A
                        // local (non-shared) move commits its backlink here as before.
                        if (accepted && preserved && !isSharedWithMe) {
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
                            // CROSS-REALM transfer (issue: round 17 #16): the copy is DEEP —
                            // the complete cached ownership graph travels with the row. A
                            // depth-one copy truncated each child's own children while their
                            // isComplete flag survived, so the shares realm would believe it
                            // holds complete (empty) folders and an offline walk or root
                            // removal inside the shared scope would treat loaded descendants
                            // as never having existed. Embedded metadata (rights, version,
                            // external import) rides the same deep copy.
                            val movedChild = freshRow?.let { currentDriveRealm.copyFromRealm(it, Int.MAX_VALUE) }?.apply {
                                parentId = newParent.id
                                path = ""
                            } ?: file.apply {
                                parentId = newParent.id
                                path = ""
                            }
                            // THE ACTUAL DRIVE IN THE LOCK SCOPE (issue: round 22 #12): the
                            // scope previously omitted the driveId, so the merge's descendant
                            // lock keys differed from the coordinated byte workers' keys for
                            // the same rows — two lock key-spaces serialized nothing against
                            // each other. The moved file's own drive qualifies every key.
                            val sharedScope = UserDrive(
                                userId = presentationScope.userId,
                                driveId = file.driveId,
                                sharedWithMe = true,
                            )
                            var destinationVerified = false
                            FileController.getRealmInstance(sharedScope).use { sharedRealm ->
                                // EVERY affected identity locked (issue: round 18 #10): the
                                // destination root, the moved root and every id the deep graph
                                // carries — one ascending, constant-stack acquisition — so a
                                // destination writer cannot interleave between the merge's
                                // per-row decisions and its commit.
                                val graphIds = sortedSetOf(newParent.id, file.id)
                                runCatching {
                                    val pending = ArrayDeque<File>().apply { add(movedChild) }
                                    val visited = mutableSetOf<Int>()
                                    while (pending.isNotEmpty()) {
                                        val node = pending.removeFirst()
                                        if (!visited.add(node.id)) continue
                                        graphIds.add(node.id)
                                        node.children.forEach { pending.add(it) }
                                    }
                                }.onFailure(Sentry::captureException)
                                // DESTINATION-ONLY DESCENDANTS (issue: round 21 #7): the tree
                                // rename below relocates the destination's EXISTING subtree
                                // too — descendants that exist only in the destination (not
                                // in the source snapshot) previously fell outside the
                                // source-derived lock set. Their ids join the lock set (row
                                // walk here; the byte relocation itself re-walks the moved
                                // FILE tree under the locks).
                                runCatching {
                                    sharedRealm.where(File::class.java)
                                        .equalTo(File::id.name, file.id)
                                        .findFirst()?.let { existingTreeRoot ->
                                            val walker = ArrayDeque<File>().apply { add(existingTreeRoot) }
                                            val seen = mutableSetOf<Int>()
                                            while (walker.isNotEmpty()) {
                                                val node = walker.removeFirst()
                                                if (!seen.add(node.id)) continue
                                                graphIds.add(node.id)
                                                node.children.forEach { walker.add(it) }
                                            }
                                        }
                                }.onFailure(Sentry::captureException)
                                OfflineFileCoordination.withLocksFor(sharedScope, graphIds) {
                                    // COMPLETE FRESH DESTINATION ANCESTRY (issue: round 19
                                    // #10): both the inserted and the identity-changed root
                                    // derive their offline paths from the destination chain —
                                    // an ancestor row missing in the shared realm yields a
                                    // wrong path; the merge defers and the sync retries.
                                    var mergeAllowed = FileController.destinationChainExists(sharedRealm, newParent.id)
                                    // PRE-LOCK GRAPH REVALIDATION (issue: round 22 #12): the
                                    // destination tree was walked BEFORE the locks — a row
                                    // inserted beneath it since (an ancestor writer that takes
                                    // neither the intent monitor nor these per-row locks) is
                                    // neither locked nor merged, and the tree rename below
                                    // would relocate its bytes without its lock. The fresh
                                    // walk must not exceed the locked id set; otherwise the
                                    // merge defers and the sync retries with the fresh graph.
                                    runCatching {
                                        sharedRealm.where(File::class.java)
                                            .equalTo(File::id.name, file.id)
                                            .findFirst()?.let { lockTimeTreeRoot ->
                                                val walker = ArrayDeque<File>().apply { add(lockTimeTreeRoot) }
                                                val seen = mutableSetOf<Int>()
                                                while (walker.isNotEmpty()) {
                                                    val node = walker.removeFirst()
                                                    if (!seen.add(node.id)) continue
                                                    if (node.id !in graphIds) {
                                                        mergeAllowed = false
                                                        break
                                                    }
                                                    node.children.forEach { walker.add(it) }
                                                }
                                            }
                                    }.onFailure(Sentry::captureException)
                                    // DESTINATION BYTES ON IDENTITY CHANGE (issue: round 19
                                    // #10): an existing destination root keeps its OWN local
                                    // copy — the identity re-point changes its derived offline
                                    // path, so the bytes are reconciled to the post-move
                                    // location BEFORE the identity commit. An incomplete
                                    // reconciliation defers the whole merge (the source realm
                                    // keeps its row; the sync retries).
                                    if (mergeAllowed) {
                                        sharedRealm.where(File::class.java)
                                            .equalTo(File::id.name, file.id)
                                            .findFirst()?.let { preRoot ->
                                                if (preRoot.isOffline || preRoot.isInOfflineFolder) {
                                                    val oldBytes = preRoot.getOfflineFile(application, sharedScope.userId)
                                                        ?.takeIf { it.exists() }
                                                    if (oldBytes != null) {
                                                        val newBytes = newParent
                                                            .getOfflineFile(application, sharedScope.userId)
                                                            ?.let { parentDir -> IOFile(parentDir, preRoot.name) }
                                                        mergeAllowed = when {
                                                            newBytes == null || newBytes.path == oldBytes.path -> true
                                                            preRoot.isFolder() -> runCatching {
                                                                // A folder root's bytes are its whole
                                                                // directory tree: the rename relocates
                                                                // every descendant copy at once. Every
                                                                // coordinated byte worker serializes on
                                                                // these same descendant locks (all held),
                                                                // so the rename cannot race publication,
                                                                // cleanup or registration.
                                                                //
                                                                // ACTIVE-OWNERSHIP DEFERRAL + PROVENANCE
                                                                // RELOCATION (issue: round 21 #7): the
                                                                // renamed tree takes EVERY copy under the
                                                                // old directory — destination-only
                                                                // descendants included (locked via the
                                                                // destination walk above). A pending or
                                                                // retained-unsent record on ANY relocated
                                                                // copy's OLD literal URI defers the merge:
                                                                // the record would open a URI the rename
                                                                // destroys, and the deferral keeps the tree
                                                                // until the upload completes. Otherwise every
                                                                // moved copy's path-keyed PROVENANCE record
                                                                // travels with the bytes — a stranded
                                                                // old-path record could lend its generation
                                                                // to later bytes at the old location.
                                                                val relocatedPairs = mutableListOf<Pair<IOFile, IOFile>>()
                                                                var subtreeOk = true
                                                                val treeWalker = ArrayDeque<IOFile>().apply { add(oldBytes) }
                                                                while (treeWalker.isNotEmpty()) {
                                                                    val node = treeWalker.removeFirst()
                                                                    if (node.isDirectory) node.listFiles()?.forEach { treeWalker.add(it) }
                                                                    val nodeUri = Uri.fromFile(node).toString()
                                                                    if (UploadFile.hasPendingUpload(uri = nodeUri) ||
                                                                        UploadFile.hasRetainedUnsentUpload(uri = nodeUri)
                                                                    ) {
                                                                        subtreeOk = false
                                                                        break
                                                                    }
                                                                    if (node.isFile) {
                                                                        relocatedPairs +=
                                                                            node to IOFile(node.path.replaceFirst(oldBytes.path, newBytes.path))
                                                                    }
                                                                }
                                                                if (!subtreeOk) {
                                                                    false
                                                                } else {
                                                                    val renamed = oldBytes.renameTo(newBytes)
                                                                    if (renamed) {
                                                                        relocatedPairs.forEach { (oldCopy, newCopy) ->
                                                                            if (oldCopy.path != newCopy.path) {
                                                                                val proofMoved = runCatching {
                                                                                    DownloadOfflineFileManager.movePublishedProvenance(
                                                                                        application,
                                                                                        oldCopy,
                                                                                        newCopy,
                                                                                        sharedWithMe = sharedScope.sharedWithMe,
                                                                                    )
                                                                                }.getOrDefault(false)
                                                                                if (!proofMoved) {
                                                                                    // PROOF-FAILURE RECOVERY STATE
                                                                                    // (issue: round 22 #12): the
                                                                                    // rename already moved the bytes —
                                                                                    // the failed relocation leaves the
                                                                                    // copy UNPROVEN at the new path
                                                                                    // (never falsely proven, and no
                                                                                    // stranded old-path record:
                                                                                    // movePublishedProvenance
                                                                                    // invalidates the source record in
                                                                                    // every outcome), and the walk's
                                                                                    // verification re-establishes the
                                                                                    // proof.
                                                                                    SentryLog.i(
                                                                                        TAG,
                                                                                        "Provenance relocation failed during shared tree rename; ${oldCopy.name} stays unproven",
                                                                                    )
                                                                                }
                                                                            }
                                                                        }
                                                                    }
                                                                    renamed
                                                                }
                                                            }.getOrDefault(false)
                                                            else -> OfflineFolderController.reconcileMovedInheritedCopy(
                                                                context = application,
                                                                oldOfflineFile = oldBytes,
                                                                oldIdentityName = preRoot.name,
                                                                oldIdentityParentId = preRoot.parentId,
                                                                oldLastModifiedAt = preRoot.effectivePublishedBaseline,
                                                                newIdentity = File(id = preRoot.id, driveId = preRoot.driveId).apply {
                                                                    parentId = newParent.id
                                                                    name = preRoot.name
                                                                },
                                                                userDrive = sharedScope,
                                                                newOfflineTarget = newBytes,
                                                                realm = sharedRealm,
                                                            )
                                                        }
                                                    }
                                                }
                                            }
                                    }
                                    // PER-ROW DESTINATION MERGE (issue: round 18 #10): the
                                    // previous insert added the UNMANAGED deep graph whole —
                                    // Realm's primary-key upsert OVERWROTE every destination
                                    // descendant that already existed, replacing newer
                                    // identity, local flags and edited-copy provenance with
                                    // the source snapshot. The merge keeps an EXISTING
                                    // destination row authoritative (untouched, including its
                                    // own subtree) and supplies only the rows the destination
                                    // lacks; the moved root's identity (parentId/path) is the
                                    // one field an existing row takes, with its destination-
                                    // local state preserved.
                                    val insertedIds = mutableSetOf<Int>()
                                    val mergeOk = mergeAllowed && runCatching {
                                        sharedRealm.executeTransaction {
                                            val destinationProxy = FileController.getFileById(sharedRealm, newParent.id)
                                                ?: return@executeTransaction
                                            fun mergeChild(parentList: io.realm.RealmList<File>, parentOwnerId: Int, copy: File) {
                                                val existing = sharedRealm.where(File::class.java)
                                                    .equalTo(File::id.name, copy.id)
                                                    .findFirst()
                                                if (existing != null) {
                                                    // Destination row wins: PARENT-CONSISTENT link
                                                    // (issues: round 13 #12, round 19 #10) — an
                                                    // existing row whose authoritative parent is
                                                    // NOT this container must not gain a wrong
                                                    // backlink. But its MISSING source descendants
                                                    // still merge beneath it WHEREVER it lives
                                                    // (issue: round 21 #8): the previous immediate
                                                    // return stopped at every existing intermediate,
                                                    // so missing source grandchildren never merged
                                                    // and no retry could converge.
                                                    val detachedChildren = copy.children.toList()
                                                    if (existing.parentId == parentOwnerId &&
                                                        parentList.none { it.id == copy.id }
                                                    ) {
                                                        parentList.add(existing)
                                                    }
                                                    for (childCopy in detachedChildren) mergeChild(existing.children, existing.id, childCopy)
                                                    return
                                                }
                                                val detachedChildren = copy.children.toList()
                                                copy.children.clear()
                                                val inserted = sharedRealm.copyToRealm(copy)
                                                // The inserted row is LINKED under its parent
                                                // (issue: round 19 #10): copyToRealm alone never
                                                // attaches it to the parent's children list — the
                                                // previous order inserted unlinked rows, whose
                                                // committed verification then failed and deferred
                                                // every fresh-root merge forever.
                                                if (parentList.none { it.id == inserted.id }) parentList.add(inserted)
                                                insertedIds.add(inserted.id)
                                                for (childCopy in detachedChildren) mergeChild(inserted.children, inserted.id, childCopy)
                                            }
                                            val existingRoot = sharedRealm.where(File::class.java)
                                                .equalTo(File::id.name, file.id)
                                                .findFirst()
                                            if (existingRoot != null) {
                                                // Identity moves; destination-local state preserved;
                                                // the destination subtree stays authoritative.
                                                val previousParentId = existingRoot.parentId
                                                existingRoot.parentId = newParent.id
                                                existingRoot.path = ""
                                                if (destinationProxy.children.none { it.id == file.id }) {
                                                    destinationProxy.children.add(existingRoot)
                                                }
                                                if (previousParentId != newParent.id && previousParentId > 0) {
                                                    FileController.getFileById(sharedRealm, previousParentId)?.children
                                                        ?.removeAll { it.id == file.id }
                                                }
                                                // The source graph's descendants the destination
                                                // LACKS are still merged (issue: round 19 #10):
                                                // destination rows stay authoritative per-row; a
                                                // child that exists only in the source must not
                                                // vanish with the source removal.
                                                movedChild.children.forEach { mergeChild(existingRoot.children, existingRoot.id, it) }
                                            } else {
                                                mergeChild(destinationProxy.children, newParent.id, movedChild)
                                            }
                                            // PERSISTED-PATH INVALIDATION ACROSS THE WHOLE
                                            // RELOCATED TREE (issue: round 22 #12): the tree
                                            // rename relocated every descendant's bytes, but only
                                            // the moved root's persisted path was cleared — every
                                            // descendant row (destination-existing rows pointing at
                                            // the old destination location, and inserted rows
                                            // carrying their SOURCE persisted paths) kept a path
                                            // that no longer holds its bytes, and getRemotePath
                                            // PREFERS a non-blank persisted path — destination-only
                                            // unregistered edits became inaccessible at their
                                            // recorded paths. Every row in the committed tree is
                                            // invalidated (path = ""); the path regenerates from
                                            // the live parent chain on next use.
                                            runCatching {
                                                FileController.getFileById(sharedRealm, file.id)?.let { committedTreeRoot ->
                                                    val pathStack = ArrayDeque<File>().apply { add(committedTreeRoot) }
                                                    val pathSeen = mutableSetOf<Int>()
                                                    while (pathStack.isNotEmpty()) {
                                                        val node = pathStack.removeFirst()
                                                        if (!pathSeen.add(node.id)) continue
                                                        node.path = ""
                                                        node.children.forEach { pathStack.add(it) }
                                                    }
                                                }
                                            }.onFailure(Sentry::captureException)
                                        }
                                    }.onFailure(Sentry::captureException).isSuccess
                                    // REQUIRED-CONTENT TRANSFER OR DURABLE REPLACEMENT WORK
                                    // (issue: round 22 #13): merged descendants carry their
                                    // offline flags, but their bytes remain at the SOURCE-derived
                                    // paths (a different drive base) — source cleanup would
                                    // delete the clean copies and the destination would appear
                                    // offline while its bytes are absent. Every committed
                                    // flagged file copy whose destination-derived bytes are
                                    // absent gets its source bytes TRANSFERRED (a copy — the
                                    // source removal owns deletion; copies backing pending or
                                    // retained-unsent uploads are never duplicated) or, when
                                    // no transferable source bytes exist, the DURABLE download
                                    // intent (isMarkedAsOffline) — the worker establishes the
                                    // bytes at the destination path. The verification below
                                    // then requires, for every required flagged row, bytes OR
                                    // the durable intent before the source metadata is
                                    // removed.
                                    if (mergeOk) {
                                        runCatching {
                                            FileController.getFileById(sharedRealm, file.id)?.let { transferredTreeRoot ->
                                                val transferStack = ArrayDeque<File>().apply { add(transferredTreeRoot) }
                                                val transferSeen = mutableSetOf<Int>()
                                                while (transferStack.isNotEmpty()) {
                                                    val node = transferStack.removeFirst()
                                                    if (!transferSeen.add(node.id)) continue
                                                    node.children.forEach { transferStack.add(it) }
                                                    if (node.isFolder() || !(node.isOffline || node.isInOfflineFolder)) continue
                                                    // SOURCE FIRST (issues: round 25 #1/#2): the source
                                                    // row, bytes and CONTENT CLASSIFICATION are resolved
                                                    // and decided BEFORE any destination effect — the
                                                    // previous order staged, renamed, and only then
                                                    // inspected the source proof, letting an mtime-only
                                                    // fallback register foreign or unproven historical
                                                    // bytes as an edit under the destination identity.
                                                    val sourceRow = FileController.getFileById(currentDriveRealm, node.id)
                                                    val sourceBytes = sourceRow
                                                        ?.getOfflineFile(application, userDrive.userId)
                                                        ?.takeIf { it.exists() }
                                                    val sourceHash = sourceBytes?.let { UploadFile.sha256OrNull(it) }
                                                    val sourceClaimed = sourceBytes == null || runCatching {
                                                        val sourceUri = Uri.fromFile(sourceBytes).toString()
                                                        UploadFile.hasPendingUpload(uri = sourceUri) ||
                                                            UploadFile.hasRetainedUnsentUpload(uri = sourceUri)
                                                    }.getOrDefault(true)
                                                    val sourceProof = if (sourceBytes != null) {
                                                        DownloadOfflineFileManager.readPublishedProvenance(application, sourceBytes)
                                                    } else null
                                                    // POSITIVE SOURCE CONTENT CLASSIFICATION (issue: round
                                                    // 25 #2): only a POSITIVELY ESTABLISHED edit may be
                                                    // registered under the destination identity — a
                                                    // positively foreign record and unknown/hashless
                                                    // historical bytes are never auto-uploaded on an
                                                    // mtime comparison; certified clean bytes are
                                                    // relocatable published content, and proven drift is
                                                    // the established edit.
                                                    val sourceClassifiedEdit = sourceRow != null && sourceBytes != null &&
                                                        sourceHash != null && run {
                                                        val anchored = (sourceRow.publishedCleanBaseline ?: 0L) > 0L
                                                        when {
                                                            sourceProof?.positivelyForeignTo(sourceRow, userDrive) == true -> false
                                                            sourceProof?.identifies(sourceRow, userDrive) == true &&
                                                                sourceProof.contentHash == sourceHash -> false
                                                            sourceProof?.identifies(sourceRow, userDrive) == true &&
                                                                sourceProof.contentHash != null &&
                                                                sourceProof.contentHash != sourceHash -> true
                                                            anchored && sourceProof == null &&
                                                                OfflineFolderPolicy.isLocallyModified(
                                                                    offlineFileLastModifiedMillis = sourceBytes.lastModified(),
                                                                    remoteLastModifiedAtSeconds = sourceRow.effectivePublishedBaseline,
                                                                ) -> true
                                                            else -> false
                                                        }
                                                    }
                                                    val destinationBytes = sharedRealmOfflineFile(application, node, sharedScope)
                                                    val destinationExists = destinationBytes != null && destinationBytes.exists()
                                                    val destinationProof = if (destinationExists) {
                                                        DownloadOfflineFileManager.readPublishedProvenance(application, destinationBytes!!)
                                                    } else null
                                                    // POSITIVELY FOREIGN DESTINATION CONTENT IS NEVER
                                                    // TOUCHED (issue: round 24 #12): another identity's
                                                    // bytes at the destination are a conflict — the
                                                    // transfer never renames over them, and the strict
                                                    // verification below defers the source removal.
                                                    if (destinationProof?.positivelyForeignTo(node, sharedScope) == true) continue
                                                    val destinationHash = if (destinationExists) {
                                                        UploadFile.sha256OrNull(destinationBytes!!)
                                                    } else null
                                                    // STRICT VERIFIED-USABLE DESTINATION CONTENT (issue:
                                                    // round 24 #13): a proof that CERTIFIES the current
                                                    // bytes for this row, or EXACT unsent-revision
                                                    // ownership proven over them.
                                                    val destinationVerified = destinationHash != null && (
                                                        (destinationProof?.identifies(node, sharedScope) == true &&
                                                            destinationProof.contentHash != null &&
                                                            destinationProof.contentHash == destinationHash) ||
                                                            UploadFile.hasDurableOriginalRevisionOwnership(
                                                                fileName = node.name,
                                                                remoteFolder = node.parentId,
                                                                driveId = sharedScope.driveId,
                                                                userId = sharedScope.userId,
                                                                fileModifiedAt = Date(destinationBytes!!.lastModified()),
                                                                fileSize = destinationBytes.length(),
                                                                contentHash = destinationHash,
                                                                originalFileId = node.id,
                                                            )
                                                        )
                                                    // THE PER-NODE PRESERVATION OUTCOME (issues: round 25
                                                    // #1, round 26 #1): the source's EXACT CURRENT
                                                    // revision must exist at the destination or in a
                                                    // VERIFIED durable snapshot before the source may
                                                    // ever be unlinked. ABSENT source bytes need no
                                                    // preservation; an EXISTING source whose hash is
                                                    // UNAVAILABLE (a failed read) is UNRESOLVED —
                                                    // filesystem unlink does not require reading the
                                                    // contents, so a hash failure is never success: the
                                                    // node defers and the source stays until its exact
                                                    // revision is independently proven preserved.
                                                    val sourcePreserved = sourceBytes == null ||
                                                        (sourceHash != null &&
                                                            (destinationHash == sourceHash ||
                                                                (destinationExists && destinationBytes != null &&
                                                                    UploadFile.hasDurableOriginalRevisionOwnership(
                                                                        fileName = node.name,
                                                                        remoteFolder = node.parentId,
                                                                        driveId = sharedScope.driveId,
                                                                        userId = sharedScope.userId,
                                                                        fileModifiedAt = Date(destinationBytes.lastModified()),
                                                                        fileSize = destinationBytes.length(),
                                                                        contentHash = sourceHash,
                                                                        originalFileId = node.id,
                                                                    ))))
                                                    if (sourcePreserved && destinationVerified) continue
                                                    // REPLACE AUTHORITY BEFORE ANY EFFECT (issues: round 24
                                                    // #12, round 26 #2): the staged transfer replaces the
                                                    // destination only when it is ABSENT or byte-
                                                    // CONVERGENT with the verified source copy — AND the
                                                    // source itself is EXCLUSIVELY OWNED by this row and
                                                    // carries an ALLOWED content classification. A
                                                    // foreign or ambiguous source (positively foreign
                                                    // record, or unproven bytes the classification cannot
                                                    // establish) RETAINS ITS IDENTITY and defers the
                                                    // ENTIRE transfer — its bytes are never staged, never
                                                    // renamed, never registered; a pending/retained-unsent
                                                    // upload likewise keeps ownership of its bytes.
                                                    val sourceExclusivelyOwned = sourceRow != null && sourceBytes != null &&
                                                        sourceHash != null &&
                                                        OfflineFolderController.exclusivePathOwner(
                                                            realm = currentDriveRealm,
                                                            context = application,
                                                            userDrive = userDrive,
                                                            claimedPath = sourceBytes.absolutePath,
                                                        ) == sourceRow.id
                                                    val destinationReplaceable = sourceExclusivelyOwned && !sourceClaimed && (
                                                        sourceClassifiedEdit ||
                                                            (sourceProof?.identifies(sourceRow, userDrive) == true &&
                                                                sourceProof.contentHash == sourceHash)
                                                        ) && (
                                                        !destinationExists || destinationHash == sourceHash
                                                        )
                                                    // STAGED, VERIFIED TRANSFER (issues: round 23 #13, round
                                                    // 24 #12): the copy is STAGED, verified against the
                                                    // source's complete size and hash, the mtime travels,
                                                    // and only then is the destination published by an
                                                    // atomic rename — and only onto an authorized
                                                    // destination (absent or convergent).
                                                    val transferred = destinationReplaceable &&
                                                        sourceBytes != null && destinationBytes != null && runCatching {
                                                        val stagedTransfer = DownloadOfflineFileManager.newStagedFile(application, node)
                                                        try {
                                                            sourceBytes.copyTo(stagedTransfer, overwrite = true)
                                                            val stagedHash = UploadFile.sha256OrNull(stagedTransfer)
                                                            val stagedOk = stagedTransfer.exists() &&
                                                                stagedTransfer.length() == sourceBytes.length() &&
                                                                stagedHash != null && stagedHash == sourceHash
                                                            if (!stagedOk) return@runCatching false
                                                            stagedTransfer.setLastModified(sourceBytes.lastModified())
                                                            destinationBytes.parentFile?.mkdirs()
                                                            if (!stagedTransfer.renameTo(destinationBytes)) return@runCatching false
                                                            // THE PROOF DECISION IS THE TRANSFER'S OUTCOME,
                                                            // NEVER AN IGNORED SIDE CALL (issue: round 24
                                                            // #12/#13): a proof that did not travel (absent,
                                                            // foreign, or a failed move) leaves the
                                                            // transferred bytes UNPROVEN — the durable
                                                            // resolution below re-establishes ownership; the
                                                            // final verification never accepts them bare.
                                                            val sourceProof = DownloadOfflineFileManager.readPublishedProvenance(application, sourceBytes)
                                                            val proofMoved = sourceProof != null &&
                                                                !sourceProof.positivelyForeignTo(node, sharedScope) &&
                                                                DownloadOfflineFileManager.movePublishedProvenance(
                                                                    application,
                                                                    sourceBytes,
                                                                    destinationBytes,
                                                                    sharedWithMe = sharedScope.sharedWithMe,
                                                                )
                                                            if (!proofMoved) {
                                                                // POSITIVE EDIT REGISTRATION ONLY (issue:
                                                                // round 25 #2): the classification was
                                                                // established BEFORE any destination effect
                                                                // from the source's own proof, anchor and
                                                                // bytes — an mtime comparison alone is
                                                                // NEVER authority to register foreign or
                                                                // ambiguous historical bytes as this
                                                                // destination's edit (they stay unproven
                                                                // and retained).
                                                                if (sourceClassifiedEdit) {
                                                                    // UNSENT-REVISION OWNERSHIP AT THE DESTINATION
                                                                    // (issue: round 24 #13): a dirty edit's
                                                                    // transferred bytes are registered as the
                                                                    // DESTINATION row's durable snapshot — the
                                                                    // upload carries the revision to the new
                                                                    // location and the verification's
                                                                    // exact-ownership arm can accept it; the bare
                                                                    // marker would let a worker download over
                                                                    // the unsent edit.
                                                                    runCatching {
                                                                        OfflineFolderController.registerDirtySnapshot(
                                                                            context = application,
                                                                            file = node,
                                                                            offlineFile = destinationBytes,
                                                                            userDrive = sharedScope,
                                                                        )
                                                                    }.onFailure(Sentry::captureException)
                                                                } else {
                                                                    // Clean unproven bytes: the durable download
                                                                    // intent makes the worker establish a PROVEN
                                                                    // publication at the destination.
                                                                    FileController.markFileAsOffline(
                                                                        customRealm = sharedRealm,
                                                                        fileId = node.id,
                                                                        isMarkedAsOffline = true,
                                                                    )
                                                                }
                                                            }
                                                            true
                                                        } finally {
                                                            runCatching { if (stagedTransfer.exists()) stagedTransfer.delete() }
                                                        }
                                                    }.getOrDefault(false)
                                                    // THE PRESERVATION GAP IS A DURABLE INTENT (issue:
                                                    // round 25 #1): a node whose source revision is
                                                    // not yet preserved (valid old destination
                                                    // generation over a newer unsent source) keeps
                                                    // the pending marker — the source bytes stay in
                                                    // place and the final verification's
                                                    // preservation arm keeps the source metadata
                                                    // until a worker or snapshot resolves them.
                                                    if (!transferred && (!destinationVerified || !sourcePreserved)) {
                                                        FileController.markFileAsOffline(
                                                            customRealm = sharedRealm,
                                                            fileId = node.id,
                                                            isMarkedAsOffline = true,
                                                        )
                                                    }
                                                }
                                            }
                                        }.onFailure(Sentry::captureException)
                                    }
                                    // COMMITTED VERIFICATION BEFORE SOURCE REMOVAL (issues:
                                    // round 17 #16, round 18 #10, round 19 #10): the root's
                                    // COMMITTED identity AND destination membership, plus the
                                    // FULL REQUIRED GRAPH — every source-graph id (the root
                                    // and each descendant) must exist in the destination
                                    // realm, whether it merged as an inserted row or as a
                                    // pre-existing destination row. Mere existence of SOME
                                    // row with the id never certified a failed or skipped
                                    // insertion, and verifying only the rows this merge
                                    // inserted let a gap in the merged graph erase source
                                    // metadata the destination never received.
                                    destinationVerified = mergeOk && runCatching {
                                        val committedRoot = sharedRealm.where(File::class.java)
                                            .equalTo(File::id.name, file.id)
                                            .equalTo(File::parentId.name, newParent.id)
                                            .findFirst()
                                        val membershipOk = committedRoot != null && FileController.getFileById(sharedRealm, newParent.id)
                                            ?.children?.any { it.id == file.id } == true
                                        // REQUIRED-GRAPH RELATIONSHIPS (issue: round 21 #8):
                                        // every required id must be REACHABLE from the committed
                                        // root through the destination's LIVE children lists —
                                        // id existence alone previously satisfied verification
                                        // with the required parent/child relationships
                                        // unverified (a required id existing ELSEWHERE in the
                                        // realm — unreachable from the moved tree — passed
                                        // while the graph under the moved root stayed
                                        // incomplete, erasing source metadata the reachable
                                        // destination never received).
                                        val reachable = mutableSetOf<Int>()
                                        var ownedCopiesOk = true
                                        if (committedRoot != null) {
                                            val relationWalker = ArrayDeque<File>().apply { add(committedRoot) }
                                            val relationSeen = mutableSetOf<Int>()
                                            while (relationWalker.isNotEmpty()) {
                                                val node = relationWalker.removeFirst()
                                                if (!relationSeen.add(node.id)) continue
                                                reachable.add(node.id)
                                                // AUTHORITATIVE EDGES (issue: round 22 #13): a
                                                // children-list backlink whose row's own parentId
                                                // does NOT name this container is not an
                                                // authoritative relationship — the walk does not
                                                // descend through it and the id stays unverifiable
                                                // (the source metadata is retained below).
                                                node.children.createSnapshot().forEach { child ->
                                                    if (child.parentId == node.id) relationWalker.add(child)
                                                }
                                                // OWNED-COPY OUTCOMES (issues: round 22 #13, round
                                                // 23 #12/#13, round 24 #13): every flagged file row in
                                                // the committed tree must hold VERIFIED-USABLE content
                                                // at its ACTUAL-SHARED-REALM destination path — a proof
                                                // that CERTIFIES the current bytes for this row, or
                                                // EXACT unsent-revision ownership proven over them.
                                                // Existence alone (a partial copy, or bytes resolved
                                                // from the wrong realm), a bare identifying proof with
                                                // drifted bytes, and the pending marker are NOT
                                                // authority to remove the source.
                                                if (!node.isFolder() && (node.isOffline || node.isInOfflineFolder)) {
                                                    val nodeBytes = sharedRealmOfflineFile(application, node, sharedScope)
                                                    val nodeProof = nodeBytes?.takeIf { it.exists() }?.let {
                                                        DownloadOfflineFileManager.readPublishedProvenance(application, it)
                                                    }
                                                    val nodeHash = nodeBytes?.takeIf { it.exists() }?.let { UploadFile.sha256OrNull(it) }
                                                    val nodeVerified = nodeHash != null && (
                                                        (nodeProof?.identifies(node, sharedScope) == true &&
                                                            nodeProof.contentHash != null &&
                                                            nodeProof.contentHash == nodeHash) ||
                                                            UploadFile.hasDurableOriginalRevisionOwnership(
                                                                fileName = node.name,
                                                                remoteFolder = node.parentId,
                                                                driveId = sharedScope.driveId,
                                                                userId = sharedScope.userId,
                                                                fileModifiedAt = Date(nodeBytes.lastModified()),
                                                                fileSize = nodeBytes.length(),
                                                                contentHash = nodeHash,
                                                                originalFileId = node.id,
                                                            )
                                                        )
                                                    // THE SOURCE'S EXACT REVISION IS PRESERVED (issues:
                                                    // round 25 #1, round 26 #1): verified DESTINATION
                                                    // content alone never authorizes removing a NEWER
                                                    // unsent source revision. ABSENT source bytes need
                                                    // no preservation; an EXISTING source whose hash
                                                    // read FAILED is UNRESOLVED — the node stays
                                                    // unverified and the source metadata is retained.
                                                    val sourceRow = FileController.getFileById(currentDriveRealm, node.id)
                                                    val sourceBytes = sourceRow
                                                        ?.getOfflineFile(application, userDrive.userId)
                                                        ?.takeIf { it.exists() }
                                                    val sourceHash = sourceBytes?.let { UploadFile.sha256OrNull(it) }
                                                    val sourcePreserved = sourceBytes == null ||
                                                        (sourceHash != null &&
                                                            (nodeHash == sourceHash ||
                                                                (nodeBytes != null &&
                                                                    UploadFile.hasDurableOriginalRevisionOwnership(
                                                                        fileName = node.name,
                                                                        remoteFolder = node.parentId,
                                                                        driveId = sharedScope.driveId,
                                                                        userId = sharedScope.userId,
                                                                        fileModifiedAt = Date(nodeBytes.lastModified()),
                                                                        fileSize = nodeBytes.length(),
                                                                        contentHash = sourceHash,
                                                                        originalFileId = node.id,
                                                                    ))))
                                                    if (!nodeVerified || !sourcePreserved) ownedCopiesOk = false
                                                }
                                            }
                                        }
                                        val requiredIds = graphIds - newParent.id
                                        val descendantsOk = requiredIds.all { it in reachable }
                                        membershipOk && descendantsOk && ownedCopiesOk
                                    }.getOrDefault(false)
                                }
                            }
                            if (destinationVerified) {
                                // DURABLE SHARED PATHS BEFORE SOURCE UNLINKING (issue: round
                                // 24 #14): the merge clears the relocated tree's persisted
                                // paths and generateAndSavePath persists replacements in a
                                // DETACHED coroutine — the source removal could finish before
                                // those writes, and an interruption in that window left the
                                // shared row with a blank path that ordinary consumers cannot
                                // regenerate (their scope reads the realm the source row no
                                // longer lives in). Every required flagged row's shared path
                                // is resolved and persisted SYNCHRONOUSLY, under this
                                // section's locks, before the source metadata is removed.
                                // FAIL-CLOSED PERSISTENCE (issue: round 26 #16): the gate starts
                                // FALSE and becomes true ONLY when the whole walk completed — a
                                // missing committed root, an exception opening or resolving the
                                // shared graph, or any per-path failure RETAINS the source
                                // metadata; the previous default-true let exceptions and a
                                // missing root proceed to unlinking without durable paths.
                                var requiredPathsPersisted = false
                                runCatching {
                                    // THE CAPTURED SCOPE'S OWN REALM INSTANCE (round-24 build
                                    // correction): the merge's sharedRealm session closed with
                                    // its use block — the walk opens a FRESH instance on the
                                    // SAME captured sharedScope (never a global/current-account
                                    // fallback) and closes it deterministically.
                                    FileController.getRealmInstance(sharedScope).use { pathRealm ->
                                        val pathRoot = FileController.getFileById(pathRealm, file.id)
                                            ?: return@use // missing required root: the gate stays false
                                        val pathStack = ArrayDeque<File>().apply { add(pathRoot) }
                                        val pathSeen = mutableSetOf<Int>()
                                        var allPersisted = true
                                        while (pathStack.isNotEmpty()) {
                                            val node = pathStack.removeFirst()
                                            if (!pathSeen.add(node.id)) continue
                                            node.children.forEach { pathStack.add(it) }
                                            if (node.isFolder() || !(node.isOffline || node.isInOfflineFolder)) continue
                                            val nodeScope = UserDrive(
                                                userId = sharedScope.userId,
                                                driveId = node.driveId.takeIf { it > 0 } ?: sharedScope.driveId,
                                                sharedWithMe = true,
                                            )
                                            val resolvedPath = FileController.generateAndSavePath(node.id, nodeScope)
                                            // EVERY REQUIRED PATH PERSISTS WITH READ-BACK
                                            // BEFORE THE SOURCE UNLINK (issue: round 25 #13):
                                            // ALL required shared path persistence (the
                                            // synchronous persist AND the row's stored path
                                            // reading back as the resolved value) must
                                            // succeed — the previous walk ignored the persist
                                            // result, so a failed or deferred write left the
                                            // relocated shared row pathless while the source
                                            // metadata was removed anyway.
                                            val persisted = resolvedPath.isNotBlank() &&
                                                FileController.persistResolvedPath(node.id, nodeScope, resolvedPath) &&
                                                runCatching { pathRealm.refresh() }.isSuccess &&
                                                FileController.getFileById(pathRealm, node.id)?.path == resolvedPath
                                            if (!persisted) {
                                                allPersisted = false
                                                break
                                            }
                                        }
                                        if (allPersisted) requiredPathsPersisted = true
                                    }
                                }.onFailure {
                                    requiredPathsPersisted = false
                                    Sentry.captureException(it)
                                }
                                // THE UNLINK GATE (issue: round 25 #13): an unresolved required
                                // path RETAINS the source metadata (row, ancestry and bytes) —
                                // the move re-processes on a later pass.
                                if (requiredPathsPersisted) {
                                    // The source backlink is removed only AFTER the destination's
                                    // committed graph verified (issue: round 21 #8).
                                    FileController.updateFile(file.parentId, currentDriveRealm) { localFolder ->
                                        runCatching { localFolder.children.remove(file) }
                                    }
                                    FileController.removeFile(
                                        fileId = file.id,
                                        customRealm = currentDriveRealm,
                                        userDrive = userDrive,
                                    )
                                } else {
                                    // A required shared path failed to persist durably (issue:
                                    // round 25 #13): the source metadata is retained and the
                                    // move re-processes on a later pass.
                                    SentryLog.i("MainViewModel", "Deferred shared move of ${file.id}: a required shared path did not persist durably")
                                }
                            } else {
                                // The row keeps its OLD identity and location in this realm;
                                // the offline sync re-processes the move until the shared
                                // destination accepts it (issues: round 8 #5, round 17 #16).
                                SentryLog.i("MainViewModel", "Deferred shared move of ${file.id}: destination insertion was not verified")
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
                        // offline paths keep pointing at the location the bytes left.
                        // SAME-REALM moves mutate the ACCEPTED MANAGED row in place (issue:
                        // round 16 #10): a depth-zero detached copy inserted into the managed
                        // children list UPSERTS by primary key and would overwrite the embedded
                        // metadata (rights, version, external import) and null a folder's child
                        // list while retaining isComplete. Mutating the identity/path on the
                        // live row and linking it preserves the whole graph, and the link's
                        // transaction-fresh container check now applies (the committed row
                        // names this destination).
                        freshRow?.let { acceptedRow ->
                            FileController.updateFile(file.id, currentDriveRealm, userDrive = userDrive) { localFile ->
                                localFile.parentId = newParent.id
                                localFile.path = ""
                            }
                            FileController.addChild(newParent.id, acceptedRow, currentDriveRealm, userDrive)
                        } ?: run {
                            // The row vanished mid-section: recreate the moved identity from the
                            // captured request object — an INSERT (no existing row is overwritten;
                            // later listings restore the full model).
                            val movedChild = file.apply {
                                parentId = newParent.id
                                path = ""
                            }
                            FileController.addChild(newParent.id, movedChild, currentDriveRealm, userDrive)
                        }
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
                                when (OfflineFolderController.decisionForReplacingOwnedCopy(liveRow, offlineFile, userDrive, realm)) {
                                    OfflineFolderController.OwnedCopyReplacementDecision.KEEP_LOCAL -> Unit
                                    // Contested/foreign path (issue: round 18 #5): preserve the
                                    // bytes like KEEP_LOCAL — they are NOT certified as this
                                    // file's content, so they are also never deleted here.
                                    OfflineFolderController.OwnedCopyReplacementDecision.DEFERRED_FOREIGN -> Unit
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
                // round 16 #6): never a later, uncoordinated coroutine on captured paths. The
                // provenance sidecar dies with the bytes (issue: round 16 #12).
                deleteOfflinePath?.let { path ->
                    mediaFile?.let { file -> file.deleteInMediaScan(context, userDrive) }
                    DownloadOfflineFileManager.deletePublishedProvenance(context, IOFile(path))
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
     * Resolves a shared row's offline file through the ACTUAL SHARED REALM (issue: round
     * 23 #12): [File.getOfflineFile] builds `UserDrive(userId, driveId)` with
     * sharedWithMe = false, so path generation for a shared row reads the ORDINARY
     * realm's ancestry — while the ordinary source row still exists, destination
     * verification resolved the SOURCE path, saw the source bytes, skipped the transfer
     * and the source removal then deleted them. The base directory layout is identical;
     * only the ancestry realm differs.
     */
    private fun sharedRealmOfflineFile(context: Context, row: File, scope: UserDrive): IOFile? {
        val sharedUserDrive = UserDrive(userId = scope.userId, driveId = row.driveId, sharedWithMe = true)
        val remotePath = FileController.generateAndSavePath(row.id, sharedUserDrive)
        if (remotePath.isEmpty()) return null
        val rootFolder = IOFile(File.getOfflineFolder(context), "${scope.userId}/${row.driveId}")
        val child = if (row.isFolder()) remotePath else remotePath.substringBeforeLast("/")
        val folder = IOFile(rootFolder, child)
        if (!folder.exists()) folder.mkdirs()
        return if (row.isFolder()) folder else IOFile(folder, row.name)
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
        // COMPLETE FRESH DESTINATION ANCESTRY BEFORE THE FIRST PRESERVATION EFFECT
        // (issue: round 22 #9): the relocation below anchors and moves/snapshots bytes
        // beneath the destination's parent chain — a locally missing ancestor strands
        // them there when the metadata commit later fails. Returning false keeps the
        // old bytes and metadata; the move retries once the chain resolves.
        if (!FileController.destinationChainExists(realm, newParent.id)) return false
        // The snapshot registers under the POST-MOVE identity (issue: round 7 #5): the edit
        // belongs to the moved file, and registering the source parent would queue the upload
        // for the old location while the caller commits the new one. Protection queries keep
        // the source identity separately (oldIdentity* below).
        val postMoveIdentity = File(id = file.id, driveId = userDrive.driveId).apply {
            parentId = newParent.id
            name = file.name
        }
        // Historical provenance anchored BEFORE the relocation (issue: round 17 #3).
        OfflineFolderController.anchorHistoricalBaseline(application, realm, file, ioFile, userDrive)
        return OfflineFolderController.reconcileMovedInheritedCopy(
            context = application,
            oldOfflineFile = ioFile,
            oldIdentityName = file.name,
            oldIdentityParentId = file.parentId,
            // The bytes' provenance baseline (issue: round 16 #2), never bare server metadata
            // that a backward-dated replacement may have moved away from the bytes.
            oldLastModifiedAt = file.effectivePublishedBaseline,
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
