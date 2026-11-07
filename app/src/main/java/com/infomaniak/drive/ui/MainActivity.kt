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

import android.annotation.SuppressLint
import android.app.Dialog
import android.content.ContentResolver
import android.content.Context
import android.content.Intent
import android.content.Intent.FLAG_ACTIVITY_REORDER_TO_FRONT
import android.graphics.Bitmap
import android.graphics.Canvas
import android.graphics.Paint
import android.graphics.drawable.Drawable
import android.graphics.drawable.StateListDrawable
import android.net.Uri
import android.os.Build
import android.os.Build.VERSION.SDK_INT
import android.os.Bundle
import android.os.FileObserver
import android.provider.DocumentsContract
import android.provider.MediaStore
import android.view.GestureDetector
import android.view.MotionEvent
import android.view.View
import androidx.activity.result.IntentSenderRequest
import androidx.activity.result.contract.ActivityResultContracts
import androidx.activity.result.contract.ActivityResultContracts.StartIntentSenderForResult
import androidx.activity.viewModels
import androidx.annotation.OptIn
import androidx.core.content.ContextCompat
import androidx.core.content.pm.ShortcutManagerCompat
import androidx.core.graphics.drawable.toBitmap
import androidx.core.graphics.drawable.toDrawable
import androidx.core.net.toUri
import androidx.core.view.get
import androidx.core.view.isGone
import androidx.core.view.isVisible
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.flowWithLifecycle
import androidx.lifecycle.lifecycleScope
import androidx.media3.common.util.UnstableApi
import androidx.navigation.NavController
import androidx.navigation.NavDestination
import androidx.navigation.findNavController
import androidx.navigation.fragment.NavHostFragment
import androidx.work.WorkInfo
import androidx.work.WorkManager
import coil3.asDrawable
import coil3.request.ImageRequest
import coil3.request.crossfade
import coil3.request.error
import coil3.request.fallback
import coil3.request.placeholder
import coil3.request.transformations
import coil3.transform.CircleCropTransformation
import com.google.android.material.bottomnavigation.BottomNavigationMenuView
import com.google.android.material.floatingactionbutton.FloatingActionButton
import com.google.android.material.navigation.NavigationBarItemView
import com.google.android.material.snackbar.Snackbar
import com.infomaniak.core.applock.AppLockManager
import com.infomaniak.core.applock.view.AppLockViewActivity
import com.infomaniak.core.coil.ImageLoaderProvider.simpleImageLoader
import com.infomaniak.core.coil.generateInitialsAvatarDrawable
import com.infomaniak.core.coil.getBackgroundColorBasedOnId
import com.infomaniak.core.common.doesFileExist
import com.infomaniak.core.common.extensions.isNightModeEnabled
import com.infomaniak.core.common.observe
import com.infomaniak.core.inappreview.BaseInAppReviewManager
import com.infomaniak.core.inappreview.reviewmanagers.InAppReviewManager
import com.infomaniak.core.inappreview.view.ReviewAlertDialog
import com.infomaniak.core.inappreview.view.ReviewAlertDialogData
import com.infomaniak.core.legacy.utils.whenResultIsOk
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.core.ui.view.extension.setMargins
import com.infomaniak.core.ui.view.utils.SnackbarUtils.showIndefiniteSnackbar
import com.infomaniak.core.ui.view.utils.SnackbarUtils.showSnackbar
import com.infomaniak.drive.GeniusScanUtils.getScanFlowContract
import com.infomaniak.drive.GeniusScanUtils.scanResultProcessing
import com.infomaniak.drive.GeniusScanUtils.startScanFlow
import com.infomaniak.drive.MainApplication
import com.infomaniak.drive.MatomoDrive.MatomoCategory
import com.infomaniak.drive.MatomoDrive.MatomoName
import com.infomaniak.drive.MatomoDrive.trackAccountEvent
import com.infomaniak.drive.MatomoDrive.trackEvent
import com.infomaniak.drive.MatomoDrive.trackInAppReview
import com.infomaniak.drive.MatomoDrive.trackInAppUpdate
import com.infomaniak.drive.MatomoDrive.trackMyKSuiteEvent
import com.infomaniak.drive.R
import com.infomaniak.drive.data.models.AppSettings
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.File.VisibilityType
import com.infomaniak.drive.data.models.MqttAction
import com.infomaniak.drive.data.models.PhotoPairing
import com.infomaniak.drive.data.models.UiSettings
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.deeplink.DeeplinkType.DeeplinkAction
import com.infomaniak.drive.data.sync.PhotoTrashController
import com.infomaniak.drive.data.services.BaseDownloadWorker
import com.infomaniak.drive.data.services.BaseDownloadWorker.Companion.HAS_SPACE_LEFT_AFTER_DOWNLOAD_KEY
import com.infomaniak.drive.data.services.MqttClientWrapper
import com.infomaniak.drive.databinding.ActivityMainBinding
import com.infomaniak.drive.extensions.addSentryBreadcrumb
import com.infomaniak.drive.extensions.onApplyWindowInsetsListener
import com.infomaniak.drive.extensions.trackDestination
import com.infomaniak.drive.ui.addFiles.AddFileBottomSheetDialogArgs
import com.infomaniak.drive.ui.bottomSheetDialogs.FileInfoActionsBottomSheetDialogArgs
import com.infomaniak.drive.ui.deeplink.DeeplinkHandler
import com.infomaniak.drive.ui.fileList.FileListFragmentArgs
import com.infomaniak.drive.ui.fileList.preview.playback.VideoActivity
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.DownloadOfflineFileManager
import com.infomaniak.drive.utils.DrivePermissions
import com.infomaniak.drive.utils.NavigationUiUtils.setupWithNavControllerCustom
import com.infomaniak.drive.utils.SyncUtils.launchAllUpload
import com.infomaniak.drive.utils.SyncUtils.startContentObserverService
import com.infomaniak.drive.utils.Utils
import com.infomaniak.drive.utils.Utils.Shortcuts
import com.infomaniak.drive.utils.copyToDriveResultMessage
import com.infomaniak.drive.utils.openSupport
import com.infomaniak.drive.utils.showQuotasExceededSnackbar
import dagger.hilt.android.AndroidEntryPoint
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.collectLatest
import kotlinx.coroutines.invoke
import kotlinx.coroutines.launch
import kotlinx.coroutines.suspendCancellableCoroutine
import kotlinx.coroutines.withContext
import javax.inject.Inject
import kotlin.coroutines.resume
import com.infomaniak.core.legacy.R as RCore

@AndroidEntryPoint
class MainActivity : BaseActivity() {

    private val binding by lazy { ActivityMainBinding.inflate(layoutInflater) }

    private val mainViewModel: MainViewModel by viewModels()
    private val myKSuiteViewModel: MyKSuiteViewModel by viewModels()
    private val navigationArgs: MainActivityArgs? by lazy { intent?.extras?.let { MainActivityArgs.fromBundle(it) } }
    private val uiSettings by lazy { UiSettings(this) }
    private val navController by lazy { setupNavController() }

    private var hasDisplayedInformationPanel: Boolean = false

    private lateinit var syncPermissions: DrivePermissions

    private var deleteLocalMediaRequestDialog: Dialog? = null
    private val pendingFilesUrisQueue = ArrayDeque<List<Uri>>()

    private val filesDeletionResult = registerForActivityResult(StartIntentSenderForResult()) {
        it.whenResultIsOk {
            val filesUris = pendingFilesUrisQueue.removeFirstOrNull() ?: return@whenResultIsOk
            lifecycleScope.launch(Dispatchers.IO) { UploadFile.deleteAllFromUris(filesUris) }
            if (pendingFilesUrisQueue.isNotEmpty()) launchNextDeleteRequest()
        }
    }

    /**
     * The cloud->device consent request currently outstanding, with its FULL scoped
     * operation identity. One system dialog at a time; the identity is persisted in saved
     * instance state so a result delivered to a RECREATED activity is still attributed to
     * the request it answers — never dropped or misattributed to a newly drained request.
     */
    /**
     * The Main-confined consent dialog state (see [PhotoTrashConsentUiQueue]): the durable
     * pending row state IS the queue; this object carries only the per-activity phases
     * (draining / validating / launched) with their distinct persistence semantics — only a
     * LAUNCHED dialog persists across recreation and can receive a result.
     */
    private val photoTrashConsentQueue = PhotoTrashConsentUiQueue()

    private val photoTrashConsentResult = registerForActivityResult(StartIntentSenderForResult()) { result ->
        // Exactly the shown request, taken out once: a duplicate or late delivery finds
        // nothing here, and the controller-side identity checks drop stale stamps.
        val request = photoTrashConsentQueue.onResultDelivered() ?: return@registerForActivityResult
        val granted = result.resultCode == RESULT_OK
        lifecycleScope.launch(Dispatchers.IO) {
            PhotoTrashController.onRemoteTrashConsentResult(request, granted)
            drainPhotoTrashConsents()
        }
    }

    private val fileObserver: FileObserver by lazy {
        fun onEvent() {
            mainViewModel.syncOfflineFiles()
        }

        val offlineFolder = File.getOfflineFolder(this)

        if (SDK_INT >= 29) {
            object : FileObserver(offlineFolder) {
                override fun onEvent(event: Int, path: String?) = onEvent()
            }
        } else {
            object : FileObserver(offlineFolder.path) {
                override fun onEvent(event: Int, path: String?) = onEvent()
            }
        }
    }

    private val scanFlowResultLauncher =
        registerForActivityResult(getScanFlowContract()) { scanFlowOutput ->
            scanResultProcessing(scanFlowOutput, folder = null)
        }

    @Inject
    lateinit var inAppReviewManager: InAppReviewManager

    private var inAppUpdateSnackbar: Snackbar? = null

    private val deeplinkHandler = DeeplinkHandler(registryOwner = this)

    override fun onCreate(savedInstanceState: Bundle?) {
        super.onCreate(savedInstanceState)
        // Restore the LAUNCHED consent request FIRST (a merely VALIDATING one is intentionally
        // not persisted: no dialog was ever shown, the durable row stays armed, and the first
        // drain re-surfaces it — persisting it was the zombie-drain bug): a system-dialog
        // result delivered to the recreated activity must be attributed to the request it
        // answers (not dropped, not misattributed to a newly drained request).
        savedInstanceState?.getString(SAVED_PHOTO_TRASH_URI)?.let { uri ->
            photoTrashConsentQueue.onLaunchedRequestRestored(
                PhotoTrashController.ConsentRequest(
                    uri = uri,
                    userId = savedInstanceState.getInt(SAVED_PHOTO_TRASH_USER),
                    driveId = savedInstanceState.getInt(SAVED_PHOTO_TRASH_DRIVE),
                    remoteFileId = savedInstanceState.getInt(SAVED_PHOTO_TRASH_REMOTE_FILE),
                    pendingSince = savedInstanceState.getLong(SAVED_PHOTO_TRASH_STAMP),
                    pairedAt = savedInstanceState.getLong(SAVED_PHOTO_TRASH_PAIRED_AT),
                    activationEpoch = savedInstanceState.getLong(SAVED_PHOTO_TRASH_EPOCH),
                    bindingGeneration = savedInstanceState.getLong(SAVED_PHOTO_TRASH_BINDING_GENERATION),
                )
            )
        }
        setContentView(binding.root)

        addTwoFactorAuthOverlay(isDarkTheme = isNightModeEnabled())

        mainViewModel.initUploadFilesHelper(fragmentActivity = this, navController)

        fileObserver.startWatching()

        setupBottomNavigation()

        navController.addOnDestinationChangedListener { _, dest, args -> onDestinationChanged(dest, args) }

        setupFabs()
        setupDrivePermissions()
        handleShortcuts()

        initAppUpdateManager()
        initAppReviewManager()
        observeCurrentFolder()
        observeBulkDownloadRunning()
        observeDownloadCancellation()
        observeFailureDownloadWorkerOffline()
        observeCurrentUserAvatar()
        observeCopyToDriveResult()
        observePhotoTrashConsents()

        AppLockManager.scheduleLockIfNeeded(
            targetActivity = this,
            lockActivityCls = AppLockViewActivity::class,
            isAppLockEnabled = { AppSettings.appSecurityLock }
        )

        binding.bottomNavigation.onApplyWindowInsetsListener { view, windowInsets ->
            view.setMargins(
                bottom = resources.getDimension(R.dimen.bottomNavigationMargin).toInt() + windowInsets.bottom,
            )
            binding.searchFab.setMargins(bottom = resources.getDimension(R.dimen.marginStandard).toInt() + windowInsets.bottom)
        }
    }

    override fun onStart() {
        super.onStart()
        mainViewModel.loadRootFiles()
        myKSuiteViewModel.refreshMyKSuite()
        handleDeletionOfUploadedPhotos()
        // The durable consent queue is rebuilt on every app entry: armed requests that no
        // in-memory collector saw (process death, closed app) are revalidated and surfaced.
        drainPhotoTrashConsents()
        deeplinkHandler.handle(navigationArgs?.deeplinkType, this)
    }

    private fun getNavHostFragment() = supportFragmentManager.findFragmentById(R.id.hostFragment) as NavHostFragment

    private fun setupNavController(): NavController {
        return getNavHostFragment().navController.apply {
            if (currentDestination == null) navigate(graph.startDestinationId)
        }
    }

    // We use this SuppressLint because we don't want to execute performClick on profileItem when double tapping.
    @SuppressLint("ClickableViewAccessibility")
    private fun setupBottomNavigation() = with(binding) {

        val gestureDetector = GestureDetector(this@MainActivity, object : GestureDetector.SimpleOnGestureListener() {
            override fun onDoubleTap(e: MotionEvent): Boolean {
                trackAccountEvent(MatomoName.SwitchDoubleTap)
                mainViewModel.switchToNextUser { navController.navigate(R.id.homeFragment) }
                return true
            }

            override fun onLongPress(e: MotionEvent) {
                trackAccountEvent(MatomoName.LongPressDirectAccess)
                navController.navigate(R.id.switchUserActivity)
            }
        })

        bottomNavigation.findViewById<View>(R.id.menuFragment).setOnTouchListener { _, event ->
            gestureDetector.onTouchEvent(event)
        }

        bottomNavigation.apply {
            setupWithNavControllerCustom(navController)
            itemIconTintList = ContextCompat.getColorStateList(this@MainActivity, R.color.item_icon_tint_bottom)
            selectedItemId = uiSettings.bottomNavigationSelectedItem
            setOnItemReselectedListener { item ->
                navController.popBackStack(item.itemId, false)
            }
        }
    }

    private fun setupFabs() = with(binding) {
        setupFab(mainFab)
        setupFab(searchFab, shouldShowSmallFab = true)

        mainViewModel.currentFolder.observe(this@MainActivity) { file ->
            val canCreateFile = file?.rights?.canCreateFile == true
            mainFab.isEnabled = canCreateFile
            searchFab.isEnabled = canCreateFile
        }
    }

    private fun setupFab(fab: FloatingActionButton, shouldShowSmallFab: Boolean = false) {
        val args = AddFileBottomSheetDialogArgs(shouldShowSmallFab).toBundle()
        fab.setOnClickListener {
            val drive = AccountUtils.getCurrentDrive() ?: return@setOnClickListener
            if (drive.isDriveFull) {
                trackMyKSuiteEvent(MatomoName.TryAddingFileWithDriveFull.value)
                showQuotasExceededSnackbar(navController, drive)
            } else {
                navController.navigate(R.id.addFileBottomSheetDialog, args)
            }
        }
    }

    private fun setupDrivePermissions() {
        syncPermissions = DrivePermissions(DrivePermissions.Type.ReadingMediaForSync).apply {
            registerPermissions(this@MainActivity)
        }
    }

    //region In-App Updates
    private fun initAppUpdateManager() {
        inAppUpdateManager.init(
            onUserChoice = { isWantingUpdate -> trackInAppUpdate(if (isWantingUpdate) MatomoName.DiscoverNow else MatomoName.DiscoverLater) },
            onInstallStart = { trackInAppUpdate(MatomoName.InstallUpdate) },
            onInstallFailure = { showSnackbar(title = R.string.errorUpdateInstall, anchor = getMainFab()) },
            onInAppUpdateUiChange = { isUpdateDownloaded ->
                if (isUpdateDownloaded && canDisplayInAppSnackbar()) {
                    inAppUpdateSnackbar = showIndefiniteSnackbar(
                        title = R.string.updateReadyTitle,
                        actionButtonTitle = R.string.updateInstallButton,
                        anchor = getMainFab(),
                        onActionClicked = inAppUpdateManager::installDownloadedUpdate,
                    )
                } else if (!isUpdateDownloaded) {
                    inAppUpdateSnackbar?.dismiss()
                }
            },
            onFDroidResult = { updateIsAvailable ->
                if (updateIsAvailable) navController.navigate(R.id.updateAvailableBottomSheetDialog)
            },
        )
    }

    private fun observeBulkDownloadRunning() {
        // We need to check if the bulk download is running to avoid any
        // conflicts between the two ways of downloading offline files
        mainViewModel.isBulkDownloadRunning.observe(this) { isRunning ->
            if (!isRunning) mainViewModel.syncOfflineFiles()
        }
    }

    private fun observeDownloadCancellation() {
        BaseDownloadWorker.notifyRefreshUi.observe(this) { mainViewModel.updateVisibleFiles.value = true }
    }

    private fun observeFailureDownloadWorkerOffline() {
        DownloadOfflineFileManager.getFailedDownloadWorkerOffline(this)
            .observe(this) { workInfoList ->
                workInfoList.firstOrNull { it.state == WorkInfo.State.FAILED }?.let { failedWork ->
                    val hasLeftSpaceAfterDownload = failedWork.outputData.getBoolean(HAS_SPACE_LEFT_AFTER_DOWNLOAD_KEY, true)
                    if (!hasLeftSpaceAfterDownload) showSnackbar(R.string.notEnoughSpaceAfterDownload)
                    // We have to clear previous works info because otherwise, next time we'll start the app,
                    // we'll still have the previously failed result here.
                    WorkManager.getInstance(this@MainActivity).pruneWork()
                }
            }
    }

    private fun observeCurrentUserAvatar() {
        AccountUtils.currentConnectedUserFlow.observe(this) {
            setBottomNavigationUserAvatar(this@MainActivity)
        }
    }

    private fun observeCopyToDriveResult() {
        MqttClientWrapper.observe(this) { notification ->
            mainViewModel.resolveCopyToDriveNotification(notification)?.let { result ->
                showSnackbar(title = copyToDriveResultMessage(result.isSuccess, result.fileName), anchor = getMainFab())
            }
        }
    }

    /**
     * Photo trash propagation (opt-in): a paired file trashed in kDrive asks for Android's
     * trash consent before anything happens on the device. The pending row state IS the
     * durable queue — armed requests survive process death, are revalidated and re-surfaced
     * on every app entry ([drainPhotoTrashConsents]), and every event path (MQTT, upload-pass
     * catch-up) only WAKES the same serialized drain, so exactly one revalidated dialog is
     * ever outstanding.
     */
    private fun observePhotoTrashConsents() {
        MqttClientWrapper.observe(this) { notification ->
            with(notification) {
                if (!isFileActionNotification()) return@observe
                if (action != MqttAction.FILE_TRASH) return@observe
                if (driveId != AccountUtils.getCurrentDrive()?.id) return@observe

                lifecycleScope.launch(Dispatchers.IO) {
                    // Validates and arms (or clears) the pairing; the dialog itself is only
                    // ever launched through the drain's launch-time revalidation.
                    PhotoTrashController.onRemoteTrashEvent(
                        context = applicationContext,
                        remoteFileId = fileId ?: return@launch,
                        driveId = driveId,
                    )
                    drainPhotoTrashConsents()
                }
            }
        }

        lifecycleScope.launch {
            PhotoTrashController.pendingConsents.collect { _ ->
                drainPhotoTrashConsents()
            }
        }
    }

    /**
     * Serialized consent drain: every wake — lifecycle entry, ActivityResult, MQTT event,
     * controller emission — funnels onto MAIN before any queue state is touched, so the
     * single-outstanding guard is exact (the previous plain-field guard was read and written
     * from both Main and IO and could double-poll). Polls the durable pending queue for the
     * next STILL-valid request (fully revalidated at launch: opt-in, fresh remote trash with
     * an unchanged bound identity, unchanged local digest) and shows its system dialog. The
     * Android approval applies the trash itself at the moment the user approves — no app
     * pre-apply callback exists, so these launch-time checks are the last app-controlled
     * gate (the residual same-metadata window is disclosed in the controller, not closable).
     */
    private fun drainPhotoTrashConsents() {
        lifecycleScope.launch { drainPhotoTrashConsentsOnMain() }
    }

    private fun drainPhotoTrashConsentsOnMain() {
        if (!photoTrashConsentQueue.canBeginDrain()) return
        photoTrashConsentQueue.onDrainBegan()
        lifecycleScope.launch(Dispatchers.IO) {
            val next = runCatching { PhotoTrashController.pollNextPendingConsent(applicationContext) }
                .onFailure { exception ->
                    // A cancelled scope means the activity is going away: rethrow instead of
                    // swallowing it into a null result.
                    if (exception is CancellationException) throw exception
                    SentryLog.e("PhotoTrash", "consent drain failed", exception)
                }
                .getOrNull()
            withContext(Dispatchers.Main) {
                if (next == null) {
                    photoTrashConsentQueue.onDrainEndedWithNothing()
                } else {
                    photoTrashConsentQueue.onDrainEndedWithRequest(next)
                    requestPhotoTrashConsent(next)
                }
            }
        }
    }

    /**
     * Launches the system trash-consent dialog for a surfaced [request]: first the
     * launch-time revalidation on IO (the poll returned it earlier; opt-in/scope/pending
     * identity must still match the LIVE row immediately before the launch), then the FINAL
     * Main-side gates — current account, STARTED lifecycle, and the activation mirror checked
     * for freshness against the COMMIT watermark (a settings write that committed after the
     * IO verdict refuses the still-stale mirror here, with no realm open on Main) —
     * evaluated synchronously in this same Main block. That closes the application-side
     * window between the IO verdict and this queued continuation (a change landing there is
     * caught HERE, without any realm open on Main); the platform's own approval-time
     * mutation race stays unclosable and disclosed (distinct from this closable gap).
     */
    private fun requestPhotoTrashConsent(request: PhotoTrashController.ConsentRequest) {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) {
            photoTrashConsentQueue.onValidationFailed()
            return
        }
        lifecycleScope.launch(Dispatchers.IO) {
            val stillPending = runCatching { PhotoTrashController.isConsentStillPending(request) }
                .onFailure { exception ->
                    if (exception is CancellationException) throw exception
                    SentryLog.e("PhotoTrash", "consent revalidation failed", exception)
                }
                .getOrDefault(false)
            withContext(Dispatchers.Main) {
                if (!stillPending) {
                    photoTrashConsentQueue.onValidationFailed()
                    drainPhotoTrashConsentsOnMain() // the request expired: surface the next one
                    return@withContext
                }
                if (!PhotoTrashConsentUiQueue.launchAllowedNow(
                        request = request,
                        currentUserId = AccountUtils.currentUserId,
                        activationSnapshot = PhotoTrashController.currentActivationSnapshot,
                        activationWatermark = PhotoTrashController.activationCommitWatermark(),
                        bindingGenerationCurrent = PhotoTrashController.bindingGeneration(request.uri),
                        lifecycleStarted = lifecycle.currentState.isAtLeast(Lifecycle.State.STARTED),
                    )
                ) {
                    // Scope/opt-in/activation/binding changed after the IO verdict: nothing
                    // launches, nothing is erased — the durable row stays armed for a later
                    // drain.
                    photoTrashConsentQueue.onValidationFailed()
                    return@withContext
                }
                runCatching {
                    val pendingIntent = MediaStore.createTrashRequest(contentResolver, listOf(request.uri.toUri()), true)
                    photoTrashConsentResult.launch(IntentSenderRequest.Builder(pendingIntent.intentSender).build())
                    // The system accepted the launch: from here the result (or process death)
                    // owns the LAUNCHED identity. Set only after launch returned on Main — a
                    // result callback can only run on a LATER main-loop pass, so the identity
                    // is in place before any result; a throwing launch leaves nothing set.
                    photoTrashConsentQueue.onValidationLaunched(request)
                }.onFailure { exception ->
                    photoTrashConsentQueue.onValidationFailed()
                    SentryLog.e("PhotoTrash", "createTrashRequest failed", exception)
                }
            }
        }
    }

    override fun onSaveInstanceState(outState: Bundle) {
        super.onSaveInstanceState(outState)
        // The LAUNCHED consent request's full scoped identity survives recreation so a late
        // system-dialog result stays attributable (and the single-outstanding guard holds
        // across recreation, so no second dialog can launch while one is out). A merely
        // VALIDATING request is deliberately not persisted: no dialog was shown, and a
        // restored non-null field would suppress every future drain.
        photoTrashConsentQueue.requestForSavedState()?.let { request ->
            outState.putString(SAVED_PHOTO_TRASH_URI, request.uri)
            outState.putInt(SAVED_PHOTO_TRASH_USER, request.userId)
            outState.putInt(SAVED_PHOTO_TRASH_DRIVE, request.driveId)
            outState.putInt(SAVED_PHOTO_TRASH_REMOTE_FILE, request.remoteFileId)
            outState.putLong(SAVED_PHOTO_TRASH_STAMP, request.pendingSince)
            outState.putLong(SAVED_PHOTO_TRASH_PAIRED_AT, request.pairedAt)
            outState.putLong(SAVED_PHOTO_TRASH_EPOCH, request.activationEpoch)
            outState.putLong(SAVED_PHOTO_TRASH_BINDING_GENERATION, request.bindingGeneration)
        }
    }

    private fun canDisplayInAppSnackbar() = inAppUpdateSnackbar?.isShown != true && getMainFab().isShown
    //endregion

    //region In-App Review
    private fun initAppReviewManager() {
        lifecycleScope.launch {
            inAppReviewManager.shouldDisplayReviewDialog
                .flowWithLifecycle(lifecycle, Lifecycle.State.STARTED)
                .collect { shouldDisplayReviewDialog ->
                    if (shouldDisplayReviewDialog) {
                        trackInAppReview(MatomoName.PresentAlert)
                        inAppReviewManager.onReviewDialogShown()
                        when (askForAppReview()) {
                            ReviewAlertDialog.DialogAction.Positive -> inAppReviewManager.onUserWantsToReview()
                            ReviewAlertDialog.DialogAction.Negative -> {
                                inAppReviewManager.onUserWantsToGiveFeedback(getString(R.string.urlUserReportAndroid))
                            }
                            ReviewAlertDialog.DialogAction.Dismiss -> inAppReviewManager.onUserWantsToDismiss()
                        }
                    }
                }
        }

        inAppReviewManager.init(
            countdownBehavior = BaseInAppReviewManager.Behavior.LifecycleBased,
            appReviewThreshold = DEFAULT_APP_REVIEW_LAUNCHES,
            onUserWantToReview = { trackInAppReview(MatomoName.Like) },
            onUserWantToGiveFeedback = { trackInAppReview(MatomoName.Dislike) },
        )
    }

    private suspend fun askForAppReview(): ReviewAlertDialog.DialogAction = suspendCancellableCoroutine { continuation ->
        var userChoice = ReviewAlertDialog.DialogAction.Dismiss
        val dialog = ReviewAlertDialog(
            activityContext = this@MainActivity,
            reviewDialogTheme = R.style.DialogStyle,
            reviewDialogTitleStyle = R.style.DialogReviewStyleTextAppearance,
            reviewAlertDialogData = ReviewAlertDialogData(
                title = getString(R.string.reviewAlertTitle),
                positiveText = getString(RCore.string.buttonYes),
                negativeText = getString(RCore.string.buttonNo),
                onPositiveButtonClicked = { userChoice = ReviewAlertDialog.DialogAction.Positive },
                onNegativeButtonClicked = { userChoice = ReviewAlertDialog.DialogAction.Negative },
                onDismiss = { continuation.resume(userChoice) }
            )
        )
        dialog.show()
        continuation.invokeOnCancellation { dialog.dismiss() }
    }
    //endregion

    override fun onResume() {
        super.onResume()

        launchAllUpload(syncPermissions)

        mainViewModel.checkBulkDownloadStatus()

        displayInformationPanel()

        setBottomNavigationUserAvatar(this)
        startContentObserverService()

        finishPIPActivity()
    }

    @OptIn(UnstableApi::class)
    private fun finishPIPActivity() {
        if ((application as MainApplication).isVideoActivityInPIPMode) {
            startActivity(Intent(this@MainActivity, VideoActivity::class.java).apply {
                flags = FLAG_ACTIVITY_REORDER_TO_FRONT
            })
        }
    }

    private fun handleDeletionOfUploadedPhotos() = lifecycleScope.launch {
        retrieveFilesUriToDelete()?.let { uris -> showDeleteFileConfirmation(uris) }
    }

    private suspend fun retrieveFilesUriToDelete(): List<Uri>? = withContext(Dispatchers.IO) {
        takeIf { isDeleteEnable() && hasNoPendingUpload() }
            ?.let { UploadFile.getAllUploadedFiles() }
            ?.let { getFilesUriToDelete(it) }
            ?.takeIf { it.size >= SYNCED_FILES_DELETION_FILES_AMOUNT }
    }

    private fun isDeleteEnable(): Boolean = UploadFile.getAppSyncSettings()?.deleteAfterSync ?: false

    private fun hasNoPendingUpload(): Boolean = UploadFile.getCurrentUserPendingUploadsCount() == 0

    private fun getFilesUriToDelete(uploadFiles: List<UploadFile>): List<Uri> {
        val (filesToDelete, filesAlreadyDeleted) = uploadFiles.map(UploadFile::getUriObject)
            .filter { it.ensureIsNotFileOrDocument() }
            .partition(Uri::doesFileExist)

        UploadFile.deleteAllFromUris(filesAlreadyDeleted)

        return filesToDelete
    }

    private fun Uri.ensureIsNotFileOrDocument(): Boolean {
        return (scheme != ContentResolver.SCHEME_FILE && !DocumentsContract.isDocumentUri(this@MainActivity, this))
            .also {
                if (!it) SentryLog.wtf(
                    tag = "MainActivity",
                    msg = "A file or document was marked as a media to upload $this," +
                            "So to dev, keep the test ensureIsNotFileOrDocument and remove the sentry," +
                            "else if this sentry is never sent in some years, you can remove the test ensureIsNotFileOrDocument "
                )
            }
    }

    private fun showDeleteFileConfirmation(uris: List<Uri>) {
        deleteLocalMediaRequestDialog = Utils.createConfirmation(
            context = this@MainActivity,
            title = getString(R.string.modalDeletePhotosTitle),
            message = getString(R.string.modalDeletePhotosNumericDescription, uris.size),
            buttonText = getString(R.string.buttonDelete),
            isDeletion = true,
            onConfirmation = { onDeleteFileConfirmation(uris) }
        )
    }

    fun onDeleteFileConfirmation(filesUriToDelete: List<Uri>) {
        if (SDK_INT >= 30) {
            lifecycleScope.launch(Dispatchers.Default) {
                pendingFilesUrisQueue.clear()
                pendingFilesUrisQueue.addAll(filesUriToDelete.chunked(MEDIASTORE_DELETE_BATCH_LIMIT))
                launchNextDeleteRequest()
            }
        } else {
            mainViewModel.deleteSynchronizedFilesOnDevice(filesUriToDelete)
        }
    }

    private fun launchNextDeleteRequest() {
        if (SDK_INT >= 30) {
            pendingFilesUrisQueue.firstOrNull()
                ?.let { runCatching { MediaStore.createDeleteRequest(contentResolver, it) }.getOrNull() }
                ?.let { IntentSenderRequest.Builder(it.intentSender).build() }
                ?.let(filesDeletionResult::launch)
        }
    }

    private fun onDestinationChanged(destination: NavDestination, navigationArgs: Bundle?) {
        destination.addSentryBreadcrumb()

        val shouldHideBottomNavigation =
            navigationArgs?.let(FileListFragmentArgs::fromBundle)?.shouldHideBottomNavigation == true
        val shouldShowSmallFab = (navigationArgs?.let(FileListFragmentArgs::fromBundle)?.shouldShowSmallFab
            ?: navigationArgs?.let(AddFileBottomSheetDialogArgs::fromBundle)?.shouldShowSmallFab
            ?: navigationArgs?.let(FileInfoActionsBottomSheetDialogArgs::fromBundle)?.shouldShowSmallFab) == true

        handleBottomNavigationVisibility(destination.id, shouldHideBottomNavigation, shouldShowSmallFab)

        when (destination.id) {
            R.id.favoritesFragment,
            R.id.homeFragment,
            R.id.menuFragment,
            R.id.menuGalleryFragment,
            R.id.mySharesFragment,
            R.id.offlineFileFragment,
            R.id.recentChangesFragment,
            R.id.rootFilesFragment,
            R.id.searchFragment,
            R.id.trashFragment -> {
                // Defining default root folder
                mainViewModel.setCurrentFolderAsRoot()
            }
        }

        destination.trackDestination()
    }

    private fun handleBottomNavigationVisibility(
        destinationId: Int,
        shouldHideBottomNavigation: Boolean,
        shouldShowSmallFab: Boolean,
    ) = with(binding) {

        val isGone = when (destinationId) {
            R.id.addFileBottomSheetDialog,
            R.id.fileInfoActionsBottomSheetDialog,
            R.id.fileListFragment -> shouldHideBottomNavigation || shouldShowSmallFab
            R.id.favoritesFragment,
            R.id.homeFragment,
            R.id.menuFragment,
            R.id.menuGalleryFragment,
            R.id.mySharesFragment,
            R.id.offlineFileFragment,
            R.id.recentChangesFragment,
            R.id.rootFilesFragment,
            R.id.sharedWithMeFragment,
            R.id.trashFragment -> shouldHideBottomNavigation
            else -> true
        }

        mainFab.isGone = isGone
        bottomNavigation.isGone = isGone
        bottomNavigationBackgroundView.isGone = isGone

        searchFab.isVisible = shouldShowSmallFab
    }

    /**
     * Handle shortcuts, the [Shortcuts.UPLOAD] case is already handled in [observeCurrentFolder].
     */
    private fun handleShortcuts() {
        navigationArgs?.shortcutId?.let { shortcutId ->
            trackEvent(MatomoCategory.Shortcuts.value, name = shortcutId)

            when (shortcutId) {
                Shortcuts.SEARCH.id -> {
                    ShortcutManagerCompat.reportShortcutUsed(this@MainActivity, Shortcuts.SEARCH.id)
                    navController.navigate(R.id.searchFragment)
                }
                Shortcuts.SCAN.id -> startScanFlow(scanFlowResultLauncher)
                Shortcuts.FEEDBACK.id -> openSupport()
                Shortcuts.UPLOAD.id -> Unit // Already handled elsewhere, @see kdoc
            }
        }
    }

    private fun observeCurrentFolder() = with(mainViewModel) {
        currentFolder.observe(this@MainActivity) { parentFolder ->
            binding.mainFab.isEnabled = parentFolder?.rights?.canCreateFile == true

            // TODO : We need to find a way to handle the case where the app has never fetched the private folder and
            //  therefore can't find it in Realm
            if (navigationArgs?.shortcutId == Shortcuts.UPLOAD.id &&
                mustOpenUploadShortcut &&
                // We only want to allow upload at the private folder's root
                parentFolder?.getVisibilityType() == VisibilityType.IS_PRIVATE
            ) {
                mainViewModel.mustOpenUploadShortcut = false
                uploadFilesHelper?.apply {
                    setParentFolder(parentFolder)
                    uploadFiles()
                }
            }
        }
    }

    override fun onStop() {
        super.onStop()
        saveLastNavigationItemSelected()
        deleteLocalMediaRequestDialog?.dismiss()
        deleteLocalMediaRequestDialog = null
    }

    fun saveLastNavigationItemSelected() {
        uiSettings.bottomNavigationSelectedItem = binding.bottomNavigation.selectedItemId
    }

    override fun onDestroy() {
        super.onDestroy()
        fileObserver.stopWatching()
    }

    private fun displayInformationPanel() = with(uiSettings) {
        if (!hasDisplayedInformationPanel) {
            val destinationId = when {
                !hasDisplayedSyncDialog && !AccountUtils.isEnableAppSync() -> {
                    hasDisplayedSyncDialog = true
                    R.id.syncConfigureBottomSheetDialog
                }
                else -> null
            }
            destinationId?.let {
                hasDisplayedInformationPanel = true
                findNavController(R.id.hostFragment).navigate(it)
            }
        }
    }

    @SuppressLint("RestrictedApi")
    private fun setBottomNavigationUserAvatar(context: Context) {
        AccountUtils.currentUser?.apply {
            lifecycleScope.launch(Dispatchers.IO) {
                val fallback =
                    context.generateInitialsAvatarDrawable(
                        initials = getInitials(),
                        background = context.getBackgroundColorBasedOnId(id)
                    )
                val bottomNavigationMenuView = binding.bottomNavigation.getChildAt(0) as BottomNavigationMenuView
                val menuItemView = bottomNavigationMenuView[4] as NavigationBarItemView
                val request = ImageRequest.Builder(context)
                    .data(avatar)
                    .crossfade(true)
                    .transformations(CircleCropTransformation())
                    .fallback(fallback)
                    .error(fallback)
                    .placeholder(R.drawable.ic_account)
                    .build()

                val userAvatar = this@MainActivity.simpleImageLoader.execute(request).image?.asDrawable(context.resources)

                userAvatar?.let {
                    val selectedAvatar = generateSelectedAvatar(userAvatar)
                    val stateListDrawable = StateListDrawable()
                    stateListDrawable.addState(
                        intArrayOf(android.R.attr.state_checked),
                        selectedAvatar.toDrawable(resources)
                    )
                    stateListDrawable.addState(intArrayOf(), userAvatar)

                    withContext(Dispatchers.Main) {
                        menuItemView.setIconTintList(null)
                        menuItemView.setIcon(stateListDrawable)
                        binding.bottomNavigation.menu.findItem(R.id.menuFragment).icon = stateListDrawable
                    }
                }
            }
        }
    }

    private fun generateSelectedAvatar(userAvatar: Drawable): Bitmap {
        val bitmap = userAvatar.toBitmap(100, 100)
        val canvas = Canvas(bitmap)
        val paint = Paint().apply {
            color = ContextCompat.getColor(this@MainActivity, R.color.primary)
            strokeWidth = 8F
            style = Paint.Style.STROKE
            isAntiAlias = true
            isDither = true
        }

        canvas.drawCircle(50F, 50F, 46F, paint)
        return bitmap
    }

    fun getMainFab() = binding.mainFab

    fun getBottomNavigation() = binding.bottomNavigation

    fun clickOnBottomBarFolders() {
        binding.bottomNavigation.findViewById<View>(R.id.rootFilesFragment).performClick()
    }

    fun showSnackbarWithFabAnchor(title: Int) {
        binding.mainFab.apply {
            post { showSnackbar(title = title, anchor = this) }
        }
    }

    suspend fun navigateFromDriveDeeplink(deeplink: DeeplinkAction.Drive) {
        Dispatchers.Main { clickOnBottomBarFolders() }
        mainViewModel.navigateDeeplink.emit(deeplink)
    }

    companion object {
        private const val SYNCED_FILES_DELETION_FILES_AMOUNT = 10

        /**
         * Maximum number of elements in the list supported by the mediastore when Uris are to be deleted.
         * When you exceed this value, the system may not propagate dialog to delete the images,
         * and when you exceed 10_000 you receive a `NullPointerException`.
         *
         * Note: if your app targets Build.VERSION_CODES.BAKLAVA and above, you can send a maximum of 2000 uris in each request.
         * Attempting to send more than 2000 uris will result in a IllegalArgumentException.
         * https://developer.android.com/reference/android/provider/MediaStore#createDeleteRequest(android.content.ContentResolver,%20java.util.Collection%3Candroid.net.Uri%3E)
         *
         */
        private const val MEDIASTORE_DELETE_BATCH_LIMIT = 2_000

        private const val DEFAULT_APP_REVIEW_LAUNCHES = 20

        // Saved-instance-state keys for the outstanding photo-trash consent request
        // (its full scoped operation identity, restored in onCreate).
        private const val SAVED_PHOTO_TRASH_URI = "savedPhotoTrashConsentUri"
        private const val SAVED_PHOTO_TRASH_USER = "savedPhotoTrashConsentUserId"
        private const val SAVED_PHOTO_TRASH_DRIVE = "savedPhotoTrashConsentDriveId"
        private const val SAVED_PHOTO_TRASH_REMOTE_FILE = "savedPhotoTrashConsentRemoteFileId"
        private const val SAVED_PHOTO_TRASH_STAMP = "savedPhotoTrashConsentPendingSince"
        private const val SAVED_PHOTO_TRASH_PAIRED_AT = "savedPhotoTrashConsentPairedAt"
        private const val SAVED_PHOTO_TRASH_EPOCH = "savedPhotoTrashConsentActivationEpoch"
        private const val SAVED_PHOTO_TRASH_BINDING_GENERATION = "savedPhotoTrashConsentBindingGeneration"
    }
}
