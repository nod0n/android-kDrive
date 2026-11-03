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
package com.infomaniak.drive.data.documentprovider

import android.annotation.SuppressLint
import android.app.PendingIntent
import android.content.Context
import android.content.Intent
import android.content.res.AssetFileDescriptor
import android.database.Cursor
import android.database.MatrixCursor
import android.graphics.Point
import android.net.Uri
import android.os.Bundle
import android.os.CancellationSignal
import android.os.Handler
import android.os.ParcelFileDescriptor
import android.os.PowerManager
import android.os.StrictMode
import android.provider.DocumentsContract
import android.provider.DocumentsProvider
import android.provider.Settings
import androidx.core.app.NotificationManagerCompat
import androidx.core.content.edit
import androidx.core.net.toUri
import androidx.core.os.bundleOf
import com.infomaniak.core.common.cancellable
import com.infomaniak.core.legacy.utils.NotificationUtilsCore
import com.infomaniak.core.network.api.ApiController
import com.infomaniak.core.network.models.ApiResponse
import com.infomaniak.core.notifications.notifyCompat
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.R
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.ApiRoutes
import com.infomaniak.drive.data.api.ApiRoutes.uploadFileDirectlyUrl
import com.infomaniak.drive.data.api.UploadTask.Companion.ConflictOption
import com.infomaniak.drive.data.cache.DriveInfosController
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.cache.FolderFilesProvider
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.DownloadOfflineFileManager
import com.infomaniak.drive.utils.IOFile
import com.infomaniak.drive.utils.NotificationUtils.buildGeneralNotification
import com.infomaniak.drive.utils.NotificationUtils.cancelNotification
import com.infomaniak.drive.utils.SyncUtils.syncImmediately
import com.infomaniak.drive.utils.Utils
import com.infomaniak.drive.utils.copyToCancellable
import io.realm.Realm
import io.sentry.Sentry
import io.sentry.SentryEvent
import io.sentry.SentryLevel
import io.sentry.protocol.Message
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineName
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.async
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import splitties.init.appCtx
import java.io.IOException
import java.net.URLEncoder
import java.util.Date
import java.util.UUID
import java.util.concurrent.Executors
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext

class CloudStorageProvider : DocumentsProvider() {

    private lateinit var cacheDir: IOFile
    private val cloudScope = CoroutineScope(
        CoroutineName("CloudStorage") + Executors.newSingleThreadExecutor().asCoroutineDispatcher()
    )

    /** Executes admitted remote mutations off the Binder thread; cancellable by transitions. */
    private val mutationScope = CoroutineScope(CoroutineName("CloudStorageMutation") + SupervisorJob() + Dispatchers.IO)

    private fun isProviderDisabled(): Boolean {
        val ctx = context ?: return true
        return isDisabled(ctx)
    }

    private fun accessDecision(): ProviderAccessPolicy.Reason = ProviderAccessPolicy.evaluate(
        appLockEnabled = AppLockState.isAppLockActive(),
        providerDisabledByUser = isProviderDisabled(),
    )

    private fun isExternalAccessBlocked(): Boolean = accessDecision() != ProviderAccessPolicy.Reason.ALLOWED

    /**
     * Revalidates the policy right before access is granted. Used at Binder entry points and
     * again before descriptor issuance, cursor page publication and mutation dispatch, so work
     * that crossed an app-lock transition (slow downloads, queued pages) cannot complete with
     * newly granted access.
     */
    private fun ensureExternalAccessAllowed() {
        val decision = accessDecision()
        if (decision != ProviderAccessPolicy.Reason.ALLOWED) {
            throw SecurityException(blockedReasonMessage(decision))
        }
    }

    /**
     * Shared transition boundary: runs [block] atomically against app-lock transitions after
     * revalidating the policy. Slow preparation (metadata fetch, downloads) must happen BEFORE
     * this call; only fast issuance (descriptor/pipe creation, cursor publication) and dispatch
     * decisions belong inside.
     */
    private fun <T> withFinalAdmission(block: () -> T): T = AppLockState.withAdmission {
        ensureExternalAccessAllowed()
        block()
    }

    /**
     * Cancellable admission for remote mutations (Astra review R2-2 follow-up).
     *
     * Linearization/issuance point: the final policy revalidation, the registration of the
     * dispatch handle and the start of the request job happen atomically inside the admission
     * boundary. The boundary is then released — no transition lock is held across network I/O —
     * and the response is awaited outside it. Any transition after admission cancels the handle,
     * so the request's network I/O only proceeds if it starts running before that cancellation
     * lands; a request already on the wire may complete remotely (blocking call, not
     * interruptible) but its result is refused, no local state applies and the caller is denied.
     * Local Realm bookkeeping must therefore stay in the caller, on its original thread.
     */
    private fun <T> admitAndDispatch(block: suspend () -> T): T {
        var handle: AppLockTransitions.AdmittedWork? = null
        val deferred: Deferred<T> = withFinalAdmission {
            val dispatch = mutationScope.async(start = CoroutineStart.LAZY) { block() }
            handle = AppLockTransitions.AdmittedWork { dispatch.cancel() }
                .also { AppLockState.registerAdmittedWork(it) }
            dispatch.start()
            dispatch
        }

        return try {
            runBlocking { deferred.await() }
        } catch (exception: CancellationException) {
            // The admitted dispatch was cancelled by a transition: deny with the current reason.
            if (isExternalAccessBlocked()) {
                throw SecurityException(blockedReasonMessage(accessDecision()))
            }
            throw exception
        } finally {
            handle?.let { AppLockState.unregisterAdmittedWork(it) }
        }
    }

    private fun blockedReasonMessage(decision: ProviderAccessPolicy.Reason): String = when (decision) {
        ProviderAccessPolicy.Reason.BLOCKED_BY_APP_LOCK ->
            context?.getString(R.string.cloudStorageAppLockError) ?: "kDrive is locked by App lock"
        ProviderAccessPolicy.Reason.BLOCKED_BY_SETTING ->
            context?.getString(R.string.fileProviderExtensionError) ?: "kDrive access is disabled"
        ProviderAccessPolicy.Reason.ALLOWED -> ""
    }

    /**
     * Indicates whether the current platform is Chrome OS.
     *
     * ### Why this matters:
     * There is a known issue on Chrome OS :
     * Where the file explorer **does not reliably refresh its view** when file content changes are notified **asynchronously**.
     * As a result, file listings must be **loaded synchronously** on Chrome OS to ensure the UI reflects the latest state.
     *
     * This flag is used to adapt execution behavior in [runOnPlatformAdaptively] and [runSuspendOnPlatformAdaptively].
     */
    private var isChromeOs = false

    override fun onCreate(): Boolean {
        SentryLog.d(TAG, "onCreate")

        var result = false
        runBlocking {
            context?.let {
                cacheDir = IOFile(it.filesDir, "cloud_storage_temp_files")
                it.initRealm()
                result = true
                // As describe here: https://developer.android.com/games/playgames/pg-chromeos#detect_the_platform
                isChromeOs = context?.packageManager?.hasSystemFeature("org.chromium.arc") == true
            }
        }
        return result
    }

    override fun queryRoots(projection: Array<out String>?): Cursor = withLaxStrictMode {
        SentryLog.d(TAG, "queryRoots")
        val cursor = MatrixCursor(projection ?: DEFAULT_ROOT_PROJECTION)

        val isAccessBlocked = isExternalAccessBlocked()
        AccountUtils.getAllUsersSync().forEach { user ->
            // While external access is blocked, do not disclose account identities via root summaries.
            cursor.addRoot(user.id.toString(), user.id.toString(), if (isAccessBlocked) "" else user.email)

            if (!isAccessBlocked) {
                cloudScope.launch {
                    context?.let {
                        val okHttpClient = AccountUtils.getHttpClient(user.id)
                        AccountUtils.updateCurrentUserAndDrives(it, fromCloudStorage = true, okHttpClient = okHttpClient)
                    }
                }
            }
        }
        return cursor
    }

    override fun queryDocument(
        documentId: String,
        projection: Array<out String>?
    ): Cursor = withLaxStrictMode {
        SentryLog.d(TAG, "queryDocument(), documentId=$documentId")

        val decision = accessDecision()
        if (decision != ProviderAccessPolicy.Reason.ALLOWED) {
            return MatrixCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION).apply {
                extras = bundleOf(DocumentsContract.EXTRA_ERROR to blockedReasonMessage(decision))
            }
        }

        return MatrixCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION).apply {
            val userId = getUserId(documentId)
            val driveDocument = getDriveFromDocId(documentId)
            val isDriveFolder = getFileIdFromDocumentId(documentId) == 1
            val isRootFolder = documentId == userId
            val isSharedWithMeFolder = getFileIdFromDocumentId(documentId) == SHARED_WITHME_FOLDER_ID
            val isMySharesFolder = getFileIdFromDocumentId(documentId) == MY_SHARES_FOLDER_ID
            val fromSharedWithMe = comeFromSharedWithMe(documentId)

            when {
                isRootFolder || isDriveFolder -> addFile(null, documentId, driveDocument.name)
                isSharedWithMeFolder -> addFile(
                    null,
                    documentId,
                    context?.getString(R.string.sharedWithMeTitle) ?: "Shared with me"
                )
                isMySharesFolder -> addFile(
                    null,
                    documentId,
                    context?.getString(R.string.mySharesTitle) ?: "My Shares"
                )
                else -> { // isFile
                    val fileId = getFileIdFromDocumentId(documentId)
                    val userDrive = UserDrive(userId.toInt(), driveDocument.id, fromSharedWithMe)
                    FileController.getRealmInstance(userDrive).use { realm ->
                        FileController.getFileProxyById(fileId, customRealm = realm)?.let { file ->
                            addFile(file, documentId)
                        }
                    }
                }
            }
        }
    }

    override fun queryChildDocuments(
        parentDocumentId: String,
        projection: Array<out String>?,
        sortOrder: String?
    ): Cursor = withLaxStrictMode {
        val cursor = DocumentCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION, isAutoCloseableJob = false)

        // Gate before any cursor restoration, so a listing captured before the lock cannot be replayed.
        val decision = accessDecision()
        if (decision != ProviderAccessPolicy.Reason.ALLOWED) {
            cursor.extras = bundleOf(DocumentsContract.EXTRA_ERROR to blockedReasonMessage(decision))
            // Install the notification URI so observers re-query once access is restored.
            context?.let { cursor.setNotificationUri(it.contentResolver, DocumentCursor.createUri(it, parentDocumentId)) }
            return cursor
        }

        val uri = DocumentCursor.createUri(context, parentDocumentId)

        val restoredCursor = retainedCursors.acquireChild(uri, cursor, parentDocumentId)
        SentryLog.d(TAG, "queryChildDocuments(), parentDocumentId=$parentDocumentId, sort=$sortOrder, isRestore=${restoredCursor != null}")

        if (restoredCursor != null) {
            SentryLog.i(TAG, "Restore old cursor")
            cursor.restore(restoredCursor)
            cursor.extras = bundleOf(DocumentsContract.EXTRA_LOADING to false)
            cursor.setNotificationUri(context?.contentResolver, uri)
            return cursor
        }

        val fileFolderId = getFileIdFromDocumentId(parentDocumentId)
        val isRootFolder = parentDocumentId == getUserId(parentDocumentId)
        val isSharedWithMeFolder = fileFolderId == SHARED_WITHME_FOLDER_ID
        val isMySharesDrive = fileFolderId == MY_SHARES_FOLDER_ID

        val userId = getUserId(parentDocumentId).toInt()
        val driveId = getDriveFromDocId(parentDocumentId).id
        val sortType = getSortType(sortOrder)

        val userDrive = UserDrive(userId, driveId, sharedWithMe = parentDocumentId.contains("$SHARED_WITHME_FOLDER_ID"))

        val isMySharesRoot = isSharedUri(parentDocumentId)
                && fileFolderId == Utils.ROOT_ID
                && parentDocumentId.contains("$MY_SHARES_FOLDER_ID")

        when {
            isRootFolder -> {
                cursor.addRootDrives(userId, isRootFolder = true)

                // Show MyShares folder
                var documentId = parentDocumentId + SEPARATOR + MY_SHARES_FOLDER_ID
                var name = context?.getString(R.string.mySharesTitle) ?: "My Shares"
                cursor.addFile(null, documentId, name)

                // Show SharedWithMe folder
                documentId = parentDocumentId + SEPARATOR + SHARED_WITHME_FOLDER_ID
                name = context?.getString(R.string.sharedWithMeTitle) ?: "Shared with me"
                cursor.addFile(null, documentId, name)
            }
            isSharedWithMeFolder -> {
                runOnPlatformAdaptively(cursor.job) {
                    FileController.getRealmInstance(userDrive).use { realm ->
                        FolderFilesProvider.getCloudStorageFiles(
                            realm = realm,
                            folderId = Utils.ROOT_ID,
                            userDrive = UserDrive(AccountUtils.currentUserId, AccountUtils.currentDriveId, sharedWithMe = true),
                            sortType = sortType,
                            transaction = cursor.addFiles(parentDocumentId, uri, isSharedWithMeFolder = true)
                        )
                    }
                }
            }
            isMySharesDrive -> cursor.addRootDrives(userId, MY_SHARES_FOLDER_ID)
            isMySharesRoot -> {
                runSuspendOnPlatformAdaptively(cursor.job) {
                    FileController.getMySharedFiles(
                        userDrive = UserDrive(userId, driveId),
                        sortType = sortType,
                        transaction = { files, _ -> cursor.addFiles(parentDocumentId, uri)(files) })
                }
            }
            else -> {
                runOnPlatformAdaptively(cursor.job) {
                    FileController.getRealmInstance(userDrive).use { realm ->
                        FolderFilesProvider.getCloudStorageFiles(
                            realm = realm,
                            folderId = fileFolderId,
                            userDrive = userDrive,
                            sortType = sortType,
                            transaction = cursor.addFiles(parentDocumentId, uri)
                        )
                    }
                }
            }
        }

        return withFinalAdmission {
            cursor.extras = bundleOf(DocumentsContract.EXTRA_LOADING to true)
            cursor.setNotificationUri(context?.contentResolver, uri)
            cursor
        }
    }

    override fun isChildDocument(parentDocumentId: String, documentId: String): Boolean {
        // Relationship metadata is protected like any other content: deny before parsing IDs or
        // touching the cache when external access is blocked.
        ensureExternalAccessAllowed()

        // Roots and other virtual containers must only match documents that are actually
        // inside their subtree; otherwise SAF relationship checks can be bypassed.
        val parentDocumentType = computeDocumentType(parentDocumentId)
        if (parentDocumentType !is CloudDocumentType.FileOrFolder) {
            return isDocumentIdDescendantOf(parentDocumentId, documentId)
        }

        // Folder from a Drive
        return when (val documentType = computeDocumentType(documentId)) {
            is CloudDocumentType.FileOrFolder -> isChildDocument(parentDocumentType, documentType)
            else -> super.isChildDocument(parentDocumentId, documentId)
        }
    }

    private fun isChildDocument(
        parentDocumentType: CloudDocumentType.FileOrFolder,
        documentType: CloudDocumentType.FileOrFolder,
    ): Boolean = when {
        parentDocumentType.userDrive == documentType.userDrive -> {
            val userDrive = documentType.userDrive
            FileController.getRealmInstance(userDrive).use { realm ->
                val parentFolderProxy = FileController.getFileProxyById(parentDocumentType.fileId, customRealm = realm)
                    ?: return@use false
                val folderProxy = FileController.getFileProxyById(documentType.fileId, customRealm = realm)
                    ?: return@use false

                fun isDirectChild() = folderProxy.parentId == parentDocumentType.fileId
                fun isGrandChild() = folderProxy.getRemotePath(userDrive).startsWith(parentFolderProxy.getRemotePath(userDrive))

                return@use isDirectChild() || isGrandChild()
            }
        }
        else -> false
    }

    private fun DocumentCursor.addFiles(
        parentDocumentId: String,
        uri: Uri,
        isSharedWithMeFolder: Boolean = false,
    ): (files: ArrayList<File>) -> Unit = { files ->
        // A queued page must not be appended if the lock was enabled while it was being fetched.
        if (isExternalAccessBlocked()) job.cancel()
        job.ensureActive()
        files.forEach { file ->
            val sharedWithMePath = StringBuilder(SHARED_WITHME_FOLDER_ID.toString())
                .append(SEPARATOR)
                .append("${file.name}$DRIVE_SEPARATOR${file.driveId}")
            val drivePath = if (isSharedWithMeFolder) sharedWithMePath.toString() else null
            this.addFile(file, createFileDocumentId(parentDocumentId, file.id, parent = drivePath))
            job.ensureActive()
        }
        context?.contentResolver?.notifyChange(uri, null)
    }

    override fun openDocument(
        documentId: String,
        mode: String,
        signal: CancellationSignal?
    ): ParcelFileDescriptor? = withLaxStrictMode {
        SentryLog.d(TAG, "openDocument(), id=$documentId, mode=$mode, signalIsCancelled: ${signal?.isCanceled}")
        ensureExternalAccessAllowed()
        val context = context ?: return null

        fun getRemoteFile(localFile: File?, fileId: Int, driveId: Int): File? {
            val okHttpClient = runBlocking { AccountUtils.getHttpClient(getUserId(documentId).toInt()) }
            return ApiRepository.getFileDetails(localFile ?: File(id = fileId, driveId = driveId), okHttpClient).data
        }

        val isWrite = mode.indexOf('w') != -1
        val accessMode = ParcelFileDescriptor.parseMode(mode)
        val fileId = getFileIdFromDocumentId(documentId)
        val userDrive = createUserDrive(documentId)
        val localFile = FileController.getFileByUidOrId(fileId, userDrive)

        val updatedFile = runCatching {
            getRemoteFile(localFile, fileId, userDrive.driveId)
        }.getOrNull() ?: localFile

        // The fetch above can outlive an app-lock transition; revalidate before issuing a descriptor.
        ensureExternalAccessAllowed()

        return updatedFile?.let {
            if (isWrite) {
                writeDataFile(context, updatedFile, userDrive, accessMode)
            } else {
                // Resolve the local file first (may download: slow prep), then create and return
                // the descriptor atomically against app-lock transitions.
                resolveDataFile(context, updatedFile, userDrive)?.let { target ->
                    withFinalAdmission { ParcelFileDescriptor.open(target, accessMode) }
                }
            }
        }
    }

    override fun querySearchDocuments(
        rootId: String,
        query: String,
        projection: Array<out String>?
    ): Cursor = withLaxStrictMode {
        val cursor = DocumentCursor(projection ?: DEFAULT_DOCUMENT_PROJECTION, isAutoCloseableJob = false)

        val decision = accessDecision()
        if (decision != ProviderAccessPolicy.Reason.ALLOWED) {
            cursor.extras = bundleOf(DocumentsContract.EXTRA_ERROR to blockedReasonMessage(decision))
            // Install the notification URI so observers re-query once access is restored.
            context?.let { cursor.setNotificationUri(it.contentResolver, DocumentCursor.createUri(it, "$rootId/search/$query")) }
            return cursor
        }

        val browsingParentId = retainedCursors.parentDocumentId()
        if (browsingParentId == null ||
            browsingParentId == getUserId(browsingParentId) ||
            getFileIdFromDocumentId(browsingParentId) == SHARED_WITHME_FOLDER_ID ||
            getFileIdFromDocumentId(browsingParentId) == MY_SHARES_FOLDER_ID
        ) {
            val errorText = context?.getString(R.string.cloudStorageQuerySearchImpossibleSelectDrive)
            cursor.extras = bundleOf(DocumentsContract.EXTRA_ERROR to errorText)
            return cursor
        }

        val parentDocumentId = browsingParentId

        SentryLog.d(TAG, "querySearchDocuments(), rootId=$rootId, projectionSize=${projection?.size}, $parentDocumentId")

        val uri = DocumentCursor.createUri(context, "$rootId/search/$query")

        val restoredSearchCursor = retainedCursors.acquireSearch(uri, cursor)

        if (restoredSearchCursor != null) {
            cursor.restore(restoredSearchCursor)
            cursor.extras = bundleOf(DocumentsContract.EXTRA_LOADING to false)
            cursor.setNotificationUri(context?.contentResolver, uri)
            return cursor
        }

        val driveId = getDriveFromDocId(parentDocumentId).id
        val userId = getUserId(parentDocumentId)
        val userDrive = UserDrive(userId.toInt(), driveId, comeFromSharedWithMe(parentDocumentId))

        runSuspendOnPlatformAdaptively(cursor.job) {
            FileController.cloudStorageSearch(userDrive, query, onResponse = { files ->
                // A queued result page must not be appended after the lock was enabled.
                if (isExternalAccessBlocked()) cursor.job.cancel()
                cursor.job.ensureActive()
                files.forEach { file ->
                    cursor.addFile(file, createFileDocumentId(parentDocumentId, file.id))
                    cursor.job.ensureActive()
                }
                if (files.isNotEmpty()) context?.contentResolver?.notifyChange(uri, null)
            })
        }

        return withFinalAdmission {
            cursor.extras = bundleOf(DocumentsContract.EXTRA_LOADING to true)
            cursor.setNotificationUri(context?.contentResolver, uri)
            cursor
        }
    }

    override fun openDocumentThumbnail(
        documentId: String,
        sizeHint: Point?,
        signal: CancellationSignal?
    ): AssetFileDescriptor = withLaxStrictMode {
        SentryLog.d(TAG, "openDocumentThumbnail(), id=$documentId, signalIsCancelled: ${signal?.isCanceled}")

        ensureExternalAccessAllowed()

        val fileId = getFileIdFromDocumentId(documentId)
        val userId = getUserId(documentId)

        return FileController.getRealmInstance(createUserDrive(documentId)).use { realm ->

            FileController.getFileProxyById(fileId, customRealm = realm)?.let { file ->

                // Revalidate before creating the pipe: a fetch may have crossed a lock transition.
                ensureExternalAccessAllowed()

                generateThumbnail(fileId, file, userId, signal)?.let { parcel ->
                    AssetFileDescriptor(parcel, 0, AssetFileDescriptor.UNKNOWN_LENGTH)
                }

            } ?: super.openDocumentThumbnail(documentId, sizeHint, signal)
        }
    }

    /**
     * Typed-content hook actually exposed by [DocumentsProvider] (the Uri-level
     * openTypedAssetFile wrappers are final and funnel every typed request through here,
     * before delegating to the gated openDocument by default).
     */
    override fun openTypedDocument(
        documentId: String,
        mimeTypeFilter: String,
        opts: Bundle?,
        signal: CancellationSignal?
    ): AssetFileDescriptor = withLaxStrictMode {
        SentryLog.d(TAG, "openTypedDocument(), id=$documentId, mimeTypeFilter=$mimeTypeFilter")

        ensureExternalAccessAllowed()

        return super.openTypedDocument(documentId, mimeTypeFilter, opts, signal)
    }

    private fun generateThumbnail(fileId: Int, file: File, userId: String, signal: CancellationSignal?): ParcelFileDescriptor? {
        val outputFolder = IOFile(context?.cacheDir, "thumbnails").apply { if (!exists()) mkdirs() }
        val name = "${fileId}_${file.name}"
        val outputFile = IOFile(outputFolder, name)
        val thumbnailUrl = ApiRoutes.getThumbnailUrl(file)

        // Issue the pipe atomically against transitions; the download below rechecks the policy
        // generation before spending network I/O on behalf of an already-issued pipe.
        return withFinalAdmission {
            val issuedGeneration = AppLockState.policyGeneration()
            val (readPipe, writePipe) = ParcelFileDescriptor.createReliablePipe()

            val job = cloudScope.launch(Dispatchers.IO) {
                ParcelFileDescriptor.AutoCloseOutputStream(writePipe).use { writeStream ->
                    runCatching {
                        if (AppLockState.policyGeneration() != issuedGeneration || isExternalAccessBlocked()) {
                            writePipe.closeWithError("kDrive is locked")
                            return@launch
                        }
                        val okHttpClient = AccountUtils.getHttpClient(userId.toInt())
                        val response = DownloadOfflineFileManager.downloadFileResponseAsync(thumbnailUrl, okHttpClient)

                        if (response.isSuccessful && DownloadOfflineFileManager.saveRemoteData(TAG, response, outputFile)) {
                            writeStream.loadFromFile(outputFile)
                        } else if (outputFile.exists()) {
                            writeStream.loadFromFile(outputFile)
                        } else {
                            writePipe.closeWithError("No thumbnail available")
                        }

                    }.cancellable().onFailure { exception ->
                        val event = SentryEvent(exception).apply {
                            message = Message().apply { message = "An error has occurred on generateThumbnail" }
                        }
                        if (exception !is IOException) Sentry.captureEvent(event)
                        writePipe.closeWithError(exception.message)
                    }
                }
            }

            signal?.setOnCancelListener { job.cancel() }

            readPipe
        }
    }

    override fun createDocument(
        parentDocumentId: String,
        mimeType: String,
        displayName: String
    ): String = withLaxStrictMode {
        SentryLog.d(TAG, "createDocument(), parentId=$parentDocumentId, mimeType=$mimeType")

        ensureExternalAccessAllowed()

        return if (mimeType.equals(DocumentsContract.Document.MIME_TYPE_DIR, true)) {
            createNewFolder(parentDocumentId, displayName) // If we want to create a new folder
        } else {
            createNewFile(parentDocumentId, displayName) // If another provider copies or moves a file into kDrive
        }
    }

    override fun deleteDocument(documentId: String): Unit = withLaxStrictMode {
        SentryLog.d(TAG, "deleteDocument(), id=$documentId")

        ensureExternalAccessAllowed()

        val context = context ?: throw IllegalStateException("Delete document failed: missing Android Context")

        FileController.getRealmInstance(createUserDrive(documentId)).use { realm ->
            FileController.getFileProxyById(getFileIdFromDocumentId(documentId), customRealm = realm)?.let { file ->

                // Remote delete: admitted through the transition boundary; local bookkeeping
                // below stays on this thread with the provider-owned Realm.
                val apiResponse = admitAndDispatch { ApiRepository.deleteFile(file) }

                if (apiResponse.isSuccess()) {
                    runCatching {
                        FileController.deleteFileAndChildrenRec(
                            filesToDelete = mutableListOf(file),
                            realm = realm,
                            userDrive = null,
                            context = context,
                        )
                    }.onFailure(Sentry::captureException)

                    // Delete orphans
                    FileController.removeOrphanAndActivityFiles(realm)

                    // Refresh
                    scheduleRefresh()

                } else {
                    throw RuntimeException("Delete document failed")
                }
            }
        }
    }

    override fun renameDocument(
        documentId: String,
        displayName: String
    ): String? = withLaxStrictMode {
        SentryLog.d(TAG, "renameDocument(), id=$documentId")

        ensureExternalAccessAllowed()

        FileController.getRealmInstance(createUserDrive(documentId)).use { realm ->
            FileController.getFileProxyById(getFileIdFromDocumentId(documentId), customRealm = realm)?.let { file ->

                // Remote rename: admitted through the transition boundary; the local update
                // below stays on this thread with the provider-owned Realm.
                val apiResponse = admitAndDispatch { ApiRepository.renameFile(file, displayName) }

                if (apiResponse.isSuccess()) {
                    FileController.updateFile(file.id, realm) { localFile -> localFile.name = displayName }

                    // Refresh
                    scheduleRefresh()

                } else {
                    throw RuntimeException("Rename document failed")
                }
            }
        }

        return null
    }

    override fun copyDocument(
        sourceDocumentId: String,
        targetParentDocumentId: String
    ): String = withLaxStrictMode {
        SentryLog.d(TAG, "copyDocument(), sourceId=$sourceDocumentId, targetParentId=$targetParentDocumentId")

        ensureExternalAccessAllowed()

        return FileController.getRealmInstance(createUserDrive(sourceDocumentId)).use { realm ->

            val fileId = getFileIdFromDocumentId(sourceDocumentId)
            val file =
                FileController.getFileProxyById(fileId, customRealm = realm) ?: throw IllegalStateException("File not found")
            val targetParentFileId = getFileIdFromDocumentId(targetParentDocumentId)

            val apiResponse = admitAndDispatch { ApiRepository.duplicateFile(file, targetParentFileId) }

            if (apiResponse.isSuccess()) {

                // Refresh
                scheduleRefresh()

                return@use sourceDocumentId
            } else {
                throw RuntimeException("Copy document failed")
            }
        }
    }

    override fun moveDocument(
        sourceDocumentId: String,
        sourceParentDocumentId: String,
        targetParentDocumentId: String
    ): String = withLaxStrictMode {
        SentryLog.d(
            TAG, "moveDocument(), " +
                    "sourceId=$sourceDocumentId, " +
                    "sourceParentId=$sourceParentDocumentId, " +
                    "targetParentId=$targetParentDocumentId"
        )

        ensureExternalAccessAllowed()

        return FileController.getRealmInstance(createUserDrive(sourceDocumentId)).use { realm ->

            val fileNotFoundException = IllegalStateException("File not found")

            val fileId = getFileIdFromDocumentId(sourceDocumentId)
            val file = FileController.getFileProxyById(fileId, customRealm = realm) ?: throw fileNotFoundException

            val targetParentFileId = getFileIdFromDocumentId(targetParentDocumentId)
            val targetParentFile =
                FileController.getFileProxyById(targetParentFileId, customRealm = realm) ?: throw fileNotFoundException

            val apiResponse = admitAndDispatch { ApiRepository.moveFile(file, targetParentFile) }

            if (apiResponse.isSuccess()) {
                realm.executeTransaction { file.deleteFromRealm() }

                // Refresh
                scheduleRefresh(sourceParentDocumentId)

                return@use sourceDocumentId

            } else {
                throw RuntimeException("Move document failed")
            }
        }
    }

    private fun createNewFolder(parentDocumentId: String, displayName: String): String {

        val userDrive = createUserDrive(parentDocumentId)
        val parentId = getFileIdFromDocumentId(parentDocumentId)

        val apiResponse = admitAndDispatch { FileController.createFolder(displayName, parentId, true, userDrive) }
        val file = apiResponse.data

        if (apiResponse.isSuccess() && file != null) {

            FileController.addFileTo(parentId, file, userDrive)

            // Refresh
            scheduleRefresh()

            return createFileDocumentId(parentDocumentId, file.id)
        }

        throw RuntimeException("Create folder failed")
    }

    private fun createNewFile(parentDocumentId: String, displayName: String): String {

        // If we don't have the permissions, notify the user
        showSyncPermissionNotification()

        val driveId = getDriveFromDocId(parentDocumentId).id
        val parentFolderId = getFileIdFromDocumentId(parentDocumentId)
        val userDrive = createUserDrive(parentDocumentId)

        val newDate = Date()
        val uploadUrl = uploadFileDirectlyUrl(
            driveId = driveId,
            directoryId = parentFolderId,
            fileName = displayName,
            fileSize = 0L,
            conflictOption = ConflictOption.RENAME,
            createdAt = newDate,
            lastModifiedAt = newDate,
        )

        // Slow client preparation happens before the admission boundary.
        val okHttpClient = runBlocking { AccountUtils.getHttpClient(userDrive.userId, 120) }
        val apiResponse = admitAndDispatch {
            ApiController.callApiBlocking<ApiResponse<File>>(
                uploadUrl,
                ApiController.ApiMethod.POST,
                okHttpClient = okHttpClient,
            )
        }
        val file = apiResponse.data

        if (apiResponse.isSuccess() && file != null) {
            FileController.addFileTo(parentFolderId, file, userDrive)

            createTempFile(getFileIdFromDocumentId(parentDocumentId), displayName)

            return createFileDocumentId(parentDocumentId, file.id)
        }

        throw RuntimeException("Copy file failed")
    }

    private fun createTempFile(parentFileId: Int, displayName: String): IOFile {
        val tempFileFolder = IOFile(cacheDir, "$parentFileId").apply { if (!exists()) mkdirs() }
        return IOFile(tempFileFolder, displayName).apply { if (!exists()) createNewFile() }
    }

    private fun writeDataFile(context: Context, file: File, userDrive: UserDrive, accessMode: Int): ParcelFileDescriptor? {
        val tempFile = createTempFile(file.parentId, file.name)
        val cacheFile = file.getCacheFile(context, userDrive)
        val handler = Handler(context.mainLooper)

        return withFinalAdmission {
            ParcelFileDescriptor.open(tempFile, accessMode, handler) { exception: IOException? ->
                if (exception == null) {
                    cloudScope.launch {
                        // Completion capability for an already-authorized writable descriptor:
                        // edits written before lock activation must not be silently dropped, so
                        // the close-time upload runs even if access is now blocked (the remote
                        // mutation it dispatches was authorized at issuance time).
                        UploadFile(
                            uri = tempFile.toUri().toString(),
                            driveId = userDrive.driveId,
                            fileCreatedAt = Date(),
                            fileName = file.name,
                            fileSize = tempFile.length(),
                            remoteFolder = file.parentId,
                            type = UploadFile.Type.CLOUD_STORAGE.name,
                            userId = userDrive.userId,
                        ).store()
                        context.syncImmediately(isAutomaticTrigger = false)
                        cacheFile.delete() // Delete old cache
                    }
                } else {
                    exception.printStackTrace()
                    Sentry.captureException(exception) { scope -> scope.level = SentryLevel.INFO }
                }
            }
        }
    }

    /**
     * Resolves the local file backing [file] without opening it: the intact offline file, the
     * fresh cache file, or a freshly downloaded cache file. Slow preparation only — callers open
     * the returned file under [withFinalAdmission].
     */
    private fun resolveDataFile(context: Context, file: File, userDrive: UserDrive): IOFile? {
        val cacheFile = file.getCacheFile(context, userDrive)
        val offlineFile = file.getOfflineFile(context, userDrive.userId)

        return try {
            // Use the offline file if it's intact
            if (offlineFile != null && file.isOfflineAndIntact(offlineFile)) {
                return offlineFile
            }

            // Use the cache file if it exists and if the file is updated
            if (!file.isObsolete(cacheFile) && file.size == cacheFile.length()) {
                return cacheFile
            }

            // Download data file
            val okHttpClient = runBlocking { AccountUtils.getHttpClient(userDrive.userId) }
            val response = DownloadOfflineFileManager.downloadFileResponse(
                fileUrl = ApiRoutes.downloadFile(file),
                okHttpClient = okHttpClient,
                downloadInterceptor = DownloadOfflineFileManager.downloadProgressInterceptor(onProgress = { progress ->
                    SentryLog.i(TAG, "open currentProgress: $progress")
                })
            )

            if (response.isSuccessful) {
                runBlocking {
                    DownloadOfflineFileManager.saveRemoteData(TAG, response, cacheFile)
                }
                cacheFile.setLastModified(file.getLastModifiedInMilliSecond())
                cacheFile
            } else {
                null
            }
        } catch (exception: Exception) {
            SentryLog.e(TAG, "An error has occurred on getDataFile", exception)
            null
        }
    }

    private suspend fun ParcelFileDescriptor.AutoCloseOutputStream.loadCachedData(
        remoteDataHasBeenSaved: Boolean,
        cacheFile: IOFile,
        file: File,
        writePipe: ParcelFileDescriptor,
    ) {
        if (remoteDataHasBeenSaved) {
            cacheFile.setLastModified(file.getLastModifiedInMilliSecond())
            cacheFile.inputStream().use { it.copyToCancellable(this) }
        } else {
            writePipe.closeWithError("Error occurred when download remote data")
        }
    }

    private suspend fun ParcelFileDescriptor.AutoCloseOutputStream.loadFromFile(outputFile: IOFile) {
        outputFile.inputStream().use { inputStream ->
            inputStream.copyToCancellable(this)
        }
    }

    private fun scheduleRefresh(sourceParentDocumentId: String? = retainedCursors.parentDocumentId()) {
        sourceParentDocumentId?.let {
            retainedCursors.requestRefresh()
            context?.contentResolver?.notifyChange(DocumentCursor.createUri(context, it), null)
        }
    }

    @SuppressLint("BatteryLife")
    private fun showSyncPermissionNotification() {
        context?.let { context ->

            val powerManager = context.getSystemService(Context.POWER_SERVICE) as PowerManager?

            if (powerManager?.isIgnoringBatteryOptimizations(context.packageName) == false) {

                // Cancel previous notification
                context.cancelNotification(syncPermissionNotifId)

                // Display new notification
                context.buildGeneralNotification(context.getString(R.string.uploadPermissionError)).apply {
                    setContentText(context.getString(R.string.cloudStorageMissingPermissionNotifDescription))
                    setContentIntent(
                        PendingIntent.getActivity(
                            context,
                            syncPermissionNotifId,
                            Intent(
                                Settings.ACTION_REQUEST_IGNORE_BATTERY_OPTIMIZATIONS,
                                "package:${context.packageName}".toUri(),
                            ),
                            NotificationUtilsCore.PENDING_INTENT_FLAGS,
                        ),
                    )
                    NotificationManagerCompat.from(context).notifyCompat(syncPermissionNotifId, this)
                }
            }
        }
    }

    private fun MatrixCursor.addRoot(rootId: String, documentId: String, summary: String) {
        val flags = DocumentsContract.Root.FLAG_SUPPORTS_CREATE or
                DocumentsContract.Root.FLAG_SUPPORTS_RECENTS or
                DocumentsContract.Root.FLAG_SUPPORTS_SEARCH or
                DocumentsContract.Root.FLAG_SUPPORTS_IS_CHILD

        newRow().apply {
            add(DocumentsContract.Root.COLUMN_ROOT_ID, rootId)
            add(DocumentsContract.Root.COLUMN_DOCUMENT_ID, documentId)
            add(DocumentsContract.Root.COLUMN_TITLE, context?.getString(R.string.app_name))
            add(DocumentsContract.Root.COLUMN_SUMMARY, summary)
            add(DocumentsContract.Root.COLUMN_FLAGS, flags)
            add(DocumentsContract.Root.COLUMN_ICON, R.mipmap.ic_launcher)
        }
    }

    private fun MatrixCursor.addRootDrives(
        userId: Int,
        shareFolderId: Int? = null,
        shareWithMe: Boolean = false,
        isRootFolder: Boolean = false
    ) {
        runBlocking {
            DriveInfosController.getDrives(userId, sharedWithMe = shareWithMe).forEach { drive ->
                val driveDocument = "${drive.name}$DRIVE_SEPARATOR${drive.id}"
                val documentId =
                    if (shareFolderId == null) "$userId$SEPARATOR$driveDocument$SEPARATOR${Utils.ROOT_ID}"
                    else "$userId$SEPARATOR$shareFolderId$SEPARATOR$driveDocument$SEPARATOR${Utils.ROOT_ID}"
                addFile(null, documentId, drive.name, isRootFolder)
            }
        }
    }

    private fun MatrixCursor.addFile(file: File?, documentId: String, name: String = "", isRootFolder: Boolean = false) {

        if (context == null && file != null) return

        var flags = 0

        if (file?.hasCreationRight() == true || isRootFolder) {
            flags = flags or DocumentsContract.Document.FLAG_DIR_SUPPORTS_CREATE
        }

        val mimetype = if (file == null || file.isFolder()) {
            DocumentsContract.Document.MIME_TYPE_DIR
        } else {
            file.getMimeType()
        }

        if (file?.isFolder() == false && file.hasThumbnail) {
            flags = flags or DocumentsContract.Document.FLAG_SUPPORTS_THUMBNAIL
        }

        if (file != null) {
            flags = flags or
                    DocumentsContract.Document.FLAG_SUPPORTS_DELETE or
                    DocumentsContract.Document.FLAG_SUPPORTS_RENAME or
                    DocumentsContract.Document.FLAG_SUPPORTS_COPY or
                    DocumentsContract.Document.FLAG_SUPPORTS_MOVE or
                    DocumentsContract.Document.FLAG_SUPPORTS_WRITE or
                    DocumentsContract.Document.FLAG_SUPPORTS_REMOVE
        }

        val fileName = context?.let { file?.getDisplayName(it) } ?: ""

        newRow().apply {
            add(DocumentsContract.Document.COLUMN_DOCUMENT_ID, documentId)
            add(DocumentsContract.Document.COLUMN_MIME_TYPE, mimetype)
            add(DocumentsContract.Document.COLUMN_DISPLAY_NAME, file?.let { fileName } ?: name)
            add(DocumentsContract.Document.COLUMN_LAST_MODIFIED, file?.lastModifiedAt?.times(1000))
            add(DocumentsContract.Document.COLUMN_SIZE, file?.size)
            add(DocumentsContract.Document.COLUMN_FLAGS, flags)
        }
    }

    private fun DocumentCursor.restore(cursor: DocumentCursor) {
        var position = 0
        while (cursor.moveToPosition(position)) {
            newRow().apply {
                val docId = cursor.getString(cursor.getColumnIndex(DocumentsContract.Document.COLUMN_DOCUMENT_ID))
                val mimetype = cursor.getString(cursor.getColumnIndex(DocumentsContract.Document.COLUMN_MIME_TYPE))
                val displayName = cursor.getString(cursor.getColumnIndex(DocumentsContract.Document.COLUMN_DISPLAY_NAME))
                val lastModified = cursor.getLong(cursor.getColumnIndex(DocumentsContract.Document.COLUMN_LAST_MODIFIED))
                val size = cursor.getLong(cursor.getColumnIndex(DocumentsContract.Document.COLUMN_SIZE))
                val flags = cursor.getInt(cursor.getColumnIndex(DocumentsContract.Document.COLUMN_FLAGS))
                add(DocumentsContract.Document.COLUMN_DOCUMENT_ID, docId)
                add(DocumentsContract.Document.COLUMN_MIME_TYPE, mimetype)
                add(DocumentsContract.Document.COLUMN_DISPLAY_NAME, displayName)
                add(DocumentsContract.Document.COLUMN_LAST_MODIFIED, lastModified)
                add(DocumentsContract.Document.COLUMN_SIZE, size)
                add(DocumentsContract.Document.COLUMN_FLAGS, flags)
            }
            position++
        }
        job = cursor.job
    }

    private fun isDocumentIdDescendantOf(parentDocumentId: String, documentId: String): Boolean {
        if (parentDocumentId == documentId) return false
        return documentId.startsWith(parentDocumentId + SEPARATOR)
    }

    private fun computeDocumentType(documentId: String): CloudDocumentType {
        val userId = getUserId(documentId)
        if (documentId == userId) return CloudDocumentType.RootFolder

        val fileFolderId = getFileIdFromDocumentId(documentId)
        val isMySharesRoot = isSharedUri(documentId)
                && fileFolderId == Utils.ROOT_ID
                && documentId.split("/").getOrNull(1) == MY_SHARES_FOLDER_ID.toString()

        if (isMySharesRoot) return CloudDocumentType.DriveFromMySharesFolder

        return when (fileFolderId) {
            SHARED_WITHME_FOLDER_ID -> CloudDocumentType.SharedWithMeFolder
            MY_SHARES_FOLDER_ID -> CloudDocumentType.MySharesFolder
            Utils.ROOT_ID -> CloudDocumentType.DriveFolder
            else -> {
                val userDrive = UserDrive(
                    userId = userId.toInt(),
                    driveId = getDriveFromDocId(documentId).id,
                    sharedWithMe = comeFromSharedWithMe(documentId)
                )
                CloudDocumentType.FileOrFolder(fileFolderId, userDrive = userDrive)
            }
        }
    }

    /**
     * Executes the given block either synchronously on the current thread (if on Chrome OS)
     * or asynchronously in [cloudScope] otherwise.
     */
    private fun runOnPlatformAdaptively(context: CoroutineContext = EmptyCoroutineContext, block: () -> Unit) {
        if (isChromeOs) {
            block()
        } else {
            cloudScope.launch(context) { block() }
        }
    }

    /**
     * Executes the given suspend block adaptively:
     * - On Chrome OS: runs it synchronously (blocking the current thread) using [runBlocking].
     * - Otherwise: launches it asynchronously in [cloudScope].
     */
    private fun runSuspendOnPlatformAdaptively(context: CoroutineContext = EmptyCoroutineContext, block: suspend () -> Unit) {
        if (isChromeOs) {
            runBlocking { block() }
        } else {
            cloudScope.launch(context) { block() }
        }
    }

    private data class DriveDocument(val name: String, val id: Int)

    companion object {

        init {
            // React to app-lock transitions wherever they originate in this process.
            AppLockState.onTransition = { onAppLockTransition() }
        }

        /**
         * Synchronized registry of the retained child/search cursors and their browsing state.
         * Each entry is tagged with the AppLockState policy generation it was created under;
         * restoration is refused when a transition happened in between, and invalidation
         * (cancelling jobs, clearing state) is atomic against registration and restoration.
         */
        private class RetainedCursors {

            private var childUri: Uri? = null
            private var childCursor: DocumentCursor? = null
            private var childGeneration: Long = -1
            private var searchUri: Uri? = null
            private var searchCursor: DocumentCursor? = null
            private var searchGeneration: Long = -1
            private var parentDocumentId: String? = null
            private var needRefresh = false

            /**
             * Registers [cursor] as a new child job, or returns the retained cursor when it can
             * be restored (same URI, no pending refresh, same policy generation).
             */
            fun acquireChild(uri: Uri, cursor: DocumentCursor, parentDocumentId: String): DocumentCursor? = synchronized(this) {
                val retained = childCursor
                val canRestore = retained != null
                        && childUri == uri
                        && !needRefresh
                        && childGeneration == AppLockState.policyGeneration()
                if (canRestore) {
                    retained
                } else {
                    retained?.cancelJob()
                    childUri = uri
                    childCursor = cursor
                    childGeneration = AppLockState.policyGeneration()
                    this.parentDocumentId = parentDocumentId
                    needRefresh = false
                    null
                }
            }

            /** Same contract as [acquireChild] for search cursors (a new search closes the old one). */
            fun acquireSearch(uri: Uri, cursor: DocumentCursor): DocumentCursor? = synchronized(this) {
                val retained = searchCursor
                val canRestore = retained != null
                        && searchUri == uri
                        && searchGeneration == AppLockState.policyGeneration()
                if (canRestore) {
                    retained
                } else {
                    retained?.close()
                    searchUri = uri
                    searchCursor = cursor
                    searchGeneration = AppLockState.policyGeneration()
                    null
                }
            }

            fun invalidateAll() = synchronized(this) {
                childCursor?.cancelJob()
                searchCursor?.cancelJob()
                childUri = null
                childCursor = null
                searchUri = null
                searchCursor = null
                parentDocumentId = null
                needRefresh = false
            }

            fun parentDocumentId(): String? = synchronized(this) { parentDocumentId }

            fun requestRefresh() = synchronized(this) { needRefresh = true }
        }

        private val retainedCursors = RetainedCursors()

        /**
         * Invalidates listing/search state retained across calls (so stale or blocked cursors are
         * never restored) and reannounces the roots, letting observers replace stale content.
         */
        internal fun onAppLockTransition() {
            retainedCursors.invalidateAll()
            notifyProviderChanged()
        }

        private val mutex = Mutex()

        private const val TAG = "CloudStorageProvider"
        private const val SEPARATOR = "/"
        private const val DRIVE_SEPARATOR = "@"
        private const val MY_SHARES_FOLDER_ID = -1
        private const val SHARED_WITHME_FOLDER_ID = -2
        private const val PREFS_NAME = "cloud_storage_provider"
        private const val KEY_PROVIDER_DISABLED = "provider_disabled"

        private val SHARED_URI_REGEX = Regex("\\d+/-\\d+/.+$DRIVE_SEPARATOR\\d+/\\d+")

        private val syncPermissionNotifId = UUID.randomUUID().hashCode()

        private val DEFAULT_ROOT_PROJECTION = arrayOf(
            DocumentsContract.Root.COLUMN_ROOT_ID,
            DocumentsContract.Root.COLUMN_DOCUMENT_ID,
            DocumentsContract.Root.COLUMN_TITLE,
            DocumentsContract.Root.COLUMN_SUMMARY,
            DocumentsContract.Root.COLUMN_FLAGS,
            DocumentsContract.Root.COLUMN_ICON
        )

        private val DEFAULT_DOCUMENT_PROJECTION = arrayOf(
            DocumentsContract.Document.COLUMN_DOCUMENT_ID,
            DocumentsContract.Document.COLUMN_MIME_TYPE,
            DocumentsContract.Document.COLUMN_DISPLAY_NAME,
            DocumentsContract.Document.COLUMN_LAST_MODIFIED,
            DocumentsContract.Document.COLUMN_FLAGS,
            DocumentsContract.Document.COLUMN_SIZE
        )

        suspend fun Context.initRealm() {
            mutex.withLock {
                try {
                    Realm.getDefaultInstance()
                } catch (_: Exception) {
                    Realm.init(this)
                    AccountUtils.init()
                }
            }
        }

        private fun getUserId(documentId: String) = documentId.substringBefore(SEPARATOR)

        private fun getFileIdFromDocumentId(documentId: String) = documentId.substringAfterLast(SEPARATOR).toInt()

        private fun createFileDocumentId(parentDocumentId: String, fileId: Int, parent: String? = null): String {
            val parentPath = if (parent == null) "" else "$parent$SEPARATOR"
            return parentDocumentId.substringBeforeLast(SEPARATOR) + SEPARATOR + parentPath + fileId.toString()
        }

        private fun isSharedUri(documentId: String) = documentId.matches(SHARED_URI_REGEX)

        private fun getDriveFromDocId(documentId: String): DriveDocument {
            val parentDocumentId = documentId.substringBeforeLast(SEPARATOR)
            return if (documentId.contains(DRIVE_SEPARATOR)) {
                val drivePath = parentDocumentId.substringAfter(SEPARATOR)
                val driveName = drivePath.substringBeforeLast(DRIVE_SEPARATOR)
                val driveId = drivePath.substringAfterLast(DRIVE_SEPARATOR).toInt()
                DriveDocument(name = driveName, id = driveId)
            } else DriveDocument(name = "", id = -1)
        }

        private fun getSortType(sortOrder: String?): File.SortType {
            return when (sortOrder) {
                "_display_name ASC" -> File.SortType.NAME_AZ
                "_display_name DESC" -> File.SortType.NAME_ZA
                "last_modified ASC" -> File.SortType.OLDER
                "last_modified DESC" -> File.SortType.RECENT
                "_size ASC" -> File.SortType.SMALLER
                "_size DESC" -> File.SortType.BIGGER
                else -> File.SortType.NAME_AZ
            }
        }

        fun createShareFileUri(context: Context, file: File, userDrive: UserDrive): Uri? {
            val currentUserId = userDrive.userId
            val currentDriveId = userDrive.driveId

            DriveInfosController.getDrive(currentUserId, currentDriveId)?.let { drive ->
                val baseContentUri = "content://${context.getString(R.string.CLOUD_STORAGE_AUTHORITY)}/document/"
                val sharedId = if (userDrive.sharedWithMe) "${SHARED_WITHME_FOLDER_ID}$SEPARATOR" else ""
                val content = "$currentUserId$SEPARATOR$sharedId${drive.name}$DRIVE_SEPARATOR$currentDriveId/${file.id}"
                return "$baseContentUri${URLEncoder.encode(content, "UTF-8")}".toUri()
            }

            return null
        }

        private fun comeFromSharedWithMe(documentId: String): Boolean =
            documentId.split("/").getOrNull(1) == SHARED_WITHME_FOLDER_ID.toString()

        private fun createUserDrive(documentId: String): UserDrive =
            UserDrive(
                getUserId(documentId).toInt(),
                getDriveFromDocId(documentId).id,
                comeFromSharedWithMe(documentId)
            )

        fun isDisabled(context: Context): Boolean {
            return context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE).getBoolean(KEY_PROVIDER_DISABLED, false)
        }

        fun setDisabled(context: Context, disabled: Boolean) {
            val prefs = context.getSharedPreferences(PREFS_NAME, Context.MODE_PRIVATE)
            val old = prefs.getBoolean(KEY_PROVIDER_DISABLED, false)
            if (old == disabled) return

            prefs.edit { putBoolean(KEY_PROVIDER_DISABLED, disabled) }
            notifyProviderChanged()
        }

        fun notifyProviderChanged() {
            val authority = appCtx.getString(R.string.CLOUD_STORAGE_AUTHORITY)
            val docBase = "content://$authority/document".toUri()
            val rootsUri = DocumentsContract.buildRootsUri(authority)
            appCtx.contentResolver.notifyChange(docBase, null)
            appCtx.contentResolver.notifyChange(rootsUri, null)
        }

        fun notifyRootsChanged() {
            val authority = appCtx.getString(R.string.CLOUD_STORAGE_AUTHORITY)
            val rootsUri = DocumentsContract.buildRootsUri(authority)
            appCtx.contentResolver.notifyChange(rootsUri, null)
        }
    }
}

private inline fun <R> withLaxStrictMode(block: () -> R): R {
    val currentPolicy = StrictMode.getThreadPolicy()
    try {
        StrictMode.setThreadPolicy(StrictMode.ThreadPolicy.LAX)
        return block()
    } finally {
        StrictMode.setThreadPolicy(currentPolicy)
    }
}
