/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2023-2026 Infomaniak Network SA
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
package com.infomaniak.drive.data.cache

import android.util.Log
import androidx.collection.ArrayMap
import androidx.collection.arrayMapOf
import com.infomaniak.core.common.cancellable
import com.infomaniak.core.network.networking.DefaultHttpClientProvider
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.ApiRoutes
import com.infomaniak.drive.data.api.ErrorCode
import com.infomaniak.drive.data.api.CursorApiResponse
import com.infomaniak.drive.data.cache.FileController.saveRemoteFiles
import com.infomaniak.drive.data.cache.FolderFilesProvider.SourceRestrictionType.ONLY_FROM_REMOTE
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.File.SortType
import com.infomaniak.drive.data.models.FileAction
import com.infomaniak.drive.data.models.FileActivityType
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.models.file.SpecialFolder.SharedWithMe
import com.infomaniak.drive.data.services.MqttClientWrapper
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.FileId
import com.infomaniak.drive.utils.OfflineFileCoordination
import com.infomaniak.drive.utils.OfflineFolderUtils
import com.infomaniak.drive.utils.Utils.ROOT_ID
import io.realm.Realm
import io.realm.RealmQuery
import io.sentry.Sentry
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import java.util.Calendar

object FolderFilesProvider {

    private val TAG = FolderFilesProvider::class.java.simpleName

    // Bump this when we want to force-refresh files that are too old.
    // Example: We did it when we added Categories & Colored folders, to automatically display them when updating the app.
    private const val MIN_VERSION_CODE = 5_00_010_01

    private val minDateToIgnoreCache = Calendar.getInstance().apply { add(Calendar.MONTH, -2) }.timeInMillis / 1000 // 3 month

    fun getFiles(folderFilesProviderArgs: FolderFilesProviderArgs): FolderFilesProviderResult? {
        val realm = folderFilesProviderArgs.realm ?: FileController.getRealmInstance(folderFilesProviderArgs.userDrive)
        try {
            val folderProxy = FileController.getFileById(realm, folderFilesProviderArgs.folderId)
            val sourceRestrictionType = folderFilesProviderArgs.sourceRestrictionType
            val needToLoadFromRemote = needToLoadFromRemote(sourceRestrictionType, folderProxy)
            SentryLog.i(
                TAG,
                "getFiles with folder: ${folderProxy?.id}, sourceRestrictionType: $sourceRestrictionType, needToLoadFromRemote: $needToLoadFromRemote"
            )
            val files = when {
                needToLoadFromRemote && sourceRestrictionType != SourceRestrictionType.ONLY_FROM_LOCAL -> {
                    loadFromRemote(realm, folderProxy, folderFilesProviderArgs)
                }
                folderFilesProviderArgs.isFirstPage -> {
                    loadFromLocal(realm, folderProxy, folderFilesProviderArgs.withChildren, folderFilesProviderArgs.order)
                }
                else -> {
                    SentryLog.i(TAG, "getFiles: files is null")
                    null
                }
            }
            SentryLog.i(TAG, "Load files finished")
            return files
        } finally {
            if (folderFilesProviderArgs.realm == null) realm.close()
        }
    }

    suspend fun loadRootFiles(userDrive: UserDrive, hasNetwork: Boolean) = withContext(Dispatchers.IO) {
        if (hasNetwork) {
            runCatching {
                getFiles(
                    FolderFilesProviderArgs(
                        folderId = ROOT_ID,
                        isFirstPage = true,
                        order = SortType.NAME_AZ,
                        sourceRestrictionType = ONLY_FROM_REMOTE,
                        userDrive = userDrive,
                    )
                )
            }.cancellable().onFailure { throwable ->
                SentryLog.e(TAG, "recursiveDownload failed", throwable)
            }.getOrNull()
        }
    }

    suspend fun loadSharedWithMeFiles(
        folderFilesProviderArgs: FolderFilesProviderArgs,
        onRecursionStart: (() -> Unit)? = null,
    ) {
        with(folderFilesProviderArgs) {
            val rootFolder = File(id = SharedWithMe.id, name = "/")
            val okHttpClient = AccountUtils.getHttpClient(userDrive.userId)

            FileController.getRealmInstance(userDrive).use { realm ->
                loadSharedWithMeFilesRec(
                    realm = realm,
                    okHttpClient = okHttpClient,
                    folderFilesProviderArgs = this,
                    isRoot = folderId == ROOT_ID,
                    rootFolder = rootFolder,
                    onRecursionStart = onRecursionStart,
                    // CANCELLATION AFTER EACH RESOLVING HTTP (issue: round 20 #22): the
                    // suspend caller's job is checked between the resolving responses and
                    // the side effects.
                    checkCancelled = { coroutineContext.ensureActive() },
                )
            }
        }
    }

    private tailrec fun loadSharedWithMeFilesRec(
        realm: Realm,
        okHttpClient: OkHttpClient,
        isRoot: Boolean,
        folderFilesProviderArgs: FolderFilesProviderArgs,
        cursor: String? = null,
        rootFolder: File? = null,
        onRecursionStart: (() -> Unit)? = null,
        observedById: Map<Int, File> = emptyMap(),
        checkCancelled: () -> Unit = {},
    ) {
        val folderId = if (isRoot) SharedWithMe.id else folderFilesProviderArgs.folderId
        // PRE-REQUEST observations (issue: round 18 #21): the container's children detached
        // BEFORE the first page of this request sequence — the same anchor the recursive
        // walker commits against, so a same-folder identity tie (e.g. a same-second rename
        // between the request and the commit) resolves through the shared boundary instead
        // of deferring on every pass. The map is captured ONCE per sequence and threaded
        // through every page and the recursion: all pages belong to one request sequence
        // and share its immutable pre-request state. A row moving INTO this folder from
        // another container is not among the pre-request children — its observation is
        // unknowable before the response names it; it defers to the next listing pass,
        // exactly as before (the offline walk resolves such candidates with fresh
        // details requests instead, issue: round 18 #4).
        val sequenceObservations = if (cursor == null) {
            // DURABLE PRE-HTTP MEMBERSHIP (issue: round 19 #25): the same basis as the
            // walk's observations — the presentation children list is cleared and rebuilt
            // by every first-page commit, so a retained pending candidate whose link a
            // previous pass dropped lost its observation and its retry authority.
            realm.where(File::class.java)
                .equalTo(File::parentId.name, folderId)
                .findAll()
                .associateBy({ child -> child.id }) { child -> realm.copyFromRealm(child, 1) }
        } else {
            observedById
        }
        val apiResponse = if (isRoot) {
            ApiRepository.getSharedWithMeFiles(
                okHttpClient = okHttpClient,
                order = folderFilesProviderArgs.order,
                cursor = cursor
            )
        } else {
            ApiRepository.getFolderFiles(
                okHttpClient = okHttpClient,
                driveId = folderFilesProviderArgs.userDrive.driveId,
                parentId = folderFilesProviderArgs.folderId,
                cursor = cursor,
                order = folderFilesProviderArgs.order,
            )
        }
        val folderProxy = FileController.getFileById(realm, folderId)

        fun saveFiles() = saveRemoteFiles(
            realm = realm,
            localFolderProxy = folderProxy,
            remoteFolder = if (folderFilesProviderArgs.folderId == ROOT_ID) rootFolder else folderProxy,
            apiResponse = apiResponse,
            isFirstPage = cursor == null,
            isCompleteFolder = !apiResponse.hasMore,
            userDrive = folderFilesProviderArgs.userDrive,
            observedById = sequenceObservations,
        )

        when {
            apiResponse.hasMoreAndCursorExists -> {
                saveFiles()
                if (cursor == null) onRecursionStart?.invoke()
                loadSharedWithMeFilesRec(
                    realm = realm,
                    okHttpClient = okHttpClient,
                    isRoot = isRoot,
                    folderFilesProviderArgs = folderFilesProviderArgs,
                    cursor = apiResponse.cursor,
                    observedById = sequenceObservations,
                    checkCancelled = checkCancelled,
                )
            }
            apiResponse.isSuccess() -> {
                val commitOutcomes = saveFiles()
                // ONE budgeted resolution pass for this listing's deferred candidates
                // (issue: round 19 #20) — ordinary and shared listings resolve their
                // move-in candidates exactly like the offline walk: fresh pre-observed
                // details requests, fair per-item outcomes. Unresolved candidates wait
                // for the NEXT listing pass; this never converges in place.
                resolveDeferredCandidatesOnce(
                    realm = realm,
                    okHttpClient = okHttpClient,
                    folderId = folderId,
                    userDrive = folderFilesProviderArgs.userDrive,
                    deferredIds = commitOutcomes.deferredIds,
                    checkCancelled = checkCancelled,
                )
            }
        }
    }

    /**
     * ONE budgeted resolution pass for a listing's deferred candidates (issue: round 19
     * #20): each selected id gets a NEW details request issued after its detached
     * pre-request observation is captured, and THAT response is committed against THAT
     * observation through the shared preservation boundary — the same protocol as the
     * offline walk's resolution (round 18 #4), with FAIR per-item outcomes (a rotating
     * selection, one outcome per candidate). The accepted resolution is linked under this
     * container — a single-file commit bypasses the listing's child linking. ONE pass per
     * listing: unresolved candidates wait for the NEXT listing pass — this loop never
     * converges in place (no loop-until-done), and the listing's own commit outcome is
     * unaffected.
     */
    private fun resolveDeferredCandidatesOnce(
        realm: Realm,
        okHttpClient: OkHttpClient,
        folderId: Int,
        userDrive: UserDrive,
        deferredIds: Set<Int>,
        checkCancelled: () -> Unit = {},
    ) {
        if (deferredIds.isEmpty()) return
        for (deferredId in OfflineFolderUtils.fairDeferredSelection(
            deferredIds,
            folderId,
            OfflineFolderUtils.DEFERRED_RESOLUTION_BUDGET,
        )) {
            val preObservation = FileController.getFileById(realm, deferredId)
                ?.let { row -> realm.copyFromRealm(row, 1) } ?: continue
            val details = runCatching {
                ApiRepository.getFileDetails(
                    file = File(id = deferredId, driveId = userDrive.driveId),
                    okHttpClient = okHttpClient,
                )
            }.getOrNull()?.data?.takeIf { it.id == deferredId } ?: continue
            // Cancellation after the resolving HTTP and again after the coordination
            // wait, before any side effect (issue: round 20 #22).
            checkCancelled()
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withLocksFor(userDrive, listOf(deferredId, folderId).distinct()) {
                    val committable = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                        realm = realm,
                        remoteFiles = listOf(details),
                        userDrive = userDrive,
                        observedById = mapOf(deferredId to preObservation),
                    ).first()
                    if (committable === details) {
                        val committed = FileController.updateExistingFileIfNotOlder(
                            newFile = committable,
                            realm = realm,
                            userDrive = userDrive,
                            observed = preObservation,
                        )
                        if (committed) {
                            // MEMBERSHIP: link the accepted row under this container.
                            realm.executeTransaction {
                                val freshRow = FileController.getFileById(realm, deferredId)
                                    ?: return@executeTransaction
                                val container = FileController.getFileById(realm, folderId)
                                if (container != null && freshRow.parentId == folderId &&
                                    container.children.none { it.id == deferredId }
                                ) {
                                    container.children.add(freshRow)
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    fun getCloudStorageFiles(
        realm: Realm,
        folderId: Int,
        userDrive: UserDrive,
        sortType: SortType,
        isFirstPage: Boolean = true,
        transaction: (files: ArrayList<File>) -> Unit,
        okHttpClient: OkHttpClient? = null,
    ) {
        val folderProxy = FileController.getFileById(realm, folderId)
        val folderFilesProviderArgs = FolderFilesProviderArgs(
            folderId = folderId,
            isFirstPage = isFirstPage,
            order = sortType,
            realm = realm,
            sourceRestrictionType = ONLY_FROM_REMOTE,
            userDrive = userDrive,
        )
        val currentOkHttpClient = okHttpClient ?: runBlocking { AccountUtils.getHttpClient(userDrive.userId) }
        val folderFilesProviderResult = loadCloudStorageFromRemote(
            realm = realm,
            folderProxy = folderProxy,
            folderFilesProviderArgs = folderFilesProviderArgs,
            okHttpClient = currentOkHttpClient,
        )

        transaction(folderFilesProviderResult?.folderFiles ?: arrayListOf())

        if (folderFilesProviderResult?.isComplete == false) getCloudStorageFiles(
            realm = realm,
            folderId = folderId,
            userDrive = userDrive,
            sortType = sortType,
            isFirstPage = false,
            transaction = transaction,
            okHttpClient = currentOkHttpClient,
        )
    }

    fun tryLoadActivitiesFromFolder(folder: File, userDrive: UserDrive, activitiesJob: Job): Boolean {
        val realm = FileController.getRealmInstance(userDrive)
        val folderProxy = FileController.getFileById(realm, folder.id) ?: return false
        if (!folderProxy.isComplete) return false

        val okHttpClient = runBlocking { AccountUtils.getHttpClient(userDrive.userId, 30) }
        val result = loadActivitiesFromFolderRec(activitiesJob, folderProxy, userDrive, okHttpClient)

        realm.close()

        return result.isNotEmpty()
    }

    private fun loadFromRemote(
        realm: Realm,
        folderProxy: File?,
        folderFilesProviderArgs: FolderFilesProviderArgs,
    ): FolderFilesProviderResult? = with(Dispatchers.IO) {

        SentryLog.i(TAG, "loadFromRemote")
        val userDrive = folderFilesProviderArgs.userDrive
        val (okHttpClient, driveId) = runBlocking { AccountUtils.getHttpClient(userDrive.userId) } to userDrive.driveId

        // COMPLETE PRE-REQUEST OBSERVATIONS (issue: round 16 #16): the container's children as
        // they stood before the listing HTTP request, detached at depth one (embedded version
        // included, issue: round 16 #9). They anchor tied identity changes through the shared
        // boundary and the committing transaction, so ordinary listings converge on
        // same-second renames instead of deferring them on every pass.
        val observedById = folderProxy?.children
            ?.associateBy({ child -> child.id }) { child -> realm.copyFromRealm(child, 1) }
            ?: emptyMap()

        val apiResponse = when {
            folderFilesProviderArgs.folderId == ROOT_ID -> {
                ApiRepository.getFolderFiles(
                    okHttpClient = okHttpClient,
                    driveId = driveId,
                    parentId = folderFilesProviderArgs.folderId,
                    cursor = if (folderFilesProviderArgs.isFirstPage) null else folderProxy?.cursor,
                    order = folderFilesProviderArgs.order,
                )

            }
            else -> {
                ApiRepository.getListingFiles(
                    okHttpClient = okHttpClient,
                    driveId = driveId,
                    parentId = folderFilesProviderArgs.folderId,
                    cursor = if (folderFilesProviderArgs.isFirstPage) null else folderProxy?.cursor,
                    order = folderFilesProviderArgs.order,
                ).let {
                    CursorApiResponse(
                        result = it.result,
                        data = it.data?.files,
                        error = it.error,
                        responseAt = it.responseAt,
                        cursor = it.cursor,
                        hasMore = it.hasMore,
                    )
                }
            }
        }

        ensureActive()

        handleRemoteFiles(realm, apiResponse, folderFilesProviderArgs, folderProxy, okHttpClient, observedById)
    }

    private fun loadCloudStorageFromRemote(
        realm: Realm,
        folderProxy: File?,
        folderFilesProviderArgs: FolderFilesProviderArgs,
        okHttpClient: OkHttpClient,
    ): FolderFilesProviderResult? = with(Dispatchers.IO) {
        val userDrive = folderFilesProviderArgs.userDrive

        // COMPLETE PRE-REQUEST OBSERVATIONS (issue: round 16 #16), as in [loadFromRemote].
        val observedById = folderProxy?.children
            ?.associateBy({ child -> child.id }) { child -> realm.copyFromRealm(child, 1) }
            ?: emptyMap()

        val apiResponse = when {
            folderFilesProviderArgs.folderId == ROOT_ID && userDrive.sharedWithMe -> {
                ApiRepository.getSharedWithMeFiles(
                    okHttpClient = okHttpClient,
                    order = folderFilesProviderArgs.order,
                    cursor = if (folderFilesProviderArgs.isFirstPage) null else folderProxy?.cursor,
                )
            }
            else -> {
                ApiRepository.getFolderFiles(
                    okHttpClient = okHttpClient,
                    driveId = userDrive.driveId,
                    parentId = folderFilesProviderArgs.folderId,
                    cursor = if (folderFilesProviderArgs.isFirstPage) null else folderProxy?.cursor,
                    order = folderFilesProviderArgs.order,
                )
            }
        }

        ensureActive()

        handleRemoteFiles(realm, apiResponse, folderFilesProviderArgs, folderProxy, observedById = observedById)
    }

    private fun handleRemoteFiles(
        realm: Realm,
        apiResponse: CursorApiResponse<List<File>>,
        folderFilesProviderArgs: FolderFilesProviderArgs,
        folderProxy: File?,
        okHttpClient: OkHttpClient = DefaultHttpClientProvider.okHttpClient,
        observedById: Map<Int, File> = emptyMap(),
    ): FolderFilesProviderResult? {
        val userDrive = folderFilesProviderArgs.userDrive
        val apiResponseData = apiResponse.data
        val folderWithChildren = folderFilesProviderArgs.withChildren

        val localFolder = folderProxy?.freeze()
            ?: ApiRepository.getFileDetails(
                File(id = folderFilesProviderArgs.folderId, driveId = userDrive.driveId),
                okHttpClient
            ).data
            ?: return null

        return when {
            apiResponseData != null -> {
                val isCompleteFolder = !apiResponse.hasMore || apiResponseData.count() < ApiRoutes.PER_PAGE
                saveRemoteFiles(
                    realm = realm,
                    localFolderProxy = folderProxy,
                    remoteFolder = localFolder,
                    apiResponse = apiResponse,
                    isFirstPage = folderFilesProviderArgs.isFirstPage,
                    isCompleteFolder = isCompleteFolder,
                    userDrive = userDrive,
                    observedById = observedById
                )
                val folderFiles = if (folderWithChildren) ArrayList(apiResponseData) else arrayListOf()
                FolderFilesProviderResult(folder = localFolder, folderFiles = folderFiles, isComplete = isCompleteFolder)
            }
            folderFilesProviderArgs.isFirstPage -> {
                loadFromLocal(realm, folderProxy, folderWithChildren, folderFilesProviderArgs.order)
            }
            else -> {
                null
            }
        }
    }

    private fun loadFromLocal(
        realm: Realm,
        folderProxy: File?,
        withChildren: Boolean,
        order: SortType
    ): FolderFilesProviderResult? {
        SentryLog.i(TAG, "loadFromLocal")
        val localFolderWithoutChildren = folderProxy?.let { realm.copyFromRealm(it, 1) } ?: return null
        val sortedFolderFiles = if (withChildren) FileController.getLocalSortedFolderFiles(folderProxy, order) else arrayListOf()
        return FolderFilesProviderResult(folder = localFolderWithoutChildren, folderFiles = sortedFolderFiles, isComplete = true)
    }

    private fun needToLoadFromRemote(sourceRestrictionType: SourceRestrictionType, folderProxy: File?): Boolean {

        fun hasDuplicatesFiles(query: RealmQuery<File>): Boolean = query.count() != query.distinct(File::id.name).count()

        return sourceRestrictionType == ONLY_FROM_REMOTE
                || folderProxy == null
                || folderProxy.children.isEmpty()
                || !folderProxy.isComplete
                || folderProxy.versionCode <= MIN_VERSION_CODE
                || folderProxy.children.where().let(::hasDuplicatesFiles)
                || minDateToIgnoreCache >= folderProxy.responseAt
    }

    /**
     * Pagination-pass state (issue: round 16 #17): a deferral on ANY page retains the folder
     * cursor for the WHOLE pass — a later fully-handled page must never persist a cursor
     * beyond an earlier unresolved action.
     */
    private class FolderActivitiesPassState {
        var anyPageDeferred = false
    }

    private tailrec fun loadActivitiesFromFolderRec(
        activitiesJob: Job,
        folderProxy: File,
        userDrive: UserDrive,
        okHttpClient: OkHttpClient,
        cursor: String? = folderProxy.cursor,
        returnResponse: ArrayMap<Int, FileAction> = arrayMapOf(),
        passState: FolderActivitiesPassState = FolderActivitiesPassState(),
    ): Map<out Int, FileAction> {
        SentryLog.i(TAG, "loadActivitiesFromFolderRec with folderId ${folderProxy.id} cursor: ${cursor != null}")
        val realm = folderProxy.realm
        // DETACHED pre-request observations (issues: round 15 #10, round 16 #9): the
        // container's children as they stood when the listing/activities request was issued —
        // managed objects would refresh together with the rows inside each activity's
        // coordination and defeat tie resolution, leaving tied directory changes deferred
        // forever. The copy is DEPTH ONE (issue: round 16 #9): a depth-zero copy drops the
        // embedded FileVersion, and the tie-resolution observation comparison needs the
        // version identifier the live row carries — an observation missing its own version
        // defers every legitimate tied rename.
        val observedChildren = folderProxy.children.associateBy({ child -> child.id }) { child ->
            realm.copyFromRealm(child, 1)
        }
        val apiResponse = ApiRepository.getListingFiles(
            okHttpClient = okHttpClient,
            driveId = userDrive.driveId,
            parentId = folderProxy.id,
            cursor = cursor,
            order = SortType.NAME_AZ,
        )

        if (!apiResponse.isSuccess()) return returnResponse

        activitiesJob.ensureActive()

        val apiResponseData = apiResponse.data

        var allActivitiesHandled = true
        if (apiResponseData != null && apiResponseData.actions.isNotEmpty()) {
            val actionsFiles = apiResponseData.actionsFiles.associateBy(File::id)
            SentryLog.i(TAG, "loadActivitiesFromFolderRec: actions ${apiResponseData.actions.count()}")
            apiResponseData.actions.asReversed().forEach { fileActivity ->
                allActivitiesHandled = fileActivity.applyFileAction(
                    realm,
                    actionsFiles,
                    returnResponse,
                    folderProxy,
                    userDrive,
                    observedChildren,
                ) && allActivitiesHandled
            }
        }

        // Cross-page deferral memory (issue: round 16 #17): a deferral on THIS page marks the
        // whole pagination pass unresolved, so a later fully-handled page cannot persist a
        // cursor beyond the earlier action.
        if (!allActivitiesHandled) passState.anyPageDeferred = true

        // The folder cursor advances ONLY when every activity of the WHOLE pagination pass
        // was applied (issues: round 15 #10, round 16 #17): a deferred tied change retains
        // unresolved work — the next activities fetch re-runs from the retained cursor and
        // the periodic folder walk is the resolving refresh. The write happens ONCE at the
        // pass's final page: per-page persistence previously let a fully-handled later page
        // advance the cursor past an earlier deferred page's action, permanently skipping
        // the unresolved work (whose duplicate suppression inside the pass cannot hide it).
        if (allActivitiesHandled) {
            SentryLog.d(TAG, "loadActivitiesFromFolderRec: page fully handled for folder ${folderProxy.id}")
        } else {
            SentryLog.i(
                TAG,
                "loadActivitiesFromFolderRec: deferred folder ${folderProxy.id} activities on this page; pass cursor retained for fair retry"
            )
        }

        return if (apiResponse.hasMoreAndCursorExists) {
            SentryLog.i(TAG, "loadActivitiesFromFolderRec: loading next page")
            // Loading the next page, then the cursor is required
            loadActivitiesFromFolderRec(
                activitiesJob = activitiesJob,
                folderProxy = folderProxy,
                cursor = apiResponse.cursor,
                userDrive = userDrive,
                okHttpClient = okHttpClient,
                returnResponse = returnResponse,
                passState = passState,
            )
        } else {
            SentryLog.i(TAG, "loadActivitiesFromFolderRec: no more pages")
            if (!passState.anyPageDeferred) {
                if (apiResponse.responseAt > 0L) {
                    FileController.updateFile(folderProxy.id, realm) { file ->
                        file.responseAt = apiResponse.responseAt
                        apiResponse.cursor?.let { file.cursor = it }
                    }
                } else {
                    Sentry.captureMessage("response at is null") { scope -> scope.setExtra("data", apiResponse.toString()) }
                }
            } else {
                SentryLog.i(
                    TAG,
                    "loadActivitiesFromFolderRec: folder ${folderProxy.id} pass deferred on an earlier page; cursor retained for fair retry"
                )
            }
            returnResponse
        }
    }

    /** Returns whether the activity was fully applied — a deferral retains the folder cursor (round 15 #10). */
    private fun FileAction.applyFileAction(
        realm: Realm,
        actionFiles: Map<Int, File>,
        returnResponse: ArrayMap<Int, FileAction>,
        currentFolder: File,
        userDrive: UserDrive,
        observedChildren: Map<Int, File>,
    ): Boolean {
        val actionFile = actionFiles[fileId]

        return when (actionType) {
            FileActivityType.FILE_DELETE,
            FileActivityType.FILE_MOVE_OUT,
            FileActivityType.FILE_TRASH -> {
                // We used to have this condition, but it doesn't exist on the ios side, according to commit it was an api fix.
                // returnResponse[fileId]?.createdAt?.time == createdAt.time
                if (returnResponse[fileId] == null) {
                    // A DEFERRED destructive action is not recorded as handled (issue: round
                    // 16 #15/#17): its unresolved work must stay visible to the retained
                    // cursor and to later duplicates.
                    val handled = removeAction(realm, currentFolder, actionFile, userDrive, observedChildren[fileId])
                    if (handled) returnResponse[fileId] = this
                    handled
                } else {
                    true
                }
            }
            else -> {
                if (actionType == null) {
                    SentryLog.e(TAG, "The action with value '$actionString' is unknown")
                }
                // The file has not yet been managed and is not the parent folder.
                if (returnResponse[fileId] == null && actionFile?.id != currentFolder.id) {
                    val handled = upsertAction(realm, currentFolder, actionFile, userDrive, observedChildren[fileId])
                    returnResponse[fileId] = this
                    handled
                } else {
                    true
                }
            }
        }
    }

    /**
     * Destructive folder-activity removal with complete request authority (issue: round 16
     * #15): the removal applies only against the FRESH live row through the complete action
     * predicate — the request observation resolves identity ties. A payload-less folder
     * activity carries NO timestamp at all ([FileAction] has none): no deletion authority can
     * be established for it, so it defers — the copy and the unresolved cursor are retained
     * for a payload-bearing retry. Returns false when deferred.
     */
    private fun FileAction.removeAction(
        realm: Realm,
        currentFolder: File,
        actionFile: File?,
        userDrive: UserDrive,
        observed: File?,
    ): Boolean {
        // [FileAction] carries no timestamp: a payload-less destructive activity has NO
        // deletion authority of its own (issue: round 16 #15) — but the SERVER has.
        // FRESH OBSERVATION IMMEDIATELY BEFORE THE PROBE (issue: round 19 #21): the
        // caller's children-map observation can be stale — or ABSENT, in which case the
        // previous null guard SKIPPED the freshness check entirely and the probe's
        // absence could erase a row restored or moved since. The observation is the
        // row's identity captured HERE, immediately before the probe, at FUNCTION scope
        // so the coordinated section below compares against it.
        val preProbeObservation = if (actionFile == null) {
            FileController.getFileById(realm, fileId)?.let { realm.copyFromRealm(it, 1) }
        } else {
            null
        }
        if (actionFile == null) {
            // RESOLVED ABSENCE IS CONSUMED (issue: round 18 #18): a later pass may already
            // have removed the row — the event has nothing left to delete and must not
            // block the folder cursor forever on an immutable event that will never grow
            // its missing payload.
            if (FileController.getFileById(realm, fileId) == null) return true
            // NEW AUTHORITATIVE RESOLVING PATH (issue: round 18 #18): the resolving probe
            // answers CURRENT (a live remote row — the event is stale or misattributed),
            // VERIFIED MISSING (the explicit OBJECT_NOT_FOUND code — the same
            // missing-object authority every other route uses: deletion authority is
            // ESTABLISHED and the coordinated section runs), or INDETERMINATE (transport
            // and other errors — defer). The previous behavior rejected every payload-less
            // destructive activity unconditionally, so a verified-absent object deferred
            // forever.
            val okHttpClient = runCatching {
                runBlocking { AccountUtils.getHttpClient(userDrive.userId) }
            }.getOrNull() ?: return false
            val probe = runCatching {
                ApiRepository.getFileDetails(
                    file = File(id = fileId, driveId = userDrive.driveId),
                    okHttpClient = okHttpClient,
                )
            }.getOrNull()
            when {
                probe == null -> return false
                probe.data != null -> {
                    // VERIFIED CURRENT OBJECT CONSUMES THE STALE DELETION (issue: round
                    // 19 #21): the object exists server-side — this deletion event is
                    // stale or misattributed and is consumed WITHOUT any delete; the
                    // retained row is the current state, and the cursor moves past an
                    // event that can never grow its payload.
                    return true
                }
                probe.error?.code == ErrorCode.OBJECT_NOT_FOUND -> Unit
                else -> return false
            }
        }
        // ONE coordinated operation (issue: round 17 #12): intent monitor → ordered locks
        // (container + file) → fresh Realm; the COMPLETE action predicate re-applies against
        // the FRESH row INSIDE the section (a restore or newer generation committed since the
        // pre-gate read defers the deletion), preservation runs before any byte or row
        // deletion, and a directory's descendant-ownership revalidates INSIDE the deleting
        // transaction.
        // The section's outcome is the function's outcome (issue: round 18 compile
        // corrections): the synchronized block's value must be RETURNED — an unreturned
        // last expression leaves the Boolean function without a return statement.
        return synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withLocksFor(userDrive, listOf(currentFolder.id, fileId)) {
                runCatching { realm.refresh() }
                val liveRow = FileController.getFileById(realm, fileId) ?: return@withLocksFor true
                if (actionFile == null) {
                    // VERIFIED-MISSING authority, freshness still required (issues: round 18
                    // #18, round 19 #21): the fresh row must still match the FRESH
                    // PRE-PROBE observation — a restore or move committed since the probe
                    // defers; the probe's server absence applies only to the row the probe
                    // was issued against. A null observation can no longer SKIP this
                    // check: the capture above guarantees one for any row that existed
                    // before the probe, and a row that vanished since defers to the
                    // section's own null-row consumption below.
                    if (preProbeObservation != null && !FileController.isSameObservation(liveRow, preProbeObservation)) {
                        return@withLocksFor false
                    }
                } else if (FileController.isStaleForAuthoritativeAction(liveRow, actionFile, null, observed)) {
                    return@withLocksFor false
                }
                val parentRow = FileController.getFileById(realm, currentFolder.id)
                if (parentRow == null) return@withLocksFor false
                when (actionType) {
                    FileActivityType.FILE_MOVE_OUT -> {
                        realm.executeTransaction { parentRow.children.removeAll { it.id == fileId } }
                        true
                    }
                    else -> {
                        val context = Realm.getApplicationContext() ?: return@withLocksFor false
                        if (liveRow.isFolder()) {
                            // Required ancestry revalidated INSIDE the deleting transaction
                            // (issue: round 17 #12): coverage or pin writers can establish
                            // required descendants up to the last moment.
                            var deletable = false
                            realm.executeTransaction {
                                deletable = !OfflineFolderController.hasLocallyOwnedDescendant(liveRow.id, realm)
                                if (deletable) liveRow.deleteFromRealm()
                            }
                            deletable
                        } else {
                            when (OfflineFolderController.preserveInheritedCopyBeforeRemoval(liveRow, userDrive, realm)) {
                                OfflineFolderController.PreservationOutcome.CONTENT_SAFE -> {
                                    runCatching { liveRow.deleteCaches(context, userDrive.userId) }
                                    realm.executeTransaction { if (liveRow.isValid) liveRow.deleteFromRealm() }
                                    true
                                }
                                // Preservation pending: row, bytes and cursor retained.
                                OfflineFolderController.PreservationOutcome.PRESERVED -> false
                            }
                        }
                    }
                }
            }
        }
    }

    private fun FileAction.upsertAction(
        realm: Realm,
        currentFolder: File,
        actionFile: File?,
        userDrive: UserDrive,
        observed: File?,
    ): Boolean {
        if (actionFile == null) {
            // SCOPED RESOLVING FETCH (issue: round 19 #21): a payload-less update/unknown
            // activity previously deferred forever on an immutable event that will never
            // grow its missing payload, blocking the folder cursor. The scoped details
            // fetch answers like the removal probe: a live object is committed from the
            // fetched AUTHORITATIVE current state (the event's intent — refresh this row —
            // is thereby satisfied) with the pre-fetch observation resolving ties, a
            // verified-missing object CONSUMES the event (nothing left to update), and an
            // indeterminate transport defers with the cursor retained.
            val preFetchObservation = FileController.getFileById(realm, fileId)?.let { realm.copyFromRealm(it, 1) }
            val okHttpClient = runCatching {
                runBlocking { AccountUtils.getHttpClient(userDrive.userId) }
            }.getOrNull() ?: return false
            val probe = runCatching {
                ApiRepository.getFileDetails(
                    file = File(id = fileId, driveId = userDrive.driveId),
                    okHttpClient = okHttpClient,
                )
            }.getOrNull()
            val details = probe?.data?.takeIf { it.id == fileId }
            return when {
                details != null -> synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    OfflineFileCoordination.withFileLock(userDrive, fileId) {
                        val committable = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                            realm = realm,
                            remoteFiles = listOf(details),
                            userDrive = userDrive,
                            observedById = fileId.let { id ->
                                preFetchObservation?.let { obs -> mapOf(id to obs) } ?: emptyMap()
                            },
                        ).first()
                        if (committable !== details) {
                            false
                        } else {
                            FileController.updateExistingFileIfNotOlder(
                                newFile = committable,
                                realm = realm,
                                userDrive = userDrive,
                                observed = preFetchObservation,
                            )
                        }
                    }
                }
                probe?.error?.code == ErrorCode.OBJECT_NOT_FOUND -> true
                else -> false
            }
        } else {
            if (actionFile.isImporting()) MqttClientWrapper.start(actionFile.externalImport?.id)
            return FileController.upsertActionFile(realm, currentFolder.id, actionFile, userDrive, observed = observed)
        }
    }

    data class FolderFilesProviderArgs(
        val folderId: FileId,
        val isFirstPage: Boolean = true,
        val order: SortType = SortType.NAME_AZ,
        val realm: Realm? = null,
        val sourceRestrictionType: SourceRestrictionType = SourceRestrictionType.UNRESTRICTED,
        val userDrive: UserDrive = UserDrive(),
        val withChildren: Boolean = true,
    )

    data class FolderFilesProviderResult(
        val folder: File,
        val folderFiles: ArrayList<File>,
        val isComplete: Boolean,
        val cursor: String? = null,
    )

    enum class SourceRestrictionType {
        UNRESTRICTED,
        ONLY_FROM_LOCAL,
        ONLY_FROM_REMOTE,
    }
}
