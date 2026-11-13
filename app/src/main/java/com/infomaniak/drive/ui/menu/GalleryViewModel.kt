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
package com.infomaniak.drive.ui.menu

import androidx.annotation.VisibleForTesting
import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.IsComplete
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.launch

/** A gallery page stamped with the request scope it was computed for. */
data class GalleryPage(
    val files: ArrayList<File>,
    val isComplete: IsComplete,
    val generation: Int,
    val userDrive: UserDrive,
    val scopeRevision: Long,
)

class GalleryViewModel : ViewModel() {

    val period = MutableStateFlow(GalleryPeriod.MONTH)

    /** In-memory only, like [period]: never persisted, so it cannot leak between users and always resets to All. */
    private val mutableMediaFilter = MutableStateFlow(GalleryMediaFilter.ALL)
    val mediaFilter = mutableMediaFilter.asStateFlow()

    private val commitLock = Any()
    private var lastScope: AccountUtils.CurrentScope? = null

    private var getGalleryJob: Job? = null

    // Internal (not private) for orchestration tests only: superseded-scope assertions must
    // observe that a dropped response left the cursor untouched. Volatile: written on the IO
    // load coroutine, read by loadMoreGallery on the caller thread.
    @Volatile
    @VisibleForTesting
    internal var currentCursor: String? = null

    private var lastGalleryFiles = arrayListOf<File>()

    /** Bumped on every request and scope change; responses of a superseded scope are dropped. */
    private var loadGeneration = 0

    val currentLoadGeneration: Int get() = synchronized(commitLock) { loadGeneration }

    val galleryApiResult = MutableLiveData<GalleryPage?>()
    val needToRestoreFiles get() = galleryApiResult.isInitialized

    /**
     * A detached load result: computed fully off the shared state and stamped with the scope of
     * its request. Nothing (cursor, cache, accumulated list, UI) is touched until [applyLoad]
     * re-validates that this scope is still the current one.
     */
    private data class GalleryLoad(
        val generation: Int,
        val filter: GalleryMediaFilter,
        val scope: AccountUtils.CurrentScope,
        val files: ArrayList<File>,
        val isComplete: Boolean,
        val nextCursor: String?,
        val isFirstPage: Boolean,
    )

    fun loadLastGallery(driveId: Int, ignoreCloud: Boolean) = synchronized(commitLock) {
        lastGalleryFiles = arrayListOf()
        loadGallery(driveId, ignoreCloud, isFirstPage = true)
    }

    fun loadMoreGallery(driveId: Int, ignoreCloud: Boolean) = synchronized(commitLock) {
        currentCursor?.let { loadGallery(driveId, ignoreCloud, isFirstPage = false, cursor = it) }
    }

    /**
     * Switching the filter drops the accumulated files and cursor (they belong to the previous
     * query) together with any in-flight load, then updates [mediaFilter]. The UI observes the
     * filter and triggers the actual reload.
     */
    fun setMediaFilter(filter: GalleryMediaFilter) = synchronized(commitLock) {
        if (filter == mediaFilter.value) return
        loadGeneration++
        getGalleryJob?.cancel()
        currentCursor = null
        lastGalleryFiles = arrayListOf()
        mutableMediaFilter.value = filter
    }

    fun restoreGalleryFiles() = synchronized(commitLock) {
        val scope = lastScope
        if (needToRestoreFiles && scope != null && scope == AccountUtils.captureCurrentScope()) {
            val isComplete = galleryApiResult.value?.isComplete ?: true
            galleryApiResult.value = GalleryPage(
                ArrayList(lastGalleryFiles), isComplete, loadGeneration,
                UserDrive(scope.userId, scope.driveId), scope.revision,
            )
        }
    }

    private fun loadGallery(
        driveId: Int,
        ignoreCloud: Boolean,
        isFirstPage: Boolean,
        cursor: String? = null,
    ) {
        val generation = ++loadGeneration
        val filter = mediaFilter.value
        val scope = AccountUtils.captureCurrentScope()
        if (scope.driveId != driveId) return
        getGalleryJob?.cancel()
        getGalleryJob = viewModelScope.launch(Dispatchers.IO) {
            val load = if (ignoreCloud) {
                buildRealmLoad(generation, filter, scope)
            } else {
                fetchApiLoad(generation, filter, scope, isFirstPage, cursor)
            }
            applyLoad(load)
        }
    }

    private fun buildRealmLoad(
        generation: Int,
        filter: GalleryMediaFilter,
        scope: AccountUtils.CurrentScope,
    ): GalleryLoad {
        val allFiles = FileController.getGalleryDrive(UserDrive(scope.userId, scope.driveId))
        val files = if (filter.writesSharedCache) {
            allFiles
        } else {
            ArrayList(allFiles.filter { it.matchesMediaFilter(filter) })
        }
        return GalleryLoad(generation, filter, scope, files, isComplete = true, nextCursor = null, isFirstPage = true)
    }

    private fun fetchApiLoad(
        generation: Int,
        filter: GalleryMediaFilter,
        scope: AccountUtils.CurrentScope,
        isFirstPage: Boolean,
        cursor: String?,
    ): GalleryLoad {
        val apiResponse = ApiRepository.getLastGallery(
            driveId = scope.driveId,
            types = filter.apiTypes.map { it.value },
            cursor = cursor,
        )
        if (!apiResponse.isSuccess()) return buildRealmLoad(generation, filter, scope)

        val data = apiResponse.data
        val files = when {
            data.isNullOrEmpty() -> arrayListOf()
            // Filtered results are matched locally too, so a response outside the requested
            // types can never be presented as filtered; only ALL may fill the shared cache.
            filter.writesSharedCache -> ArrayList(data)
            else -> ArrayList(data.filter { it.matchesMediaFilter(filter) })
        }
        return GalleryLoad(
            generation = generation,
            filter = filter,
            scope = scope,
            files = files,
            isComplete = !apiResponse.hasMore,
            nextCursor = apiResponse.cursor,
            isFirstPage = isFirstPage,
        )
    }

    /**
     * Applies a detached load only if its scope still matches the current request. Every side
     * effect (cursor, accumulated list, shared cache writes, orphan cleanup, UI event) happens
     * after — and only after — this validation.
     */
    private suspend fun applyLoad(load: GalleryLoad) {
        currentCoroutineContext().ensureActive()

        // IO only. The local cache commit and its state commit precede a coordinated
        // filter/account change, or observe it and decline. No network runs in this hold.
        synchronized(commitLock) {
            AccountUtils.withCurrentScope {
                if (load.generation != loadGeneration || load.filter != mediaFilter.value ||
                    load.scope != AccountUtils.captureCurrentScope()
                ) return@withCurrentScope
                val userDrive = UserDrive(load.scope.userId, load.scope.driveId)
                if (load.filter.writesSharedCache && load.files.isNotEmpty()) {
                    FileController.storeGalleryDrive(load.files, load.isFirstPage, userDrive)
                    if (load.isFirstPage) FileController.removeOrphanFiles(userDrive)
                }
                currentCursor = load.nextCursor
                if (load.isFirstPage) lastGalleryFiles = arrayListOf()
                lastGalleryFiles.addAll(load.files)
                lastScope = load.scope
                galleryApiResult.postValue(
                    GalleryPage(load.files, load.isComplete, load.generation, userDrive, load.scope.revision),
                )
            }
        }
    }

    fun isPageCurrent(page: GalleryPage): Boolean = synchronized(commitLock) {
        val scope = AccountUtils.captureCurrentScope()
        page.generation == loadGeneration && page.userDrive == UserDrive(scope.userId, scope.driveId) &&
            page.scopeRevision == scope.revision
    }

    override fun onCleared() {
        synchronized(commitLock) { getGalleryJob?.cancel() }
        super.onCleared()
    }
}
