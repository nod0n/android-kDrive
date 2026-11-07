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
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.IsComplete
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch

/** A gallery page stamped with the request scope it was computed for. */
data class GalleryPage(
    val files: ArrayList<File>,
    val isComplete: IsComplete,
    val generation: Int,
)

class GalleryViewModel : ViewModel() {

    val period = MutableStateFlow(GalleryPeriod.MONTH)

    /** In-memory only, like [period]: never persisted, so it cannot leak between users and always resets to All. */
    val mediaFilter = MutableStateFlow(GalleryMediaFilter.ALL)

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

    val currentLoadGeneration: Int get() = loadGeneration

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
        val userId: Int,
        val driveId: Int,
        val files: ArrayList<File>,
        val isComplete: Boolean,
        val nextCursor: String?,
        val isFirstPage: Boolean,
    )

    fun loadLastGallery(driveId: Int, ignoreCloud: Boolean) {
        lastGalleryFiles = arrayListOf()
        loadGallery(driveId, ignoreCloud, isFirstPage = true)
    }

    fun loadMoreGallery(driveId: Int, ignoreCloud: Boolean) {
        currentCursor?.let { loadGallery(driveId, ignoreCloud, isFirstPage = false, cursor = it) }
    }

    /**
     * Switching the filter drops the accumulated files and cursor (they belong to the previous
     * query) together with any in-flight load, then updates [mediaFilter]. The UI observes the
     * filter and triggers the actual reload.
     */
    fun setMediaFilter(filter: GalleryMediaFilter) {
        if (filter == mediaFilter.value) return
        loadGeneration++
        getGalleryJob?.cancel()
        currentCursor = null
        lastGalleryFiles = arrayListOf()
        mediaFilter.value = filter
    }

    fun restoreGalleryFiles() {
        if (needToRestoreFiles) {
            val isComplete = galleryApiResult.value?.isComplete ?: true
            galleryApiResult.value = GalleryPage(ArrayList(lastGalleryFiles), isComplete, loadGeneration)
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
        val userId = AccountUtils.currentUserId
        getGalleryJob?.cancel()
        getGalleryJob = viewModelScope.launch(Dispatchers.IO) {
            val load = if (ignoreCloud) {
                buildRealmLoad(generation, filter, userId, driveId)
            } else {
                fetchApiLoad(generation, filter, userId, driveId, isFirstPage, cursor)
            }
            applyLoad(load)
        }
    }

    private fun buildRealmLoad(
        generation: Int,
        filter: GalleryMediaFilter,
        userId: Int,
        driveId: Int,
    ): GalleryLoad {
        val allFiles = FileController.getGalleryDrive()
        val files = if (filter.writesSharedCache) {
            allFiles
        } else {
            ArrayList(allFiles.filter { it.matchesMediaFilter(filter) })
        }
        return GalleryLoad(generation, filter, userId, driveId, files, isComplete = true, nextCursor = null, isFirstPage = true)
    }

    private fun fetchApiLoad(
        generation: Int,
        filter: GalleryMediaFilter,
        userId: Int,
        driveId: Int,
        isFirstPage: Boolean,
        cursor: String?,
    ): GalleryLoad {
        val apiResponse = ApiRepository.getLastGallery(
            driveId = driveId,
            types = filter.apiTypes.map { it.value },
            cursor = cursor,
        )
        if (!apiResponse.isSuccess()) return buildRealmLoad(generation, filter, userId, driveId)

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
            userId = userId,
            driveId = driveId,
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

        val scopeIsCurrent = load.generation == loadGeneration
                && load.filter == mediaFilter.value
                && load.userId == AccountUtils.currentUserId
                && load.driveId == AccountUtils.currentDriveId
        if (!scopeIsCurrent) return

        currentCursor = load.nextCursor
        if (load.isFirstPage) lastGalleryFiles = arrayListOf()
        lastGalleryFiles.addAll(load.files)

        if (load.filter.writesSharedCache && load.files.isNotEmpty()) {
            FileController.storeGalleryDrive(load.files, load.isFirstPage)
            if (load.isFirstPage) FileController.removeOrphanFiles()
        }

        galleryApiResult.postValue(GalleryPage(load.files, load.isComplete, load.generation))
    }

    override fun onCleared() {
        getGalleryJob?.cancel()
        super.onCleared()
    }
}
