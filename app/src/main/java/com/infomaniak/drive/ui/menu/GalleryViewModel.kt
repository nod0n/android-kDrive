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

import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.models.File
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import kotlin.coroutines.coroutineContext

/** Result of a gallery load, posted on [GalleryViewModel.galleryApiResult]. */
sealed interface GalleryResult {
    val files: ArrayList<File>
    val isComplete: Boolean

    /**
     * Authoritative data, either freshly scanned from the API (possibly across several pages) or
     * read from the local cache. [isComplete] is false when the scan was suspended on its page
     * budget and more matching files may exist server-side.
     */
    data class Success(
        override val files: ArrayList<File>,
        override val isComplete: Boolean,
    ) : GalleryResult

    /**
     * A page failed mid-pagination, which is not an empty completion. [files] holds the matches
     * gathered before the failure; the next-page cursor is preserved so scrolling (or refreshing)
     * resumes from the failed page.
     */
    data class PageFailure(override val files: ArrayList<File>) : GalleryResult {
        override val isComplete: Boolean get() = false
    }
}

class GalleryViewModel : ViewModel() {

    val period = MutableStateFlow(GalleryPeriod.MONTH)

    /** In-memory only (like [period]): never persisted, so it cannot leak between users and always resets to All. */
    val mediaFilter = MutableStateFlow(GalleryMediaFilter.ALL)

    private var getGalleryJob: Job? = null

    private var currentCursor: String? = null

    private var lastGalleryFiles = arrayListOf<File>()

    val galleryApiResult = MutableLiveData<GalleryResult>()
    val needToRestoreFiles get() = galleryApiResult.isInitialized

    fun setMediaFilter(filter: GalleryMediaFilter) {
        mediaFilter.value = filter
    }

    fun loadLastGallery(driveId: Int, ignoreCloud: Boolean) {
        lastGalleryFiles = arrayListOf()
        currentCursor = null
        loadGallery(driveId, ignoreCloud, isFirstPage = true)
    }

    fun loadMoreGallery(driveId: Int, ignoreCloud: Boolean) {
        currentCursor?.let {
            loadGallery(driveId, ignoreCloud, isFirstPage = false, startCursor = it)
        }
    }

    fun restoreGalleryFiles() {
        if (needToRestoreFiles) {
            val isComplete = (galleryApiResult.value as? GalleryResult.Success)?.isComplete ?: false
            galleryApiResult.value = GalleryResult.Success(ArrayList(lastGalleryFiles), isComplete)
        }
    }

    private fun loadGallery(driveId: Int, ignoreCloud: Boolean, isFirstPage: Boolean, startCursor: String? = null) {
        getGalleryJob?.cancel()
        getGalleryJob = viewModelScope.launch(Dispatchers.IO) {
            val job = requireNotNull(coroutineContext[Job])
            val result = if (ignoreCloud) {
                emitCachedGallery()
            } else {
                fetchGallery(driveId, isFirstPage, startCursor) { job.ensureActive() }
            }

            galleryApiResult.postValue(result)
            lastGalleryFiles.addAll(result.files)
        }
    }

    private fun emitCachedGallery(): GalleryResult {
        currentCursor = null
        val filter = mediaFilter.value
        val cachedFiles = ArrayList(FileController.getGalleryDrive().filter { it.matchesMediaFilter(filter) })
        return GalleryResult.Success(cachedFiles, isComplete = true)
    }

    private fun fetchGallery(
        driveId: Int,
        isFirstPage: Boolean,
        startCursor: String?,
        checkActive: () -> Unit,
    ): GalleryResult {
        var isStoringFirstPage = isFirstPage
        var didFetchAnyPage = false

        val outcome = GalleryPageScanner(predicate = { it.matchesMediaFilter(mediaFilter.value) }).scan(
            startCursor = startCursor,
            onRawPage = { page ->
                didFetchAnyPage = true
                // Store every raw page unfiltered so the offline cache keeps covering the whole
                // gallery, whatever the active filter is.
                FileController.storeGalleryDrive(page, isStoringFirstPage)
                isStoringFirstPage = false
            },
            checkActive = checkActive,
        ) { cursor ->
            val apiResponse = ApiRepository.getLastGallery(driveId = driveId, cursor = cursor)
            if (apiResponse.isSuccess()) {
                GalleryPage.Success(apiResponse.data.orEmpty(), apiResponse.cursor, apiResponse.hasMore)
            } else {
                GalleryPage.Error
            }
        }

        if (isFirstPage && didFetchAnyPage) FileController.removeOrphanFiles()

        return when (outcome) {
            is GalleryScanResult.Complete -> {
                currentCursor = null
                GalleryResult.Success(ArrayList(outcome.items), isComplete = true)
            }
            is GalleryScanResult.Suspended -> {
                currentCursor = outcome.nextCursor
                GalleryResult.Success(ArrayList(outcome.items), isComplete = false)
            }
            GalleryScanResult.Unreachable -> emitCachedGallery()
            is GalleryScanResult.Interrupted -> {
                currentCursor = outcome.retryCursor
                GalleryResult.PageFailure(ArrayList(outcome.items))
            }
        }
    }

    override fun onCleared() {
        getGalleryJob?.cancel()
        super.onCleared()
    }
}
