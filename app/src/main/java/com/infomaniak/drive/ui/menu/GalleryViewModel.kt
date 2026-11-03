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
import com.infomaniak.drive.utils.AccountUtils
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import kotlin.coroutines.coroutineContext

/** Result of a gallery load, posted on [GalleryViewModel.galleryApiResult]. */
sealed interface GalleryResult {
    val files: ArrayList<File>
    val driveId: Int
    val loadGeneration: Long
    val isComplete: Boolean

    /**
     * Authoritative data, either freshly scanned from the API (possibly across several pages) or
     * read from the local cache. [isComplete] is false when the scan paused with [nextCursor]
     * preserved and more matching files may exist server-side.
     */
    data class Success(
        override val files: ArrayList<File>,
        override val driveId: Int,
        override val loadGeneration: Long,
        override val isComplete: Boolean,
        val nextCursor: String? = null,
    ) : GalleryResult

    /**
     * A page failed mid-pagination, which is not an empty completion. [files] holds the matches
     * gathered before the failure; [retryCursor] is preserved (the cursor the failed page was
     * attempted with) so scrolling (or retrying) resumes from exactly that page.
     */
    data class PageFailure(
        override val files: ArrayList<File>,
        override val driveId: Int,
        override val loadGeneration: Long,
        val retryCursor: String?,
    ) : GalleryResult {
        override val isComplete: Boolean get() = false
    }
}

class GalleryViewModel : ViewModel() {

    val period = MutableStateFlow(GalleryPeriod.MONTH)

    /** In-memory only (like [period]): never persisted, so it cannot leak between users and always resets to All. */
    val mediaFilter = MutableStateFlow(GalleryMediaFilter.ALL)

    /** Identifies the current load: results of superseded loads are dropped instead of applied. */
    val currentLoadGeneration: Long get() = loadGeneration

    private var loadGeneration = 0L

    private var getGalleryJob: Job? = null

    private var currentCursor: String? = null

    private var lastGalleryFiles = arrayListOf<File>()

    val galleryApiResult = MutableLiveData<GalleryResult>()
    val needToRestoreFiles get() = galleryApiResult.isInitialized

    fun setMediaFilter(filter: GalleryMediaFilter) {
        // Invalidate any in-flight load produced with the previous filter: its queued results
        // must not apply on top of the new filter's list.
        loadGeneration++
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
            // Restore the exact subtype: a mid-pagination failure must stay a failure (with its
            // retry cursor), not be coerced into a successful completion.
            when (val current = galleryApiResult.value) {
                is GalleryResult.PageFailure -> galleryApiResult.value = GalleryResult.PageFailure(
                    files = ArrayList(lastGalleryFiles),
                    driveId = current.driveId,
                    loadGeneration = current.loadGeneration,
                    retryCursor = current.retryCursor,
                )
                is GalleryResult.Success -> galleryApiResult.value = GalleryResult.Success(
                    files = ArrayList(lastGalleryFiles),
                    driveId = current.driveId,
                    loadGeneration = current.loadGeneration,
                    isComplete = current.isComplete,
                    nextCursor = current.nextCursor,
                )
                null -> Unit
            }
        }
    }

    private fun loadGallery(driveId: Int, ignoreCloud: Boolean, isFirstPage: Boolean, startCursor: String? = null) {
        getGalleryJob?.cancel()
        val generation = ++loadGeneration
        val filter = mediaFilter.value
        getGalleryJob = viewModelScope.launch(Dispatchers.IO) {
            val job = requireNotNull(coroutineContext[Job])
            val isLoadCurrent = {
                generation == loadGeneration && driveId == AccountUtils.currentDriveId && !job.isCancelled
            }
            val result = if (ignoreCloud) {
                emitCachedGallery(driveId, filter, generation)
            } else {
                fetchGallery(driveId, filter, isFirstPage, startCursor, generation, { job.ensureActive() }, isLoadCurrent)
            }

            // The fetch above was blocking: a cancelled (superseded or cleared) load stops here,
            // before publishing anything or mutating any state.
            job.ensureActive()
            if (!isLoadCurrent()) return@launch

            galleryApiResult.postValue(result)
            currentCursor = when (result) {
                is GalleryResult.Success -> result.nextCursor
                is GalleryResult.PageFailure -> result.retryCursor
            }
            lastGalleryFiles.addAll(result.files)
        }
    }

    private fun emitCachedGallery(driveId: Int, filter: GalleryMediaFilter, generation: Long): GalleryResult {
        val cachedFiles = ArrayList(FileController.getGalleryDrive().filter { it.matchesMediaFilter(filter) })
        return GalleryResult.Success(cachedFiles, driveId, generation, isComplete = true)
    }

    private fun fetchGallery(
        driveId: Int,
        filter: GalleryMediaFilter,
        isFirstPage: Boolean,
        startCursor: String?,
        generation: Long,
        checkActive: () -> Unit,
        isLoadCurrent: () -> Boolean,
    ): GalleryResult {
        var isStoringFirstPage = isFirstPage
        var didFetchAnyPage = false

        val outcome = GalleryPageScanner<File>(predicate = { it.matchesMediaFilter(filter) }).scan(
            startCursor = startCursor,
            onRawPage = { page ->
                // Cache writes happen only while this load is still the current one: a
                // superseded scan (filter or drive/account changed meanwhile) must never write
                // the old response into the now-current account's Realm.
                if (isLoadCurrent()) {
                    didFetchAnyPage = true
                    // Store every raw page unfiltered so the offline cache keeps covering the
                    // whole gallery, whatever the active filter is.
                    FileController.storeGalleryDrive(page, isStoringFirstPage)
                    isStoringFirstPage = false
                }
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

        if (isFirstPage && didFetchAnyPage && isLoadCurrent()) FileController.removeOrphanFiles()

        return when (outcome) {
            is GalleryScanResult.Complete -> GalleryResult.Success(ArrayList(outcome.items), driveId, generation, isComplete = true)
            is GalleryScanResult.Suspended -> GalleryResult.Success(
                files = ArrayList(outcome.items),
                driveId = driveId,
                loadGeneration = generation,
                isComplete = false,
                nextCursor = outcome.nextCursor,
            )
            is GalleryScanResult.Unreachable -> when {
                // Cache fallback only answers an initial load: a continuation scan that cannot
                // reach the server must keep its cursor instead of replaying the whole cache.
                isFirstPage -> emitCachedGallery(driveId, filter, generation)
                // Nothing was scanned, so there are no matches to carry: an empty failure with
                // the preserved start cursor lets the empty state offer a cursor-based retry.
                else -> GalleryResult.PageFailure(
                    files = arrayListOf(),
                    driveId = driveId,
                    loadGeneration = generation,
                    retryCursor = startCursor,
                )
            }
            is GalleryScanResult.Interrupted -> GalleryResult.PageFailure(
                files = ArrayList(outcome.items),
                driveId = driveId,
                loadGeneration = generation,
                retryCursor = outcome.retryCursor,
            )
        }
    }

    override fun onCleared() {
        getGalleryJob?.cancel()
        super.onCleared()
    }
}
