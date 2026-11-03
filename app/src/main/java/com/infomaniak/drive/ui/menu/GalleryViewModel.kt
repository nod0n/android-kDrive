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
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.AccountUtils
import io.realm.Realm
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlin.coroutines.coroutineContext

/** Result of a gallery load, published on [GalleryViewModel.galleryApiResult]. */
sealed interface GalleryResult {
    val files: ArrayList<File>
    val userDrive: UserDrive
    val loadGeneration: Long
    val isComplete: Boolean

    /**
     * Authoritative data, either freshly scanned from the API (possibly across several pages) or
     * read from the local cache. [isComplete] is false when the scan paused with [nextCursor]
     * preserved and more matching files may exist server-side.
     */
    data class Success(
        override val files: ArrayList<File>,
        override val userDrive: UserDrive,
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
        override val userDrive: UserDrive,
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

    /** Identifies the current page request: results of superseded requests are dropped instead of applied. */
    val currentLoadGeneration: Long get() = loadGeneration

    /** True between a load's start and its main-thread application (blocking fetches included). */
    val isLoadRunning: Boolean get() = getGalleryJob?.isActive == true

    /** A preserved cursor exists: pagination can resume from it instead of restarting. */
    val hasResumableCursor: Boolean get() = currentCursor != null

    private var loadGeneration = 0L

    private var getGalleryJob: Job? = null

    private var currentCursor: String? = null

    private var lastGalleryFiles = arrayListOf<File>()

    val galleryApiResult = MutableLiveData<GalleryResult>()
    val needToRestoreFiles get() = galleryApiResult.isInitialized

    fun setMediaFilter(filter: GalleryMediaFilter) {
        // Selecting the already-selected filter must not invalidate anything: the equal
        // StateFlow value would not re-emit, so the invalidated in-flight load would never be
        // replaced and loading would never finish.
        if (filter == mediaFilter.value) return

        // The accumulated files and cursor belong to the previous query: drop them together
        // with its in-flight loads.
        loadGeneration++
        currentCursor = null
        lastGalleryFiles = arrayListOf()
        mediaFilter.value = filter
    }

    fun loadLastGallery(driveId: Int, ignoreCloud: Boolean) {
        lastGalleryFiles = arrayListOf()
        currentCursor = null
        loadGallery(driveId, ignoreCloud, isFirstPage = true)
    }

    fun loadMoreGallery(driveId: Int, ignoreCloud: Boolean) {
        // One active continuation at a time: a repeat trigger (scroll or empty-state button)
        // while a load is running must neither cancel it nor restart from the still-published
        // cursor, which would fetch and cache the same raw pages again.
        if (getGalleryJob?.isActive == true) return

        currentCursor?.let {
            loadGallery(driveId, ignoreCloud, isFirstPage = false, startCursor = it)
        }
    }

    fun restoreGalleryFiles() {
        if (needToRestoreFiles) {
            // Restore the exact subtype and its retry/next-cursor state. The accumulated data
            // belongs to the current gallery query (it is cleared on any query change), so it is
            // stamped with the current page-request generation: the delivery guard then accepts
            // it as long as the query is unchanged, while query or account changes still reject it.
            when (val current = galleryApiResult.value) {
                is GalleryResult.PageFailure -> galleryApiResult.value = GalleryResult.PageFailure(
                    files = ArrayList(lastGalleryFiles),
                    userDrive = current.userDrive,
                    loadGeneration = loadGeneration,
                    retryCursor = current.retryCursor,
                )
                is GalleryResult.Success -> galleryApiResult.value = GalleryResult.Success(
                    files = ArrayList(lastGalleryFiles),
                    userDrive = current.userDrive,
                    loadGeneration = loadGeneration,
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
        // Capture the exact account+drive this load belongs to: every cache operation runs
        // against this identity's own Realm and every acceptance check compares against it,
        // never against the live global account.
        val userDrive = UserDrive(userId = AccountUtils.currentUserId, driveId = driveId)
        getGalleryJob = viewModelScope.launch(Dispatchers.IO) {
            val job = requireNotNull(coroutineContext[Job])
            val realm = FileController.getRealmInstance(userDrive)
            try {
                val result = if (ignoreCloud) {
                    emitCachedGallery(userDrive, filter, generation, realm)
                } else {
                    fetchGallery(userDrive, filter, isFirstPage, startCursor, generation, { job.ensureActive() }, realm)
                }

                // Apply on Main as one atomic step: re-check the load identity there (Main is
                // where invalidation happens), then mutate the cursor and accumulator first and
                // publish synchronously last. A replacement load started meanwhile can neither
                // interleave between the check and the mutations nor observe a half-applied one.
                withContext(Dispatchers.Main) {
                    job.ensureActive()
                    val isLoadCurrent = generation == loadGeneration &&
                        userDrive.userId == AccountUtils.currentUserId &&
                        userDrive.driveId == AccountUtils.currentDriveId
                    if (isLoadCurrent) {
                        currentCursor = when (result) {
                            is GalleryResult.Success -> result.nextCursor
                            is GalleryResult.PageFailure -> result.retryCursor
                        }
                        lastGalleryFiles.addAll(result.files)
                        galleryApiResult.value = result
                    }
                }
            } finally {
                realm.close()
            }
        }
    }

    private fun emitCachedGallery(
        userDrive: UserDrive,
        filter: GalleryMediaFilter,
        generation: Long,
        realm: Realm,
    ): GalleryResult {
        val cachedFiles = ArrayList(FileController.getGalleryDrive(customRealm = realm).filter { it.matchesMediaFilter(filter) })
        return GalleryResult.Success(cachedFiles, userDrive, generation, isComplete = true)
    }

    private fun fetchGallery(
        userDrive: UserDrive,
        filter: GalleryMediaFilter,
        isFirstPage: Boolean,
        startCursor: String?,
        generation: Long,
        checkActive: () -> Unit,
        realm: Realm,
    ): GalleryResult {
        var isStoringFirstPage = isFirstPage
        var didFetchAnyPage = false

        val outcome = GalleryPageScanner<File>(predicate = { it.matchesMediaFilter(filter) }).scan(
            startCursor = startCursor,
            onRawPage = { page ->
                // Store only while this load is still the newest request for its query: a
                // superseded first page must not reset the captured account's gallery cache.
                // The Realm itself is the captured account's own, so a racing account switch
                // can never write into another account's database.
                if (generation == loadGeneration) {
                    didFetchAnyPage = true
                    // Store every raw page unfiltered so the offline cache keeps covering the
                    // whole gallery, whatever the active filter is.
                    FileController.storeGalleryDrive(page, isStoringFirstPage, customRealm = realm)
                    isStoringFirstPage = false
                }
            },
            checkActive = checkActive,
        ) { cursor ->
            val apiResponse = ApiRepository.getLastGallery(driveId = userDrive.driveId, cursor = cursor)
            if (apiResponse.isSuccess()) {
                GalleryPage.Success(apiResponse.data.orEmpty(), apiResponse.cursor, apiResponse.hasMore)
            } else {
                GalleryPage.Error
            }
        }

        if (isFirstPage && didFetchAnyPage && generation == loadGeneration) FileController.removeOrphanFiles(realm)

        return when (outcome) {
            is GalleryScanResult.Complete -> GalleryResult.Success(ArrayList(outcome.items), userDrive, generation, isComplete = true)
            is GalleryScanResult.Suspended -> GalleryResult.Success(
                files = ArrayList(outcome.items),
                userDrive = userDrive,
                loadGeneration = generation,
                isComplete = false,
                nextCursor = outcome.nextCursor,
            )
            is GalleryScanResult.Unreachable -> when {
                // Cache fallback only answers an initial load: a continuation scan that cannot
                // reach the server must keep its cursor instead of replaying the whole cache.
                isFirstPage -> emitCachedGallery(userDrive, filter, generation, realm)
                // Nothing was scanned, so there are no matches to carry: an empty failure with
                // the preserved start cursor lets the empty state offer a cursor-based retry.
                else -> GalleryResult.PageFailure(
                    files = arrayListOf(),
                    userDrive = userDrive,
                    loadGeneration = generation,
                    retryCursor = startCursor,
                )
            }
            is GalleryScanResult.Interrupted -> GalleryResult.PageFailure(
                files = ArrayList(outcome.items),
                userDrive = userDrive,
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
