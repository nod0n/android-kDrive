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
import com.infomaniak.drive.data.api.CursorApiResponse
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.IsComplete
import kotlinx.coroutines.CoroutineDispatcher
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

class GalleryViewModel internal constructor(
    private val data: GalleryDataSource = GalleryDataSource(),
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) : ViewModel() {

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
    private val consumedCursors = mutableSetOf<String>()

    /** Bumped on every request and scope change; responses of a superseded scope are dropped. */
    private var loadGeneration = 0

    val currentLoadGeneration: Int get() = synchronized(commitLock) { loadGeneration }

    val galleryApiResult = MutableLiveData<GalleryPage?>()
    private var hasLoadedCurrentFilter = false
    val needToRestoreFiles get() = synchronized(commitLock) {
        hasLoadedCurrentFilter && galleryApiResult.isInitialized
    }
    internal val needsGalleryLoad get() = synchronized(commitLock) {
        !hasLoadedCurrentFilter && getGalleryJob?.isActive != true
    }

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
        val fromApi: Boolean = true,
        val consumedCursors: Set<String> = emptySet(),
    )

    /** Empty-view refresh continues a bounded sparse scan instead of restarting its first page. */
    fun loadLastGallery(driveId: Int, ignoreCloud: Boolean, continueEmpty: Boolean = false) = synchronized(commitLock) {
        val cursor = currentCursor
        if (continueEmpty && !ignoreCloud && lastGalleryFiles.isEmpty() && cursor != null &&
            lastScope == data.captureScope()
        ) {
            loadGallery(driveId, ignoreCloud = false, isFirstPage = false, cursor = cursor)
        } else {
            lastGalleryFiles = arrayListOf()
            consumedCursors.clear()
            currentCursor = null
            loadGallery(driveId, ignoreCloud, isFirstPage = true)
        }
    }

    fun loadMoreGallery(driveId: Int, ignoreCloud: Boolean) = synchronized(commitLock) {
        if (lastScope != data.captureScope()) return@synchronized
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
        consumedCursors.clear()
        lastGalleryFiles = arrayListOf()
        // Cleared rows are not an empty response for the new filter. A stopped/recreated
        // view must request it before it can restore a completed page.
        hasLoadedCurrentFilter = false
        mutableMediaFilter.value = filter
    }

    fun restoreGalleryFiles() = synchronized(commitLock) {
        val scope = lastScope
        if (needToRestoreFiles && scope != null && scope == data.captureScope()) {
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
        val scope = data.captureScope()
        if (scope.driveId != driveId) return
        getGalleryJob?.cancel()
        val priorCursors = consumedCursors.toSet()
        getGalleryJob = viewModelScope.launch(ioDispatcher) {
            val load = if (ignoreCloud) {
                fallbackLoad(generation, filter, scope, isFirstPage)
            } else {
                fetchApiLoad(generation, filter, scope, isFirstPage, cursor, priorCursors)
            }
            load?.let { applyLoad(it) }
        }
    }

    private fun buildRealmLoad(
        generation: Int,
        filter: GalleryMediaFilter,
        scope: AccountUtils.CurrentScope,
    ): GalleryLoad {
        val allFiles = data.cachedFiles(UserDrive(scope.userId, scope.driveId))
        val files = if (filter.writesSharedCache) {
            allFiles
        } else {
            ArrayList(allFiles.filter { it.matchesMediaFilter(filter) })
        }
        return GalleryLoad(
            generation, filter, scope, files, isComplete = true, nextCursor = null, isFirstPage = true, fromApi = false,
        )
    }

    private fun fallbackLoad(
        generation: Int,
        filter: GalleryMediaFilter,
        scope: AccountUtils.CurrentScope,
        isFirstPage: Boolean,
    ): GalleryLoad = if (isFirstPage) {
        buildRealmLoad(generation, filter, scope)
    } else {
        // A full cache is a replacement, not a next page. Preserve both displayed and
        // retained rows on failed pagination, and never rewrite cache membership on error.
        GalleryLoad(generation, filter, scope, arrayListOf(), true, null, false, fromApi = false)
    }

    private fun isRequestCurrent(generation: Int, filter: GalleryMediaFilter, scope: AccountUtils.CurrentScope) =
        synchronized(commitLock) {
            generation == loadGeneration && filter == mediaFilter.value && scope == data.captureScope()
        }

    private suspend fun fetchApiLoad(
        generation: Int,
        filter: GalleryMediaFilter,
        scope: AccountUtils.CurrentScope,
        isFirstPage: Boolean,
        cursor: String?,
        priorCursors: Set<String>,
    ): GalleryLoad? {
        var requestCursor = cursor
        val consumed = priorCursors.toMutableSet()
        // Bounded per user load: no recursive requests or unbounded scan behind an empty UI.
        repeat(MAX_SPARSE_PAGES) { index ->
            currentCoroutineContext().ensureActive()
            if (!isRequestCurrent(generation, filter, scope)) return null
            requestCursor?.let(consumed::add)
            val response = data.fetch(scope.driveId, filter.apiTypes.map { it.value }, requestCursor)
            currentCoroutineContext().ensureActive()
            if (!isRequestCurrent(generation, filter, scope)) return null
            if (!response.isSuccess()) return fallbackLoad(generation, filter, scope, isFirstPage)

            val files = ArrayList(response.data.orEmpty().filter { it.matchesMediaFilter(filter) })
            // hasMore without a new usable cursor cannot progress. This also detects cycles
            // across separate loads, without ever requesting a previously consumed cursor.
            val next = response.cursor?.takeIf { response.hasMore && it.isNotBlank() && it !in consumed }
            val load = GalleryLoad(
                generation, filter, scope, files, isComplete = next == null, nextCursor = next,
                isFirstPage = isFirstPage, consumedCursors = consumed.toSet(),
            )
            if (files.isNotEmpty() || next == null || index == MAX_SPARSE_PAGES - 1) return load
            requestCursor = next
        }
        error("bounded Gallery scan must return")
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
            data.withScope {
                if (load.generation != loadGeneration || load.filter != mediaFilter.value ||
                    load.scope != data.captureScope()
                ) return@withScope
                val userDrive = UserDrive(load.scope.userId, load.scope.driveId)
                if (load.fromApi && load.filter.writesSharedCache && (load.isFirstPage || load.files.isNotEmpty())) {
                    data.store(load.files, load.isFirstPage, userDrive)
                    // An empty refresh only withdraws Gallery membership. Running the
                    // general orphan sweep here could erase unrelated offline root rows.
                    if (load.isFirstPage && load.files.isNotEmpty()) data.removeOrphans(userDrive)
                } else if (!load.filter.writesSharedCache) {
                    // Preserve offline presentation for detached filtered rows without
                    // replacing Gallery membership or writing any Realm row.
                    data.restoreOfflineState(load.files, userDrive)
                }
                currentCursor = load.nextCursor
                if (load.isFirstPage) {
                    lastGalleryFiles = arrayListOf()
                    consumedCursors.clear()
                }
                consumedCursors.addAll(load.consumedCursors)
                lastGalleryFiles.addAll(load.files)
                lastScope = load.scope
                hasLoadedCurrentFilter = true
                galleryApiResult.postValue(
                    GalleryPage(load.files, load.isComplete, load.generation, userDrive, load.scope.revision),
                )
            }
        }
    }

    fun isPageCurrent(page: GalleryPage): Boolean = synchronized(commitLock) {
        val scope = data.captureScope()
        page.generation == loadGeneration && page.userDrive == UserDrive(scope.userId, scope.driveId) &&
            page.scopeRevision == scope.revision
    }

    internal companion object {
        const val MAX_SPARSE_PAGES = 8
    }

    override fun onCleared() {
        synchronized(commitLock) { getGalleryJob?.cancel() }
        super.onCleared()
    }
}

/** Gallery IO boundaries only; pagination and completion remain in the ViewModel. */
internal open class GalleryDataSource {
    open fun captureScope() = AccountUtils.captureCurrentScope()
    open fun withScope(block: () -> Unit) = AccountUtils.withCurrentScope(block)
    open fun fetch(driveId: Int, types: List<String>, cursor: String?): CursorApiResponse<ArrayList<File>> =
        ApiRepository.getLastGallery(driveId, types, cursor)
    open fun cachedFiles(scope: UserDrive) = FileController.getGalleryDrive(scope)
    open fun store(files: List<File>, first: Boolean, scope: UserDrive) = FileController.storeGalleryDrive(files, first, scope)
    open fun removeOrphans(scope: UserDrive) = FileController.removeOrphanFiles(scope)
    open fun restoreOfflineState(files: List<File>, scope: UserDrive) = FileController.restoreGalleryOfflineState(files, scope)
}
