/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2026 Infomaniak Network SA
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

import androidx.arch.core.executor.ArchTaskExecutor
import androidx.arch.core.executor.TaskExecutor
import androidx.lifecycle.Observer
import androidx.lifecycle.ViewModelProvider
import androidx.lifecycle.viewModelScope
import com.infomaniak.core.network.models.ApiResponseStatus
import com.infomaniak.drive.data.api.CursorApiResponse
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.AccountUtils
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.job
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/** Actual VM fetch/fallback/aggregation and real adapter completion; only external IO is supplied. */
@OptIn(ExperimentalCoroutinesApi::class)
class GalleryCompletionTest {
    private val dispatcher = StandardTestDispatcher()
    private val models = mutableListOf<GalleryViewModel>()
    private val observers = mutableListOf<Pair<GalleryViewModel, Observer<GalleryPage?>>>()
    private val scope = UserDrive(101, 201)

    private class Data : GalleryDataSource() {
        var scope = AccountUtils.CurrentScope(101, 201, 1)
        val requests = mutableListOf<String?>()
        val stores = mutableListOf<Pair<List<Int>, Boolean>>()
        var orphanCalls = 0
        var cacheReads = 0
        var cached = arrayListOf<File>()
        var answer: (String?) -> CursorApiResponse<ArrayList<File>> = { error("unexpected request") }
        override fun captureScope() = scope
        override fun withScope(block: () -> Unit) = block()
        override fun fetch(driveId: Int, types: List<String>, cursor: String?): CursorApiResponse<ArrayList<File>> {
            assertEquals(scope.driveId, driveId)
            requests += cursor
            return answer(cursor)
        }
        override fun cachedFiles(scope: UserDrive): ArrayList<File> {
            cacheReads++
            return ArrayList(cached)
        }
        override fun store(files: List<File>, first: Boolean, scope: UserDrive) { stores += files.map { it.id } to first }
        override fun removeOrphans(scope: UserDrive) { orphanCalls++ }
        override fun restoreOfflineState(files: List<File>, scope: UserDrive) = Unit
    }

    @BeforeEach
    fun setUp() {
        Dispatchers.setMain(dispatcher)
        ArchTaskExecutor.getInstance().setDelegate(object : TaskExecutor() {
            override fun executeOnDiskIO(runnable: Runnable) = runnable.run()
            override fun postToMainThread(runnable: Runnable) = runnable.run()
            override fun isMainThread() = true
        })
    }

    @AfterEach
    fun tearDown() {
        observers.forEach { (vm, observer) -> vm.galleryApiResult.removeObserver(observer) }
        models.forEach { it.viewModelScope.cancel() }
        dispatcher.scheduler.advanceUntilIdle()
        ArchTaskExecutor.getInstance().setDelegate(null)
        Dispatchers.resetMain()
    }

    private fun model(data: Data) = GalleryViewModel(data, dispatcher).also(models::add)
    private fun drain(vm: GalleryViewModel) {
        dispatcher.scheduler.advanceUntilIdle()
        assertFalse(vm.viewModelScope.coroutineContext.job.children.any(), "every load finished")
    }
    private fun detach(vm: GalleryViewModel) {
        observers.filter { it.first == vm }.forEach { vm.galleryApiResult.removeObserver(it.second) }
        observers.removeAll { it.first == vm }
    }
    private fun observe(vm: GalleryViewModel, adapter: GalleryAdapter) {
        val observer = Observer<GalleryPage?> { adapter.completePage(it, vm::isPageCurrent) }
        vm.galleryApiResult.observeForever(observer)
        observers += vm to observer
    }
    private fun file(id: Int, type: String = "image") = File(id = id, driveId = scope.driveId, name = "fixture-$id").apply {
        extensionType = type
    }
    private fun page(vararg files: File, cursor: String? = null, more: Boolean = false) =
        CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = arrayListOf(*files), cursor = cursor, hasMore = more)
    private fun failure() = CursorApiResponse<ArrayList<File>>(result = ApiResponseStatus.ERROR)

    @Test
    fun `failed later page preserves displayed rows and recreation without writing full cached fallback`() {
        val data = Data().apply { cached = arrayListOf(file(1), file(9)) }
        val vm = model(data)
        val adapter = galleryAdapterForTest()
        observe(vm, adapter)
        data.answer = { page(file(1), cursor = "next", more = true) }
        vm.loadLastGallery(scope.driveId, false)
        drain(vm)
        val priorRows = adapter.itemList.toList()
        val priorStores = data.stores.toList()
        data.answer = { failure() }
        adapter.showLoading()
        vm.loadMoreGallery(scope.driveId, false)
        drain(vm)
        assertEquals(priorRows, adapter.itemList, "full cache must not duplicate/reorder displayed rows or headers")
        assertEquals(listOf(1), adapter.galleryList.map { it.id })
        assertEquals(priorStores, data.stores, "failure must not rewrite membership")
        assertEquals(0, data.cacheReads, "failed pagination must not replace prior rows with a full cache")
        assertNull(vm.currentCursor)
        assertTrue(adapter.isComplete)
        detach(vm)
        vm.restoreGalleryFiles()
        val recreated = galleryAdapterForTest()
        assertTrue(recreated.completePage(vm.galleryApiResult.value, vm::isPageCurrent))
        assertEquals(priorRows, recreated.itemList)
        assertEquals(listOf(1), recreated.galleryList.map { it.id })
    }

    @Test
    fun `first page failure displays nonempty cache read only and restores it`() {
        val data = Data().apply { cached = arrayListOf(file(7)); answer = { failure() } }
        val vm = model(data)
        val adapter = galleryAdapterForTest()
        observe(vm, adapter)
        adapter.showLoading()
        vm.loadLastGallery(scope.driveId, false)
        drain(vm)
        assertEquals(listOf(7), adapter.galleryList.map { it.id })
        assertEquals(1, data.cacheReads)
        assertTrue(data.stores.isEmpty(), "reading fallback must not clear or rewrite shared membership")
        assertEquals(0, data.orphanCalls)
        assertEquals(2, adapter.itemCount)
        detach(vm)
        vm.restoreGalleryFiles()
        assertEquals(listOf(7), vm.galleryApiResult.value!!.files.map { it.id })
    }

    @Test
    fun `sparse filtered first pages advance without scroll and retain actual cursor`() {
        val data = Data()
        data.answer = { cursor -> when (cursor) {
            null -> page(cursor = "a", more = true)
            "a" -> page(file(8, "image"), cursor = "b", more = true)
            "b" -> page(file(2, "video"), cursor = "c", more = true)
            else -> error("unexpected cursor")
        } }
        val vm = model(data)
        vm.setMediaFilter(GalleryMediaFilter.VIDEOS)
        val adapter = galleryAdapterForTest()
        observe(vm, adapter)
        adapter.showLoading()
        vm.loadLastGallery(scope.driveId, false)
        drain(vm)
        assertEquals(listOf(null, "a", "b"), data.requests)
        assertEquals(listOf(2), adapter.galleryList.map { it.id })
        assertEquals("c", vm.currentCursor)
        assertFalse(adapter.isComplete)
        assertEquals(2, adapter.itemCount)
        assertTrue(data.stores.isEmpty())
    }

    @Test
    fun `empty later pages progress and append without duplicating section headers`() {
        val data = Data()
        data.answer = { cursor -> when (cursor) {
            null -> page(file(1), cursor = "a", more = true)
            "a" -> page(cursor = "b", more = true)
            "b" -> page(file(2))
            else -> error("unexpected cursor")
        } }
        val vm = model(data)
        val adapter = galleryAdapterForTest()
        observe(vm, adapter)
        vm.loadLastGallery(scope.driveId, false)
        drain(vm)
        adapter.showLoading()
        vm.loadMoreGallery(scope.driveId, false)
        drain(vm)
        assertEquals(listOf(1, 2), adapter.galleryList.map { it.id })
        assertEquals(3, adapter.itemCount, "one header, two files, no loader")
        assertEquals(listOf(listOf(1) to true, listOf(2) to false), data.stores)
        detach(vm)
        vm.restoreGalleryFiles()
        assertEquals(listOf(1, 2), vm.galleryApiResult.value!!.files.map { it.id })
    }

    @Test
    fun `repeated cursor terminates current pagination instead of requesting same page again`() {
        val data = Data().apply { answer = { page(cursor = "a", more = true) } }
        val vm = model(data)
        val adapter = galleryAdapterForTest()
        observe(vm, adapter)
        adapter.showLoading()
        vm.loadLastGallery(scope.driveId, false)
        drain(vm)
        assertEquals(listOf(null, "a"), data.requests)
        assertNull(vm.currentCursor)
        assertTrue(adapter.isComplete)
        assertEquals(0, adapter.itemCount)
        vm.loadMoreGallery(scope.driveId, false)
        drain(vm)
        assertEquals(2, data.requests.size)
    }

    @Test
    fun `missing next cursor with has more cannot leave empty UI nonterminal`() {
        val data = Data().apply { answer = { page(more = true) } }
        val vm = model(data)
        val adapter = galleryAdapterForTest()
        observe(vm, adapter)
        adapter.showLoading()
        vm.loadLastGallery(scope.driveId, false)
        drain(vm)
        assertNull(vm.currentCursor)
        assertTrue(adapter.isComplete)
        assertEquals(listOf<String?>(null), data.requests)
        assertEquals(0, adapter.itemCount)
    }

    @Test
    fun `bounded empty scan resumes valid cursor on empty view refresh instead of restarting`() {
        val data = Data()
        data.answer = { cursor ->
            val index = cursor?.removePrefix("p")?.toInt() ?: 0
            if (index == GalleryViewModel.MAX_SPARSE_PAGES) page(file(5))
            else page(cursor = "p${index + 1}", more = true)
        }
        val vm = model(data)
        var adapter = galleryAdapterForTest()
        observe(vm, adapter)
        adapter.showLoading()
        vm.loadLastGallery(scope.driveId, false)
        drain(vm)
        assertEquals(GalleryViewModel.MAX_SPARSE_PAGES, data.requests.size)
        assertEquals("p8", vm.currentCursor)
        assertFalse(adapter.isComplete, "a budget boundary is not server exhaustion")
        assertEquals(0, adapter.itemCount, "budget ends loading even while cursor remains valid")
        detach(vm)
        vm.restoreGalleryFiles()
        adapter = galleryAdapterForTest()
        observe(vm, adapter)
        assertFalse(adapter.isComplete)
        assertEquals("p8", vm.currentCursor, "recreation must retain sparse continuation")
        adapter.showLoading()
        vm.loadLastGallery(scope.driveId, false, continueEmpty = true)
        drain(vm)
        assertEquals("p8", data.requests.last())
        assertEquals(1, data.requests.count { it == null }, "empty-view refresh must not restart page one")
        assertEquals(listOf(5), adapter.galleryList.map { it.id })
        assertTrue(adapter.isComplete)
        assertNull(vm.currentCursor)
        assertEquals(2, adapter.itemCount)
        assertEquals(listOf(emptyList<Int>() to true, listOf(5) to false), data.stores)
    }

    @Test
    fun `cursor cycle across successful nonempty pages stops without losing accepted rows`() {
        val data = Data().apply { answer = { cursor -> when (cursor) {
            null -> page(file(1), cursor = "a", more = true)
            "a" -> page(file(2), cursor = "b", more = true)
            "b" -> page(file(3), cursor = "a", more = true)
            else -> error("unexpected cursor")
        } } }
        val vm = model(data)
        val adapter = galleryAdapterForTest()
        observe(vm, adapter)
        vm.loadLastGallery(scope.driveId, false)
        drain(vm)
        repeat(2) { vm.loadMoreGallery(scope.driveId, false); drain(vm) }
        assertEquals(listOf(null, "a", "b"), data.requests)
        assertEquals(listOf(1, 2, 3), adapter.galleryList.map { it.id })
        assertTrue(adapter.isComplete)
        assertNull(vm.currentCursor)
        assertEquals(4, adapter.itemCount)
    }

    @Test
    fun `blank cursor terminates sparse scan without requesting invalid token`() {
        for (cursor in listOf("", " ")) {
            val data = Data().apply { answer = { page(cursor = cursor, more = true) } }
            val vm = model(data)
            vm.loadLastGallery(scope.driveId, false)
            drain(vm)
            assertNull(vm.currentCursor)
            assertTrue(vm.galleryApiResult.value!!.isComplete)
            assertEquals(listOf<String?>(null), data.requests)
        }
    }

    @Test
    fun `filter cancellation after sparse response prevents continuation and every completion effect`() {
        val data = Data()
        val vm = model(data)
        data.answer = {
            vm.setMediaFilter(GalleryMediaFilter.VIDEOS)
            page(cursor = "stale", more = true)
        }
        val adapter = galleryAdapterForTest()
        observe(vm, adapter)
        adapter.showLoading()
        vm.loadLastGallery(scope.driveId, false)
        drain(vm)
        assertEquals(listOf<String?>(null), data.requests)
        assertFalse(vm.galleryApiResult.isInitialized)
        assertTrue(data.stores.isEmpty())
        assertEquals(0, data.cacheReads)
        assertNull(vm.currentCursor)
        assertEquals(3, adapter.itemCount, "stale completion must not hide a current loader")
    }

    @Test
    fun `account epoch change after sparse response prevents next request without relying on cancellation`() {
        val data = Data().apply { answer = {
            scope = AccountUtils.CurrentScope(102, 201, 2)
            page(cursor = "stale", more = true)
        } }
        val vm = model(data)
        vm.loadLastGallery(scope.driveId, false)
        drain(vm)
        assertEquals(listOf<String?>(null), data.requests)
        assertFalse(vm.galleryApiResult.isInitialized)
        assertTrue(data.stores.isEmpty())
        assertEquals(0, data.orphanCalls)
        assertNull(vm.currentCursor)
    }

    @Test
    fun `account change cannot reuse committed cursor on load more or empty refresh`() {
        val data = Data().apply { answer = { page(cursor = "old-account", more = true) } }
        val vm = model(data)
        vm.loadLastGallery(scope.driveId, false)
        drain(vm)
        data.scope = AccountUtils.CurrentScope(102, 201, 2)
        val count = data.requests.size
        vm.loadMoreGallery(scope.driveId, false)
        drain(vm)
        assertEquals(count, data.requests.size)
        data.answer = { cursor -> assertNull(cursor); page(file(6)) }
        vm.loadLastGallery(scope.driveId, false, continueEmpty = true)
        drain(vm)
        assertEquals(listOf(6), vm.galleryApiResult.value!!.files.map { it.id })
        assertEquals(102, vm.galleryApiResult.value!!.userDrive.userId)
    }

    @Test
    fun `later page failure after sparse hop still preserves rows and cache`() {
        val data = Data().apply {
            cached = arrayListOf(file(99))
            answer = { cursor -> when (cursor) {
                null -> page(file(1), cursor = "a", more = true)
                "a" -> page(cursor = "b", more = true)
                "b" -> failure()
                else -> error("unexpected cursor")
            } }
        }
        val vm = model(data)
        val adapter = galleryAdapterForTest()
        observe(vm, adapter)
        vm.loadLastGallery(scope.driveId, false)
        drain(vm)
        vm.loadMoreGallery(scope.driveId, false)
        drain(vm)
        assertEquals(listOf(null, "a", "b"), data.requests)
        assertEquals(listOf(1), adapter.galleryList.map { it.id })
        assertEquals(listOf(listOf(1) to true), data.stores)
        assertEquals(0, data.cacheReads)
        assertTrue(adapter.isComplete)
        assertNull(vm.currentCursor)
    }

    @Test
    fun `offline first page reads retained cache without shared writes or network`() {
        val data = Data().apply { cached = arrayListOf(file(7)) }
        val vm = model(data)
        vm.loadLastGallery(scope.driveId, true, continueEmpty = true)
        drain(vm)
        assertEquals(listOf(7), vm.galleryApiResult.value!!.files.map { it.id })
        assertEquals(1, data.cacheReads)
        assertTrue(data.requests.isEmpty())
        assertTrue(data.stores.isEmpty())
        assertEquals(0, data.orphanCalls)
        assertNull(vm.currentCursor)
    }

    @Test
    fun `normal Android ViewModel factory retains public no argument construction`() {
        val vm = ViewModelProvider.NewInstanceFactory().create(GalleryViewModel::class.java)
        models += vm
        assertFalse(vm.galleryApiResult.isInitialized)
    }
}
