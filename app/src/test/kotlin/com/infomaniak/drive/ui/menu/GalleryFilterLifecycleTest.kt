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
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.LifecycleRegistry
import androidx.lifecycle.Observer
import androidx.lifecycle.viewModelScope
import com.infomaniak.core.network.models.ApiResponseStatus
import com.infomaniak.drive.data.api.CursorApiResponse
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.AccountUtils
import io.mockk.every
import io.mockk.spyk
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.cancel
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/** Real fragment lifecycle collector and load gate, with actual VM/adapter and deterministic IO. */
@OptIn(ExperimentalCoroutinesApi::class)
class GalleryFilterLifecycleTest {
    private val dispatcher = StandardTestDispatcher()
    private val data = Data()
    private lateinit var vm: GalleryViewModel
    private val owner = object : LifecycleOwner {
        override val lifecycle = LifecycleRegistry.createUnsafe(this)
    }
    private val png = File(id = 7, driveId = 201, name = "known.png").apply { extensionType = "image" }

    private class Data : GalleryDataSource() {
        val requests = mutableListOf<List<String>>()
        var files = arrayListOf<File>()
        override fun captureScope() = AccountUtils.CurrentScope(101, 201, 1)
        override fun withScope(block: () -> Unit) = block()
        override fun fetch(driveId: Int, types: List<String>, cursor: String?): CursorApiResponse<ArrayList<File>> {
            requests += types
            return CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = ArrayList(files), hasMore = false)
        }
        override fun store(files: List<File>, first: Boolean, scope: UserDrive) = Unit
        override fun removeOrphans(scope: UserDrive) = Unit
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
        vm = GalleryViewModel(data, dispatcher)
        data.files += png
        owner.lifecycle.currentState = Lifecycle.State.CREATED
    }

    @AfterEach
    fun tearDown() {
        owner.lifecycle.currentState = Lifecycle.State.DESTROYED
        vm.viewModelScope.cancel()
        dispatcher.scheduler.advanceUntilIdle()
        ArchTaskExecutor.getInstance().setDelegate(null)
        Dispatchers.resetMain()
    }

    private fun fragment(adapter: GalleryAdapter): GalleryFragment {
        val fragment = spyk(GalleryFragment(), recordPrivateCalls = true)
        every { fragment.viewLifecycleOwner } returns owner
        GalleryFragment::class.java.getDeclaredField("galleryViewModel\$delegate").apply { isAccessible = true }
            .set(fragment, lazy { vm })
        // Supply only the fragment's Android UI/account boundary. Its actual lifecycle collector,
        // real ViewModel request/matching/publication and real adapter completion all execute.
        every { fragment["onMediaFilterChanged"]() } answers {
            adapter.clearGallery()
            vm.loadLastGallery(201, false)
        }
        return fragment
    }

    private fun observeFilter(fragment: GalleryFragment) {
        GalleryFragment::class.java.getDeclaredMethod("observeMediaFilter").apply { isAccessible = true }.invoke(fragment)
    }

    @Test
    fun `filter result delivered while stopped reloads the known PNG on restart`() {
        val adapter = galleryAdapterForTest()
        val observer = Observer<GalleryPage?> { adapter.completePage(it, vm::isPageCurrent) }
        vm.galleryApiResult.observe(owner, observer)
        vm.loadLastGallery(201, false)
        val fragment = fragment(adapter)
        observeFilter(fragment)
        owner.lifecycle.currentState = Lifecycle.State.RESUMED
        dispatcher.scheduler.advanceUntilIdle()
        assertEquals(listOf(7), adapter.galleryList.map { it.id })
        owner.lifecycle.currentState = Lifecycle.State.CREATED
        dispatcher.scheduler.advanceUntilIdle()
        vm.setMediaFilter(GalleryMediaFilter.IMAGES)
        dispatcher.scheduler.advanceUntilIdle()
        assertEquals(1, data.requests.size, "no work from the stopped view")
        owner.lifecycle.currentState = Lifecycle.State.RESUMED
        dispatcher.scheduler.advanceUntilIdle()
        assertEquals(listOf(listOf("image", "video"), listOf("image")), data.requests)
        assertEquals(listOf(7), adapter.galleryList.map { it.id })
        assertTrue(adapter.isComplete)
        vm.galleryApiResult.removeObserver(observer)
    }

    @Test
    fun `filter switch cannot restore cleared rows as a successful empty Images result`() {
        vm.loadLastGallery(201, false)
        dispatcher.scheduler.advanceUntilIdle()
        val allPage = vm.galleryApiResult.value!!
        assertEquals(listOf(7), allPage.files.map { it.id })
        vm.setMediaFilter(GalleryMediaFilter.IMAGES)
        vm.restoreGalleryFiles()
        assertFalse(vm.needToRestoreFiles, "old All completion is not a loaded Images result")
        assertFalse(vm.isPageCurrent(vm.galleryApiResult.value!!), "restoration must not bless empty cleared state")
        assertEquals(1, data.requests.size, "the empty state was not a server response")
    }

    @Test
    fun `returning to All while stopped still reloads invalidated rows`() {
        val adapter = galleryAdapterForTest()
        val observer = Observer<GalleryPage?> { adapter.completePage(it, vm::isPageCurrent) }
        vm.galleryApiResult.observe(owner, observer)
        vm.loadLastGallery(201, false)
        observeFilter(fragment(adapter))
        owner.lifecycle.currentState = Lifecycle.State.RESUMED
        dispatcher.scheduler.advanceUntilIdle()
        owner.lifecycle.currentState = Lifecycle.State.CREATED
        dispatcher.scheduler.advanceUntilIdle()
        vm.setMediaFilter(GalleryMediaFilter.IMAGES)
        vm.setMediaFilter(GalleryMediaFilter.ALL)
        data.files = arrayListOf(File(id = 8, driveId = 201).apply { extensionType = "image" })
        owner.lifecycle.currentState = Lifecycle.State.RESUMED
        dispatcher.scheduler.advanceUntilIdle()
        assertEquals(2, data.requests.size, "same enum value does not mean its cleared rows are loaded")
        assertEquals(listOf(8), adapter.galleryList.map { it.id })
        vm.galleryApiResult.removeObserver(observer)
    }

    @Test
    fun `unchanged filter stop and restart retains rows without another request`() {
        val adapter = galleryAdapterForTest()
        val observer = Observer<GalleryPage?> {
            adapter.completePage(it, vm::isPageCurrent, appendFiles = adapter.galleryList.isEmpty())
        }
        vm.galleryApiResult.observe(owner, observer)
        vm.loadLastGallery(201, false)
        observeFilter(fragment(adapter))
        owner.lifecycle.currentState = Lifecycle.State.RESUMED
        dispatcher.scheduler.advanceUntilIdle()
        val rows = adapter.itemList.toList()
        owner.lifecycle.currentState = Lifecycle.State.CREATED
        dispatcher.scheduler.advanceUntilIdle()
        owner.lifecycle.currentState = Lifecycle.State.RESUMED
        dispatcher.scheduler.advanceUntilIdle()
        assertEquals(1, data.requests.size)
        assertEquals(rows, adapter.itemList)
        assertEquals(listOf(7), adapter.galleryList.map { it.id })
        vm.galleryApiResult.removeObserver(observer)
    }

    private fun initializeView(fragment: GalleryFragment) {
        GalleryFragment::class.java.getDeclaredMethod("loadGalleryIfNeeded").apply { isAccessible = true }.invoke(fragment)
    }

    @Test
    fun `recreated view loads Images instead of blessing cleared retained rows`() {
        vm.loadLastGallery(201, false)
        dispatcher.scheduler.advanceUntilIdle()
        vm.setMediaFilter(GalleryMediaFilter.IMAGES)
        vm.restoreGalleryFiles()
        val recreated = galleryAdapterForTest()
        val observer = Observer<GalleryPage?> { recreated.completePage(it, vm::isPageCurrent) }
        vm.galleryApiResult.observe(owner, observer)
        val fragment = fragment(recreated)
        initializeView(fragment)
        observeFilter(fragment)
        owner.lifecycle.currentState = Lifecycle.State.RESUMED
        dispatcher.scheduler.advanceUntilIdle()
        assertEquals(listOf(listOf("image", "video"), listOf("image")), data.requests)
        assertEquals(listOf(7), recreated.galleryList.map { it.id })
        assertEquals(2, recreated.itemCount, "one section and one PNG, no stale rows or loader")
        assertTrue(vm.needToRestoreFiles)
        assertFalse(vm.needsGalleryLoad)
        vm.galleryApiResult.removeObserver(observer)
    }

    @Test
    fun `view initialization and initial lifecycle replay share an active request`() {
        val adapter = galleryAdapterForTest()
        val observer = Observer<GalleryPage?> { adapter.completePage(it, vm::isPageCurrent) }
        vm.galleryApiResult.observe(owner, observer)
        val fragment = fragment(adapter)
        initializeView(fragment)
        val generation = vm.currentLoadGeneration
        assertFalse(vm.needsGalleryLoad, "queued request is active before its IO executes")
        initializeView(fragment)
        observeFilter(fragment)
        owner.lifecycle.currentState = Lifecycle.State.RESUMED
        dispatcher.scheduler.advanceUntilIdle()
        assertEquals(1, data.requests.size)
        assertEquals(generation, vm.currentLoadGeneration, "initial replay must not cancel and replace the request")
        assertEquals(listOf(7), adapter.galleryList.map { it.id })
        vm.galleryApiResult.removeObserver(observer)
    }

    @Test
    fun `genuine empty Images response is retained without repeated reload or cached All rows`() {
        vm.loadLastGallery(201, false)
        dispatcher.scheduler.advanceUntilIdle()
        vm.setMediaFilter(GalleryMediaFilter.IMAGES)
        data.files.clear()
        val adapter = galleryAdapterForTest()
        val observer = Observer<GalleryPage?> { adapter.completePage(it, vm::isPageCurrent) }
        vm.galleryApiResult.observe(owner, observer)
        val fragment = fragment(adapter)
        initializeView(fragment)
        observeFilter(fragment)
        owner.lifecycle.currentState = Lifecycle.State.RESUMED
        dispatcher.scheduler.advanceUntilIdle()
        assertTrue(vm.needToRestoreFiles, "an actual empty completion differs from invalidated All rows")
        assertFalse(vm.needsGalleryLoad)
        assertTrue(adapter.galleryList.isEmpty())
        assertTrue(adapter.isComplete)
        assertEquals(0, adapter.itemCount)
        owner.lifecycle.currentState = Lifecycle.State.CREATED
        dispatcher.scheduler.advanceUntilIdle()
        owner.lifecycle.currentState = Lifecycle.State.RESUMED
        dispatcher.scheduler.advanceUntilIdle()
        initializeView(fragment)
        vm.restoreGalleryFiles()
        assertEquals(2, data.requests.size)
        assertTrue(vm.galleryApiResult.value!!.files.isEmpty())
        assertTrue(vm.isPageCurrent(vm.galleryApiResult.value!!))
        vm.galleryApiResult.removeObserver(observer)
    }
}
