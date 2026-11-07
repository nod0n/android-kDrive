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
import com.infomaniak.core.network.models.ApiResponseStatus
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.CursorApiResponse
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.utils.AccountUtils
import io.mockk.MockKAnnotations
import io.mockk.every
import io.mockk.mockkObject
import io.mockk.unmockkAll
import io.mockk.verify
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit

/**
 * Orchestration tests for the gallery filter: delayed and reordered responses through the real
 * GalleryViewModel flow, asserting that a superseded request can produce NO side effect
 * (cursor, shared cache, orphan cleanup, UI event) and that filtered queries never write the
 * shared all-media cache.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class GalleryViewModelTest {

    private val testMainDispatcher = StandardTestDispatcher()

    /** Synchronous executor: LiveData delivery on the calling thread (plain JVM has no main looper). */
    private val synchronousTaskExecutor = object : TaskExecutor() {
        override fun executeOnDiskIO(runnable: Runnable) = runnable.run()
        override fun postToMainThread(runnable: Runnable) = runnable.run()
        override fun isMainThread() = true
    }

    @BeforeEach
    fun setUp() {
        MockKAnnotations.init(this)
        Dispatchers.setMain(testMainDispatcher)
        ArchTaskExecutor.getInstance().setDelegate(synchronousTaskExecutor)

        mockkObject(AccountUtils)
        every { AccountUtils.currentUserId } returns USER_ID
        every { AccountUtils.currentDriveId } returns DRIVE_ID

        mockkObject(FileController)
        every { FileController.getGalleryDrive() } returns arrayListOf()
        every { FileController.storeGalleryDrive(any(), any()) } returns Unit
        every { FileController.removeOrphanFiles() } returns Unit

        mockkObject(ApiRepository)
    }

    @AfterEach
    fun tearDown() {
        unmockkAll()
        ArchTaskExecutor.getInstance().setDelegate(null)
        Dispatchers.resetMain()
    }

    private fun apiPage(files: List<File>, cursor: String?, hasMore: Boolean) = CursorApiResponse(
        result = ApiResponseStatus.SUCCESS,
        data = ArrayList(files),
        cursor = cursor,
        hasMore = hasMore,
    )

    private fun file(id: Int, extensionType: String) = File(id = id, driveId = DRIVE_ID).apply {
        isFromUploads = false
        this.extensionType = extensionType
    }

    private fun awaitTrue(timeoutMs: Long = 5_000L, condition: () -> Boolean): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (condition()) return true
            Thread.sleep(10)
        }
        return condition()
    }

    @Test
    fun `a superseded all-media response produces no side effect after a filter switch`() {
        val vm = GalleryViewModel()
        val blockedResponse = CountDownLatch(1)
        val stalePage = apiPage(files = listOf(file(1, ExtensionTypes.IMAGE), file(2, ExtensionTypes.VIDEO)), cursor = "stale-cursor", hasMore = false)

        every { ApiRepository.getLastGallery(driveId = DRIVE_ID, types = any(), cursor = null) } answers {
            blockedResponse.await()
            stalePage
        }

        vm.loadLastGallery(DRIVE_ID, ignoreCloud = false)

        // Switch the filter while the first response is still in flight
        vm.setMediaFilter(GalleryMediaFilter.VIDEOS)
        val freshPage = apiPage(files = listOf(file(3, ExtensionTypes.VIDEO), file(4, ExtensionTypes.IMAGE)), cursor = "fresh-cursor", hasMore = true)
        every { ApiRepository.getLastGallery(driveId = DRIVE_ID, types = any(), cursor = null) } returns freshPage
        vm.loadLastGallery(DRIVE_ID, ignoreCloud = false)

        awaitTrue { vm.galleryApiResult.value?.generation == 3 }

        // Release the stale response: it must be dropped without touching anything
        blockedResponse.countDown()
        awaitTrue { vm.currentCursor == "fresh-cursor" }

        verify(exactly = 0) { FileController.storeGalleryDrive(any(), any()) } // VIDEOS never writes; stale ALL was dropped
        verify(exactly = 0) { FileController.removeOrphanFiles() }
        assert(vm.currentCursor == "fresh-cursor")
        assert(vm.galleryApiResult.value?.files?.all { it.matchesMediaFilter(GalleryMediaFilter.VIDEOS) } == true)
    }

    @Test
    fun `a reordered response of an older request is dropped within the same filter`() {
        val vm = GalleryViewModel()
        val blockedResponse = CountDownLatch(1)
        val olderPage = apiPage(files = listOf(file(1, ExtensionTypes.IMAGE)), cursor = "older-cursor", hasMore = false)
        var callCount = 0

        every { ApiRepository.getLastGallery(driveId = DRIVE_ID, types = any(), cursor = null) } answers {
            if (callCount++ == 0) {
                blockedResponse.await()
                olderPage
            } else {
                apiPage(files = listOf(file(2, ExtensionTypes.IMAGE)), cursor = "newer-cursor", hasMore = false)
            }
        }

        vm.loadLastGallery(DRIVE_ID, ignoreCloud = false)
        vm.loadLastGallery(DRIVE_ID, ignoreCloud = false) // supersedes the first request

        awaitTrue { vm.galleryApiResult.value?.generation == 2 }
        blockedResponse.countDown()
        awaitTrue { vm.currentCursor == "newer-cursor" }

        assert(vm.currentCursor == "newer-cursor")
        assert(vm.galleryApiResult.value?.files?.singleOrNull()?.id == 2)
        verify(exactly = 1) { FileController.storeGalleryDrive(match { it.size == 1 && it.first().id == 2 }, isFirstPage = true) }
    }

    @Test
    fun `a response of a changed account scope is dropped before any side effect`() {
        val vm = GalleryViewModel()
        val blockedResponse = CountDownLatch(1)
        val staleUserPage = apiPage(files = listOf(file(1, ExtensionTypes.IMAGE)), cursor = "other-user", hasMore = false)

        every { ApiRepository.getLastGallery(driveId = DRIVE_ID, types = any(), cursor = null) } answers {
            blockedResponse.await()
            staleUserPage
        }

        vm.loadLastGallery(DRIVE_ID, ignoreCloud = false)

        // Account switches while the response is in flight
        every { AccountUtils.currentUserId } returns USER_ID + 1
        blockedResponse.countDown()

        awaitTrue { blockedResponse.count == 0L }
        Thread.sleep(200) // let the in-flight load finish and (not) apply

        assert(vm.galleryApiResult.value == null)
        assert(vm.currentCursor == null)
        verify(exactly = 0) { FileController.storeGalleryDrive(any(), any()) }
    }

    @Test
    fun `filtered responses are locally matched and never write the shared cache`() {
        val vm = GalleryViewModel()
        vm.setMediaFilter(GalleryMediaFilter.VIDEOS)

        // Server ignored types[]: response mixes images and videos
        every { ApiRepository.getLastGallery(driveId = DRIVE_ID, types = any(), cursor = null) } returns
            apiPage(files = listOf(file(1, ExtensionTypes.IMAGE), file(2, ExtensionTypes.VIDEO)), cursor = "c1", hasMore = true)

        vm.loadLastGallery(DRIVE_ID, ignoreCloud = false)

        awaitTrue { vm.galleryApiResult.value != null }

        val page = vm.galleryApiResult.value!!
        assert(page.files.map { it.id } == listOf(2)) // only videos survive the local match
        assert(page.isComplete == false)
        verify(exactly = 0) { FileController.storeGalleryDrive(any(), any()) }
        verify(exactly = 0) { FileController.removeOrphanFiles() }
    }

    @Test
    fun `an all-media first page replaces the shared cache and cleans orphans`() {
        val vm = GalleryViewModel()
        every { ApiRepository.getLastGallery(driveId = DRIVE_ID, types = any(), cursor = null) } returns
            apiPage(files = listOf(file(1, ExtensionTypes.IMAGE), file(2, ExtensionTypes.VIDEO)), cursor = null, hasMore = false)

        vm.loadLastGallery(DRIVE_ID, ignoreCloud = false)

        awaitTrue { vm.galleryApiResult.value != null }

        verify(exactly = 1) { FileController.storeGalleryDrive(match { it.size == 2 }, isFirstPage = true) }
        verify(exactly = 1) { FileController.removeOrphanFiles() }
        assert(vm.galleryApiResult.value?.isComplete == true)
    }

    private object ExtensionTypes {
        const val IMAGE = "image"
        const val VIDEO = "video"
    }

    private companion object {
        const val USER_ID = 42
        const val DRIVE_ID = 7
    }
}
