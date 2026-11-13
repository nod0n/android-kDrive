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

import android.util.Log
import android.content.Context
import androidx.test.core.app.ApplicationProvider
import androidx.arch.core.executor.ArchTaskExecutor
import androidx.arch.core.executor.TaskExecutor
import androidx.lifecycle.viewModelScope
import com.infomaniak.core.network.models.ApiResponseStatus
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.CursorApiResponse
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.cache.FileMigration
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.RealmModules
import io.realm.Realm
import io.realm.RealmConfiguration
import io.mockk.MockKAnnotations
import io.mockk.every
import io.mockk.mockkObject
import io.mockk.unmockkAll
import io.mockk.verify
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.Job
import kotlinx.coroutines.TimeoutCancellationException
import kotlinx.coroutines.job
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import kotlinx.coroutines.withTimeout
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.BeforeTestExecutionCallback
import org.junit.jupiter.api.extension.ExtensionContext
import org.junit.jupiter.api.extension.RegisterExtension
import org.junit.jupiter.api.extension.TestWatcher
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
import java.nio.file.Files

/**
 * Orchestration tests for the gallery filter: delayed and reordered responses through the real
 * GalleryViewModel flow, asserting that a superseded request can produce NO side effect
 * (cursor, shared cache, orphan cleanup, UI event) and that filtered queries never write the
 * shared all-media cache.
 *
 * Deterministic discipline (RESUME-2d4d-1942): every wait is a bounded, ASSERTED latch or
 * completion-poll — a timeout fails the case, it is never ignored, and a harness-side latch
 * timeout is RECORDED and asserted not to have fired AFTER the loads have actually finished.
 * An old request's arrival at the network seam is proven by an ENTRY latch asserted BEFORE the
 * supersession. The completion proof is a JOIN, not a value check: each case captures the
 * parked request's actual Job from the ViewModel scope while its response is parked, releases
 * the response, JOINS that Job to completion, and only then asserts side effects — a broken
 * ensureActive/scope guard would have applied its stale data before the join returned, so the
 * zero-effect claims underneath are sensitive to the guards' removal. A drop branch's REACH
 * is proven at a production boundary the branch itself must cross (case 3: the scope check's
 * account read, held and released by the test), never by a timer and never by the absence of
 * an assertion failure; its DECISION is the guard's own — superseded-by-cancellation cases
 * (1, 2) claim only what the guards make true, never the scope-mismatch branch. Cleanup
 * cancellation is separate and never the mechanism a claim rests on: every parked latch is
 * hoisted and released in finally BEFORE the bounded cancel+join, and the last assertions run
 * after the released loads have actually finished.
 *
 * Evidence level: JVM orchestration guarantees only. The class has never yet completed a
 * case on device: the 9118 (240 s), 8ad8 (90 s) and 6f85 (90 s) runs all timed out in setUp,
 * and DEVICE-6f85-RESULT-2045 localized the hang to the collection-returning getGalleryDrive
 * stub recording with the MockK core-collection recursion captured — the corrected harness
 * (that stub removed; the tested flows never reach it) is COMPILED, NOT EXECUTED until a
 * device run records one. ActivityResult/lifecycle wiring is out of scope here.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class GalleryViewModelTest {

    /**
     * Bounded phase diagnostics: setUp/tearDown and test ENTER/EXIT marks via println +
     * Log.i, so a device hang is localized by the last emitted mark without any payload.
     * These markers carry no account, token or photo payloads.
     *
     * Device evidence (DEVICE-6f85-RESULT-2045): the 9118/8ad8/6f85 runs all timed out in
     * setUp. The 6f85 run localized the hang to the `every { FileController.getGalleryDrive() }
     * returns arrayListOf()` stub recording and captured the mechanism: MockK's Android advice
     * machinery recursed (AndroidMockKDispatcher.getOrigin -> ArrayList.add -> ArraysKt.zip ->
     * Advice.isOverridden) — a child-mock instrumentation of the FINAL core collection return
     * whose own comparison path then dispatches back through the instrumented collection.
     * The correction removes that stub (the tested flows never reach getGalleryDrive) and the
     * now-understood diagnostic noise; the class has never yet completed a case on device, so
     * the phase marks stay.
     */
    @RegisterExtension
    @JvmField
    val phaseLogger: PhaseLogger = PhaseLogger()

    private fun mark(message: String) {
        println("GalleryVMTest: $message")
        Log.i("GalleryVMTest", "GalleryVMTest: $message")
    }

    class PhaseLogger : BeforeTestExecutionCallback, TestWatcher {
        private fun mark(message: String, displayName: String) {
            val line = "GalleryVMTest: $message $displayName"
            println(line)
            Log.i("GalleryVMTest", line)
        }

        override fun beforeTestExecution(context: ExtensionContext) = mark("ENTER", context.displayName)
        override fun testSuccessful(context: ExtensionContext) = mark("EXIT-OK", context.displayName)
        override fun testFailed(context: ExtensionContext, cause: Throwable?) = mark("EXIT-FAIL", context.displayName)
    }

    private val testMainDispatcher = StandardTestDispatcher()

    /** Synchronous executor: LiveData delivery on the calling thread (plain JVM has no main looper). */
    private val synchronousTaskExecutor = object : TaskExecutor() {
        override fun executeOnDiskIO(runnable: Runnable) = runnable.run()
        override fun postToMainThread(runnable: Runnable) = runnable.run()
        override fun isMainThread() = true
    }

    @BeforeEach
    fun setUp() {
        mark("setUp begin")
        MockKAnnotations.init(this)
        Dispatchers.setMain(testMainDispatcher)
        ArchTaskExecutor.getInstance().setDelegate(synchronousTaskExecutor)

        mockkObject(AccountUtils)
        every { AccountUtils.currentUserId } returns USER_ID
        every { AccountUtils.currentDriveId } returns DRIVE_ID

        mockkObject(FileController)
        // DEVICE-6f85 EVIDENCE: recording `every { FileController.getGalleryDrive() } returns
        // arrayListOf()` hung the class on device — MockK's advice machinery recursed through
        // AndroidMockKDispatcher.getOrigin -> ArrayList.add -> ArraysKt.zip -> Advice.isOverridden
        // (a child-mock instrumentation of the FINAL core collection return) while comparing
        // methods across threads. The tested flows NEVER reach this method: ignoreCloud=false
        // with SUCCESS responses never falls back to buildRealmLoad — so the stub is simply not
        // registered, and the unstubbed fall-through is never exercised. A future cloud-failure
        // case must bring the Offline suite's synthetic-realm setup instead of this stub. The
        // Unit-returning stubs below stay: no concrete collection return is recorded, which the
        // Consent/Upload device runs show to be safe on object mocks.
        every { FileController.storeGalleryDrive(any(), any(), any<UserDrive>()) } returns Unit
        every { FileController.removeOrphanFiles(any<UserDrive>()) } returns Unit

        mockkObject(ApiRepository)
        mark("setUp done")
    }

    @AfterEach
    fun tearDown() {
        mark("tearDown begin")
        unmockkAll()
        ArchTaskExecutor.getInstance().setDelegate(null)
        Dispatchers.resetMain()
        mark("tearDown done")
    }

    /**
     * Cancels and joins (bounded) every load coroutine the case left in the ViewModel's
     * scope, BEFORE [tearDown] unmocks: no late apply/drop can run against restored
     * production implementations. Runs from each case's finally, after its parked latches
     * were released.
     */
    private fun cancelAndJoinLoads(vm: GalleryViewModel) {
        val scopeJob = vm.viewModelScope.coroutineContext.job
        val children = scopeJob.children.toList()
        children.forEach { it.cancel() }
        runBlocking {
            withTimeout(5_000) {
                children.forEach { it.join() }
            }
        }
    }

    /**
     * The load coroutines currently live in the ViewModel's scope: [GalleryViewModel] launches
     * every load as a child of viewModelScope (the launch attaches its child synchronously), so
     * a capture taken while a response is parked in the network seam IS that request's Job.
     */
    private fun capturedLoadJobs(vm: GalleryViewModel): List<Job> =
        vm.viewModelScope.coroutineContext.job.children.toList()

    /**
     * Joins released load Jobs to COMPLETION before the case asserts side effects: after the
     * parked response is released, a superseded coroutine either drops by its guards or applies
     * its stale data — and only a completed Job makes that outcome final and observable. Fails
     * instead of hanging.
     */
    private fun joinReleasedLoads(label: String, jobs: List<Job>) {
        assertTrue(jobs.isNotEmpty(), "$label: no load Job was captured — the completion proof would be vacuous")
        try {
            runBlocking {
                withTimeout(5_000) {
                    jobs.forEach { it.join() }
                }
            }
        } catch (timeout: TimeoutCancellationException) {
            throw AssertionError("$label: a released load did not finish within 5s — a parked thread never returned", timeout)
        }
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

    /**
     * Bounded completion-poll for an IO-thread coroutine's observable outcome (the ViewModel
     * runs its loads on real Dispatchers.IO threads — there is no schedulable seam to latch
     * every internal hop). Every call site ASSERTS the result: a timeout is a failure, never
     * an ignored false.
     */
    private fun awaitTrue(timeoutMs: Long = 5_000L, condition: () -> Boolean): Boolean {
        val deadline = System.currentTimeMillis() + timeoutMs
        while (System.currentTimeMillis() < deadline) {
            if (condition()) return true
            Thread.sleep(10)
        }
        return condition()
    }

    @Test
    @DisplayName("a superseded all-media response produces no side effect after a filter switch")
    fun supersededAllMediaResponseProducesNoSideEffectAfterFilterSwitch() {
        val vm = GalleryViewModel()
        val oldRequestEntered = CountDownLatch(1)
        val blockedResponse = CountDownLatch(1)
        val answerTimedOut = AtomicBoolean(false)
        val stalePage = apiPage(
            files = listOf(file(1, ExtensionTypes.IMAGE), file(2, ExtensionTypes.VIDEO)),
            cursor = "stale-cursor",
            hasMore = false,
        )

        every { ApiRepository.getLastGallery(driveId = DRIVE_ID, types = any(), cursor = null) } answers {
            oldRequestEntered.countDown()
            // A timed-out hold would manufacture the intended ordering: record it, and the
            // case asserts below that it never happened.
            if (!blockedResponse.await(5, TimeUnit.SECONDS)) answerTimedOut.set(true)
            stalePage
        }

        try {
            vm.loadLastGallery(DRIVE_ID, ignoreCloud = false)
            assertTrue(oldRequestEntered.await(5, TimeUnit.SECONDS), "the old request reached the network seam")
            val staleLoadJobs = capturedLoadJobs(vm)

            // Switch the filter while the first response is still parked in the seam: the
            // superseding load cancels the parked job (it can apply only via the guards).
            vm.setMediaFilter(GalleryMediaFilter.VIDEOS)
            val freshPage = apiPage(
                files = listOf(file(3, ExtensionTypes.VIDEO), file(4, ExtensionTypes.IMAGE)),
                cursor = "fresh-cursor",
                hasMore = true,
            )
            every { ApiRepository.getLastGallery(driveId = DRIVE_ID, types = any(), cursor = null) } returns freshPage
            vm.loadLastGallery(DRIVE_ID, ignoreCloud = false)

            assertTrue(awaitTrue { vm.galleryApiResult.value?.generation == 3 }, "the fresh request applied (generation 3)")
            assertTrue(awaitTrue { vm.currentCursor == "fresh-cursor" }, "the fresh cursor is the live one")

            // Release the parked stale response, then join the stale load to COMPLETION before
            // any side-effect assertion. The resumed coroutine runs into a CANCELLED job (or,
            // at most, into a failing scope check) — this case claims exactly that, not the
            // scope-mismatch branch — and the join makes the outcome final: a broken guard
            // would have applied the stale page before the join returned.
            blockedResponse.countDown()
            joinReleasedLoads("the stale all-media load", staleLoadJobs)

            assertFalse(answerTimedOut.get(), "the harness released the parked response only via timeout: the ordering claims above are void")
            verify(exactly = 0) { FileController.storeGalleryDrive(any(), any(), any<UserDrive>()) } // VIDEOS never writes; stale ALL was dropped
            verify(exactly = 0) { FileController.removeOrphanFiles(any<UserDrive>()) }
            assertEquals("fresh-cursor", vm.currentCursor)
            assertEquals(3, vm.galleryApiResult.value?.generation)
            assertTrue(
                vm.galleryApiResult.value?.files?.all { it.matchesMediaFilter(GalleryMediaFilter.VIDEOS) } == true,
                "the presented files match the switched filter",
            )
        } finally {
            blockedResponse.countDown()
            cancelAndJoinLoads(vm)
        }
    }

    @Test
    @DisplayName("a reordered response of an older request is dropped within the same filter")
    fun reorderedResponseOfAnOlderRequestIsDroppedWithinTheSameFilter() {
        val vm = GalleryViewModel()
        val oldRequestEntered = CountDownLatch(1)
        val blockedOld = CountDownLatch(1)
        val answerTimedOut = AtomicBoolean(false)
        val olderPage = apiPage(files = listOf(file(1, ExtensionTypes.IMAGE)), cursor = "older-cursor", hasMore = false)
        val callCount = AtomicInteger(0)

        every { ApiRepository.getLastGallery(driveId = DRIVE_ID, types = any(), cursor = null) } answers {
            if (callCount.incrementAndGet() == 1) {
                oldRequestEntered.countDown()
                if (!blockedOld.await(5, TimeUnit.SECONDS)) answerTimedOut.set(true)
                olderPage
            } else {
                apiPage(files = listOf(file(2, ExtensionTypes.IMAGE)), cursor = "newer-cursor", hasMore = false)
            }
        }

        try {
            vm.loadLastGallery(DRIVE_ID, ignoreCloud = false)
            assertTrue(oldRequestEntered.await(5, TimeUnit.SECONDS), "the older request reached the seam before supersession")
            val staleLoadJobs = capturedLoadJobs(vm)
            vm.loadLastGallery(DRIVE_ID, ignoreCloud = false) // supersedes (cancels) the first request

            assertTrue(awaitTrue { vm.galleryApiResult.value?.generation == 2 }, "the newer request applied (generation 2)")

            // Release the parked older response, then join the older load to COMPLETION before
            // any assertion: a reordered response of a cancelled, superseded request cannot
            // apply (the guards above), and the join makes that outcome final — if both guards
            // were removed, the stale page would have applied before the join returned and the
            // value checks underneath would fail.
            blockedOld.countDown()
            joinReleasedLoads("the older reordered load", staleLoadJobs)

            assertFalse(answerTimedOut.get(), "the harness released the parked response only via timeout: the ordering claims above are void")
            assertEquals("newer-cursor", vm.currentCursor)
            assertEquals(2, vm.galleryApiResult.value?.files?.singleOrNull()?.id)
            assertEquals(2, vm.galleryApiResult.value?.generation)
            verify(exactly = 1) { FileController.storeGalleryDrive(match { it.size == 1 && it.first().id == 2 }, isFirstPage = true, userDrive = UserDrive(USER_ID, DRIVE_ID)) }
        } finally {
            blockedOld.countDown()
            cancelAndJoinLoads(vm)
        }
    }

    @Test
    @DisplayName("a response of a changed account scope is dropped before any side effect")
    fun responseOfAChangedAccountScopeIsDroppedBeforeAnySideEffect() {
        val vm = GalleryViewModel()
        val oldRequestEntered = CountDownLatch(1)
        val blockedResponse = CountDownLatch(1)
        val branchReachedScopeCheck = CountDownLatch(1)
        val branchMayDecide = CountDownLatch(1)
        val answerTimedOut = AtomicBoolean(false)
        val staleUserPage = apiPage(files = listOf(file(1, ExtensionTypes.IMAGE)), cursor = "other-user", hasMore = false)

        every { ApiRepository.getLastGallery(driveId = DRIVE_ID, types = any(), cursor = null) } answers {
            oldRequestEntered.countDown()
            if (!blockedResponse.await(5, TimeUnit.SECONDS)) answerTimedOut.set(true)
            staleUserPage
        }

        try {
            vm.loadLastGallery(DRIVE_ID, ignoreCloud = false)
            assertTrue(oldRequestEntered.await(5, TimeUnit.SECONDS), "the old request reached the seam before the account switch")
            val staleLoadJobs = capturedLoadJobs(vm)

            // Account switches while the response is in flight. The post-switch account stub
            // is a HANDSHAKE, not a timer: the drop branch's scope check must READ the
            // account here; the test observes the read, then lets the branch decide. The
            // decision value is fixed (USER_ID + 1 -> mismatch), so the zero-effect asserts
            // below cannot race a side effect that the branch can never produce.
            every { AccountUtils.currentUserId } answers {
                branchReachedScopeCheck.countDown()
                if (!branchMayDecide.await(5, TimeUnit.SECONDS)) answerTimedOut.set(true)
                USER_ID + 1
            }
            blockedResponse.countDown()
            assertTrue(branchReachedScopeCheck.await(5, TimeUnit.SECONDS), "the stale branch reached the production scope check (the drop ran)")
            branchMayDecide.countDown()

            // The load is NOT cancelled here — the drop is decided by the scope guard alone.
            // Joining it to completion is what makes the zero-effect claims below sensitive to
            // the guard's removal: without the account check, the stale page would have been
            // applied (cache write, cursor, UI event) before the join returned.
            joinReleasedLoads("the stale-account load", staleLoadJobs)

            assertFalse(answerTimedOut.get(), "the harness released a handshake only via timeout: the ordering claims above are void")
            assertTrue(vm.galleryApiResult.value == null, "the stale response was never applied")
            assertNull(vm.currentCursor, "the stale cursor was never taken")
            verify(exactly = 0) { FileController.storeGalleryDrive(any(), any(), any<UserDrive>()) }
            verify(exactly = 0) { FileController.removeOrphanFiles(any<UserDrive>()) }
        } finally {
            branchMayDecide.countDown()
            blockedResponse.countDown()
            cancelAndJoinLoads(vm)
        }
    }

    @Test
    @DisplayName("filtered responses are locally matched and never write the shared cache")
    fun filteredResponsesAreLocallyMatchedAndNeverWriteTheSharedCache() {
        val vm = GalleryViewModel()
        vm.setMediaFilter(GalleryMediaFilter.VIDEOS)

        // Server ignored types[]: response mixes images and videos
        every { ApiRepository.getLastGallery(driveId = DRIVE_ID, types = any(), cursor = null) } returns
            apiPage(
                files = listOf(file(1, ExtensionTypes.IMAGE), file(2, ExtensionTypes.VIDEO)),
                cursor = "c1",
                hasMore = true,
            )

        try {
            vm.loadLastGallery(DRIVE_ID, ignoreCloud = false)

            assertTrue(awaitTrue { vm.galleryApiResult.value != null }, "the filtered load applied")

            val page = vm.galleryApiResult.value!!
            assertEquals(listOf(2), page.files.map { it.id }, "only videos survive the local match")
            assertEquals(false, page.isComplete)
            verify(exactly = 0) { FileController.storeGalleryDrive(any(), any(), any<UserDrive>()) }
            verify(exactly = 0) { FileController.removeOrphanFiles(any<UserDrive>()) }
        } finally {
            cancelAndJoinLoads(vm)
        }
    }

    @Test
    @DisplayName("an all-media first page replaces the shared cache and cleans orphans")
    fun allMediaFirstPageReplacesTheSharedCacheAndCleansOrphans() {
        val vm = GalleryViewModel()
        every { ApiRepository.getLastGallery(driveId = DRIVE_ID, types = any(), cursor = null) } returns
            apiPage(
                files = listOf(file(1, ExtensionTypes.IMAGE), file(2, ExtensionTypes.VIDEO)),
                cursor = null,
                hasMore = false,
            )

        try {
            vm.loadLastGallery(DRIVE_ID, ignoreCloud = false)

            assertTrue(awaitTrue { vm.galleryApiResult.value != null }, "the load applied")

            verify(exactly = 1) { FileController.storeGalleryDrive(match { it.size == 2 }, isFirstPage = true, userDrive = UserDrive(USER_ID, DRIVE_ID)) }
            verify(exactly = 1) { FileController.removeOrphanFiles(any<UserDrive>()) }
            assertEquals(true, vm.galleryApiResult.value?.isComplete)
        } finally {
            cancelAndJoinLoads(vm)
        }
    }

    @Test
    @DisplayName("an accepted gallery commit pins both real cache opens before a queued account switch")
    fun acceptedCommitPinsBothCacheOpensBeforeAccountSwitch() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        Realm.init(context)
        val directory = Files.createTempDirectory(context.cacheDir.toPath(), "gallery-scope-").toFile()
        val user = AtomicInteger(USER_ID)
        every { AccountUtils.currentUserId } answers { user.get() }
        fun configuration(scope: UserDrive) = RealmConfiguration.Builder()
            .directory(directory).name("${scope.userId}-${scope.driveId}.realm")
            .schemaVersion(FileMigration.DB_VERSION).migration(FileMigration())
            .modules(RealmModules.LocalFilesModule()).build()
        val configScope = mutableListOf<UserDrive>()
        val writerEntered = CountDownLatch(1)
        val writerFinished = CountDownLatch(1)
        val writer = Thread {
            writerEntered.countDown()
            AccountUtils.updateCurrentScope { user.set(USER_ID + 1); true }
            writerFinished.countDown()
        }
        var firstOpen = true
        every { FileController.getRealmConfiguration(any()) } answers {
            val scope = firstArg<UserDrive?>() ?: error("default Realm resolution is forbidden")
            configScope.add(scope)
            if (firstOpen) {
                firstOpen = false
                writer.start() // applyLoad's scope check has ALREADY accepted this response
                assertTrue(writerEntered.await(5, TimeUnit.SECONDS))
                assertTrue(awaitTrue { writer.state == Thread.State.BLOCKED }, "scope writer must queue at the real monitor")
                assertEquals(1L, writerFinished.count, "the switch cannot finish inside the accepted cache commit")
            }
            configuration(scope)
        }
        // Let the actual scoped stores, Realm opens, transactions and orphan cleanup run.
        every { FileController.storeGalleryDrive(any(), any(), any<UserDrive>()) } answers { callOriginal() }
        every { FileController.removeOrphanFiles(any<UserDrive>()) } answers { callOriginal() }
        val responseFile = file(1, ExtensionTypes.IMAGE).apply { initUid() }
        val responseEntered = CountDownLatch(1)
        val releaseResponse = CountDownLatch(1)
        every { ApiRepository.getLastGallery(DRIVE_ID, any(), null) } answers {
            responseEntered.countDown()
            assertTrue(releaseResponse.await(5, TimeUnit.SECONDS))
            apiPage(listOf(responseFile), null, false)
        }
        val vm = GalleryViewModel()
        try {
            vm.loadLastGallery(DRIVE_ID, false)
            assertTrue(responseEntered.await(5, TimeUnit.SECONDS))
            val jobs = capturedLoadJobs(vm)
            releaseResponse.countDown()
            assertTrue(writerFinished.await(5, TimeUnit.SECONDS), "the accepted commit and the queued scope writer complete")
            joinReleasedLoads("accepted scoped load", jobs)
            assertEquals(listOf(UserDrive(USER_ID, DRIVE_ID), UserDrive(USER_ID, DRIVE_ID)), configScope)
            Realm.getInstance(configuration(UserDrive(USER_ID, DRIVE_ID))).use { realm ->
                assertEquals(1L, realm.where(File::class.java).equalTo(File::id.name, 1).count())
            }
            Realm.getInstance(configuration(UserDrive(USER_ID + 1, DRIVE_ID))).use { realm ->
                assertEquals(0L, realm.where(File::class.java).count())
            }
            assertFalse(vm.isPageCurrent(vm.galleryApiResult.value!!), "queued old-account UI delivery is refused")
        } finally {
            releaseResponse.countDown()
            cancelAndJoinLoads(vm)
            if (writer.state != Thread.State.NEW) writer.join(5_000)
            assertFalse(writer.isAlive, "scope writer must terminate before fixture teardown")
            directory.listFiles()?.filter { it.name.endsWith(".realm") }?.forEach { file ->
                Realm.deleteRealm(RealmConfiguration.Builder().directory(directory).name(file.name)
                    .schemaVersion(FileMigration.DB_VERSION).migration(FileMigration())
                    .modules(RealmModules.LocalFilesModule()).build())
            }
            assertTrue(directory.deleteRecursively())
        }
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
