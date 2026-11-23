/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2026 Infomaniak Network SA
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.infomaniak.drive.utils

import android.app.Application
import android.content.Context
import androidx.arch.core.executor.ArchTaskExecutor
import androidx.arch.core.executor.TaskExecutor
import androidx.fragment.app.Fragment
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.LifecycleRegistry
import androidx.lifecycle.SavedStateHandle
import androidx.lifecycle.ViewModel
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.viewModelScope
import com.infomaniak.core.network.NetworkAvailability
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.ui.MainViewModel
import com.infomaniak.drive.views.FileInfoActionsView
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.unmockkAll
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.NonCancellable
import kotlinx.coroutines.cancel
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.coroutines.ContinuationInterceptor
import java.util.concurrent.Executors
import java.util.concurrent.atomic.AtomicReference

/** Real View callback -> activity VM -> request coroutine and production cancellation loop.
 * Realm/FS/network effects are controlled host boundaries, not native storage proof.
 */
@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class FolderOfflineOperationTest {
    private val main = StandardTestDispatcher()
    private val io = StandardTestDispatcher(main.scheduler, "folder-io")
    private val scope = UserDrive(101, 201)
    private val context = mockk<Context>()
    private val folder = File(id = 7, driveId = 201, name = "fixture", type = "dir")
    private val sheet = object : LifecycleOwner { override val lifecycle = LifecycleRegistry.createUnsafe(this) }
    private val data = Data()
    private lateinit var vm: MainViewModel
    private lateinit var view: FileInfoActionsView
    private val results = mutableListOf<FolderOfflineResult>()

    private inner class Data : FolderOfflineOperationData(context) {
        var current = AccountUtils.CurrentScope(101, 201, 1)
        var root = true
        var rowExists = true
        var failNextChange = false
        var refuseFirst = false
        var preserveFirst = false
        var attention = false
        var reportedState: FolderOfflineState? = null
        var folderReads = 0
        val visited = mutableListOf<Int>()
        val readScopes = mutableListOf<UserDrive>()
        val present = linkedSetOf(1, 2, 3)
        val firstEffect = CompletableDeferred<Unit>()
        val continueWork = CompletableDeferred<Unit>()
        val trace = mutableListOf<String>()
        override fun captureScope() = current
        override fun <T> withScope(block: () -> T) = block()
        override fun folder(id: Int, scope: UserDrive): File? {
            folderReads++
            readScopes += scope
            return folder.takeIf { rowExists }
        }
        override fun state(id: Int, scope: UserDrive) = reportedState ?: FolderOfflineState(root, attention)
        override fun enqueue(id: Int, scope: UserDrive) { trace += "enqueue" }
        override suspend fun change(
            folder: File, scope: UserDrive, enabled: Boolean,
            checkRequest: () -> Unit, enqueue: (Int) -> Unit,
        ): Boolean {
            assertEquals(io, currentCoroutineContext()[ContinuationInterceptor], "mutation orchestration must run on IO")
            if (failNextChange) { failNextChange = false; error("controlled transaction failure") }
            trace += if (enabled) "mark" else "unmark"
            if (enabled) {
                root = true // Actual mark commits the root BEFORE its cancellable walk.
                firstEffect.complete(Unit)
                continueWork.await()
                checkRequest()
                enqueue(folder.id)
                return true
            }
            // This is the SAME cancellation-checked loop used by production unmark.
            val confirmed = OfflineFolderSyncUtils.withdrawOfflineSubtree(linkedSetOf(1, 2, 3)) { id ->
                checkRequest()
                visited += id
                if (id == 1) {
                    if (!refuseFirst && !preserveFirst) {
                        present.remove(id) // Controlled confirmed per-file deletion.
                        trace += "remove:1"
                    }
                    firstEffect.complete(Unit)
                    // A committed, synchronous filesystem/Realm section may finish even
                    // after dismissal. The REAL loop's next ensureActive then decides.
                    withContext(NonCancellable) { continueWork.await() }
                }
                when {
                    id != 1 -> OfflineFolderSyncUtils.RemovalResult.RETAINED // Other root / direct pin storage verdict.
                    refuseFirst -> OfflineFolderSyncUtils.RemovalResult.REFUSED
                    preserveFirst -> OfflineFolderSyncUtils.RemovalResult.PRESERVED
                    else -> OfflineFolderSyncUtils.RemovalResult.REMOVED
                }
            }
            if (confirmed) { root = false; trace += "root:clear" }
            return confirmed
        }
    }

    @BeforeEach
    fun setUp() {
        Dispatchers.setMain(main)
        ArchTaskExecutor.getInstance().setDelegate(object : TaskExecutor() {
            override fun executeOnDiskIO(runnable: Runnable) = runnable.run()
            override fun postToMainThread(runnable: Runnable) = runnable.run()
            override fun isMainThread() = true
        })
        mockkObject(NetworkAvailability)
        every { NetworkAvailability.isNetworkAvailable } returns MutableSharedFlow()
        vm = MainViewModel(mockk<Application>(), SavedStateHandle())
        MainViewModel::class.java.getDeclaredField("folderOfflineOperation\$delegate").apply { isAccessible = true }
            .set(vm, lazy { FolderOfflineOperation(vm.viewModelScope, data, io) })
        vm.folderOfflineResult.observeForever { results += it }
        sheet.lifecycle.currentState = Lifecycle.State.STARTED
        val fragment = mockk<Fragment>()
        every { fragment.viewLifecycleOwner } returns sheet
        view = mockk(relaxed = true)
        every { view.context } returns context
        every { view.toggleFolderOfflineRoot(any(), any()) } answers { callOriginal() }
        for ((name, value) in listOf("ownerFragment" to fragment, "mainViewModel" to vm, "offlineUserScope" to scope)) {
            FileInfoActionsView::class.java.getDeclaredField(name).apply { isAccessible = true }.set(view, value)
        }
        // Legacy call-site storage boundary. The production View still chooses its real
        // lifecycle/dispatcher; this stub does not supply or replace its coroutine job.
        mockkObject(OfflineFolderSyncUtils)
        coEvery { OfflineFolderSyncUtils.unmarkFolderOffline(context, any(), scope) } coAnswers {
            data.change(folder, scope, false, {}, {})
        }
        coEvery { OfflineFolderSyncUtils.markFolderOffline(context, any(), scope) } coAnswers {
            data.change(folder, scope, true, {}, { data.enqueue(it, scope) })
        }
    }

    @AfterEach
    fun tearDown() {
        sheet.lifecycle.currentState = Lifecycle.State.DESTROYED
        vm.viewModelScope.cancel()
        main.scheduler.runCurrent()
        unmockkAll()
        ArchTaskExecutor.getInstance().setDelegate(null)
        Dispatchers.resetMain()
    }

    @Test
    fun unmarkSurvivesSheetDismissalAfterFirstWithdrawal() {
        view.toggleFolderOfflineRoot(folder, false)
        main.scheduler.runCurrent()
        runBlocking { withTimeout(5_000) { data.firstEffect.await() } }
        assertTrue(data.firstEffect.isCompleted, "must reach the actual per-file phase before dismissal")
        assertFalse(1 in data.present)
        assertTrue(data.root)
        sheet.lifecycle.currentState = Lifecycle.State.DESTROYED
        main.scheduler.runCurrent()
        data.continueWork.complete(Unit)
        main.scheduler.advanceUntilIdle()
        println("unmark dismissal trace=${data.trace} root=${data.root}")
        assertFalse(data.root, "ordinary sheet dismissal must not abandon withdrawal before the root closer")
        assertEquals(listOf(FolderOfflineResult(false, true, false)), results)
    }

    @Test
    fun markSurvivesSheetDismissalBetweenRootCommitAndEnqueue() {
        data.root = false
        view.toggleFolderOfflineRoot(folder, true)
        main.scheduler.runCurrent()
        runBlocking { withTimeout(5_000) { data.firstEffect.await() } }
        assertTrue(data.firstEffect.isCompleted, "must reach committed root before dismissal")
        assertTrue(data.root)
        sheet.lifecycle.currentState = Lifecycle.State.DESTROYED
        main.scheduler.runCurrent()
        data.continueWork.complete(Unit)
        main.scheduler.advanceUntilIdle()
        assertEquals(listOf("mark", "enqueue"), data.trace)
        assertEquals(listOf(FolderOfflineResult(true, true, false)), results)
    }

    @Test
    fun ordinaryUnmarkRetainsOtherRootAndDirectPinStorageVerdicts() {
        data.continueWork.complete(Unit)
        view.toggleFolderOfflineRoot(folder, false)
        main.scheduler.advanceUntilIdle()
        assertFalse(data.root)
        assertEquals(setOf(2, 3), data.present)
        assertEquals(listOf(1, 2, 3), data.visited)
        assertEquals(listOf(FolderOfflineResult(false, true, false)), results)
    }

    @Test
    fun refusalDoesNotClearRootOrReportSuccess() {
        data.refuseFirst = true
        data.attention = true
        data.continueWork.complete(Unit)
        view.toggleFolderOfflineRoot(folder, false)
        main.scheduler.advanceUntilIdle()
        assertTrue(data.root)
        assertEquals(setOf(1, 2, 3), data.present)
        assertEquals(listOf(1, 2, 3), data.visited, "a refusal must not skip the rest of the existing loop")
        assertEquals(listOf(FolderOfflineResult(false, false, true)), results)
    }

    @Test
    fun preservedEditedBytesAreNotTurnedIntoADeletionOrFailure() {
        data.preserveFirst = true
        data.attention = true
        data.continueWork.complete(Unit)
        view.toggleFolderOfflineRoot(folder, false)
        main.scheduler.advanceUntilIdle()
        assertFalse(data.root)
        assertEquals(setOf(1, 2, 3), data.present)
        assertEquals(listOf(FolderOfflineResult(false, true, true)), results)
        assertEquals(com.infomaniak.drive.R.string.offlineCopyPreserved, results.single().messageResId)
    }

    @Test
    fun overlappingMarkThenUnmarkRequestsCommitAndPublishInCallOrder() {
        data.root = false
        view.toggleFolderOfflineRoot(folder, true)
        main.scheduler.runCurrent()
        view.toggleFolderOfflineRoot(folder, false)
        main.scheduler.runCurrent()
        assertEquals(listOf("mark"), data.trace, "second request cannot enter IO while first is held")
        data.continueWork.complete(Unit)
        main.scheduler.advanceUntilIdle()
        assertEquals(listOf("mark", "enqueue", "unmark", "remove:1", "root:clear"), data.trace)
        assertEquals(listOf(true, false), results.map { it.requestedEnabled })
        assertTrue(results.all { it.completed })
        assertFalse(data.root)
    }

    @Test
    fun transactionFailureCannotEnqueueAndDoesNotPoisonTheNextRequest() {
        data.root = false
        data.failNextChange = true
        data.continueWork.complete(Unit)
        view.toggleFolderOfflineRoot(folder, true)
        main.scheduler.advanceUntilIdle()
        assertFalse(data.root)
        assertFalse(data.trace.contains("enqueue"))
        view.toggleFolderOfflineRoot(folder, true)
        main.scheduler.advanceUntilIdle()
        assertEquals(listOf(false, true), results.map { it.completed })
        assertEquals(1, data.trace.count { it == "enqueue" })
    }

    @Test
    fun missingScopedRowDoesNotChangeOrEnqueue() {
        data.rowExists = false
        view.toggleFolderOfflineRoot(folder, true)
        main.scheduler.advanceUntilIdle()
        assertTrue(data.trace.isEmpty())
        assertEquals(listOf(FolderOfflineResult(true, false, false)), results)
    }

    @Test
    fun accountChangeWhileQueuedPreventsEvenTheScopedRowRead() {
        view.toggleFolderOfflineRoot(folder, true)
        data.current = data.current.copy(userId = 999, revision = 2)
        main.scheduler.advanceUntilIdle()
        assertEquals(0, data.folderReads)
        assertTrue(data.trace.isEmpty())
        assertTrue(results.isEmpty())
    }

    @Test
    fun awayAndBackRevisionCannotAuthorizeAQueuedOldRequest() {
        view.toggleFolderOfflineRoot(folder, true)
        data.current = data.current.copy(revision = 3)
        main.scheduler.advanceUntilIdle()
        assertEquals(0, data.folderReads)
        assertTrue(results.isEmpty())
    }

    @Test
    fun driveChangeDuringMarkWalkPreventsEnqueueAndStaleResult() {
        view.toggleFolderOfflineRoot(folder, true)
        main.scheduler.runCurrent()
        assertTrue(data.firstEffect.isCompleted)
        data.current = data.current.copy(driveId = 999, revision = 2)
        data.continueWork.complete(Unit)
        main.scheduler.advanceUntilIdle()
        assertEquals(listOf("mark"), data.trace)
        assertEquals(listOf(scope), data.readScopes)
        assertTrue(results.isEmpty())
    }

    @Test
    fun scopeChangeDuringWithdrawalStopsAtNextRealLoopCheckpoint() {
        view.toggleFolderOfflineRoot(folder, false)
        main.scheduler.runCurrent()
        data.current = data.current.copy(userId = 999, revision = 2)
        data.continueWork.complete(Unit)
        main.scheduler.advanceUntilIdle()
        assertEquals(listOf(1), data.visited)
        assertTrue(data.root, "scope cancellation must retain the existing retriable closer semantics")
        assertEquals(listOf(scope), data.readScopes)
        assertTrue(results.isEmpty())
    }

    @Test
    fun activityViewModelCancellationStillStopsTheRealWithdrawalLoop() {
        view.toggleFolderOfflineRoot(folder, false)
        main.scheduler.runCurrent()
        vm.viewModelScope.cancel()
        data.continueWork.complete(Unit)
        main.scheduler.advanceUntilIdle()
        assertTrue(data.root)
        assertEquals(listOf(1), data.visited)
        assertTrue(results.isEmpty())
    }

    @Test
    fun mismatchedDriveAndSharedScopeCannotStartWork() {
        vm.setFolderOffline(folder.id, scope.copy(driveId = 999), true)
        vm.setFolderOffline(folder.id, scope.copy(sharedWithMe = true), true)
        main.scheduler.advanceUntilIdle()
        assertEquals(0, data.folderReads)
        assertTrue(results.isEmpty())
    }

    @Test
    fun aContradictingFreshRootStateCannotBeReportedAsSuccess() {
        data.reportedState = FolderOfflineState(true, false)
        data.continueWork.complete(Unit)
        view.toggleFolderOfflineRoot(folder, false)
        main.scheduler.advanceUntilIdle()
        assertFalse(data.root)
        assertEquals(listOf(FolderOfflineResult(false, false, false)), results)
    }

    private class PhysicalOwner : ViewModel()

    @Test
    fun snapshotMutationEnqueueAndStateReadRunOnPhysicalIOWithMainDelivery() = runBlocking {
        val uiThread = AtomicReference<Thread>()
        val ioThread = AtomicReference<Thread>()
        Executors.newSingleThreadExecutor { Thread(it, "folder-main-test").also(uiThread::set) }.asCoroutineDispatcher().use { ui ->
            Executors.newSingleThreadExecutor { Thread(it, "folder-io-test").also(ioThread::set) }.asCoroutineDispatcher().use { disk ->
                Dispatchers.setMain(ui)
                val owner = PhysicalOwner()
                val delivered = CompletableDeferred<FolderOfflineResult>()
                val storage = object : FolderOfflineOperationData(context) {
                    override fun captureScope() = AccountUtils.CurrentScope(101, 201, 1)
                    override fun <T> withScope(block: () -> T) = block()
                    private fun requireIO() = assertTrue(Thread.currentThread() === ioThread.get())
                    override fun folder(id: Int, scope: UserDrive): File { requireIO(); return folder }
                    override fun state(id: Int, scope: UserDrive): FolderOfflineState {
                        requireIO(); return FolderOfflineState(true, false)
                    }
                    override fun enqueue(id: Int, scope: UserDrive) { requireIO() }
                    override suspend fun change(
                        folder: File, scope: UserDrive, enabled: Boolean,
                        checkRequest: () -> Unit, enqueue: (Int) -> Unit,
                    ): Boolean { requireIO(); checkRequest(); enqueue(folder.id); return true }
                }
                try {
                    val job = FolderOfflineOperation(owner.viewModelScope, storage, disk).request(folder.id, scope, true) {
                        assertTrue(Thread.currentThread() === uiThread.get())
                        delivered.complete(it)
                    }
                    withTimeout(5_000) {
                        job.join()
                        assertEquals(FolderOfflineResult(true, true, false), delivered.await())
                    }
                } finally {
                    owner.viewModelScope.cancel()
                    Dispatchers.setMain(main)
                }
            }
        }
    }
}
