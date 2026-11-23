/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2026 Infomaniak Network SA
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.infomaniak.drive.utils

import android.content.Context
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import io.mockk.mockk
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import kotlin.coroutines.CoroutineContext
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicReference

/** Real request coroutine/dispatch/lifetime; only disk/Realm/WorkManager boundaries are controlled. */
@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class DirectOfflinePinTest {
    private var lane = "caller"
    private inner class HeldDispatcher(private val name: String) : CoroutineDispatcher() {
        val pending = ArrayDeque<Runnable>()
        override fun dispatch(context: CoroutineContext, block: Runnable) { pending += block }
        fun drain() {
            while (pending.isNotEmpty()) {
                val previous = lane
                lane = name
                try { pending.removeFirst().run() } finally { lane = previous }
            }
        }
    }

    private val main = HeldDispatcher("main")
    private val io = HeldDispatcher("io")
    private class PinOwner : ViewModel()
    private val owner = PinOwner()
    private val scope = UserDrive(101, 201)
    private val data = Data()
    private val results = mutableListOf<Boolean>()
    private val pin by lazy { DirectOfflinePin(owner.viewModelScope, data, io) }

    private inner class Data : DirectOfflinePinData(mockk<Context>()) {
        var current = AccountUtils.CurrentScope(101, 201, 1)
        val file = File(id = 7, driveId = 201, name = "fixture.txt")
        var rowExists = true
        var failCommit = false
        var durablePin = false
        var afterCommit: () -> Unit = {}
        var cache = CacheResult.UNAVAILABLE
        val trace = mutableListOf<String>()
        val scopes = mutableListOf<UserDrive>()
        var cacheScope: AccountUtils.CurrentScope? = null

        override fun captureScope() = current
        override fun <T> withScope(block: () -> T): T = block()
        override fun commitPin(fileId: Int, userDrive: UserDrive): File? {
            // Models Realm's actual UI-write prohibition, not a permissive mock transaction.
            check(lane == "io") { "transactions forbidden on Main" }
            assertEquals(7, fileId)
            scopes += userDrive
            trace += "commit:start"
            assertTrue(results.isEmpty())
            if (!rowExists) return null
            if (failCommit) error("controlled transaction failure")
            durablePin = true
            trace += "commit:durable"
            afterCommit()
            // Same detached identity is passed onward; no UI-managed Realm object is used.
            return file
        }

        override fun publishCached(file: File, userDrive: UserDrive, captured: AccountUtils.CurrentScope): CacheResult {
            assertEquals("io", lane)
            assertTrue(durablePin, "cache publication must never precede the durable pin")
            assertSame(this.file, file)
            scopes += userDrive
            cacheScope = captured
            trace += "cache:$cache"
            return cache
        }

        override fun enqueue(file: File, userDrive: UserDrive) {
            assertEquals("io", lane)
            assertTrue(durablePin, "the worker can run immediately on enqueue")
            assertSame(this.file, file)
            scopes += userDrive
            trace += "enqueue"
        }
    }

    @BeforeEach
    fun setUp() { Dispatchers.setMain(main) }

    @AfterEach
    fun tearDown() { owner.viewModelScope.cancel(); finish(); Dispatchers.resetMain() }

    private fun request(userDrive: UserDrive = scope) = pin.request(7, userDrive) {
        assertEquals("main", lane)
        data.trace += "result:$it"
        results += it
    }

    private fun finish() { repeat(4) { main.drain(); io.drain() }; main.drain() }

    @Test
    fun `no cache commits on IO before worker and Main result`() {
        request()
        main.drain()
        assertFalse(data.durablePin)
        assertTrue(data.trace.isEmpty(), "IO commit is held; neither enqueue nor success may escape")
        finish()
        assertEquals(listOf("commit:start", "commit:durable", "cache:UNAVAILABLE", "enqueue", "result:true"), data.trace)
        assertEquals(listOf(scope, scope, scope), data.scopes)
    }

    @Test
    fun `cache hit commits on IO before publication and never enqueues`() {
        data.cache = DirectOfflinePinData.CacheResult.PUBLISHED
        request(); finish()
        assertEquals(listOf("commit:start", "commit:durable", "cache:PUBLISHED", "result:true"), data.trace)
        assertEquals(data.current, data.cacheScope)
    }

    @Test
    fun `refused cache replacement keeps pin but reports failure without download`() {
        data.cache = DirectOfflinePinData.CacheResult.REFUSED
        request(); finish()
        assertTrue(data.durablePin)
        assertFalse(data.trace.contains("enqueue"))
        assertEquals(listOf(false), results)
    }

    @Test
    fun `failed transaction never publishes cache or enqueues or reports success`() {
        data.failCommit = true
        request(); finish()
        assertEquals(listOf("commit:start", "result:false"), data.trace)
        assertFalse(data.durablePin)
    }

    @Test
    fun `missing row is failure rather than a successful empty transaction`() {
        data.rowExists = false
        request(); finish()
        assertEquals(listOf("commit:start", "result:false"), data.trace)
    }

    @Test
    fun `captured explicit account and drive must match active scope`() {
        request(scope.copy(userId = 999)); request(scope.copy(driveId = 999)); finish()
        assertTrue(data.trace.isEmpty())
        assertFalse(data.durablePin)
    }

    @Test
    fun `scope change while IO queued prevents any commit or publication`() {
        request(); main.drain()
        data.current = data.current.copy(userId = 999, revision = 2)
        finish()
        assertTrue(data.trace.isEmpty())
    }

    @Test
    fun `away and back scope revision cannot authorize an old request`() {
        request(); main.drain()
        data.current = data.current.copy(revision = 3)
        finish()
        assertTrue(data.trace.isEmpty())
    }

    @Test
    fun `scope change after commit leaves old pin but cannot enqueue or deliver stale UI success`() {
        data.afterCommit = { data.current = data.current.copy(driveId = 999, revision = 2) }
        request(); finish()
        assertEquals(listOf("commit:start", "commit:durable"), data.trace)
        assertTrue(data.durablePin)
        assertTrue(results.isEmpty())
    }

    @Test
    fun `sheet destruction cannot cancel ViewModel owned pin`() {
        val sheet = CoroutineScope(Job() + main)
        request(); main.drain()
        sheet.cancel() // Matches the immediate pop; the request belongs to the actual ViewModel scope.
        finish()
        assertEquals(listOf(true), results)
        assertTrue(data.trace.contains("enqueue"))
    }

    @Test
    fun `owner cancellation before IO prevents commit and enqueue`() {
        request(); main.drain()
        owner.viewModelScope.cancel()
        finish()
        assertFalse(data.durablePin)
        assertTrue(results.isEmpty())
        assertTrue(data.trace.isEmpty())
    }

    @Test
    fun `physical IO thread commits before enqueue and survives actual caller coroutine cancellation`() = runBlocking {
        val uiThread = AtomicReference<Thread>()
        val diskThread = AtomicReference<Thread>()
        Executors.newSingleThreadExecutor { Thread(it, "pin-owner-test").also(uiThread::set) }.asCoroutineDispatcher().use { ui ->
            Executors.newSingleThreadExecutor { Thread(it, "pin-io-test").also(diskThread::set) }.asCoroutineDispatcher().use { disk ->
                Dispatchers.setMain(ui)
                val vm = PinOwner()
                val sheet = CoroutineScope(Job() + ui)
                val commitStarted = CountDownLatch(1)
                val releaseCommit = CountDownLatch(1)
                val enqueued = AtomicBoolean(false)
                val delivered = CompletableDeferred<Boolean>()
                val requestJob = CompletableDeferred<Job>()
                val storage = object : DirectOfflinePinData(mockk<Context>()) {
                    override fun captureScope() = AccountUtils.CurrentScope(101, 201, 1)
                    override fun <T> withScope(block: () -> T) = block()
                    override fun commitPin(fileId: Int, userDrive: UserDrive): File {
                        check(Thread.currentThread() === diskThread.get()) { "transactions forbidden on Main" }
                        commitStarted.countDown()
                        check(releaseCommit.await(5, TimeUnit.SECONDS))
                        return File(id = fileId, driveId = userDrive.driveId, name = "fixture.txt")
                    }
                    override fun publishCached(file: File, userDrive: UserDrive, captured: AccountUtils.CurrentScope) =
                        CacheResult.UNAVAILABLE
                    override fun enqueue(file: File, userDrive: UserDrive) {
                        assertSame(diskThread.get(), Thread.currentThread())
                        assertEquals(0L, releaseCommit.count)
                        enqueued.set(true)
                    }
                }
                try {
                    sheet.launch {
                        requestJob.complete(DirectOfflinePin(vm.viewModelScope, storage, disk).request(7, scope) {
                            assertSame(uiThread.get(), Thread.currentThread())
                            delivered.complete(it)
                        })
                    }
                    assertTrue(commitStarted.await(5, TimeUnit.SECONDS))
                    assertFalse(enqueued.get(), "commit is held on a real IO thread")
                    sheet.cancel()
                    releaseCommit.countDown()
                    withTimeout(5_000) {
                        assertTrue(delivered.await())
                        requestJob.await().join()
                    }
                    assertTrue(enqueued.get())
                } finally {
                    releaseCommit.countDown()
                    sheet.cancel()
                    vm.viewModelScope.cancel()
                    Dispatchers.setMain(main)
                }
            }
        }
    }
}
