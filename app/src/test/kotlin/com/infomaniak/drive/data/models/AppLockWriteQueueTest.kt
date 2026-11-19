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
package com.infomaniak.drive.data.models

import com.infomaniak.drive.data.documentprovider.ProviderAccessPolicy
import com.infomaniak.drive.data.documentprovider.ProviderAccessPolicy.Reason
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineExceptionHandler
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel
import kotlinx.coroutines.withContext
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import kotlin.coroutines.CoroutineContext

class AppLockWriteQueueTest {

    private class HeldDispatcher : CoroutineDispatcher() {
        val pending = ArrayDeque<Runnable>()
        override fun dispatch(context: CoroutineContext, block: Runnable) { pending.addLast(block) }
        fun oldest() = pending.removeFirst().run()
        fun newest() = pending.removeLast().run()
        fun drain() { while (pending.isNotEmpty()) oldest() }
    }

    private class Writes(initial: Boolean, val providerDisabled: Boolean) : AutoCloseable {
        val default = HeldDispatcher()
        val io = HeldDispatcher()
        val main = HeldDispatcher()
        val failures = mutableListOf<Throwable>()
        private val scope = CoroutineScope(Job() + default + CoroutineExceptionHandler { _, error -> failures += error })
        val queue = AppLockWriteQueue(scope)
        var durable = initial
        var cached = initial
        var failTransaction = false
        val trace = mutableListOf<String>()
        val notifications = mutableListOf<Triple<Boolean, Boolean, Reason>>()

        // Production queue, actual coroutine context switches; only Realm/storage
        // and dispatch execution are controlled host boundaries. No sleeps.
        fun request(value: Boolean) = queue.submit {
            withContext(io) {
                if (failTransaction) error("controlled transaction failure")
                durable = value
                trace += "commit:$value"
            }
            withContext(main) {
                cached = value
                trace += "publish:$value"
                notifications += Triple(durable, cached, ProviderAccessPolicy.evaluate(durable, providerDisabled))
            }
        }

        // Adversarial schedule: drain commits/Default continuations while Main is
        // held, then choose the NEWEST Main task. Independent launches would
        // commit A,B yet publish B,A under exactly this schedule.
        fun finish() {
            repeat(30) {
                io.drain()
                default.drain()
                if (main.pending.isNotEmpty()) main.newest()
                if (io.pending.isEmpty() && default.pending.isEmpty() && main.pending.isEmpty()) return
            }
            error("write dispatch did not settle")
        }

        override fun close() { scope.cancel(); finish() }
    }

    private fun overlapping(initial: Boolean, providerDisabled: Boolean) {
        Writes(initial, providerDisabled).use { writes ->
            val first = !initial
            val second = initial
            val a = writes.request(first)
            // A is held before its IO commit; enqueue B before completing A.
            val b = writes.request(second)
            writes.finish()
            println("lastIntent=$second providerDisabled=$providerDisabled trace=${writes.trace} durable=${writes.durable} cached=${writes.cached}")
            assertEquals(second, writes.durable, writes.trace.toString())
            assertEquals(second, writes.cached, writes.trace.toString())
            assertEquals(
                listOf("commit:$first", "publish:$first", "commit:$second", "publish:$second"),
                writes.trace,
            )
            assertEquals(listOf(first, second), writes.notifications.map { it.first })
            assertTrue(writes.notifications.all { it.first == it.second }, writes.notifications.toString())
            assertEquals(
                listOf(first, second).map { ProviderAccessPolicy.evaluate(it, providerDisabled) },
                writes.notifications.map { it.third },
            )
            assertTrue(a.isCompleted && b.isCompleted)
            assertTrue(writes.failures.isEmpty())
        }
    }

    @Test
    fun `last OFF intent wins with independent provider OFF`() = overlapping(initial = false, providerDisabled = true)

    @Test
    fun `last ON intent wins with independent provider OFF`() = overlapping(initial = true, providerDisabled = true)

    @Test
    fun `last OFF intent wins with independent provider ON`() = overlapping(initial = false, providerDisabled = false)

    @Test
    fun `last ON intent wins with independent provider ON`() = overlapping(initial = true, providerDisabled = false)

    @Test
    fun `later request cannot start committing while first IO is held`() {
        Writes(initial = false, providerDisabled = true).use { writes ->
            writes.request(true)
            writes.request(false)
            writes.default.drain()
            assertEquals(1, writes.io.pending.size)
            assertTrue(writes.trace.isEmpty())
            writes.finish()
            assertEquals(listOf("commit:true", "publish:true", "commit:false", "publish:false"), writes.trace)
        }
    }

    @Test
    fun `next commit waits for held cache publication and notification`() {
        Writes(initial = false, providerDisabled = false).use { writes ->
            writes.request(true)
            writes.io.drain()
            writes.default.drain()
            assertTrue(writes.durable)
            assertFalse(writes.cached)
            assertEquals(1, writes.main.pending.size)
            writes.request(false)
            writes.default.drain()
            assertTrue(writes.io.pending.isEmpty())
            assertTrue(writes.notifications.isEmpty())
            writes.main.oldest()
            assertEquals(listOf(Triple(true, true, Reason.BLOCKED_BY_APP_LOCK)), writes.notifications)
            writes.finish()
            assertFalse(writes.durable)
            assertFalse(writes.cached)
        }
    }

    @Test
    fun `transaction failure does not publish and retains parent cancellation behavior`() {
        Writes(initial = false, providerDisabled = true).use { writes ->
            writes.failTransaction = true
            val a = writes.request(true)
            val b = writes.request(false)
            writes.finish()
            assertTrue(a.isCancelled && b.isCancelled)
            assertEquals(1, writes.failures.size)
            assertFalse(writes.durable)
            assertFalse(writes.cached)
            assertTrue(writes.notifications.isEmpty())
            // Same regular parent Job as AppSettings: no recovery/retry promise.
            // UNDISPATCHED must not execute work in an already cancelled scope.
            var ran = false
            val afterFailure = writes.queue.submit { ran = true }
            writes.finish()
            assertTrue(afterFailure.isCancelled)
            assertFalse(ran)
        }
    }
}
