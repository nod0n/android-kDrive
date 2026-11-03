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
package com.infomaniak.drive.data.documentprovider

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.util.concurrent.Executor

/**
 * Pure tests for the app-lock transition machine: queued requests, generation identity,
 * supersede skipping and failure handling. No Android or Realm involved; the writer is a manual
 * executor the test drains in a chosen order, and persistence is scripted.
 */
class AppLockTransitionsTest {

    /** Queues tasks for explicit draining, emulating a delayed writer thread. */
    private class ManualExecutor : Executor {
        private val tasks = ArrayDeque<Runnable>()
        val pendingCount: Int get() = tasks.size
        override fun execute(command: Runnable) = tasks.addLast(command)
        fun runNext() = tasks.removeFirst().run()
    }

    private class Fixture(successScript: List<Boolean>) {
        val executor = ManualExecutor()
        val persistedValues = mutableListOf<Boolean>()
        var transitions = 0
        private val script = ArrayDeque(successScript)

        fun persist(enabled: Boolean): Boolean {
            persistedValues += enabled
            return script.removeFirst()
        }

        val state = AppLockTransitions(
            persist = ::persist,
            onTransition = { transitions++ },
            writer = executor,
        )
    }

    @Test
    fun `an older completed write cannot clear a newer pending transition`() {
        // R2-1 trace: enable A, disable B, enable C enqueued while the writer is delayed.
        val fixture = Fixture(successScript = listOf(true))
        fixture.state.request(enabled = true)
        fixture.state.request(enabled = false)
        val resultC = mutableListOf<Boolean>()
        fixture.state.request(enabled = true, onResult = { resultC += it })

        assertEquals(3, fixture.executor.pendingCount)
        assertTrue(fixture.state.isPending())

        fixture.executor.runNext() // A commits? No: superseded by C, must not persist at all
        fixture.executor.runNext() // B: superseded too
        assertTrue(fixture.state.isPending(), "pending C must still deny access")
        assertEquals(emptyList(), fixture.persistedValues, "superseded requests never persist")

        fixture.executor.runNext() // C: latest, persists
        assertFalse(fixture.state.isPending())
        assertEquals(listOf(true), fixture.persistedValues)
        assertEquals(listOf(true), resultC)
    }

    @Test
    fun `a failed latest persist keeps denying until a later request succeeds`() {
        val fixture = Fixture(successScript = listOf(false, true))

        val firstResult = mutableListOf<Boolean>()
        fixture.state.request(enabled = true, onResult = { firstResult += it })
        fixture.executor.runNext()

        assertTrue(fixture.state.isPending(), "failed persist must keep the pending denial")
        assertEquals(listOf(false), firstResult)
        assertEquals(listOf(true), fixture.persistedValues, "the failed attempt was made, but did not complete")

        fixture.state.request(enabled = true)
        fixture.executor.runNext()
        assertFalse(fixture.state.isPending())
        assertEquals(listOf(true, true), fixture.persistedValues)
    }

    @Test
    fun `a superseded request reports failure without persisting`() {
        val fixture = Fixture(successScript = listOf(true))

        val results = mutableListOf<Pair<String, Boolean>>()
        fixture.state.request(enabled = true, onResult = { results += "A" to it })
        fixture.state.request(enabled = false, onResult = { results += "B" to it })

        fixture.executor.runNext() // A superseded
        fixture.executor.runNext() // B persists

        assertEquals(listOf("A" to false, "B" to true), results)
        assertEquals(listOf(false), fixture.persistedValues)
        assertFalse(fixture.state.isPending())
    }

    @Test
    fun `policyGeneration advances on request and on commit`() {
        val fixture = Fixture(successScript = listOf(true))

        val atRequest = fixture.state.policyGeneration()
        fixture.state.request(enabled = true)
        val afterRequest = fixture.state.policyGeneration()
        fixture.executor.runNext()
        val afterCommit = fixture.state.policyGeneration()

        assertTrue(afterRequest > atRequest)
        assertTrue(afterCommit > afterRequest)
    }

    @Test
    fun `withAdmission observes pending transitions atomically`() {
        val fixture = Fixture(successScript = listOf(true))
        fixture.state.request(enabled = false)

        val observedPendingInAdmission = fixture.state.withAdmission { fixture.state.isPending() }

        assertTrue(observedPendingInAdmission)
        assertTrue(fixture.state.isPending())
    }
}
