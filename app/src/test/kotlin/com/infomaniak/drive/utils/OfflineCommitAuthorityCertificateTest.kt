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
package com.infomaniak.drive.utils

import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * The mutation-generation certificate against the REAL authority (RESUME-962c item 1 /
 * FHNW-962c P1-1/P1-2). The former semantics advanced the generation on EVERY outermost
 * acquisition — including the evidence capture's own hold — so every capture→validate pair
 * self-invalidated and no positive path could be certified. These regressions pin the
 * coherent contract: observation (evidence) acquisitions never advance the generation;
 * mutating acquisitions advance it exactly once per outermost hold, published UNDER the
 * lock; nested mutations and cross-thread writer handoffs obey the same rule; exceptions in
 * a mutating hold still advance it (a partially applied mutation is unprovable), exceptions
 * in an observation hold do not.
 *
 * Every assertion uses generation DELTAS: the authority is a process singleton shared by the
 * whole test JVM.
 */
class OfflineCommitAuthorityCertificateTest {

    private fun generationNow(): Long = OfflineCommitAuthority.withCommitAuthorityForEvidence {
        OfflineCommitAuthority.currentGenerationUnderAuthority()
    }

    @Test
    fun evidenceStaysValidAcrossCaptureAndValidationWithoutAnyMutation() {
        // THE self-invalidation regression: capture, then validate through ANY number of
        // further observation holds — no mutation happened, so the certificate must hold.
        val captured = generationNow()
        repeat(3) {
            OfflineCommitAuthority.withCommitAuthorityForEvidence {
                assertEquals(captured, OfflineCommitAuthority.currentGenerationUnderAuthority())
            }
        }
        val validated = OfflineCommitAuthority.withCommitAuthorityForEvidence {
            OfflineCommitAuthority.currentGenerationUnderAuthority()
        }
        assertEquals(captured, validated, "unmodified evidence remains valid across capture/validation")
    }

    @Test
    fun anInterveningRealMutationInvalidatesCapturedEvidence() {
        val captured = generationNow()
        // A REAL coordinated mutation (the writer lever shape every covered writer uses).
        OfflineCommitAuthority.withCommitAuthority { }
        val validated = generationNow()
        assertNotEquals(captured, validated, "an intervening real mutation invalidates the captured evidence")
        assertEquals(captured + 1, validated, "one outermost mutating hold advances the generation exactly once")
    }

    @Test
    fun aNestedMutationInsideAnObservationHoldAdvancesAtTheOuterRelease() {
        val captured = generationNow()
        var inHoldAfterMutation: Long? = null
        OfflineCommitAuthority.withCommitAuthorityForEvidence {
            OfflineCommitAuthority.withCommitAuthority { }
            // The nested mutation is REAL, but its advance happens only when the OUTERMOST
            // hold releases — inside the hold the generation still describes the captured
            // state, and the in-hold fresh reads (not the certificate) judge it.
            inHoldAfterMutation = OfflineCommitAuthority.currentGenerationUnderAuthority()
        }
        assertEquals(captured, inHoldAfterMutation, "the nested mutation's advance waits for the outermost release")
        assertEquals(captured + 1, generationNow(), "the observation hold that NESTED a mutation advances the generation at its release")
    }

    @Test
    fun writerHandoffAcrossThreadsObeyTheSameContract() {
        val captured = generationNow()
        val release = CountDownLatch(1)
        val mutationDone = CountDownLatch(1)
        val writer = Thread {
            try {
                release.await(10, TimeUnit.SECONDS)
                OfflineCommitAuthority.withCommitAuthority { }
            } finally {
                mutationDone.countDown()
            }
        }
        writer.start()
        release.countDown()
        assertTrue(mutationDone.await(10, TimeUnit.SECONDS), "the writer completed")
        writer.join(10_000)
        assertEquals(captured + 1, generationNow(), "another thread's real mutation invalidates this thread's captured evidence")
    }

    @Test
    fun anExceptionInsideAMutatingHoldStillAdvancesTheGeneration() {
        val captured = generationNow()
        val thrown = runCatching {
            OfflineCommitAuthority.withCommitAuthority { error("a mutation that failed mid-way") }
        }
        assertTrue(thrown.isFailure, "the exception propagates")
        assertEquals(captured + 1, generationNow(), "a partially applied mutation is unprovable: the advance is conservative")
    }

    @Test
    fun anExceptionInsideAnObservationHoldDoesNotAdvanceTheGeneration() {
        val captured = generationNow()
        val thrown = runCatching {
            OfflineCommitAuthority.withCommitAuthorityForEvidence { error("a read that failed") }
        }
        assertTrue(thrown.isFailure, "the exception propagates")
        assertEquals(captured, generationNow(), "a read-only hold never advances the generation, even on failure")
    }

    @Test
    fun reentrantMutationsAdvanceOnlyOncePerOutermostHold() {
        val captured = generationNow()
        OfflineCommitAuthority.withCommitAuthority {
            repeat(3) { OfflineCommitAuthority.withCommitAuthority { } }
        }
        assertEquals(captured + 1, generationNow(), "nested participation advances the generation only at the outermost release")
    }

    @Test
    fun concurrentMutatorsNeverLoseAnIncrement() {
        // The former increment ran AFTER lock.unlock(): concurrent releases could interleave
        // the read-modify-write and lose an increment (FHNW-962c P1-2). The increment is now
        // published under the lock.
        val before = generationNow()
        val threads = 8
        val perThread = 64
        val start = CountDownLatch(1)
        val writers = (0 until threads).map {
            Thread {
                start.await(10, TimeUnit.SECONDS)
                repeat(perThread) { OfflineCommitAuthority.withCommitAuthority { } }
            }
        }
        writers.forEach { it.start() }
        start.countDown()
        writers.forEach { it.join(30_000) }
        assertEquals(
            before + threads.toLong() * perThread,
            generationNow(),
            "every outermost mutating hold advances the generation exactly once — no lost increments",
        )
    }

    @Test
    fun theObservationAcquisitionExcludesWritersLikeAnyHold() {
        val writer = Thread {
            OfflineCommitAuthority.withCommitAuthority { }
        }
        OfflineCommitAuthority.withCommitAuthorityForEvidence {
            writer.start()
            // While the observation hold is active, the writer cannot complete its
            // mutation: the modes share the ONE lock.
            writer.join(150)
            assertTrue(writer.isAlive, "the writer is excluded for the whole observation hold")
        }
        writer.join(10_000)
        assertFalse(writer.isAlive, "the writer completes after the hold releases")
    }

    @Test
    fun theQueuedWriterProbeIsAttributableToTheRetainedThreadIdentity() {
        // FHNW-962c P3-7: hasQueuedThreads proves ANY waiter; the identity probe proves THE
        // retained writer reached THIS authority's acquisition.
        var writer: Thread? = null
        OfflineCommitAuthority.withCommitAuthority {
            val queuedWriter = Thread {
                OfflineCommitAuthority.withCommitAuthority { }
            }
            writer = queuedWriter
            queuedWriter.start()
            val deadline = System.currentTimeMillis() + 10_000
            while (!OfflineCommitAuthority.hasQueuedWriter(queuedWriter) && System.currentTimeMillis() < deadline) {
                Thread.sleep(5)
            }
            assertTrue(
                OfflineCommitAuthority.hasQueuedWriter(queuedWriter),
                "the retained writer thread is provably QUEUED at this authority's acquisition",
            )
        }
        writer!!.join(10_000)
        assertFalse(writer!!.isAlive, "the queued writer completes once the hold releases")
    }
}
