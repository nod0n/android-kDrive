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

import androidx.annotation.VisibleForTesting
import java.util.concurrent.locks.ReentrantLock

/**
 * The offline commit authority: ONE process-local lock ordering every app mutation that can
 * change an offline publication/withdrawal decision's predicates — offline intent markers,
 * direct pins, cached parent/tree links, row deletions, listing-driven row refreshes
 * (gallery/recent-changes/activities stores), path persistence, and the offline root
 * registry — against the publication and withdrawal critical sections that read those
 * predicates and then perform their first irreversible filesystem effect (a cache-file
 * move, an atomic rename over the previous copy, a checked byte deletion).
 *
 * Without this exclusion, a predicate writer committing between a critical section's final
 * fresh read and its filesystem effect could invalidate the decision the effect was about
 * to execute (a cached tree move-out during a staged rename, a pin landing before a checked
 * deletion, a gallery store rewriting a row's path mid-hold). With it, exactly two orders
 * exist:
 *  - the WRITER wins: its transaction commits before the section's in-hold fresh authority
 *    check, the check observes the new state, and the section declines BEFORE the first
 *    filesystem effect;
 *  - the PUBLICATION/WITHDRAWAL wins: the effect and its record complete inside the hold,
 *    and the writer commits afterwards, observing the completed operation.
 *
 * SYNCHRONOUS BY DESIGN (RESUME-b528-2212 item 1): the predicate writers are synchronous
 * functions called from arbitrary threads (including the UI thread, which must stay able to
 * commit its pin/offline intent before enqueueing work), so a suspending mutex cannot exclude
 * them without runBlocking. The lock is thread-owned and every critical-section body is
 * NON-suspending (the block type enforces it): no hold ever spans a suspension. In exchange,
 * every hold must stay as short as local IO allows: long byte copies and file digests are
 * computed OUTSIDE the hold; the hold re-verifies the evidence with a cheap stat and
 * conservatively preserves bytes when the evidence went stale. Reentrancy makes a
 * coordinated writer called from inside a section on the same thread (nested participation)
 * safe.
 *
 * Covered writers and sections (RESUME-b528-2212 item 1 audit, extended by
 * RESUME-1189-2245 item 1 / FHNW-1189 A1): FileController's
 * [com.infomaniak.drive.data.cache.FileController.setDirectlyPinned], markFile(s)AsOffline,
 * updateIsOfflineForFiles, updateFile, insertOrUpdateFile (the row-refresh lever),
 * upsertMissingRemoteFile (the missing-root recovery insert), saveRemoteFiles, saveFiles,
 * addChild, addFileTo, upsertActionFile, removeFile, storeRecentChanges,
 * storeFileActivities, storeGalleryDrive, removeOrphanFiles, removeOrphanAndActivityFiles,
 * saveMySharesFiles, savePath, deleteUserDriveFiles; FileListViewModel's
 * updateOfflineFilesIfNeeded direct intent write; OfflineFolderSyncUtils' registry insert,
 * walkSubtree's marking transaction (with its in-transaction registry re-check), the
 * staged/accepted publication seams, the unmark withdrawal and its sweeps, the maintenance
 * pass's union discharge, the missing-root resolution, and the bulk chain's cache-move
 * publication seam. PRESENCE evidence, not a completed per-caller lock-order audit (FHNW-962c
 * qualification): the final acceptance retains the actual per-caller coverage.
 *
 * MUTATION GENERATION (RESUME-1189-2245 item 4 / FHNW-1189 A5+C, REDEFINED by RESUME-962c
 * item 1 / FHNW-962c P1-1): the generation certifies ACTUAL MUTATIONS, never acquisitions.
 * Two acquisition modes share the one lock:
 *  - [withCommitAuthority] — the MUTATING acquisition, used by every writer and every
 *    critical section that performs a state change or filesystem effect. The generation
 *    advances ONCE per outermost hold that contained at least one mutating acquisition
 *    (nested/reentrant mutations advance it only when the outermost hold releases), and the
 *    increment is published BEFORE the lock is released, so a queued operation can never
 *    enter and read a pre-increment value and concurrent releases can never lose an
 *    increment (the FHNW-962c P1-2 post-unlock race). An exception inside a mutating block
 *    still advances the generation: a partially applied mutation is unprovable, and
 *    over-invalidation only costs a conservative retry/preserve.
 *  - [withCommitAuthorityForEvidence] — the OBSERVATION acquisition, used by the evidence
 *    capture and evidence validation holds. It NEVER advances the generation by itself: the
 *    certificate's whole point is that unmodified evidence stays VALID across the
 *    capture→digest→validate sequence even though each step takes its own hold. A mutation
 *    NESTED inside an observation hold still advances the generation at the outermost
 *    release (the hold was not mutation-free).
 * THE CERTIFICATE CONTRACT: evidence (a byte stat, a row snapshot, a derived path) is
 * captured TOGETHER with [currentGenerationUnderAuthority] under an observation hold; the
 * effect's hold re-reads the generation and treats any CHANGE as invalidated evidence
 * (preserve/retry). Under these semantics the change happens exactly when a real mutation
 * committed in between — the capture's own release, the validation's own acquisition, and
 * any number of intermediate observation holds leave it untouched. No call site compares
 * against a hand-computed "+1"; the comparison is always capture-value vs. re-read value.
 *
 * REAL LIMITS (documented, not claimed away): writers OUTSIDE this process — other apps
 * editing the offline bytes, adb, other processes — never participate, exactly as they never
 * participated in any in-process lock; and because the whole-file digests stay outside the
 * holds, a non-participating SAME-STAT byte edit (same length, same second-rounded mtime)
 * that lands during the evidence-to-effect span — which now includes the unbounded wait for
 * the lock itself, and equally the destination-content evidence of the cache-move seam — is
 * invisible to the stat re-check: the generation certifies only PARTICIPATING mutations, and
 * this external same-stat window is a disclosed platform-level limitation, not a solved one.
 * A crash or power loss between the filesystem effect and the record can still split the
 * pair (such bytes classify as UNKNOWN and are preserved by every later decision); the
 * filesystem and Realm are not one atomic resource. LATENCY COUPLING: a hold that waits on
 * (or runs) a Realm transaction serializes against every OTHER feature's write transactions
 * on the same realm file, and synchronous UI-thread writers wait on the fair lock for the
 * whole hold — the pass-wide release and the unmark closer batch their row work into few
 * transactions for exactly this reason, but a hold spanning many released rows is still
 * bounded only by that row count (disclosed).
 */
object OfflineCommitAuthority {

    /** Fair, so a stream of publications cannot starve a pending writer indefinitely. */
    private val lock = ReentrantLock(true)

    /**
     * The mutation generation: advanced once per outermost hold that contained at least one
     * MUTATING acquisition, before that hold releases the lock. Guarded by the lock — read
     * it only while holding the authority (either mode).
     */
    private var generation = 0L

    /**
     * Whether the current outermost hold has seen at least one mutating acquisition. Only
     * ever touched while holding the lock (there is exactly one holder), so no further
     * synchronization is needed.
     */
    private var outermostHoldMutated = false

    /**
     * Runs [block] while excluding every other participating writer and critical section,
     * as a MUTATING acquisition: the mutation generation advances when this hold's outermost
     * release happens (once per outermost hold, however many nested mutations it contained).
     * The block MUST NOT suspend (it is a plain function type: the compiler rejects suspend
     * calls inside), MUST NOT dispatch work to other threads whose completion it awaits, and
     * MUST stay local-IO sized (no network transfers, no whole-file digests of unbounded
     * inputs — compute those before acquiring).
     */
    fun <R> withCommitAuthority(block: () -> R): R {
        lock.lock()
        outermostHoldMutated = true
        return try {
            block()
        } finally {
            val outermost = lock.holdCount == 1
            if (outermost) {
                // Published UNDER the lock, before unlock (FHNW-962c P1-2): a queued
                // operation acquires only after this increment is visible, and concurrent
                // releases cannot interleave the read-modify-write.
                generation += 1
                outermostHoldMutated = false
            }
            lock.unlock()
        }
    }

    /**
     * Runs [block] while excluding every other participating writer and critical section, as
     * an OBSERVATION acquisition for evidence capture and evidence validation: read-only
     * state certification that NEVER advances the mutation generation by itself — unmodified
     * evidence stays valid across any number of observation holds. The same body contract as
     * [withCommitAuthority] applies (no suspension, no awaited dispatch, local-IO sized); a
     * mutation NESTED inside (a coordinated writer called on this thread) still advances the
     * generation at the outermost release.
     */
    fun <R> withCommitAuthorityForEvidence(block: () -> R): R {
        lock.lock()
        return try {
            block()
        } finally {
            if (lock.holdCount == 1 && outermostHoldMutated) {
                // A mutating acquisition was nested inside this observation hold: the hold
                // was not mutation-free, so previously captured evidence is invalidated.
                generation += 1
                outermostHoldMutated = false
            }
            lock.unlock()
        }
    }

    /**
     * The mutation generation to capture WITH pre-hold evidence and re-check at the effect's
     * acquisition: a real participating mutation that committed in between advanced it, and
     * the effect must then treat its evidence as invalidated (preserve/retry). MUST be called
     * while holding the authority (either mode) — a generation read outside the lock
     * certifies nothing.
     */
    fun currentGenerationUnderAuthority(): Long {
        check(lock.isHeldByCurrentThread) {
            "the mutation generation is only meaningful while holding the commit authority"
        }
        return generation
    }

    /** Test evidence: true while some thread holds the authority. */
    @VisibleForTesting
    internal val isHeld: Boolean get() = lock.isLocked

    /**
     * Test evidence: a non-blocking probe from ANOTHER thread — false exactly while the
     * authority is held elsewhere, true when it is free. Never use in production logic.
     */
    @VisibleForTesting
    internal fun tryProbeFromOtherThread(): Boolean = lock.tryLock().also { if (it) lock.unlock() }

    /**
     * Test evidence: true when at least one thread is currently WAITING to acquire the
     * authority. This is the positive-contention observation for the coordination proofs:
     * it shows THAT writer (the one the test launched at its real entry point) reached the
     * real acquisition while the hold was active — a held-lock probe alone does not show
     * which writer contended. Never use in production logic.
     */
    @VisibleForTesting
    internal fun hasQueuedWriter(): Boolean = lock.hasQueuedThreads()

    /**
     * Test evidence (FHNW-962c P3-7): true when the SPECIFIC [writer] thread is currently
     * WAITING to acquire the authority — [hasQueuedThreads] proves only that SOME thread
     * waits, which unrelated instrumentation or background activity could satisfy. The
     * coordination proofs use the retained writer identity so the queue observation is
     * attributable. Never use in production logic.
     */
    @VisibleForTesting
    internal fun hasQueuedWriter(writer: Thread): Boolean = lock.hasQueuedThread(writer)
}
