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
 * updateIsOfflineForFiles, updateFile, insertOrUpdateFile (the row refresh lever),
 * saveRemoteFiles, saveFiles, addChild, addFileTo, upsertActionFile, removeFile,
 * storeRecentChanges, storeFileActivities, storeGalleryDrive, removeOrphanFiles,
 * removeOrphanAndActivityFiles, saveMySharesFiles, savePath, deleteUserDriveFiles;
 * FileListViewModel.updateOfflineFilesIfNeeded's direct intent write; OfflineFolderSyncUtils'
 * registry insert, walkSubtree's marking transaction (with its in-transaction registry
 * re-check), the staged/accepted publication seams, the unmark withdrawal and its sweeps,
 * the maintenance pass's union discharge, the missing-root resolution, and the bulk chain's
 * cache-move publication seam.
 *
 * MUTATION GENERATION (RESUME-1189-2245 item 4 / FHNW-1189 A5/C): every outermost
 * acquisition advances [currentGenerationUnderAuthority]. Pre-hold evidence (a digest of
 * shared bytes, a row snapshot, a derived path) is captured together with the generation
 * under one short hold; the effect's hold re-reads the generation and treats any change as
 * invalidated evidence (preserve/retry). This closes the participating-mutation class
 * between evidence and acquisition WITHOUT moving an unbounded digest into a hold.
 *
 * REAL LIMITS (documented, not claimed away): writers OUTSIDE this process — other apps
 * editing the offline bytes, adb, other processes — never participate, exactly as they never
 * participated in any in-process lock; and because the whole-file digests stay outside the
 * holds, a non-participating SAME-STAT byte edit (same length, same second-rounded mtime)
 * that lands during the evidence-to-effect span — which now includes the unbounded wait for
 * the lock itself — is invisible to the stat re-check: the generation certifies only
 * PARTICIPATING mutations, and this external same-stat window is a disclosed platform-level
 * limitation, not a solved one. A crash or power loss between the filesystem effect and the
 * record can still split the pair (such bytes classify as UNKNOWN and are preserved by every
 * later decision); the filesystem and Realm are not one atomic resource. LATENCY COUPLING:
 * a hold that waits on (or runs) a Realm transaction serializes against every OTHER
 * feature's write transactions on the same realm file, and synchronous UI-thread writers
 * wait on the fair lock for the whole hold — the pass-wide release and the unmark closer
 * batch their row work into few transactions for exactly this reason, but a hold spanning
 * many released rows is still bounded only by that row count (disclosed).
 */
object OfflineCommitAuthority {

    /** Fair, so a stream of publications cannot starve a pending writer indefinitely. */
    private val lock = ReentrantLock(true)

    /**
     * The mutation generation: advanced once per OUTERMOST acquisition (nested/reentrant
     * participation advances it only when the outermost section releases). Guarded by the
     * lock — read it only under [withCommitAuthority].
     */
    private var generation = 0L

    /**
     * Runs [block] while excluding every other participating writer and critical section.
     * The block MUST NOT suspend (it is a plain function type: the compiler rejects suspend
     * calls inside), MUST NOT dispatch work to other threads whose completion it awaits, and
     * MUST stay local-IO sized (no network transfers, no whole-file digests of unbounded
     * inputs — compute those before acquiring).
     */
    fun <R> withCommitAuthority(block: () -> R): R {
        lock.lock()
        return try {
            block()
        } finally {
            val depth = lock.holdCount
            lock.unlock()
            if (depth == 1) generation += 1
        }
    }

    /**
     * The mutation generation to capture WITH pre-hold evidence and re-check at the effect's
     * acquisition: any participating mutation that committed in between advanced it, and the
     * effect must then treat its evidence as invalidated (preserve/retry). MUST be called
     * while holding the authority — a generation read outside the lock certifies nothing.
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
}
