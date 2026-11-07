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
 * direct pins, cached parent/tree links, row deletions, and the offline root registry —
 * against the publication and withdrawal critical sections that read those predicates and
 * then perform their first irreversible filesystem effect (a cache-file move, an atomic
 * rename over the previous copy, a checked byte deletion).
 *
 * Without this exclusion, a predicate writer committing between a critical section's final
 * fresh read and its filesystem effect could invalidate the decision the effect was about
 * to execute (a cached tree move-out during a staged rename, a pin landing before a checked
 * deletion). With it, exactly two orders exist:
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
 * every hold must stay local-IO sized: long byte copies and file digests are computed
 * OUTSIDE the hold; the hold re-verifies the evidence with a cheap stat and conservatively
 * preserves bytes when the evidence went stale. Reentrancy makes a coordinated writer called
 * from inside a section on the same thread (nested participation) safe.
 *
 * Covered writers and sections (RESUME-b528-2212 item 1 audit): FileController's
 * [com.infomaniak.drive.data.cache.FileController.setDirectlyPinned], markFile(s)AsOffline,
 * updateIsOfflineForFiles, updateFile, insertOrUpdateFile (the row refresh lever),
 * saveRemoteFiles, saveFiles, addChild, addFileTo, upsertActionFile, removeFile;
 * FileListViewModel.updateOfflineFilesIfNeeded's direct intent write; OfflineFolderSyncUtils'
 * registry insert, walkSubtree's marking transaction (with its in-transaction registry
 * re-check), the staged/accepted publication seams, the unmark withdrawal and its sweeps,
 * the maintenance pass's union discharge, and the bulk chain's cache-move publication seam.
 *
 * REAL LIMITS (documented, not claimed away): writers OUTSIDE this process — other apps
 * editing the offline bytes, adb, other processes — never participate, exactly as they never
 * participated in any in-process lock; a crash or power loss between the filesystem effect
 * and the record can still split the pair (such bytes classify as UNKNOWN and are preserved
 * by every later decision); the filesystem and Realm are not one atomic resource. What this
 * lock closes is the app-controlled writer window: no participating app mutation can commit
 * between a section's final evidence read and its first irreversible filesystem effect.
 */
object OfflineCommitAuthority {

    /** Fair, so a stream of publications cannot starve a pending writer indefinitely. */
    private val lock = ReentrantLock(true)

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
            lock.unlock()
        }
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
}
