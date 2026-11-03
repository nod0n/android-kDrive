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

import kotlinx.coroutines.CompletableDeferred
import java.util.concurrent.Executor
import java.util.concurrent.Executors
import java.util.concurrent.locks.ReentrantReadWriteLock
import kotlin.concurrent.read
import kotlin.concurrent.write

/**
 * Ordered app-lock transition machine shared by the settings UI and the provider gate.
 *
 * Guarantees (see findings §9):
 *  - Every request is identified by a monotonically increasing generation; completion never
 *    matches by value, so an older completed write cannot clear a newer pending request.
 *  - Requests are registered and enqueued in the same order; only the latest requested
 *    generation persists. Older queued requests are skipped (superseded), so a stale write can
 *    neither overwrite later state nor resurrect it after a settings-file deletion.
 *  - A transition denies access from the moment it is requested until the latest request is
 *    durably committed; a failed latest persist keeps the pending denial.
 *  - [withAdmission] is the shared boundary: final authorization and issuance (descriptor/pipe
 *    creation, cursor publication, mutation dispatch decision) run atomically against
 *    transitions, which take the same lock exclusively. No I/O is performed under the lock on
 *    either side beyond the fast local reads inside [withAdmission] callers' blocks.
 *
 * [persist] and [writer] are injectable so tests can exercise queue ordering and failures
 * without Android or Realm.
 */
class AppLockTransitions(
    private val persist: (Boolean) -> Boolean,
    private val onTransition: () -> Unit = {},
    private val writer: Executor = defaultWriter(),
) {

    private val admission = ReentrantReadWriteLock()

    /** Monotonically increasing id of the latest requested transition. */
    private var latestRequestedGeneration = 0L

    /** Generation whose persistence is still owed, if any. */
    private var pendingGeneration: Long? = null

    /** Bumped on every observable policy state change (request registered, commit attempted). */
    private var policyGeneration = 0L

    /** In-flight work admitted under the boundary; cancelled by any later transition. */
    private val admittedWork = HashSet<AdmittedWork>()

    /** Guards [admittedWork]; acquired inside admission sections and by transitions. */
    private val workLock = Any()

    /** Transition tasks enqueued but not yet fully completed; the writer drains them in order. */
    private var outstandingTasks = 0

    /** Completed whenever [outstandingTasks] returns to zero; replaced when it leaves zero. */
    private var quiescenceSignal: CompletableDeferred<Unit> = CompletableDeferred(Unit).apply { complete(Unit) }

    /** True while a maintenance section (e.g. logout settings deletion) excludes the writer. */
    private var maintenanceActive = false

    /**
     * Registers work whose network issuance was admitted under the current policy. A later
     * transition cancels it, so network I/O for admitted mutations only proceeds while no
     * transition has occurred (or best-effort aborts one already on the wire).
     */
    fun registerAdmittedWork(work: AdmittedWork) = synchronized(workLock) { admittedWork.add(work) }

    fun unregisterAdmittedWork(work: AdmittedWork) = synchronized(workLock) { admittedWork.remove(work) }

    private fun cancelAdmittedWork() = synchronized(workLock) {
        admittedWork.forEach { work -> runCatching { work.cancel() } }
        admittedWork.clear()
    }

    /** Minimal handle for cancellable work admitted under the boundary. */
    fun interface AdmittedWork {
        fun cancel()
    }

    /** Runs [block] atomically against transitions; the final authorization/issuance boundary. */
    fun <T> withAdmission(block: () -> T): T = admission.read { block() }

    /** True while a requested transition has not durably completed. */
    fun isPending(): Boolean = admission.read { pendingGeneration != null }

    /** Current policy generation; cursors and in-flight work tag themselves with it. */
    fun policyGeneration(): Long = admission.read { policyGeneration }

    /**
     * Requests a change of the app-lock setting. Returns the request generation. [onResult] is
     * invoked exactly once (on the writer thread): true when the requested value became the
     * durable one, false when the persist failed or the request was superseded before persisting.
     */
    fun request(enabled: Boolean, onResult: ((Boolean) -> Unit)? = null): Long {
        // Register the generation and its pending marker atomically: request() is the only
        // writer here, but both fields must flip under one exclusive acquisition regardless.
        val generation: Long? = admission.write {
            if (maintenanceActive) {
                null // the writer is excluded (e.g. logout cleanup); refuse rather than queue
            } else {
                policyGeneration += 1
                pendingGeneration = ++latestRequestedGeneration
                outstandingTasks += 1
                if (outstandingTasks == 1) quiescenceSignal = CompletableDeferred()
                latestRequestedGeneration
            }
        }
        if (generation == null) {
            onResult?.invoke(false)
            return -1L
        }
        cancelAdmittedWork()
        onTransition()
        writer.execute { complete(generation, enabled, onResult) }
        return generation
    }

    private fun complete(generation: Long, enabled: Boolean, onResult: ((Boolean) -> Unit)?) {
        val isLatest = admission.read { generation == latestRequestedGeneration }
        try {
            if (!isLatest) {
                // A newer request supersedes this one: skip persisting so an older value can never
                // overwrite or resurrect later state (e.g. a logout reset racing a queued enable).
                onResult?.invoke(false)
                return
            }

            val persisted = persist(enabled)
            admission.write {
                policyGeneration += 1
                if (pendingGeneration == generation && persisted) pendingGeneration = null
            }
            cancelAdmittedWork()
            onTransition()
            onResult?.invoke(persisted)
        } finally {
            admission.write {
                outstandingTasks -= 1
                if (outstandingTasks == 0) quiescenceSignal.complete(Unit)
            }
        }
    }

    /**
     * Runs [block] after every queued transition task has fully completed (persist included),
     * while new requests are refused for the duration. Used to exclude settings-file deletion
     * from queued or running security writers; failure of the maintenance work itself is the
     * caller's to handle before declaring its lifecycle step complete.
     */
    suspend fun <T> runWithWriterQuiesced(block: () -> T): T {
        admission.write { maintenanceActive = true }
        try {
            quiescenceSignal.await()
            return block()
        } finally {
            admission.write { maintenanceActive = false }
        }
    }

    private companion object {
        fun defaultWriter(): Executor = Executors.newSingleThreadExecutor { runnable ->
            Thread(runnable, "AppLockState-writer").apply { isDaemon = true }
        }
    }
}
