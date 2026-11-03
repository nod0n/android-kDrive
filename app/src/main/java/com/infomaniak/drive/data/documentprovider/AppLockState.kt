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

import com.infomaniak.drive.data.models.AppSettings
import kotlinx.coroutines.CompletableDeferred

/**
 * Storage-aware, thread-safe source of truth for the app-lock gate of [CloudStorageProvider].
 *
 * [AppSettings.appSecurityLock] is an in-memory published value (updated only after the durable
 * commit) for UI readers; it must not be consulted by the gate. The gate goes through this
 * object, which wires [AppLockTransitions] to the Realm-backed settings store:
 *  - every read consults the durable store and denies when the store cannot be read,
 *  - transitions are serialized, generation-tagged and deny access from request until durable
 *    commit (see [AppLockTransitions]),
 *  - transitions are announced through [onTransition] so pending provider work can be
 *    invalidated and observers notified.
 */
object AppLockState {

    /** One-shot hook invoked whenever the effective policy may have changed. */
    @Volatile
    var onTransition: (() -> Unit)? = null

    private val transitions = AppLockTransitions(
        persist = { enabled -> AppSettings.updateAppSecurityLock(enabled) },
        onTransition = { onTransition?.invoke() },
    )

    /** Authoritative app-lock state for the provider gate. */
    fun isAppLockActive(): Boolean {
        if (transitions.isPending()) return true
        return Policy.isActive(
            pendingTarget = null,
            persistedValue = runCatching { AppSettings.getAppSettings()._appSecurityEnabled }.getOrNull(),
        )
    }

    /**
     * Requests a change of the app-lock setting. External access is denied from this call until
     * the new value is durably persisted (or, within this process, until a later request
     * succeeds, if persistence fails). See [AppLockTransitions.request] for [onResult].
     */
    fun requestAppLock(enabled: Boolean, onResult: ((Boolean) -> Unit)? = null): Long =
        transitions.request(enabled, onResult)

    /**
     * Requests a change of the app-lock setting and suspends until it has fully completed:
     * returns true only when the requested value became the durable one (false on persist
     * failure, supersession, or refusal while the writer is under maintenance).
     */
    suspend fun requestAppLockAwait(enabled: Boolean): Boolean {
        val result = CompletableDeferred<Boolean>()
        transitions.request(enabled) { persisted -> result.complete(persisted) }
        return result.await()
    }

    /**
     * Runs [block] after all queued transition tasks have completed and with new requests
     * refused; see [AppLockTransitions.runWithWriterQuiesced].
     */
    suspend fun <T> runWithWriterQuiesced(block: () -> T): T = transitions.runWithWriterQuiesced(block)

    /** Shared transition boundary for final authorization/issuance (see [AppLockTransitions]). */
    fun <T> withAdmission(block: () -> T): T = transitions.withAdmission(block)

    /** Registers/unregisters work admitted under the boundary; transitions cancel it. */
    fun registerAdmittedWork(work: AppLockTransitions.AdmittedWork) = transitions.registerAdmittedWork(work)

    fun unregisterAdmittedWork(work: AppLockTransitions.AdmittedWork) = transitions.unregisterAdmittedWork(work)

    /** Current policy generation, for tagging retained state such as cursors. */
    fun policyGeneration(): Long = transitions.policyGeneration()

    /** Pure decision core, unit-testable without Android or Realm. */
    object Policy {

        /**
         * @param pendingTarget target of a requested-but-not-durable transition, if any
         * @param persistedValue durable store value; null when the store could not be read
         * @return true while app lock must be treated as active (access denied)
         */
        fun isActive(pendingTarget: Boolean?, persistedValue: Boolean?): Boolean = when {
            pendingTarget != null -> true // deny while any transition is in flight, either direction
            persistedValue == null -> true // deny when the durable store cannot be read
            else -> persistedValue
        }
    }
}
