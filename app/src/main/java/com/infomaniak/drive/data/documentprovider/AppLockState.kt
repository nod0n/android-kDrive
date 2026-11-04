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
        publish = { enabled -> AppSettings.publishAppSecurityLock(enabled) },
        onTransition = { onTransition?.invoke() },
    )

    // Deliberately no class-initialization-time reference to AppSettings here: this object is
    // loaded during CloudStorageProvider's companion initialization, which runs before the
    // provider's onCreate() has called initRealm() — touching the Realm-backed settings
    // singleton at that point would build its RealmConfiguration before Realm.init and fail
    // startup (Astra review R6-1). The coordinator itself is Realm-free, and AppSettings'
    // ordinary writers reach it through the registration facade below at write time.

    /** Registers an ordinary AppSettings.realm write with the maintenance drain; see
     * [AppLockTransitions.registerSettingsWrite]. Returns false while maintenance is active. */
    fun registerSettingsWrite(): Boolean = transitions.registerSettingsWrite()

    /** Releases a registered ordinary settings write exactly once, on job completion. */
    fun unregisterSettingsWrite() = transitions.unregisterSettingsWrite()

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
     * Runs [block] after all queued transition tasks have completed and with new requests
     * refused; the block suspends on the caller's coroutine inside the exclusion; see
     * [AppLockTransitions.runWithWriterQuiesced].
     */
    suspend fun <T> runWithWriterQuiesced(block: suspend () -> T): T = transitions.runWithWriterQuiesced(block)

    /** Maintenance-only reset; see [AppLockTransitions.resetToDisabledInternal]. */
    fun resetToDisabledInternal(): Boolean = transitions.resetToDisabledInternal()

    /** Maintenance reconcile; see [AppLockTransitions.reconcileAfterMaintenance]. */
    fun reconcileAfterMaintenance(durableEnabled: Boolean) = transitions.reconcileAfterMaintenance(durableEnabled)

    /** Retains the unresolved-maintenance denial; see [AppLockTransitions.markMaintenanceUnresolved]. */
    fun markMaintenanceUnresolved() = transitions.markMaintenanceUnresolved()

    /** Durable store value; null when the store cannot be read (maintenance reconciliation). */
    fun readDurableAppLockEnabled(): Boolean? =
        runCatching { AppSettings.getAppSettings()._appSecurityEnabled }.getOrNull()

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
