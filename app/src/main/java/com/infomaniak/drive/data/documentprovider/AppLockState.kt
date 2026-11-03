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
import java.util.concurrent.Executors

/**
 * Storage-aware, thread-safe source of truth for the app-lock gate of [CloudStorageProvider].
 *
 * [AppSettings.appSecurityLock] is an in-memory cached value backed by an asynchronous store
 * write: its reads observe neither concurrent changes nor the durable store, and an enable could
 * be lost if the process died before the queued transaction committed. The provider gate therefore
 * consults this object instead:
 *  - every read consults the durable store and denies when the store cannot be read,
 *  - changes are serialized on a single writer thread and a transition denies access from the
 *    moment it is requested until it is durably committed (an enable denies while pending; a
 *    disable keeps denying until the store actually carries the disabled value),
 *  - transitions are announced through [onTransition] so pending provider work can be invalidated
 *    and observers notified.
 *
 * If persistence fails, the pending denial is kept until a later successful request: the gate
 * never opens on the strength of an uncommitted value.
 */
object AppLockState {

    private val stateLock = Any()
    private val writeExecutor = Executors.newSingleThreadExecutor { runnable ->
        Thread(runnable, "AppLockState-writer")
    }

    /** One-shot hook invoked whenever the effective policy may have changed. */
    @Volatile
    var onTransition: (() -> Unit)? = null

    /** Latest requested-but-not-yet-durable target value, if any. Guarded by [stateLock]. */
    private var pendingTarget: Boolean? = null

    /** Authoritative app-lock state for the provider gate. */
    fun isAppLockActive(): Boolean = Policy.isActive(
        pendingTarget = synchronized(stateLock) { pendingTarget },
        persistedValue = runCatching { AppSettings.getAppSettings()._appSecurityEnabled }.getOrNull(),
    )

    /**
     * Requests a change of the app-lock setting. External access is denied from this call until
     * the new value is durably persisted (or, within this process, until a later request succeeds,
     * if persistence fails).
     */
    fun requestAppLock(enabled: Boolean) {
        synchronized(stateLock) { pendingTarget = enabled }
        onTransition?.invoke()
        writeExecutor.execute {
            val persisted = AppSettings.updateAppSecurityLock(enabled)
            if (persisted) {
                synchronized(stateLock) { if (pendingTarget == enabled) pendingTarget = null }
                onTransition?.invoke()
            }
        }
    }

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
