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
package com.infomaniak.drive.ui.menu.settings

import com.infomaniak.drive.data.documentprovider.AppLockState
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import java.util.concurrent.atomic.AtomicInteger

/**
 * Process-wide observable state of an app-lock change request. Pending and completion states
 * live here, not in an activity instance, so every screen that observes it reconciles with the
 * transition — including instances opened while a persistence is still running (R3-5).
 */
object AppLockRequest {

    sealed interface UiState {
        data object Idle : UiState

        /** A change is being persisted; the interactive row must stay disabled. */
        data class Pending(val shouldLock: Boolean) : UiState

        /**
         * The change reached a durable outcome. [persisted] is false when it did not persist
         * (failure, supersession, or refusal while the writer is under maintenance).
         */
        data class Completed(val requestId: Int, val shouldLock: Boolean, val persisted: Boolean) : UiState
    }

    private val _state = MutableStateFlow<UiState>(UiState.Idle)
    val state: StateFlow<UiState> = _state.asStateFlow()

    private val nextRequestId = AtomicInteger(1)

    /** Starts a change; overlapping requests are ignored while one is pending. */
    fun request(shouldLock: Boolean) {
        if (_state.value is UiState.Pending) return
        val requestId = nextRequestId.getAndIncrement()
        // Publish pending before enqueueing: the writer's completion callback can run as soon
        // as the request is enqueued and must never be overwritten by the pending state.
        _state.value = UiState.Pending(shouldLock)
        AppLockState.requestAppLock(shouldLock) { persisted ->
            _state.value = UiState.Completed(requestId, shouldLock, persisted)
        }
    }

    /** Clears a delivered [UiState.Completed]; later screens reconcile from the durable value. */
    fun markDelivered() {
        if (_state.value is UiState.Completed) _state.value = UiState.Idle
    }
}
