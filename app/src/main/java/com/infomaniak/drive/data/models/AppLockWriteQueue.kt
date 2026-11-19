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
package com.infomaniak.drive.data.models

import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/** Serializes successful UI lock requests through commit, cache update and notification. */
internal class AppLockWriteQueue(private val scope: CoroutineScope) {
    private val mutex = Mutex()

    // Enter/queue for the fair mutex during the call, before Default can reorder
    // launches. Keep ownership until publication and its notification finish.
    fun submit(writeAndPublish: suspend () -> Unit) = scope.launch(start = CoroutineStart.UNDISPATCHED) {
        ensureActive()
        mutex.withLock { writeAndPublish() }
    }
}
