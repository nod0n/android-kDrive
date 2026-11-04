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

import com.infomaniak.drive.data.models.UserDrive
import java.util.concurrent.ConcurrentHashMap

/**
 * Process-wide per-file mutation coordination for offline copies (issue: round 4 #4).
 *
 * EVERY owner of a mutable offline copy must hold this lock around its read-decide-write
 * sections: publication (network download and cache promotion), completion flag writes,
 * cleanup/copy-ownership decisions, move reconciliation, upload-source registration and cache
 * deletion. A publisher-only monitor cannot prevent an upload registration or a cleanup from
 * interleaving with a publication; this shared lock can (within one process, which is how the
 * app's workers run).
 */
object OfflineFileCoordination {

    private val locks = ConcurrentHashMap<String, Any>()

    /**
     * Process-wide ROOT-INTENT monitor (issue: round 10 #5): root writers (add, remove,
     * deferred-removal processing) and destructive coverage cleanup hold it continuously
     * through their freshness decisions and byte deletion, so a nested-root addition — whose
     * own per-folder lock no cleanup traversal holds — cannot commit between a clearing
     * transaction and the deletion of a newly covered copy. Lock order is ALWAYS this monitor
     * first, then any per-file/per-root locks, then the Realm transaction.
     */
    val rootIntentMonitor: Any = Any()

    fun lockFor(userDrive: UserDrive, fileId: Int): Any =
        locks.getOrPut("${userDrive.userId}_${userDrive.driveId}_$fileId") { Any() }

    /**
     * Runs [block] while holding the per-file locks of ALL [fileIds], acquired in ASCENDING id
     * order (issue: round 10 #3): graph upserts commit many rows in one transaction, and the
     * coordinated commit boundary must precede it — a publisher could otherwise validate
     * identity and then lose the race to a graph transaction that commits a different name,
     * parent or revision while the publisher still holds its file lock. The ascending order
     * matches the publisher's two-lock scheme and the cleanup's single-lock acquisitions, so
     * no lock cycle is possible. Java monitors are reentrant: paths already holding a subset
     * of these locks nest safely.
     */
    fun <T> withLocksFor(userDrive: UserDrive, fileIds: Collection<Int>, block: () -> T): T {
        val orderedLocks = fileIds.distinct().sorted().map { lockFor(userDrive, it) }
        fun acquire(index: Int): T =
            if (index == orderedLocks.size) block() else synchronized(orderedLocks[index]) { acquire(index + 1) }
        return acquire(0)
    }
}
