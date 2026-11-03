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

    fun lockFor(userDrive: UserDrive, fileId: Int): Any =
        locks.getOrPut("${userDrive.userId}_${userDrive.driveId}_$fileId") { Any() }
}
