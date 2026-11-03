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

import io.realm.RealmObject
import io.realm.annotations.PrimaryKey
import java.util.Date

/**
 * Durable local record of a folder explicitly marked as "available offline" by the user (issue #1902).
 *
 * The root intent is stored so that descendants of the folder keep being downloaded and reconciled
 * (new remote files, remote deletions) even across app restarts, independently of the current
 * per-file [File.isOffline] pins. It is stored in the per-userDrive Realm database, which scopes
 * it to a single account/drive boundary.
 *
 * Removing a root never deletes files from the user's shared storage: it only clears the offline
 * copies maintained by this app, and never touches explicitly pinned files or files still covered
 * by another overlapping root.
 */
open class OfflineRoot(
    @PrimaryKey
    var uid: String = "", // "{folderId}_{driveId}"
    var folderId: Int = 0,
    var driveId: Int = 0,
    var userId: Int = 0,
    var name: String = "",
    var path: String = "",
    var createdAt: Date = Date(),
    var lastSyncedAt: Date? = null,
) : RealmObject() {

    fun initUid() {
        uid = "${folderId}_$driveId"
    }

    companion object {
        /**
         * A root whose last completed walk is older than this delay is considered stale and gets
         * re-walked by the offline sync to pick up remote additions/deletions.
         */
        const val STALE_SYNC_DELAY_MS: Long = 15 * 60 * 1_000L
    }
}
