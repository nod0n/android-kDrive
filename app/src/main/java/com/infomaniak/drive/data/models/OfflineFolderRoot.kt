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
 * Durable local record of a folder explicitly marked as "available offline (recursive)" by the
 * user. Descendants keep being downloaded and reconciled on later sync passes, across restarts,
 * independently of individual [File.isDirectlyPinned] pins. Scoped to one account/drive (the
 * per-drive File realm also encodes that boundary).
 */
open class OfflineFolderRoot(
    @PrimaryKey var uid: String = "", // "{folderId}_{driveId}_{userId}"
    var folderId: Int = -1,
    var driveId: Int = -1,
    var userId: Int = -1,
    var markedAt: Date = Date(),
) : RealmObject() {

    fun initUid() {
        uid = buildUid(folderId, driveId, userId)
    }

    companion object {
        fun buildUid(folderId: Int, driveId: Int, userId: Int) = "${folderId}_${driveId}_$userId"
    }
}
