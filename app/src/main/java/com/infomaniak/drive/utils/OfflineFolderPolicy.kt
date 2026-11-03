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

import com.infomaniak.drive.data.models.OfflineRoot

/**
 * Pure decision helpers for the recursive offline folders feature (issue #1902).
 *
 * Everything here takes plain values so it can be unit tested without Realm, Android or network.
 */
object OfflineFolderPolicy {

    /**
     * Whether [fileId] must keep its offline copy when an offline root is removed.
     *
     * Explicit pins ([isOffline]) are always kept: removing a folder must never strip a file the
     * user pinned separately. Files still covered by another overlapping offline root keep their
     * copy too. Finally, a copy backing a pending upload (an offline edit not yet sent to the
     * server) is kept so removing a folder cannot lose the user's work.
     */
    fun shouldKeepOfflineCopyOnRootRemoval(
        isOffline: Boolean,
        isCoveredByOtherRoot: Boolean,
        hasPendingUpload: Boolean,
    ): Boolean {
        return isOffline || isCoveredByOtherRoot || hasPendingUpload
    }

    /**
     * Whether [fileId] should be queued for download during an offline folder walk.
     *
     * A file already covered by an explicit pin and intact doesn't need the folder coverage to
     * download it again; anything without an intact offline copy does (the download engine then
     * skips files it can resolve from an intact cache).
     */
    fun needsDownload(
        isOffline: Boolean,
        hasIntactOfflineCopy: Boolean,
    ): Boolean {
        return !isOffline || !hasIntactOfflineCopy
    }

    /**
     * Whether a root needs a (re)walk by the offline sync: either some of its files are still
     * pending download (crashed/interrupted walk) or its last completed walk is stale, meaning
     * remote additions/deletions may not be reflected yet.
     */
    fun needsSync(
        hasPendingFiles: Boolean,
        lastSyncedAtMillis: Long?,
        nowMillis: Long,
    ): Boolean {
        if (hasPendingFiles) return true
        if (lastSyncedAtMillis == null) return true
        return nowMillis - lastSyncedAtMillis > OfflineRoot.STALE_SYNC_DELAY_MS
    }

    /**
     * Whether a covered file that has disappeared from a fully listed remote folder must have its
     * offline copy removed (remote deletion or move out of the folder). Never true for explicit
     * pins: those are reconciled by the per-file offline sync, not by the folder walk.
     */
    fun shouldRemoveCoveredFileAbsentFromRemote(
        isOffline: Boolean,
        isInOfflineFolder: Boolean,
    ): Boolean {
        return !isOffline && isInOfflineFolder
    }
}
