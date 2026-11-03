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

package com.infomaniak.drive

import com.infomaniak.drive.utils.OfflineFolderPolicy
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * Unit tests for the pure decision logic of the recursive offline folders (issue #1902).
 *
 * NOT RUN: per owner override of 2026-11-03, no test execution is allowed yet (a separate test
 * user must be created first). These tests are JVM-only (no Realm, no Android framework) and are
 * expected to run with: ./gradlew app:testStandardDebugUnitTest --tests "com.infomaniak.drive.OfflineFolderPolicyTest"
 */
class OfflineFolderPolicyTest {

    @Test
    fun `root removal keeps explicitly pinned files`() {
        assertTrue(
            OfflineFolderPolicy.shouldKeepOfflineCopyOnRootRemoval(
                isOffline = true,
                isCoveredByOtherRoot = false,
                hasPendingUpload = false,
            )
        )
    }

    @Test
    fun `root removal keeps files covered by another overlapping root`() {
        assertTrue(
            OfflineFolderPolicy.shouldKeepOfflineCopyOnRootRemoval(
                isOffline = false,
                isCoveredByOtherRoot = true,
                hasPendingUpload = false,
            )
        )
    }

    @Test
    fun `root removal keeps offline copies backing a pending upload`() {
        assertTrue(
            OfflineFolderPolicy.shouldKeepOfflineCopyOnRootRemoval(
                isOffline = false,
                isCoveredByOtherRoot = false,
                hasPendingUpload = true,
            )
        )
    }

    @Test
    fun `root removal strips plain covered files`() {
        assertFalse(
            OfflineFolderPolicy.shouldKeepOfflineCopyOnRootRemoval(
                isOffline = false,
                isCoveredByOtherRoot = false,
                hasPendingUpload = false,
            )
        )
    }

    @Test
    fun `covered file without intact offline copy needs download`() {
        assertTrue(
            OfflineFolderPolicy.needsDownload(isOffline = false, hasIntactOfflineCopy = false)
        )
    }

    @Test
    fun `covered file with intact offline copy does not need download`() {
        assertFalse(
            OfflineFolderPolicy.needsDownload(isOffline = false, hasIntactOfflineCopy = true)
        )
    }

    @Test
    fun `pinned file with intact offline copy is not re-downloaded by the folder walk`() {
        assertFalse(
            OfflineFolderPolicy.needsDownload(isOffline = true, hasIntactOfflineCopy = true)
        )
    }

    @Test
    fun `pinned file without offline copy is downloaded by the folder walk`() {
        assertTrue(
            OfflineFolderPolicy.needsDownload(isOffline = true, hasIntactOfflineCopy = false)
        )
    }

    @Test
    fun `pending files always need a sync`() {
        assertTrue(
            OfflineFolderPolicy.needsSync(
                hasPendingFiles = true,
                lastSyncedAtMillis = System.currentTimeMillis(),
                nowMillis = System.currentTimeMillis(),
            )
        )
    }

    @Test
    fun `never synced root needs a sync`() {
        assertTrue(
            OfflineFolderPolicy.needsSync(
                hasPendingFiles = false,
                lastSyncedAtMillis = null,
                nowMillis = System.currentTimeMillis(),
            )
        )
    }

    @Test
    fun `freshly synced root does not need a sync`() {
        val now = 1_000_000_000_000L
        assertFalse(
            OfflineFolderPolicy.needsSync(
                hasPendingFiles = false,
                lastSyncedAtMillis = now - 60_000L,
                nowMillis = now,
            )
        )
    }

    @Test
    fun `stale root needs a sync`() {
        val now = 1_000_000_000_000L
        assertTrue(
            OfflineFolderPolicy.needsSync(
                hasPendingFiles = false,
                lastSyncedAtMillis = now - 60 * 60 * 1_000L,
                nowMillis = now,
            )
        )
    }

    @Test
    fun `covered file absent from remote listing loses its offline copy`() {
        assertTrue(
            OfflineFolderPolicy.shouldRemoveCoveredFileAbsentFromRemote(
                isOffline = false,
                isInOfflineFolder = true,
            )
        )
    }

    @Test
    fun `pinned file absent from remote listing is left to the per-file offline sync`() {
        assertFalse(
            OfflineFolderPolicy.shouldRemoveCoveredFileAbsentFromRemote(
                isOffline = true,
                isInOfflineFolder = true,
            )
        )
    }
}
