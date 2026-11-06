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

import com.infomaniak.drive.data.cache.OfflineFolderController
import com.infomaniak.drive.data.models.UserDrive
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test

/**
 * The scoped root-intent generation regression (the round-64 scope-key fix): the
 * process-wide generation map is keyed by (userId, driveId, folderId), so an unrelated
 * root's add/remove in ANOTHER account or drive must never defer this scope's walk
 * completion, marker clear or destructive absent-child cleanup.
 *
 * HONEST SCOPE NOTES: the map is intentionally process-global with no reset API (a
 * production change was not acceptable), so the tests use far-out-of-range synthetic
 * scope numbers no other suite uses and assert only RELATIVE monotonicity — never
 * absolute generation values. The assertions exercise the pure Kotlin map/clock API
 * (the object's initializers are Android-free: a ConcurrentHashMap, an AtomicLong and a
 * class literal); the Realm/monitor behavior around the callers is NOT covered here and
 * is not claimed to be.
 */
class OfflineRootIntentScopeTest {

    // Synthetic scopes far from any real account: the same folderId 5 exists in BOTH
    // scopes — exactly the collision shape the fix removes.
    private val scopeA = UserDrive(userId = 9_100_001, driveId = 9_100_001)
    private val scopeB = UserDrive(userId = 9_100_002, driveId = 9_100_002)
    private val sharedFolderId = 5

    @Test
    @DisplayName("a root intent change in one scope does not defer another scope's comparison")
    fun aChangeInOneScopeIsInvisibleToAnotherScopeWithTheSameFolderId() {
        // The same folderId in scope A changes intent.
        val beforeB = OfflineFolderController.rootIntentGeneration(scopeB, sharedFolderId)
        OfflineFolderController.noteRootIntentChange(scopeA, sharedFolderId)

        // Scope B's comparison sees NOTHING (before the fix this returned true and
        // deferred B's walk completion/cleanup).
        assertFalse(
            OfflineFolderController.rootIntentChangedSince(scopeB, sharedFolderId, beforeB),
            "an unrelated scope's intent change must not defer this scope",
        )
        // And B's captured generation is unchanged.
        assertEquals(beforeB, OfflineFolderController.rootIntentGeneration(scopeB, sharedFolderId))

        // Scope A's own comparison DOES see it (no false negative introduced).
        val beforeA = OfflineFolderController.rootIntentGeneration(scopeA, sharedFolderId)
        assertTrue(
            OfflineFolderController.rootIntentChangedSince(scopeA, sharedFolderId, beforeA - 1L),
            "the change must be visible inside its own scope",
        )
        assertTrue(
            OfflineFolderController.rootIntentChangedSince(scopeA, sharedFolderId, 0L),
            "the change must be visible against the fresh-scope zero",
        )
    }

    @Test
    @DisplayName("folder ids are isolated within the same user and drive too")
    fun folderIdsAreIsolatedWithinOneScope() {
        val otherFolderId = 6
        val before = OfflineFolderController.rootIntentGeneration(scopeA, otherFolderId)
        OfflineFolderController.noteRootIntentChange(scopeA, sharedFolderId)

        assertEquals(
            before,
            OfflineFolderController.rootIntentGeneration(scopeA, otherFolderId),
            "a sibling folder's intent change must not advance this folder's generation",
        )
        assertFalse(OfflineFolderController.rootIntentChangedSince(scopeA, otherFolderId, before))
    }

    @Test
    @DisplayName("generations advance strictly monotonically per scope")
    fun generationsAreStrictlyMonotonicPerScope() {
        OfflineFolderController.noteRootIntentChange(scopeA, sharedFolderId)
        val first = OfflineFolderController.rootIntentGeneration(scopeA, sharedFolderId)
        OfflineFolderController.noteRootIntentChange(scopeA, sharedFolderId)
        val second = OfflineFolderController.rootIntentGeneration(scopeA, sharedFolderId)

        assertTrue(second > first, "successive changes must strictly advance the scope's generation")
        assertTrue(OfflineFolderController.rootIntentChangedSince(scopeA, sharedFolderId, first))
        assertFalse(
            OfflineFolderController.rootIntentChangedSince(scopeA, sharedFolderId, second),
            "nothing changed after the latest recorded generation",
        )

        // An interleaved foreign-scope change never regresses or advances this scope.
        OfflineFolderController.noteRootIntentChange(scopeB, sharedFolderId)
        assertEquals(second, OfflineFolderController.rootIntentGeneration(scopeA, sharedFolderId))
    }
}
