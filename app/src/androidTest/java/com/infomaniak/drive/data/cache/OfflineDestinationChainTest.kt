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
package com.infomaniak.drive.data.cache

import androidx.test.core.app.ApplicationProvider
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.utils.RealmModules
import com.infomaniak.drive.utils.Utils.ROOT_ID
import io.realm.Realm
import io.realm.RealmConfiguration
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * THE ACCOUNT-FIRST DESTINATION-CHAIN REGRESSION (offline-account-destination-chain-fix-
 * 20261106.txt; the real-account failure of 2026-11-06 10:13): the drive root (id ROOT_ID)
 * is a presentation boundary the listings never materialize — the root listing's
 * saveRemoteFiles receives remoteFolder = null and skips the insertion — so a chain that
 * REACHES the root must validate WITHOUT demanding a cached root row. The previous
 * parentId<=0 termination demanded exactly that and deferred EVERY chain that reached the
 * drive root: every bulk-offline download of the real account failed "its destination
 * chain is incomplete" before any byte moved, while the marking (whose own walk stops at
 * the bulk root) succeeded. These tests drive THE production validators —
 * [FileController.destinationChainExists] and [FileController.usableAncestryChain] — over
 * a seeded synthetic realm shaped like the real account tree (My folder → T0079 folder →
 * fixture, NO root row), never a mirrored predicate.
 *
 * THE PROTECTIONS UNDER TEST (preserved by the fix, each with its own case): a missing
 * REAL intermediate ancestor defers; a cycle defers; a missing mid-chain row defers.
 * Pure Realm + ids: no filesystem, no sandbox, no network, no account. The coder executes
 * nothing — the orchestrator runs this on a device.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OfflineDestinationChainTest {

    companion object {
        // THE RANDOMIZED SYNTHETIC SCOPE (per class load; a fresh band — the other
        // fixtures' scopes use the 9_100_000/9_200_000/9_300_000/9_500_000 ranges).
        private val scopeBase =
            9_800_000 + ((UUID.randomUUID().leastSignificantBits % 50_000L).toInt() * 8)
        private val MY_FOLDER_ID = scopeBase + 1
        private val TEST_ROOT_ID = scopeBase + 2
        private val DEEP_FOLDER_ID = scopeBase + 3
        private val DEEPEST_FOLDER_ID = scopeBase + 4
        private val HOLE_PARENT_ID = scopeBase + 5
        private val ORPHAN_ROW_ID = scopeBase + 6
        private val CYCLE_A_ID = scopeBase + 7
        private val CYCLE_B_ID = scopeBase + 8

        private lateinit var fixtureConfig: RealmConfiguration

        @BeforeAll
        @JvmStatic
        fun setUp() {
            fixtureConfig = RealmConfiguration.Builder()
                .schemaVersion(FileMigration.DB_VERSION)
                .modules(RealmModules.LocalFilesModule())
                .name("offline-destination-chain-${UUID.randomUUID()}")
                .directory(ApplicationProvider.getApplicationContext<android.content.Context>().cacheDir)
                .build()
            // THE WHOLE GRAPH SEEDED UP FRONT — no test depends on JUnit method order.
            Realm.getInstance(fixtureConfig).use { realm ->
                // The root row (id ROOT_ID) is deliberately NEVER seeded — the exact
                // real-account state the listings produce.
                seedRow(realm, MY_FOLDER_ID, ROOT_ID)
                seedRow(realm, TEST_ROOT_ID, MY_FOLDER_ID)
                seedRow(realm, DEEP_FOLDER_ID, MY_FOLDER_ID)
                seedRow(realm, DEEPEST_FOLDER_ID, DEEP_FOLDER_ID)
                seedRow(realm, ORPHAN_ROW_ID, HOLE_PARENT_ID)
                seedRow(realm, CYCLE_A_ID, CYCLE_B_ID)
                seedRow(realm, CYCLE_B_ID, CYCLE_A_ID)
            }
        }

        /** Seeds a managed row; the validators climb MANAGED rows via getFileById. */
        private fun seedRow(realm: Realm, id: Int, parentId: Int) {
            realm.executeTransaction { transactionRealm ->
                transactionRealm.copyToRealm(
                    File().apply {
                        uid = "${id}_fixture"
                        this.id = id
                        this.parentId = parentId
                        driveId = 1
                        name = "fixture-$id"
                        path = "/fixture/$id/"
                    },
                )
            }
        }
    }

    @Test
    @DisplayName("a chain reaching the drive root validates without a cached root row")
    fun chainReachingDriveRootIsValidWithoutRootRow() {
        Realm.getInstance(fixtureConfig).use { realm ->
            // THE EXACT REAL-ACCOUNT SHAPE (no row with id ROOT_ID exists): fixture
            // folder → My folder (parentId = ROOT_ID) → the root boundary.
            assertTrue(
                FileController.destinationChainExists(realm, TEST_ROOT_ID),
                "the bulk root's own chain (folder → My folder → ROOT_ID boundary) must validate",
            )
            assertTrue(
                FileController.destinationChainExists(realm, MY_FOLDER_ID),
                "a direct child of the root boundary must validate (the root row is not required)",
            )
            assertTrue(
                FileController.destinationChainExists(realm, ROOT_ID),
                "the root id itself is the terminal boundary and validates immediately",
            )
        }
    }

    @Test
    @DisplayName("a deep chain to the root validates at any depth")
    fun deepChainToRootIsValid() {
        Realm.getInstance(fixtureConfig).use { realm ->
            assertTrue(
                FileController.destinationChainExists(realm, DEEPEST_FOLDER_ID),
                "a four-level chain (fixture → folder → folder → My folder → ROOT_ID) must validate",
            )
        }
    }

    @Test
    @DisplayName("a missing real ancestor still defers — the protection is preserved")
    fun missingRealAncestorStillDefers() {
        Realm.getInstance(fixtureConfig).use { realm ->
            assertFalse(
                FileController.destinationChainExists(realm, ORPHAN_ROW_ID),
                "a missing non-root ancestor must still defer the commit",
            )
            assertFalse(
                FileController.destinationChainExists(realm, HOLE_PARENT_ID),
                "a missing row's own chain must defer (a hole between the destination and the root)",
            )
        }
    }

    @Test
    @DisplayName("a corrupted parent cycle still defers")
    fun cycleStillDefers() {
        Realm.getInstance(fixtureConfig).use { realm ->
            assertFalse(
                FileController.destinationChainExists(realm, CYCLE_A_ID),
                "a corrupted cycle must defer — the visited-set protection is untouched",
            )
        }
    }

    @Test
    @DisplayName("usableAncestryChain carries the same root boundary and the same protections")
    fun usableAncestryChainMatchesTheSameBoundary() {
        Realm.getInstance(fixtureConfig).use { realm ->
            assertTrue(
                FileController.usableAncestryChain(realm, TEST_ROOT_ID),
                "the single-file upsert gate must accept the root-reaching chain (the identical convention fix)",
            )
            assertTrue(
                FileController.usableAncestryChain(realm, ROOT_ID),
                "the root id itself is usable ancestry",
            )
            assertFalse(
                FileController.usableAncestryChain(realm, ORPHAN_ROW_ID),
                "a missing non-root ancestor must still defer the upsert",
            )
            assertFalse(
                FileController.usableAncestryChain(realm, CYCLE_A_ID),
                "a cycle must still defer the upsert",
            )
        }
    }

    @AfterAll
    fun cleanup() {
        // ONLY this fixture's own state: the random own-cacheDir realm. Nothing else is
        // touched — no filesystem, no sandbox, no registry, no external tree.
        runCatching { Realm.deleteRealm(fixtureConfig) }
    }
}
