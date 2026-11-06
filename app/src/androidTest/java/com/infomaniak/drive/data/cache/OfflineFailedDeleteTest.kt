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

import android.content.Context
import android.net.Uri
import androidx.test.core.app.ApplicationProvider
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.IOFile
import com.infomaniak.drive.utils.RealmModules
import io.realm.Realm
import io.realm.RealmConfiguration
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.Date
import java.util.UUID

/**
 * THE FAILED-DELETION CONTRACT OF THE OFFLINE COPY CLEANUP (the round-64 N1 fix + the
 * sidecar-release guard, on real Realm/file semantics — this is why the test is
 * instrumentation and not a JVM mock):
 *
 *  - a byte deletion reported as failed (a REAL failed unlink: the copy's directory is
 *    made unwritable) must keep the bytes, count the file as KEPT, and RESTORE the
 *    committed coverage flags so the durable machinery retries the removal;
 *  - the intent-stamp bump is retained (the restored copy re-verifies conservatively);
 *  - an abort firing between the failed deletion and the restoration leaves the flags
 *    cleared (the reviving authority owns the row);
 *  - a LIVE PIN is never overwritten by the restoration (the restoration's pin guard);
 *  - a successful deletion removes the bytes and counts them removed.
 *
 * The fixture is fully synthetic and disposable: a random realm in the instrumentation
 * target's cacheDir, a file under the OFFLINE tree of a synthetic userId (that subtree is
 * the only offline-tree content removed in cleanup), and one SYNC_OFFLINE upload record in
 * the Sync realm (deleted in cleanup by its URI). No owner data, no account, no network.
 * The coder executes nothing — the orchestrator runs this on a device.
 *
 * HONEST PRECONDITION NOTE (the orchestrator's caution): the failed-unlink scenarios make
 * the copy's directory unwritable and ASSERT that request via `check(setWritable(...))` —
 * but Android's external-media FUSE layer may IGNORE chmod semantics, in which case the
 * unlink can succeed and the failed-deletion assertions would fail. `setWritable`'s return
 * is the fixture's prerequisite, NOT proof of runtime reachability; whether the real
 * failed-unlink path is reachable at the offline tree's actual filesystem is decided by
 * the FIRST DEVICE RUN alone. The assertions are deliberately not weakened — a
 * chmod-ignoring device would surface as concrete scenario failures to adapt (a different
 * real-failure mechanism at the same site), not as passing-by-luck.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OfflineFailedDeleteTest {

    companion object {
        private val context: Context = ApplicationProvider.getApplicationContext()
        private val userDrive = UserDrive(userId = USER_ID, driveId = DRIVE_ID)

        // Synthetic scope, far from any real account or the other fixtures.
        private const val USER_ID = 9_200_001
        private const val DRIVE_ID = 9_200_001
        private const val FILE_ID = 9_200_001
        private const val PARENT_ID = 9_200_002
        private const val FILE_NAME = "fixture-failed-delete.bin"
        private const val ROW_PATH = "/fixture/failed-delete/"
        private const val PAYLOAD = "fixture offline copy payload — synthetic bytes"

        private lateinit var fixtureConfig: RealmConfiguration
        private val storedRecordUris = mutableListOf<Uri>()

        @BeforeAll
        @JvmStatic
        fun setUp() {
            fixtureConfig = RealmConfiguration.Builder()
                .schemaVersion(FileMigration.DB_VERSION)
                .modules(RealmModules.LocalFilesModule())
                .name("offline-failed-delete-${UUID.randomUUID()}")
                .directory(context.cacheDir)
                .build()
        }

        /**
         * Seeds the offline bytes, the matching SYNC_OFFLINE upload record and the covered
         * row. Returns the SEEDED BYTE FILE (an [IOFile] — NOT the model row).
         */
        private fun seedCoveredCopy(realm: Realm): IOFile {
            val offlineFile = File()
                .apply {
                    uid = "${FILE_ID}_$DRIVE_ID"
                    id = FILE_ID
                    parentId = PARENT_ID
                    driveId = DRIVE_ID
                    name = FILE_NAME
                    path = ROW_PATH
                }
                .getOfflineFile(context, USER_ID)!!
            offlineFile.parentFile?.mkdirs()
            offlineFile.writeText(PAYLOAD)

            val record = UploadFile(
                uri = Uri.fromFile(offlineFile).toString(),
                driveId = DRIVE_ID,
                fileModifiedAt = Date(offlineFile.lastModified()),
                fileName = FILE_NAME,
                fileSize = offlineFile.length(),
                remoteFolder = PARENT_ID,
                originalFileId = FILE_ID,
                contentHash = UploadFile.sha256OrNull(offlineFile),
                type = UploadFile.Type.SYNC_OFFLINE.name,
                userId = USER_ID,
            ).apply { store() }
            storedRecordUris.add(Uri.parse(record.uri))

            realm.executeTransaction { transactionRealm ->
                transactionRealm.copyToRealmOrUpdate(
                    File().apply {
                        uid = "${FILE_ID}_$DRIVE_ID"
                        id = FILE_ID
                        parentId = PARENT_ID
                        driveId = DRIVE_ID
                        name = FILE_NAME
                        path = ROW_PATH
                        isInOfflineFolder = true
                        isMarkedAsOffline = false
                    },
                )
            }
            return offlineFile
        }

        private fun liveRow(realm: Realm): File =
            FileController.getFileById(realm, FILE_ID) ?: error("the fixture row must exist")
    }

    @Test
    @DisplayName("a failed byte deletion keeps the bytes and restores the cleared coverage")
    fun failedDeletionKeepsBytesAndRestoresCoverage() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val offlineFile = seedCoveredCopy(realm)
            val stampBefore = liveRow(realm).offlineIntentStamp
            val result = OfflineFolderController.RemovalResult()
            val dir = offlineFile.parentFile!!

            // A REAL failed unlink: the copy's own directory refuses deletions.
            check(dir.setWritable(false, false)) { "the fixture directory must be lockable" }
            try {
                val detached = realm.copyFromRealm(liveRow(realm), 1)
                OfflineFolderController.clearFileCoverage(
                    context = context,
                    realm = realm,
                    file = detached,
                    userDrive = userDrive,
                    result = result,
                    isFolder = false,
                )
            } finally {
                check(dir.setWritable(true, false)) { "the fixture directory must be releasable" }
            }

            assertTrue(offlineFile.exists(), "the bytes must survive a failed deletion")
            assertEquals(1, result.keptOfflineFilesCount, "a retained file is counted kept")
            assertEquals(0, result.removedOfflineFilesCount, "a retained file is never counted removed")
            val retained = liveRow(realm)
            assertTrue(retained.isInOfflineFolder, "the committed coverage must be restored for the retry")
            assertFalse(retained.isMarkedAsOffline, "the pre-commit marker value is restored exactly")
            assertNotEquals(
                stampBefore,
                retained.offlineIntentStamp,
                "the stamp bump is retained — the restored copy re-verifies conservatively",
            )
        }
    }

    @Test
    @DisplayName("an abort between the failed deletion and the restoration leaves the flags cleared")
    fun abortedCleanupDoesNotRestoreCoverage() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val offlineFile = seedCoveredCopy(realm)
            val result = OfflineFolderController.RemovalResult()
            val dir = offlineFile.parentFile!!

            // The abort predicate: false for the committing transaction and the post-commit
            // revalidation (so the deletion IS attempted), TRUE at the restoration guard.
            var invocation = 0
            val abortsAfterDeletionAttempt = {
                invocation++ >= 3
            }

            check(dir.setWritable(false, false))
            try {
                val detached = realm.copyFromRealm(liveRow(realm), 1)
                OfflineFolderController.clearFileCoverage(
                    context = context,
                    realm = realm,
                    file = detached,
                    userDrive = userDrive,
                    result = result,
                    isFolder = false,
                    isAborted = abortsAfterDeletionAttempt,
                )
            } finally {
                check(dir.setWritable(true, false))
            }

            assertTrue(offlineFile.exists())
            assertEquals(1, result.keptOfflineFilesCount)
            val abandoned = liveRow(realm)
            assertFalse(
                abandoned.isInOfflineFolder,
                "the reviving authority owns the row — the restoration must not run",
            )
        }
    }

    @Test
    @DisplayName("the restoration never overwrites a live pin committed meanwhile")
    fun restorationNeverOverwritesALivePin() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val offlineFile = seedCoveredCopy(realm)
            val result = OfflineFolderController.RemovalResult()
            val dir = offlineFile.parentFile!!

            // Same three-call shape; the third call commits a LIVE PIN before returning
            // false, so the restoration runs into the pin guard.
            var invocation = 0
            val pinsThenAllows = {
                invocation++
                if (invocation == 3) {
                    realm.executeTransaction { transactionRealm ->
                        transactionRealm.where(File::class.java)
                            .equalTo(File::id.name, FILE_ID)
                            .findFirst()?.isOffline = true
                    }
                }
                false
            }

            check(dir.setWritable(false, false))
            try {
                val detached = realm.copyFromRealm(liveRow(realm), 1)
                OfflineFolderController.clearFileCoverage(
                    context = context,
                    realm = realm,
                    file = detached,
                    userDrive = userDrive,
                    result = result,
                    isFolder = false,
                    isAborted = pinsThenAllows,
                )
            } finally {
                check(dir.setWritable(true, false))
            }

            assertTrue(offlineFile.exists())
            val pinned = liveRow(realm)
            assertTrue(pinned.isOffline, "the committed pin must survive the cleanup untouched")
            assertFalse(
                pinned.isInOfflineFolder,
                "the restoration must not have restored inherited coverage over the pin",
            )
            assertFalse(pinned.isMarkedAsOffline, "the pin's own marker semantics are untouched")
        }
    }

    @Test
    @DisplayName("a successful deletion removes the bytes and clears the coverage exactly once")
    fun successfulDeletionRemovesBytes() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val offlineFile = seedCoveredCopy(realm)
            val result = OfflineFolderController.RemovalResult()

            val detached = realm.copyFromRealm(liveRow(realm), 1)
            OfflineFolderController.clearFileCoverage(
                context = context,
                realm = realm,
                file = detached,
                userDrive = userDrive,
                result = result,
                isFolder = false,
            )

            assertFalse(offlineFile.exists(), "a confirmed deletion removed the bytes")
            assertEquals(1, result.removedOfflineFilesCount)
            assertEquals(0, result.keptOfflineFilesCount)
            val cleared = liveRow(realm)
            assertFalse(cleared.isInOfflineFolder, "a deleted copy's coverage stays cleared")
        }
    }

    @AfterAll
    fun cleanup() {
        // ONLY this fixture's own state: the synthetic user's offline subtree, the stored
        // upload records, and the random own-cacheDir realm. No application data, no other
        // user's anything.
        runCatching {
            if (storedRecordUris.isNotEmpty()) UploadFile.deleteAllFromUris(storedRecordUris)
        }
        runCatching { Realm.deleteRealm(fixtureConfig) }
        runCatching {
            File.getOfflineFolder(context).let { offlineRoot ->
                java.io.File(offlineRoot, "$USER_ID").deleteRecursively()
            }
        }
    }
}
