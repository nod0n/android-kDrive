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
import com.infomaniak.drive.utils.DownloadOfflineFileManager
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.deletePublishedProvenance
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.provenanceFileFor
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.provenanceRecordFileExists
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.writePublishedProvenance
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
 * sidecar-release guard) on real Realm/file semantics — this is why the test is
 * instrumentation and not a JVM mock.
 *
 * THE SCENARIOS (each asserts OBSERVABLE state, never implementation internals):
 *  1. absent-zero-state + failed unlink: no registry state exists, the byte deletion is
 *     still attempted and fails, the disposal defers, the coverage is restored. THE ATTEMPT
 *     EVIDENCE is the intent-stamp ADVANCE: the committing transaction (the only writer of
 *     that bump) ran, so the cleanup reached its deletion decision — a protective
 *     short-circuit (a pin, a pending upload, an unproven-historical classification) never
 *     reaches the commit and leaves the stamp untouched.
 *  2. existing state + release SUCCEEDS + failed unlink: a REAL provenance record is seeded
 *     through the PRODUCTION writer over the actual seeded bytes; the release consumes it
 *     BEFORE the byte deletion, so its disappearance after the run is DIRECT evidence the
 *     deletion was attempted (ruling out every protective-gate short-circuit — including
 *     any misclassification the seeded SYNC_OFFLINE record could have caused).
 *  3. existing state + release FAILS: the seeded record at its EXACT production-derived
 *     path is replaced by a non-empty directory — an unlink of a non-empty directory
 *     deterministically fails on every POSIX filesystem — so the production release
 *     genuinely fails over EXISTING registry state and the disposal defers ENTIRELY: the
 *     record path is RETAINED alongside the bytes, and the coverage restoration still
 *     applies (a later pass retries sidecar and bytes together). NO shared-directory
 *     permission mutations anywhere: the obstruction lives only at this fixture's own
 *     record path and its explicit cleanup restores that exact slot.
 *  4. successful deletion: the bytes go, the record is released, the counters tell the
 *     truth, the coverage stays cleared.
 *  5. abort between the failed deletion and the restoration: the flags stay cleared (the
 *     reviving authority owns the row). The three-invocation abort shape matches
 *     clearFileCoverage's actual isAborted() call sites; a refactor changing the call count
 *     fails these tests loudly — the intended canary.
 *  6. a live pin committed meanwhile is never overwritten by the restoration.
 *
 * THE ENVIRONMENT-REFUSAL PROBE (no vacuous passes): in every locked-directory scenario a
 * sacrificial file is created in the directory BEFORE it is locked, then unlink()ed AFTER
 * the lock, and its PERSISTENCE is asserted — the actual refusal is proven before the main
 * operation runs. A FUSE-backed external-media layer may ignore chmod: if the probe shows
 * unlinks still succeeding, the test FAILS with an explicit environment-limitation message.
 * A failure is the honest report; NO pass or skip is ever counted as contract proof.
 * `setWritable`'s return is the fixture's prerequisite only.
 *
 * THE FIXTURE IS FULLY SYNTHETIC AND DISPOSABLE: a random realm in the instrumentation
 * target's cacheDir; the synthetic scope (user/drive/file ids) is RANDOMIZED PER CLASS LOAD
 * (a 50k-value space — no fixed scope collisions between fixture runs or parallel
 * residue); the seeded offline subtree, the seeded registry records (removed by their EXACT
 * production-derived paths in cleanup — never a registry DIRECTORY, which is shared-by-hash
 * infrastructure) and the upload records (by URI) are the ONLY state removed in cleanup —
 * never other users' data, and NO shared-directory permission mutations anywhere (the
 * owner installation may hold a real logged-in account, so nothing outside this fixture's
 * own paths is ever touched). No owner data, no account, no network. The coder executes
 * nothing — the orchestrator runs this on a device.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OfflineFailedDeleteTest {

    companion object {
        private val context: Context = ApplicationProvider.getApplicationContext()

        // THE RANDOMIZED SYNTHETIC SCOPE (per class load): no fixed ids, so parallel or
        // sequential fixture residue can never collide on the offline subtree, the upload
        // records or the rows.
        private val scopeBase =
            9_200_000 + ((UUID.randomUUID().leastSignificantBits % 50_000L).toInt() * 4)
        private val USER_ID = scopeBase + 1
        private val DRIVE_ID = scopeBase + 2
        private val FILE_ID = scopeBase + 3
        private val PARENT_ID = scopeBase + 4
        private val userDrive = UserDrive(userId = USER_ID, driveId = DRIVE_ID)
        private const val FILE_NAME = "fixture-failed-delete.bin"
        private const val ROW_PATH = "/fixture/failed-delete/"
        private const val PAYLOAD = "fixture offline copy payload — synthetic bytes"

        private lateinit var fixtureConfig: RealmConfiguration
        private val storedRecordUris = mutableListOf<Uri>()
        private val seededRegistryFiles = mutableListOf<IOFile>()

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
         * Seeds the offline bytes, the matching SYNC_OFFLINE upload record (which
         * classifies the copy UPLOADED — a non-short-circuiting arm: the cleanup proceeds
         * to its deletion decision), the covered row, and — when [seedProvenance] — a REAL
         * provenance record through the PRODUCTION writer over the actual bytes. Returns
         * the SEEDED BYTE FILE (an [IOFile] — NOT the model row).
         */
        private fun seedCoveredCopy(realm: Realm, seedProvenance: Boolean): IOFile {
            val rowModel = File().apply {
                uid = "${FILE_ID}_$DRIVE_ID"
                id = FILE_ID
                parentId = PARENT_ID
                driveId = DRIVE_ID
                name = FILE_NAME
                path = ROW_PATH
            }
            val offlineFile = rowModel.getOfflineFile(context, USER_ID)!!
            offlineFile.parentFile?.mkdirs()
            offlineFile.writeText(PAYLOAD)

            storedRecordUris.add(
                Uri.parse(
                    UploadFile(
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
                    ).apply { store() }.uri,
                ),
            )
            if (seedProvenance) {
                // The PRODUCTION writer over the ACTUAL seeded bytes — a structurally valid
                // synthetic record (version 0: the fixture row carries no server version),
                // never a hand-mirrored registry file.
                check(
                    writePublishedProvenance(
                        context = context,
                        target = offlineFile,
                        file = rowModel,
                        userDrive = userDrive,
                    ),
                ) { "the fixture's synthetic provenance seed must succeed" }
                seededRegistryFiles.add(offlineFile)
            }

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

        /**
         * THE ENVIRONMENT-REFUSAL PROBE, fully covered by ONE finally: the sacrificial
         * file is created while the directory is still writable, the directory is locked,
         * the sacrificial unlink must then FAIL (the file must persist) — and the unlock
         * runs in `finally` from the moment the lock is attempted, so ANY throw (the
         * creation, the lock, the probe or the block) still restores the directory and
         * removes the sacrificial file. `setWritable`'s return is only the prerequisite —
         * a chmod-ignoring FUSE layer surfaces as an explicit environment-limitation
         * failure, never as a vacuous contract pass.
         */
        private inline fun withProvenLockedDirectory(dir: IOFile, block: () -> Unit) {
            val sacrificial = IOFile(dir, "probe-${UUID.randomUUID()}.tmp")
            check(sacrificial.createNewFile()) { "the fixture must be able to create its probe file" }
            val locked = runCatching { dir.setWritable(false, false) }.getOrDefault(false)
            try {
                check(locked) { "the fixture directory must be lockable" }
                val deleted = runCatching { sacrificial.delete() }.getOrDefault(true)
                check(!deleted && sacrificial.exists()) {
                    "ENVIRONMENT LIMITATION: unlinks still succeed after the directory was made " +
                        "unwritable (external-media FUSE may ignore chmod) — the real failed-unlink " +
                        "contract cannot be exercised on this filesystem; this is a reported " +
                        "unsupported mechanism, NOT a contract pass"
                }
                block()
            } finally {
                if (locked) runCatching { dir.setWritable(true, false) }
                runCatching { sacrificial.delete() }
            }
        }
    }

    @Test
    @DisplayName("absent zero-state + failed unlink: the deletion is attempted, the disposal defers, coverage is restored")
    fun absentZeroStateFailedUnlinkDefersAndRestores() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val offlineFile = seedCoveredCopy(realm, seedProvenance = false)
            assertFalse(
                provenanceRecordFileExists(context, offlineFile),
                "scenario setup: the absent-zero-state arm holds no registry state",
            )
            val stampBefore = liveRow(realm).offlineIntentStamp
            val result = OfflineFolderController.RemovalResult()

            withProvenLockedDirectory(offlineFile.parentFile!!) {
                val detached = realm.copyFromRealm(liveRow(realm), 1)
                OfflineFolderController.clearFileCoverage(
                    context = context,
                    realm = realm,
                    file = detached,
                    userDrive = userDrive,
                    result = result,
                    isFolder = false,
                )
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
                "ATTEMPT EVIDENCE: the stamp bump proves the committing transaction ran — " +
                    "the cleanup reached its deletion decision (a protective short-circuit " +
                    "never commits)",
            )
        }
    }

    @Test
    @DisplayName("existing state + successful release + failed unlink: the released sidecar proves the deletion was attempted")
    fun releasedSidecarProvesDeletionAttempted() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val offlineFile = seedCoveredCopy(realm, seedProvenance = true)
            assertTrue(
                provenanceRecordFileExists(context, offlineFile),
                "scenario setup: the seeded synthetic proof must exist before the run",
            )
            val stampBefore = liveRow(realm).offlineIntentStamp
            val result = OfflineFolderController.RemovalResult()

            withProvenLockedDirectory(offlineFile.parentFile!!) {
                val detached = realm.copyFromRealm(liveRow(realm), 1)
                OfflineFolderController.clearFileCoverage(
                    context = context,
                    realm = realm,
                    file = detached,
                    userDrive = userDrive,
                    result = result,
                    isFolder = false,
                )
            }

            assertFalse(
                provenanceRecordFileExists(context, offlineFile),
                "ATTEMPT EVIDENCE: the release runs BEFORE the byte deletion — its absence " +
                    "after the run proves deleteOfflineCopy was reached and the deletion " +
                    "attempted (no protective-gate short-circuit passed vacuously)",
            )
            assertTrue(offlineFile.exists(), "the bytes must survive the failed deletion")
            assertEquals(1, result.keptOfflineFilesCount)
            assertEquals(0, result.removedOfflineFilesCount)
            val retained = liveRow(realm)
            assertTrue(retained.isInOfflineFolder, "the coverage restoration still applies")
            assertNotEquals(stampBefore, retained.offlineIntentStamp, "the stamp bump is retained")
        }
    }

    @Test
    @DisplayName("failed release over existing state: the disposal defers entirely and the record is retained with the bytes")
    fun failedReleaseOverExistingStateDefersEntirely() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val offlineFile = seedCoveredCopy(realm, seedProvenance = true)
            val recordFile = provenanceFileFor(context, offlineFile)
            assertTrue(
                recordFile.exists(),
                "scenario setup: the seeded record must exist at its production-derived path",
            )
            val stampBefore = liveRow(realm).offlineIntentStamp
            val result = OfflineFolderController.RemovalResult()

            // THE FIXTURE-OWNED OBSTRUCTION (no shared-directory permission mutations): the
            // seeded record file at its EXACT production-derived path is replaced by a
            // NON-EMPTY directory — an unlink of a non-empty directory deterministically
            // fails on every POSIX filesystem, so the production release genuinely fails
            // over EXISTING registry state, with no chmod anywhere and no effect on any
            // other record sharing the hash prefix. The obstruction is this fixture's own
            // record path alone and is removed by its own cleanup.
            check(recordFile.delete()) { "the seeded record must be replaceable by the obstruction" }
            check(recordFile.mkdirs()) { "the obstruction directory must be creatable at the record path" }
            val obstructionChild = IOFile(recordFile, "obstruction.bin")
            check(obstructionChild.createNewFile()) { "the obstruction must be non-empty (an empty directory would unlink)" }

            val detached = realm.copyFromRealm(liveRow(realm), 1)
            OfflineFolderController.clearFileCoverage(
                context = context,
                realm = realm,
                file = detached,
                userDrive = userDrive,
                result = result,
                isFolder = false,
            )

            assertTrue(
                recordFile.exists() && obstructionChild.exists(),
                "the failed release must RETAIN the obstructed record path — the disposal deferred entirely",
            )
            assertTrue(offlineFile.exists(), "the bytes are never deleted when the release fails")
            assertEquals(1, result.keptOfflineFilesCount)
            assertEquals(0, result.removedOfflineFilesCount)
            val retained = liveRow(realm)
            assertTrue(retained.isInOfflineFolder, "the coverage restoration applies to the deferred disposal")
            assertNotEquals(stampBefore, retained.offlineIntentStamp)

            // EXACT OWN-PATH CLEANUP (explicit, never broad): the obstruction's child, the
            // obstruction directory itself, and then the record slot's residue via the
            // production release — all at this fixture's own record path only.
            check(obstructionChild.delete()) { "the obstruction child must be removable in cleanup" }
            check(recordFile.delete()) { "the obstruction directory must be removable in cleanup" }
            runCatching { deletePublishedProvenance(context, offlineFile) }
            assertFalse(
                provenanceRecordFileExists(context, offlineFile),
                "the fixture's own record slot must be clean after its cleanup",
            )
        }
    }

    @Test
    @DisplayName("a successful deletion removes the bytes, releases the record and clears the coverage exactly once")
    fun successfulDeletionRemovesBytes() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val offlineFile = seedCoveredCopy(realm, seedProvenance = true)
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
            assertFalse(provenanceRecordFileExists(context, offlineFile), "the record is released with the bytes")
            assertEquals(1, result.removedOfflineFilesCount)
            assertEquals(0, result.keptOfflineFilesCount)
            assertFalse(liveRow(realm).isInOfflineFolder, "a deleted copy's coverage stays cleared")
        }
    }

    @Test
    @DisplayName("an abort between the failed deletion and the restoration leaves the flags cleared")
    fun abortedCleanupDoesNotRestoreCoverage() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val offlineFile = seedCoveredCopy(realm, seedProvenance = false)
            val result = OfflineFolderController.RemovalResult()

            // The abort predicate: false for the committing transaction and the post-commit
            // revalidation (so the deletion IS attempted), TRUE at the restoration guard.
            var invocation = 0
            val abortsAfterDeletionAttempt = { invocation++ >= 3 }

            withProvenLockedDirectory(offlineFile.parentFile!!) {
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
            }

            assertTrue(offlineFile.exists())
            assertEquals(1, result.keptOfflineFilesCount)
            val abandoned = liveRow(realm)
            assertFalse(
                abandoned.isInOfflineFolder,
                "the reviving authority owns the row — a restoration run here must fail this test",
            )
        }
    }

    @Test
    @DisplayName("the restoration never overwrites a live pin committed meanwhile")
    fun restorationNeverOverwritesALivePin() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val offlineFile = seedCoveredCopy(realm, seedProvenance = false)
            val result = OfflineFolderController.RemovalResult()

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

            withProvenLockedDirectory(offlineFile.parentFile!!) {
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

    @AfterAll
    fun cleanup() {
        // ONLY this fixture's own state — exact own records, exact own subtree:
        //  - the seeded provenance records: released through the production release, then
        //    deleted by their EXACT production-derived paths (record + observation);
        //  - the stored upload records by their URIs;
        //  - the random own-cacheDir realm;
        //  - the synthetic user's offline subtree (the randomized scope's own branch).
        // Never any other user's data, never a registry DIRECTORY (they are shared
        // by-hash infrastructure).
        runCatching {
            seededRegistryFiles.forEach { seeded ->
                runCatching { deletePublishedProvenance(context, seeded) }
                runCatching {
                    val recordFile = provenanceFileFor(context, seeded)
                    recordFile.delete()
                    IOFile(recordFile.path + ".obs").delete()
                }
            }
        }
        runCatching { if (storedRecordUris.isNotEmpty()) UploadFile.deleteAllFromUris(storedRecordUris) }
        runCatching { Realm.deleteRealm(fixtureConfig) }
        runCatching {
            IOFile(File.getOfflineFolder(context), "$USER_ID").deleteRecursively()
        }
    }
}
