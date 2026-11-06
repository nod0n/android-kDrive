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
import android.content.ContextWrapper
import android.net.Uri
import androidx.test.core.app.ApplicationProvider
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.DownloadOfflineFileManager
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.OldSourceDisposition
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.classifyOldSourceWithAuthority
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
import java.util.concurrent.atomic.AtomicInteger

/**
 * THE A1/P1 CONVERGENCE REGRESSION — the verified rename's advancement and its old-source
 * retirement AUTHORITY, against REAL production boundaries (offline-a1-runtime-regression-
 * plan-20261106.txt). A1's in-core ordering (the adoption gate inside publishLockedCore) is
 * NOT reachable without the application-context seam (the comparison subject, the location
 * authority and the registry writes derive from Realm.getApplicationContext(); the CAS opens
 * the app's real per-user realm) — that blocker is persisted with its source lines and is
 * NOT claimed here. What IS exercised, truthfully and fully injected, are the two production
 * halves of the converged path:
 *  - [DownloadOfflineFileManager.classifyOldSourceWithAuthority] — the post-publication
 *    advance's retirement AUTHORITY (the worker's VerifiedBasis/Absent authorize, everything
 *    else defers so the row keeps its location and the retained bytes stay discoverable);
 *  - [FileController.updateExistingFileIfNotOlder] — the advance's ACCEPTANCE gate (the
 *    round-27 #4 contract: the tied-timestamp rename moves only against the CAPTURED BASIS
 *    observation, never the fetched identity).
 * The fixture supplies only DATA (rows, bytes, hashes, upload records); every asserted
 * decision is the production function's outcome — no production predicate is copied or
 * mirrored. The retirement itself (the worker's unlink) is two inline lines in the worker
 * flow, not a callable unit — its authorization and its refusal are what these tests prove.
 *
 * THE FIXTURE IS FULLY SYNTHETIC AND DISPOSABLE (the device-proven sandbox pattern): a
 * random realm in the instrumentation target's cacheDir; a randomized synthetic scope in a
 * fresh band (the other fixtures use 9_100_000/9_200_000 ranges); per-method unique
 * identities with an EXPLICIT stored remote path (never generateAndSavePath); the offline
 * tree and the sandbox-side registry under one fixture-owned cache directory via a
 * ContextWrapper. @AfterAll deletes the ENTIRE sandbox, the own upload records by URI and
 * the own realm. The shared-by-hash registry directories, the real external-media tree and
 * the real registry are never mutated or deleted (protectionStateOf's internal application
 * context reads the real registry only — provably record-less for this random scope). No
 * owner data, no account, no network. The coder executes nothing — the orchestrator runs
 * this on a device.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OfflineVerificationConvergenceTest {

    companion object {
        private val baseContext: Context = ApplicationProvider.getApplicationContext()

        // THE RANDOMIZED SYNTHETIC SCOPE (per class load) + THE SANDBOX: the offline tree
        // and the sandbox-side registry both live under one fixture-owned cache directory.
        private val scopeBase =
            9_500_000 + ((UUID.randomUUID().leastSignificantBits % 50_000L).toInt() * 4)
        private val USER_ID = scopeBase + 1
        private val DRIVE_ID = scopeBase + 2
        private const val FILE_ID = 9_500_003
        private const val PARENT_ID = 9_500_004
        private const val OLD_PAYLOAD = "fixture retained historical bytes — generation A"
        private const val STAGED_PAYLOAD = "fixture staged fresh bytes — generation B"
        private val userDrive = UserDrive(userId = USER_ID, driveId = DRIVE_ID)
        private val sandboxRoot = IOFile(baseContext.cacheDir, "offline-sandbox-${UUID.randomUUID()}")
        private val sandboxOffline = IOFile(sandboxRoot, "offline")
        private val sandboxContext: Context = object : ContextWrapper(baseContext) {
            override fun getExternalMediaDirs(): Array<out java.io.File> = arrayOf(sandboxOffline)
        }

        private val FILE_UID = "${FILE_ID}_$DRIVE_ID"
        private val methodCounter = AtomicInteger(0)

        private lateinit var fixtureConfig: RealmConfiguration
        private val storedRecordUris = mutableListOf<Uri>()

        @BeforeAll
        @JvmStatic
        fun setUp() {
            fixtureConfig = RealmConfiguration.Builder()
                .schemaVersion(FileMigration.DB_VERSION)
                .modules(RealmModules.LocalFilesModule())
                .name("offline-verification-convergence-${UUID.randomUUID()}")
                .directory(baseContext.cacheDir)
                .build()
        }
    }

    /** This method's seeded identity: the retained bytes and the row model that names them. */
    private class SeededCopy(val bytes: IOFile, val rowModel: File)

    /**
     * Seeds THIS method's own unique identity: the covered row (an EXPLICIT stored remote
     * path — the derivation never walks or writes the realm) and the retained bytes at the
     * sandbox location the PRODUCTION path derivation resolves. Returns both.
     */
    private fun seedRetainedCopy(realm: Realm, payload: String): SeededCopy {
        val methodName = "m${methodCounter.incrementAndGet()}-${UUID.randomUUID()}"
        val fileName = "fixture-$methodName.bin"
        val rowPath = "/fixture/$methodName/"
        val rowModel = File().apply {
            uid = FILE_UID
            id = FILE_ID
            parentId = PARENT_ID
            driveId = DRIVE_ID
            name = fileName
            path = rowPath
        }
        val offlineFile = rowModel.getOfflineFile(sandboxContext, USER_ID)!!
        offlineFile.parentFile?.mkdirs()
        offlineFile.writeText(payload)

        realm.executeTransaction { transactionRealm ->
            transactionRealm.copyToRealmOrUpdate(
                File().apply {
                    uid = FILE_UID
                    id = FILE_ID
                    parentId = PARENT_ID
                    driveId = DRIVE_ID
                    name = fileName
                    path = rowPath
                    isInOfflineFolder = true
                    isMarkedAsOffline = false
                },
            )
        }
        return SeededCopy(offlineFile, rowModel)
    }

    private fun liveRow(realm: Realm): File =
        FileController.getFileById(realm, FILE_ID) ?: error("the fixture row must exist")

    @Test
    @DisplayName("the exact verified revision under full authority authorizes the retirement")
    fun verifiedBasisExactRevisionAuthorizesRetirement() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val seeded = seedRetainedCopy(realm, OLD_PAYLOAD)
            // THE OLD SOURCE'S OWN RECORD through the PRODUCTION writer — a structurally
            // valid synthetic record over the actual seeded bytes (never a mirrored
            // registry file): the authority pre-flight's non-foreign leg must PASS on a
            // record naming this row, not only on record absence.
            check(
                writePublishedProvenance(sandboxContext, seeded.bytes, seeded.rowModel, userDrive),
            ) { "the fixture's synthetic provenance seed must succeed" }
            val verifiedHash = UploadFile.sha256OrNull(seeded.bytes)

            val disposition = classifyOldSourceWithAuthority(
                realm = realm,
                context = sandboxContext,
                userDrive = userDrive,
                row = realm.copyFromRealm(liveRow(realm), 1),
                basisPath = seeded.bytes.absolutePath,
                verifiedBasisHash = verifiedHash,
                checkCancelled = {},
            )

            assertTrue(
                disposition == OldSourceDisposition.VerifiedBasis,
                "the EXACT verified revision under full current authority must be the retirement candidate",
            )
            assertTrue(
                seeded.bytes.exists(),
                "an authorized classification classifies — the caller owns any deletion",
            )
        }
    }

    @Test
    @DisplayName("differing unproven old bytes defer the advancement — the row keeps its location")
    fun differingUnprovenOldBytesDeferTheAdvancement() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val seeded = seedRetainedCopy(realm, OLD_PAYLOAD)
            // The CONFLICTING comparison's basis hash: the staged fresh content's hash —
            // exactly what the publication threads on a differing comparison (the old
            // bytes are unproven: no upload record, no anchored baseline, not proven
            // current).
            val stagedFile = IOFile(sandboxRoot, "staged-${UUID.randomUUID()}.bin")
            stagedFile.writeText(STAGED_PAYLOAD)
            val stagedHash = UploadFile.sha256OrNull(stagedFile)

            val disposition = classifyOldSourceWithAuthority(
                realm = realm,
                context = sandboxContext,
                userDrive = userDrive,
                row = realm.copyFromRealm(liveRow(realm), 1),
                basisPath = seeded.bytes.absolutePath,
                verifiedBasisHash = stagedHash,
                checkCancelled = {},
            )

            assertTrue(
                disposition == OldSourceDisposition.Deferred,
                "a differing old source must NEVER be retirable — the advancement defers and the row keeps A",
            )
            assertTrue(seeded.bytes.exists(), "the deferred old bytes stay in place")
            assertTrue(stagedFile.delete(), "the fixture's staged probe bytes must be removable")
        }
    }

    @Test
    @DisplayName("an absent old source authorizes the advance with nothing to retire")
    fun absentOldSourceAuthorizesAdvanceWithoutRetirement() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val seeded = seedRetainedCopy(realm, OLD_PAYLOAD)
            val basisPath = seeded.bytes.absolutePath
            check(seeded.bytes.delete()) { "the fixture must be able to remove its own bytes" }

            val disposition = classifyOldSourceWithAuthority(
                realm = realm,
                context = sandboxContext,
                userDrive = userDrive,
                row = realm.copyFromRealm(liveRow(realm), 1),
                basisPath = basisPath,
                verifiedBasisHash = null,
                checkCancelled = {},
            )

            assertTrue(
                disposition == OldSourceDisposition.Absent,
                "no old bytes at all must authorize the advancement (nothing to preserve, nothing to retire)",
            )
        }
    }

    @Test
    @DisplayName("a pending upload's old source defers even under a matching verified hash")
    fun pendingUploadOnOldSourceDefersDespiteVerifiedHash() {
        Realm.getInstance(fixtureConfig).use { realm ->
            val seeded = seedRetainedCopy(realm, OLD_PAYLOAD)
            // A REAL PENDING upload record over the old bytes (uploadAt null — the pending
            // gate): the URI-consumer authority leg. The verified hash MATCHES, so any
            // authorization here would prove byte identity alone was treated as authority.
            storedRecordUris.add(
                Uri.parse(
                    UploadFile(
                        uri = Uri.fromFile(seeded.bytes).toString(),
                        driveId = DRIVE_ID,
                        fileModifiedAt = Date(seeded.bytes.lastModified()),
                        fileName = seeded.bytes.name,
                        fileSize = seeded.bytes.length(),
                        remoteFolder = PARENT_ID,
                        originalFileId = FILE_ID,
                        contentHash = UploadFile.sha256OrNull(seeded.bytes),
                        type = UploadFile.Type.SYNC_OFFLINE.name,
                        userId = USER_ID,
                    ).apply { store() }.uri,
                ),
            )
            val verifiedHash = UploadFile.sha256OrNull(seeded.bytes)

            val disposition = classifyOldSourceWithAuthority(
                realm = realm,
                context = sandboxContext,
                userDrive = userDrive,
                row = realm.copyFromRealm(liveRow(realm), 1),
                basisPath = seeded.bytes.absolutePath,
                verifiedBasisHash = verifiedHash,
                checkCancelled = {},
            )

            assertTrue(
                disposition == OldSourceDisposition.Deferred,
                "a pending upload's source is never this operation's to retire — byte identity alone is not authority",
            )
            assertTrue(seeded.bytes.exists(), "the deferred old bytes stay in place")
        }
    }

    @Test
    @DisplayName("a tied-timestamp rename advances against the captured basis observation")
    fun tiedRenameAdvancesWithCapturedBasisObservation() {
        Realm.getInstance(fixtureConfig).use { realm ->
            seedRetainedCopy(realm, OLD_PAYLOAD)
            val observedBasis = realm.copyFromRealm(liveRow(realm), 1)
            // The SAME file renamed: every ordering field tied (the rename inside the
            // timestamp interval), the identity alone differs — the converged advance's
            // observed = verificationBasis (the captured A state). A SECOND detached copy
            // carries the renamed identity (a managed RealmObject has no copy()).
            val renamed = realm.copyFromRealm(liveRow(realm), 1).apply { name = "renamed-${UUID.randomUUID()}.bin" }

            val accepted = FileController.updateExistingFileIfNotOlder(
                newFile = renamed,
                realm = realm,
                userDrive = userDrive,
                observed = observedBasis,
            )

            assertTrue(accepted, "the converged advance accepts the tied rename against the captured basis")
            assertEquals(renamed.name, liveRow(realm).name, "the accepted advance moves the row to the new identity")
        }
    }

    @Test
    @DisplayName("a tied rename against a mismatching or absent observation is rejected")
    fun tiedRenameWithoutMatchingObservationIsRejected() {
        Realm.getInstance(fixtureConfig).use { realm ->
            seedRetainedCopy(realm, OLD_PAYLOAD)
            val renamed = realm.copyFromRealm(liveRow(realm), 1).apply { name = "renamed-${UUID.randomUUID()}.bin" }

            // observed = the FETCHED identity (the comparison basis the pre-gate adoption
            // effectively compared against): a tied identity difference can never prove
            // itself against a live row that still holds the old one.
            val againstFetched = FileController.updateExistingFileIfNotOlder(
                newFile = renamed,
                realm = realm,
                userDrive = userDrive,
                observed = renamed,
            )
            assertFalse(againstFetched, "the tied rename must be rejected against the fetched identity itself")

            // observed = null (no captured basis): the tie is ambiguous — deferred.
            val withoutObservation = FileController.updateExistingFileIfNotOlder(
                newFile = renamed,
                realm = realm,
                userDrive = userDrive,
            )
            assertFalse(withoutObservation, "the tied rename must be rejected without any captured basis")
            assertNotEquals(
                renamed.name,
                liveRow(realm).name,
                "a rejected advance leaves the row at its old identity",
            )
        }
    }

    @Test
    @DisplayName("a strictly newer rename advances with the basis observation")
    fun strictlyNewerRenameAdvancesWithBasisObservation() {
        Realm.getInstance(fixtureConfig).use { realm ->
            seedRetainedCopy(realm, OLD_PAYLOAD)
            val observedBasis = realm.copyFromRealm(liveRow(realm), 1)
            // The ordinary converged advance: the incoming identity demonstrates itself
            // STRICTLY NEWER — accepted without consulting the tie rule at all.
            val renamed = realm.copyFromRealm(liveRow(realm), 1).apply {
                name = "renamed-${UUID.randomUUID()}.bin"
                updatedAt = observedBasis.updatedAt + 60_000L
            }

            val accepted = FileController.updateExistingFileIfNotOlder(
                newFile = renamed,
                realm = realm,
                userDrive = userDrive,
                observed = observedBasis,
            )

            assertTrue(accepted, "the strictly newer rename is the ordinary accepted advance")
            assertEquals(renamed.name, liveRow(realm).name, "the row carries the new identity")
        }
    }

    @AfterAll
    fun cleanup() {
        // ONLY this fixture's own state: the ENTIRE sandbox (the offline subtree AND the
        // sandbox-side registry records — one fixture-owned tree), the upload records by
        // URI, and the random own-cacheDir realm. The shared-by-hash registry directories,
        // the real external-media tree and the real registry are NEVER touched.
        runCatching { sandboxRoot.deleteRecursively() }
        runCatching { if (storedRecordUris.isNotEmpty()) UploadFile.deleteAllFromUris(storedRecordUris) }
        runCatching { Realm.deleteRealm(fixtureConfig) }
    }
}
