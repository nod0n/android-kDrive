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

import androidx.test.core.app.ApplicationProvider
import androidx.work.testing.TestListenableWorkerBuilder
import androidx.work.workDataOf
import com.infomaniak.core.network.models.ApiResponseStatus
import com.infomaniak.core.network.models.ApiResponse
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.CursorApiResponse
import com.infomaniak.drive.data.cache.DriveInfosController
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.FileActivityType
import com.infomaniak.drive.data.models.OfflineFolderRoot
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.models.drive.Drive
import com.infomaniak.drive.data.models.file.LastFileAction
import com.infomaniak.drive.data.services.BaseDownloadWorker
import com.infomaniak.drive.data.services.BulkDownloadWorker
import io.mockk.MockKAnnotations
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.unmockkAll
import io.realm.Realm
import kotlinx.coroutines.delay
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.coroutines.cancellation.CancellationException
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Request
import okhttp3.Response
import okhttp3.ResponseBody.Companion.toResponseBody
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.File as JavaFile
import java.util.concurrent.atomic.AtomicLong

/**
 * THE REAL OFFLINE MAINTENANCE ORCHESTRATION (androidTest instrumentation — the row store is
 * Realm, a native engine that cannot open on the JVM; this is why the test lives here).
 *
 * Unlike the JVM decision tests (which pin the pure contract only), this drives the ACTUAL
 * production chain end to end: [OfflineFolderSyncUtils.refreshOfflineFile] -> classification
 * against the preserved basis -> the real details request -> [stagedRedownload]'s staging file,
 * sha-256 digest, atomic rename publication and basis seam — with ONLY the network boundary
 * injected (mocked [ApiRepository] details responses and a fake download body); realm, files,
 * digests, renames and state transitions are real. Request counts are OBSERVED at the network
 * boundary, not simulated.
 *
 * Fully disposable and account-free: a synthetic UserDrive realm (deleted in cleanup), a
 * synthetic offline file under the instrumentation target's offline dir (deleted), synthetic
 * constants only — NO owner data, NO account, NO real network.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OfflineFolderMaintenanceOrchestrationTest {

    private val context = ApplicationProvider.getApplicationContext<android.content.Context>()
    private val userDrive = UserDrive(userId = 951, driveId = 751)
    private val fileId = 900_401
    private val coveredFolderId = 800_401
    private val offlineRoot = JavaFile(
        File.getOfflineFolder(context),
        "${userDrive.userId}/${userDrive.driveId}",
    )

    private val bodyV1 = "fixture-body-v1".toByteArray()
    // Same length as bodyV1 ON PURPOSE: a changed-hash-under-unchanged-metadata remote still
    // reports the UNCHANGED size — a different-length body would contradict the details the
    // same fixture serves, and production's exact-byte guard correctly refuses it (runtime
    // evidence: the 2d4d run left the old bytes because the staged transfer failed the guard).
    private val bodyV2 = "fixture-body-v2".toByteArray()
    private val editedBody = "fixture-EDITED!".toByteArray() // same length as bodyV1: a same-stat edit
    // A DIFFERENT-length local edit: [File.isIntactFile] compares LENGTH ONLY, so this is the
    // shape that reaches the destructive delete branches (both the legacy download path and
    // the bulk chain's finish cleanup) exactly when the ownership protection is missing.
    private val editedBodyOtherLength = "fixture-EDITED-much-longer!".toByteArray()

    private val detailsRequests = AtomicLong(0)
    private val bodyRequests = AtomicLong(0)

    /** The ids of the details responses actually served, in order (chain-order evidence). */
    private val servedDetailIds = java.util.Collections.synchronizedList(mutableListOf<Int>())

    /** The ids of the bodies actually served, in order (transfer-order evidence). */
    private val servedBodyTags = java.util.Collections.synchronizedList(mutableListOf<String>())

    /** The baseline activities seam: counted so a no-op execution cannot pass its case. */
    private val activityRequests = AtomicLong(0)

    /**
     * The body transport runs the covered-root unmark BEFORE delivering the bytes, only when
     * the case arms it. The unmark stays inside the ONE counting transport stub — an
     * override stub would silently stop counting at the real boundary (runtime evidence:
     * the 2d4d withdrawal case replaced the stub and lost the body count).
     */
    @Volatile
    private var deliverBodyAfterUnmark = false

    /**
     * When armed with a body reference, the ONE counting transport stub PARKS (cancellable
     * delay) instead of delivering, as soon as it is about to serve exactly that body — the
     * real-worker cancellation case holds the transfer in flight at the real boundary.
     */
    @Volatile
    private var parkWhenServingBody: ByteArray? = null

    /** Counted down by the transport stub at the moment the park begins (cancellable delay). */
    private var bodyParkReached = java.util.concurrent.CountDownLatch(1)

    @BeforeAll
    fun initRealm() {
        Realm.init(context)
    }

    @BeforeEach
    fun setUp() {
        MockKAnnotations.init(this)
        detailsRequests.set(0)
        bodyRequests.set(0)
        activityRequests.set(0)
        servedDetailIds.clear()
        servedBodyTags.clear()
        deliverBodyAfterUnmark = false
        parkWhenServingBody = null
        bodyParkReached = java.util.concurrent.CountDownLatch(1)
        runCatching { Realm.deleteRealm(FileController.getRealmConfiguration(userDrive)) }
        runCatching { offlineRoot.deleteRecursively() }

        mockkObject(ApiRepository)
        mockkObject(AccountUtils)
        coEvery { AccountUtils.getHttpClient(any(), any(), any(), any()) } returns OkHttpClient()
        // The body transport is injected; staging, digest, atomic rename and the basis seam
        // below it stay REAL. saveRemoteData is deliberately NOT mocked.
        mockkObject(DownloadOfflineFileManager.Companion)
        coEvery { DownloadOfflineFileManager.downloadFileResponseAsync(any(), any(), any()) } coAnswers {
            bodyRequests.incrementAndGet()
            servedBodyTags.add(lastAnswerBody.get().toString() + "#" + bodyRequests.get())
            if (deliverBodyAfterUnmark) {
                val unmarked = OfflineFolderSyncUtils.unmarkFolderOffline(context, folderRow(800_401), userDrive)
                check(unmarked) { "fixture unmark must confirm" }
            }
            val armedFor = parkWhenServingBody
            if (armedFor != null && lastAnswerBody.get() === armedFor) {
                bodyParkReached.countDown()
                delay(45_000) // cancellable: the cancellation case cancels the worker mid-park
            }
            fakeBodyResponse(lastAnswerBody.get())
        }
    }

    /** The body the fake transport currently serves. */
    private val lastAnswerBody = java.util.concurrent.atomic.AtomicReference(bodyV1)

    private fun fakeBodyResponse(bytes: ByteArray): Response = Response.Builder()
        .request(Request.Builder().url("https://fixture.invalid/download").build())
        .protocol(Protocol.HTTP_1_1)
        .code(200)
        .message("OK")
        .body(bytes.toResponseBody("application/octet-stream".toMediaType()))
        .build()

    /** The details payload the stub serves (counting happens at INVOCATION, in stubDetails).
     *  The response id is a PARAMETER defaulting to the primary fixture file: a multi-file
     *  chain must never serve one file's id in another file's response — updateExistingFile
     *  writes the response over the row the ID selects, so a wrong id silently overwrites a
     *  different row (RESUME-caf1 item 2: the cancelled two-file case served B's metadata
     *  under A's id). */
    private fun detailsPayload(
        hash: String?,
        modifiedAtSec: Long,
        size: Long?,
        responseFileId: Int = fileId,
        parentId: Int? = null,
        name: String = "fixture-orch.bin",
        path: String = "Private/fixture-orch.bin",
    ): ApiResponse<File> {
        return ApiResponse(
            result = ApiResponseStatus.SUCCESS,
            data = File(id = responseFileId, driveId = userDrive.driveId).apply {
                initUid() // the real deserializer derives the primary key; a raw fixture must too
                this.name = name
                this.path = path
                lastModifiedAt = modifiedAtSec
                this.size = size
                serverContentHash = hash
                // A real details response carries the file's REAL parent: getFileFromRemote
                // passes the response through updateExistingFile (insertOrUpdate writes
                // parentId over the row) and returns the RAW response file, whose parentId
                // then drives isRootCoveredCopy's chain walk and the offline path. Runtime
                // evidence (device-8ad8): the default 0 clobbered the seeded covered parent,
                // ended the coverage walk, and routed the covered initial download into the
                // destructive branch instead of the staged path.
                parentId?.let { this.parentId = it }
            },
        )
    }

    private fun stubDetails(
        hash: String?,
        modifiedAtSec: Long,
        size: Long?,
        responseFileId: Int = fileId,
        parentId: Int? = null,
        name: String = "fixture-orch.bin",
        path: String = "Private/fixture-orch.bin",
    ) {
        // The counter increments from an ANSWER at actual request time — a `returns` value
        // is built once at stub SETUP, which counted setups, not network orchestration.
        every { ApiRepository.getFileDetailsWithHash(any(), any()) } answers {
            detailsRequests.incrementAndGet()
            servedDetailIds.add(firstArg<File>().id)
            detailsPayload(hash, modifiedAtSec, size, responseFileId, parentId, name, path)
        }
    }

    /** Seeds the accepted publication: row basis + real bytes on disk with the basis identity. */
    private fun seedAcceptedPublication(bodyBytes: ByteArray, modifiedAtSec: Long, hash: String) {
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${fileId}_${userDrive.driveId}").apply {
                    id = fileId
                    driveId = userDrive.driveId
                    name = "fixture-orch.bin"
                    path = "Private/fixture-orch.bin"
                    lastModifiedAt = modifiedAtSec
                    // A real server-listing row always carries the file size: without it the
                    // recorded basis size is null and every later intact copy classifies as
                    // locally edited (runtime evidence: the 2d4d deleted-copy pass 2 made no
                    // details request at all for exactly this reason).
                    size = bodyBytes.size.toLong()
                    isOffline = true
                    isFromUploads = false
                    offlineBasisModifiedAt = modifiedAtSec
                    offlineBasisSize = bodyBytes.size.toLong()
                    offlineContentDigest = sha256Hex(bodyBytes)
                    serverContentHash = hash
                }
            }
        }
        val offlineFile = getOfflineFixtureFile()
        offlineFile.parentFile?.mkdirs()
        offlineFile.writeBytes(bodyBytes)
        check(offlineFile.setLastModified(modifiedAtSec * 1000)) { "fixture bytes must carry the basis mtime" }
        // The publication seam records where the accepted bytes live; do the same here.
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.offlinePublishedPath = offlineFile.path
            }
        }
    }

    /**
     * Seeds a PENDING covered row: folder + root registry + a marked, not-yet-published file
     * (no basis, no bytes at the offline path) — the exact state the initial bulk chain
     * consumes for a covered copy.
     */
    private fun seedCoveredPendingRow() {
        val folderId = 800_401
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${folderId}_${userDrive.driveId}").apply {
                    id = folderId
                    driveId = userDrive.driveId
                    name = "fixture-orch-folder"
                    path = "Private/fixture-orch-folder"
                    type = "dir"
                }
                transactionRealm.createObject(File::class.java, "${fileId}_${userDrive.driveId}").apply {
                    id = fileId
                    driveId = userDrive.driveId
                    name = "fixture-orch.bin"
                    path = "Private/fixture-orch.bin"
                    // A real listing row always carries its type; the baseline sync's
                    // offline-file query excludes rows without one.
                    type = "file"
                    lastModifiedAt = 1_700_000_000L
                    parentId = folderId
                    isMarkedAsOffline = true
                    isOffline = false
                    isFromUploads = false
                    // The production walk marks pending work WITH its root-ownership
                    // provenance (walkSubtree): the queued state the bulk chain consumes
                    // includes it.
                    offlineOwnedByRoot = true
                }
                transactionRealm.createObject(
                    OfflineFolderRoot::class.java,
                    OfflineFolderRoot.buildUid(folderId, userDrive.driveId, userDrive.userId),
                ).apply {
                    this.folderId = folderId
                    driveId = userDrive.driveId
                    userId = userDrive.userId
                }
            }
        }
    }

    /** Publishes the seeded pending row in place (basis + identity + recorded path), as an
     *  earlier accepted publication would have: the publication seam ALWAYS records
     *  offlinePublishedPath (both record transactions write it) — a seeded publication that
     *  leaves it null is not the state production publishes from, and a move-out case
     *  asserting on the recorded path would then assert a value nothing ever wrote
     *  (RESUME-caf1 item 2). */
    private fun publishSeededRow(hash: String, modifiedAtSec: Long = 1_700_000_000L, publishedBytes: ByteArray = bodyV1) {
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.apply {
                    isOffline = true
                    offlineBasisModifiedAt = modifiedAtSec
                    offlineBasisSize = publishedBytes.size.toLong()
                    offlineContentDigest = sha256Hex(publishedBytes)
                    serverContentHash = hash
                }
            }
        }
        // The bytes must exist at the derived location BEFORE the path is recorded, so the
        // seeded row describes exactly the state a real publication leaves behind.
        val publishedFile = getOfflineFixtureFile()
        publishedFile.parentFile?.mkdirs()
        if (!publishedFile.exists()) publishedFile.writeBytes(publishedBytes)
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()
                    ?.offlinePublishedPath = publishedFile.path
            }
        }
    }

    /** A folder row anywhere in the tree (id, parent, remote path) — coverage walks resolve through it. */
    private fun seedFolderRow(id: Int, name: String, path: String, parentId: Int? = null) {
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${id}_${userDrive.driveId}").apply {
                    this.id = id
                    driveId = userDrive.driveId
                    this.name = name
                    this.path = path
                    type = "dir"
                    parentId?.let { this.parentId = it }
                }
            }
        }
    }

    /** A second root registry row (its folder may nest inside another root's subtree). */
    private fun seedRootRegistry(folderId: Int) {
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(
                    OfflineFolderRoot::class.java,
                    OfflineFolderRoot.buildUid(folderId, userDrive.driveId, userDrive.userId),
                ).apply {
                    this.folderId = folderId
                    driveId = userDrive.driveId
                    userId = userDrive.userId
                }
            }
        }
    }

    /** Bounded row-state evidence for assertion messages: no content bytes, no account data. */
    private fun offlineRowDiagnostics(): String = runCatching {
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()
                ?: return@use "absent"
            "isOffline=${row.isOffline}, marked=${row.isMarkedAsOffline}, basisM=${row.offlineBasisModifiedAt}, " +
                "basisSize=${row.offlineBasisSize}, digestNull=${row.offlineContentDigest == null}, " +
                "hash=${row.serverContentHash}, publishedPathNull=${row.offlinePublishedPath == null}"
        }
    }.getOrDefault("row read failed")

    private fun getOfflineFixtureFile(): JavaFile {
        // DETACH inside the use: a managed row is invalid the moment the realm closes, and
        // getOfflineFile reads row properties (name/path/id) on the returned file. (Runtime
        // evidence: all 14 device cases died on "Realm instance already closed" here.)
        val row = FileController.getRealmInstance(userDrive).use { realm ->
            realm.copyFromRealm(realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!, 0)
        }
        return row.getOfflineFile(context, userDrive.userId)!!
    }

    private fun refresh() = runBlocking {
        OfflineFolderSyncUtils.refreshOfflineFile(context, userDrive, fileId)
    }

    @Test
    @DisplayName("accepted basis, changed hash under unchanged metadata: exactly one body, then stable")
    fun changedHashUnderUnchangedMetadataFetchesExactlyOneBody() {
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_000L, hash = "hash-1")
        val offlineFile = getOfflineFixtureFile()
        lastAnswerBody.set(bodyV2)

        // Pass 1: details report a changed hash under UNCHANGED size/last-modified.
        stubDetails(hash = "hash-2", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())
        refresh()

        assertEquals(1L, detailsRequests.get())
        assertEquals(1L, bodyRequests.get(), "a changed server hash costs exactly one body")
        assertTrue(offlineFile.readBytes().contentEquals(bodyV2), "the staged replacement published the new bytes")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertEquals("hash-2", row.serverContentHash)
            assertEquals(sha256Hex(bodyV2), row.offlineContentDigest)
            assertEquals(bodyV2.size.toLong(), row.offlineBasisSize)
            assertTrue(row.isOffline)
        }

        // Pass 2: unchanged content -> the details request is consulted, NO body.
        refresh()
        assertEquals(2L, detailsRequests.get())
        assertEquals(1L, bodyRequests.get(), "unchanged bodies are never repeatedly downloaded")
        assertTrue(offlineFile.readBytes().contentEquals(bodyV2))
    }

    @Test
    @DisplayName("a same-size/same-mtime local edit is preserved when the server changes")
    fun sameStatLocalEditIsPreservedAgainstAChangedServer() {
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_000L, hash = "hash-1")
        val offlineFile = getOfflineFixtureFile()
        lastAnswerBody.set(bodyV2)

        // The user edits the copy WITHOUT changing size or mtime (a same-stat edit): the basis
        // digest no longer describes the bytes. The rewrite must restore the basis mtime, or
        // the copy would merely classify as edited on metadata alone.
        offlineFile.writeBytes(editedBody)
        check(offlineFile.setLastModified(1_700_000_000L * 1000)) { "fixture edit must keep the basis mtime" }
        check(offlineFile.length() == bodyV1.size.toLong()) { "fixture edit must keep the basis size" }

        stubDetails(hash = "hash-3", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())
        refresh()

        assertTrue(offlineFile.readBytes().contentEquals(editedBody), "the local edit is preserved")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertEquals("hash-1", row.serverContentHash, "the accepted identity did not advance")
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest, "the basis still describes the preserved publication")
        }
    }

    @Test
    @DisplayName("a hash-identical timestamp change adopts metadata without making a clean copy look edited")
    fun hashIdenticalTimestampChangeKeepsTheCopyClean() {
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_000L, hash = "hash-1")
        val offlineFile = getOfflineFixtureFile()
        lastAnswerBody.set(bodyV1)

        // Pass 1: same hash, NEW last-modified -> metadata-only adoption.
        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_500L, size = bodyV1.size.toLong())
        refresh()

        assertEquals(0L, bodyRequests.get(), "a metadata-only change never fetches a body")
        assertEquals(1_700_000_500L * 1000, offlineFile.lastModified(), "the local timestamp follows the adopted metadata")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertEquals(1_700_000_500L, row.offlineBasisModifiedAt)
        }

        // Pass 2: the copy must classify as INTACT again (not frozen as locally edited).
        refresh()
        assertEquals(2L, detailsRequests.get())
        assertEquals(0L, bodyRequests.get())
        assertTrue(offlineFile.readBytes().contentEquals(bodyV1))
    }

    @Test
    @DisplayName("a deleted local copy is restored with one body; the unknown identity is then adopted with one digest-checked body")
    fun deletedCopyIsRestoredWithExactlyOneBody() {
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_000L, hash = "hash-1")
        val offlineFile = getOfflineFixtureFile()
        lastAnswerBody.set(bodyV1)

        // The user (or storage cleanup) removes the local bytes; the row keeps its basis.
        check(offlineFile.delete()) { "fixture setup: the copy must be removable" }

        // Pass 1: MISSING -> the body is fetched WITHOUT a details request (the DOWNLOAD
        // policy: no response establishes this body's hash identity, so the publication
        // records an unknown identity by clearing the previous one).
        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())
        refresh()
        assertEquals(0L, detailsRequests.get(), "the missing-copy restore costs no details request (the DOWNLOAD policy)")
        assertEquals(1L, bodyRequests.get(), "a missing copy costs exactly one body")
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(bodyV1), "the copy is restored")

        // Pass 2: intact again, but the basis carries NO server identity (it was cleared by
        // the restore): the details cannot prove equality, so ONE conservative digest-checked
        // body adopts the identity — the identical body is adopted metadata-only, never a
        // byte replacement.
        refresh()
        assertEquals(1L, detailsRequests.get(), "the intact pass consults details")
        assertEquals(2L, bodyRequests.get(), "the unknown identity costs exactly one digest-checked adoption body")
        assertTrue(offlineFile.readBytes().contentEquals(bodyV1), "the adoption never replaced the bytes")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertEquals("hash-1", row.serverContentHash, "the identity was re-recorded by the adoption")
        }

        // Pass 3: identity recorded -> stable. Details only, no repeated body: the forbidden
        // repeated-unchanged-body behavior would show up exactly here.
        refresh()
        assertEquals(2L, detailsRequests.get(), "the steady state still consults details")
        assertEquals(2L, bodyRequests.get(), "a recorded identity is never re-downloaded")
        assertTrue(offlineFile.readBytes().contentEquals(bodyV1))
    }

    @Test
    @DisplayName("a server-side rename relocates the accepted copy instead of re-downloading it")
    fun serverRenameRelocatesTheAcceptedCopy() {
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_000L, hash = "hash-1")
        val oldOfflineFile = getOfflineFixtureFile()
        lastAnswerBody.set(bodyV2) // must never be fetched

        // Simulate the listing refresh for a renamed file: row name and path move on.
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.apply {
                    name = "fixture-orch-renamed.bin"
                    path = "Private/fixture-orch-renamed.bin"
                }
            }
        }
        val newOfflineFile = getOfflineFixtureFile()
        check(newOfflineFile.path != oldOfflineFile.path) { "fixture setup: the rename must move the location" }

        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())
        refresh()

        assertEquals(0L, bodyRequests.get(), "a relocated copy is never re-downloaded")
        assertEquals(1L, detailsRequests.get())
        assertTrue(newOfflineFile.exists() && newOfflineFile.readBytes().contentEquals(bodyV1), "the bytes moved to the renamed location")
        assertTrue(!oldOfflineFile.exists(), "no orphan copy is left at the old path")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertEquals(newOfflineFile.path, row.offlinePublishedPath)
            assertEquals("hash-1", row.serverContentHash)
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest)
        }
    }

    @Test
    @DisplayName("a renamed location does not un-protect a same-stat local edit")
    fun renameRelocatesAndStillPreservesASameStatEdit() {
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_000L, hash = "hash-1")
        val oldOfflineFile = getOfflineFixtureFile()
        lastAnswerBody.set(bodyV2) // must never be fetched

        // Same-stat edit at the OLD location, then the server renames the file.
        oldOfflineFile.writeBytes(editedBody)
        check(oldOfflineFile.setLastModified(1_700_000_000L * 1000)) { "fixture edit must keep the basis mtime" }
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.apply {
                    name = "fixture-orch-renamed.bin"
                    path = "Private/fixture-orch-renamed.bin"
                }
            }
        }
        val newOfflineFile = getOfflineFixtureFile()

        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())
        refresh()

        assertEquals(0L, bodyRequests.get(), "edited bytes are never re-downloaded over")
        assertTrue(newOfflineFile.exists() && newOfflineFile.readBytes().contentEquals(editedBody), "the edited bytes were relocated, not replaced")
        assertTrue(!oldOfflineFile.exists(), "no orphan copy is left at the old path")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertEquals("hash-1", row.serverContentHash, "the metadata-only adoption was refused: the digest diverges")
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest, "the basis still describes the preserved publication")
            assertEquals(1_700_000_000L, row.offlineBasisModifiedAt)
        }
    }

    @Test
    @DisplayName("a stale queued common worker after an unmark neither touches preserved bytes nor resurrects offline state")
    fun staleWorkerAfterUnmarkPreservesEditedBytes() {
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_000L, hash = "hash-1")
        val offlineFile = getOfflineFixtureFile()
        lastAnswerBody.set(bodyV2) // must never be fetched

        // The file lives inside a marked root (parent chain), and the user edited the copy
        // without changing size or mtime.
        val folderId = 800_401
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${folderId}_${userDrive.driveId}").apply {
                    id = folderId
                    driveId = userDrive.driveId
                    name = "fixture-orch-folder"
                    path = "Private/fixture-orch-folder"
                    type = "dir"
                }
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.parentId = folderId
                transactionRealm.createObject(
                    OfflineFolderRoot::class.java,
                    OfflineFolderRoot.buildUid(folderId, userDrive.driveId, userDrive.userId),
                ).apply {
                    this.folderId = folderId
                    driveId = userDrive.driveId
                    userId = userDrive.userId
                }
            }
        }
        offlineFile.writeBytes(editedBody)
        check(offlineFile.setLastModified(1_700_000_000L * 1000)) { "fixture edit must keep the basis mtime" }

        // The explicit unmark: the edited bytes are preserved, coverage released, the root row gone.
        val unmarkConfirmed = runBlocking { OfflineFolderSyncUtils.unmarkFolderOffline(context, folderRow(folderId), userDrive) }
        assertTrue(unmarkConfirmed, "the unmark completes (nothing refused)")
        FileController.getRealmInstance(userDrive).use { realm ->
            assertTrue(realm.where(OfflineFolderRoot::class.java).count() == 0L, "the root row is removed")
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(!row.isOffline && !row.isMarkedAsOffline && !row.isDirectlyPinned, "no intent signal survives")
            assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(editedBody), "the unmark preserved the edited bytes")
        }

        // The stale queued common worker then runs: the top gate sees no live intent and
        // performs no filesystem effect at all.
        val manager = DownloadOfflineFileManager(
            userDrive = userDrive,
            filesCount = 1,
            downloadWorker = mockk(relaxed = true),
            notificationManagerCompat = mockk(relaxed = true),
        )
        val result = runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }

        assertTrue(result.toString().contains("Success"), "stale work completes silently")
        assertEquals(0L, bodyRequests.get(), "the stale worker never fetches a body")
        assertEquals(0L, detailsRequests.get(), "the stale worker never even refreshes details")
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(editedBody), "the preserved bytes are untouched")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(!row.isOffline, "no offline resurrection")
            assertNull(row.serverContentHash, "no identity is bound to the preserved bytes")
        }
    }

    /**
     * The manager for the covered cases: notification parity is out of scope here (the
     * production covered branch says so itself), so the notification lookup is stubbed to the
     * legitimate null state — a relaxed mock would otherwise return a child mock whose
     * titleResId=0 drives context.getString(0) on the REAL app context inside
     * [DownloadOfflineFileManager.fileDownloaded], an unrelated crash on a boundary these
     * cases do not test.
     */
    private fun coveredManager(): DownloadOfflineFileManager {
        val worker: BaseDownloadWorker = mockk(relaxed = true)
        every { worker.downloadNotification() } returns null
        return DownloadOfflineFileManager(
            userDrive = userDrive,
            filesCount = 1,
            downloadWorker = worker,
            notificationManagerCompat = mockk(relaxed = true),
        )
    }

    @Test
    @DisplayName("a covered copy's initial download publishes through the staged path (owned staging, atomic rename)")
    fun coveredInitialDownloadPublishesThroughTheStagedPath() {
        seedCoveredPendingRow()
        val offlineFile = getOfflineFixtureFile()
        lastAnswerBody.set(bodyV1)

        // The REAL parent: the details response must keep the fixture on the covered path
        // (device-8ad8: the default parentId 0 ended the coverage walk before it began).
        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong(), parentId = coveredFolderId)
        val manager = coveredManager()
        val result = runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }

        // Bounded failure evidence (DEVICE-2d4d-TRIAGE-1958): the production outcome enums
        // and guards log their decisive branch to Sentry only — an assertion failure here
        // carries the observable outcome, request counts, offline-dir listing and row state
        // so the next run pins the failing branch without any production logging change.
        assertTrue(
            result.toString().contains("Success"),
            "the initial covered download succeeds (result=$result, bodyRequests=${bodyRequests.get()}, " +
                "detailsRequests=${detailsRequests.get()}, offlineDir=${offlineFile.parentFile?.list()?.sorted()}, " +
                "row=${offlineRowDiagnostics()})",
        )
        assertEquals(1L, bodyRequests.get(), "exactly one body was fetched")
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(bodyV1), "the bytes were published")
        assertEquals(
            listOf(offlineFile.name),
            offlineFile.parentFile?.list()?.sorted(),
            "no staging leftover remains next to the published copy",
        )
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.isOffline, "the publication recorded the persistent offline status")
            assertEquals("hash-1", row.serverContentHash, "the captured details identity was recorded")
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest)
            assertEquals(offlineFile.path, row.offlinePublishedPath)
        }
    }

    @Test
    @DisplayName("an unmark during a covered initial transfer publishes nothing and leaves the final path untouched")
    fun withdrawalDuringCoveredInitialTransferLeavesTheFinalPathUntouched() {
        seedCoveredPendingRow()
        val offlineFile = getOfflineFixtureFile()
        lastAnswerBody.set(bodyV1)

        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong(), parentId = coveredFolderId)
        // The user unmarks the root WHILE the transfer is in flight: the body is delivered
        // only after the unmark completed. The unmark runs inside the ONE counting transport
        // stub (armed below) — an override stub would stop counting at the real boundary.
        deliverBodyAfterUnmark = true
        val manager = coveredManager()
        val result = runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }

        // Bounded failure evidence, as in the covered-initial case above.
        assertTrue(
            result.toString().contains("Success"),
            "withdrawn work completes silently (no retry loop) (result=$result, bodyRequests=${bodyRequests.get()}, " +
                "detailsRequests=${detailsRequests.get()}, offlineDir=${offlineFile.parentFile?.list()?.sorted()}, " +
                "row=${offlineRowDiagnostics()})",
        )
        assertTrue(!offlineFile.exists(), "the final path was never written: the owned staging was discarded instead")
        assertEquals(1L, bodyRequests.get(), "the transfer fetched a body into staging")
        FileController.getRealmInstance(userDrive).use { realm ->
            assertTrue(realm.where(OfflineFolderRoot::class.java).count() == 0L, "the root row is removed")
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(!row.isOffline && !row.isMarkedAsOffline, "no offline state was resurrected")
            assertNull(row.serverContentHash, "no identity was bound to the discarded staging")
            assertNull(row.offlineContentDigest, "no basis was recorded for the discarded staging")
        }
    }

    @Test
    @DisplayName("a real parent loss during the details refresh keeps a root-published edited copy out of the destructive path")
    fun parentLossDuringDetailsRefreshPreservesARootPublishedEditedCopy() {
        // A root-published, locally edited copy (changed mtime, same length). The details
        // response carries NO parent (the unknown-parent-metadata variant): updateExistingFile
        // writes the default parentId 0 over the row, so the refresh-time coverage walk ends.
        // The gate-time ownership capture must keep the copy out of the destructive path —
        // runtime-analysis evidence (RESUME-6f85): without it the destructive branch deleted
        // exactly these edited bytes and streamed a replacement over them. The ACTUAL
        // preservation branch is the root-owned early return (the copy exists at the unchanged
        // derived path): the transfer is skipped entirely — ZERO body requests. (Independent
        // source review, RESUME-6877 item 2: the earlier draft asserted one body here, which
        // the early return makes unreachable; no download is forced to satisfy it.)
        seedCoveredPendingRow()
        publishSeededRow(hash = "hash-1")
        val offlineFile = getOfflineFixtureFile()
        offlineFile.parentFile?.mkdirs()
        offlineFile.writeBytes(editedBody)
        // The edit's mtime differs from BOTH the basis (1_700_000_000) and the refreshed
        // remote metadata (1_700_100_000): a stale-looking edited copy the destructive path
        // would have deleted.
        check(offlineFile.setLastModified(1_700_050_000L * 1000)) { "the fixture edit must carry its own mtime" }
        lastAnswerBody.set(bodyV1)

        stubDetails(hash = "hash-2", modifiedAtSec = 1_700_100_000L, size = bodyV1.size.toLong())
        val manager = coveredManager()
        val result = runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }

        assertTrue(
            result.toString().contains("Success"),
            "the preserved copy completes silently (result=$result, bodyRequests=${bodyRequests.get()}, " +
                "detailsRequests=${detailsRequests.get()}, offlineDir=${offlineFile.parentFile?.list()?.sorted()}, " +
                "row=${offlineRowDiagnostics()})",
        )
        assertEquals(0L, bodyRequests.get(), "the root-owned early return preserves without any body request")
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(editedBody), "the edited bytes were never destroyed")
        assertEquals(1_700_050_000L, offlineFile.lastModified().div(1000L), "the edit's own mtime is untouched")
        assertEquals(
            listOf(offlineFile.name),
            offlineFile.parentFile?.list()?.sorted(),
            "no staging leftover remains next to the preserved copy",
        )
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertEquals(0, row.parentId, "the refresh really clobbered the parent: the gate capture is what preserved the copy")
            assertTrue(row.offlineOwnedByRoot, "the recorded root ownership survived the refresh")
            assertEquals("hash-1", row.serverContentHash, "a preserved copy records nothing: the accepted identity stands")
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest, "the recorded basis is intact")
            assertEquals(1_700_000_000L, row.offlineBasisModifiedAt, "the basis metadata is intact")
        }
    }

    @Test
    @DisplayName("a cached tree update moving the ANCESTOR out before execute begins keeps the root contract")
    fun ancestorMovedOutBeforeExecuteKeepsTheRootContract() {
        // Schedule A (independent source review, RESUME-6877 item 3A): the root's work is
        // QUEUED; a cached tree update then moves the row's ANCESTOR out of the root's
        // subtree (the file row itself still carries the inside-path and its old parent id).
        // At execute BOTH coverage walks are dead — the gate reads the already-updated chain
        // and the refresh-time walk repeats it — so ONLY the recorded ownership provenance
        // survives to protect the edited copy from the destructive branch.
        seedCoveredPendingRow()
        publishSeededRow(hash = "hash-1")
        // The copy: edited bytes at a DIFFERENT length (the shape every delete branch keys on).
        val offlineFile = getOfflineFixtureFile()
        offlineFile.parentFile?.mkdirs()
        offlineFile.writeBytes(editedBodyOtherLength)
        check(offlineFile.setLastModified(1_700_050_000L * 1000)) { "the fixture edit must carry its own mtime" }

        // Deepen the chain: root(800_401) > inner(800_402) > file, then the cached tree
        // update moves the INNER folder out to a folder outside every root.
        seedFolderRow(id = 800_402, name = "fixture-inner", path = "Private/fixture-orch-folder/inner", parentId = coveredFolderId)
        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.parentId = 800_402
                transactionRealm.where(File::class.java).equalTo(File::id.name, 800_402).findFirst()?.parentId = 800_501
            }
        }
        lastAnswerBody.set(bodyV1)
        // The file itself did not move: its real parent is still the (moved) inner folder,
        // and its path is unchanged — so the derived offline path still holds the edited bytes.
        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong(), parentId = 800_402)

        val manager = coveredManager()
        val result = runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }

        assertTrue(
            result.toString().contains("Success"),
            "the root-owned copy completes silently (result=$result, bodyRequests=${bodyRequests.get()}, " +
                "offlineDir=${offlineFile.parentFile?.list()?.sorted()}, row=${offlineRowDiagnostics()})",
        )
        assertEquals(0L, bodyRequests.get(), "the root contract preserves without any body request")
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(editedBodyOtherLength), "the edited bytes were never destroyed")
        assertEquals(1_700_050_000L, offlineFile.lastModified().div(1000L), "the edit's own mtime is untouched")
        FileController.getRealmInstance(userDrive).use { realm ->
            val inner = realm.where(File::class.java).equalTo(File::id.name, 800_402).findFirst()!!
            assertEquals(800_501, inner.parentId, "the ancestor really moved out: both coverage walks are dead")
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertEquals(800_402, row.parentId, "the file row still carries its unchanged parent id")
            assertTrue(row.offlineOwnedByRoot, "the recorded root ownership is the surviving claim")
            assertNull(
                realm.where(OfflineFolderRoot::class.java).equalTo(OfflineFolderRoot::folderId.name, 800_402).findFirst(),
                "the moved folder is not itself a root",
            )
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest, "the recorded basis is intact")
        }
    }

    @Test
    @DisplayName("a real remote move with VALID metadata withdraws the root publication and keeps the edited bytes at the original path")
    fun realMoveDuringRefreshWithdrawsRootPublicationAndKeepsEditedBytes() {
        // The move-out scenario the earlier draft conflated with the parent clobber: the
        // details response carries a REAL parent outside every root AND a changed path, so
        // the derived offline location moves with it. The staged transfer then runs (one
        // body) but the publication point finds the root ownership STALE (outside every
        // root, not directly pinned) and withdraws: no publication at the new location, the
        // original published bytes and state stand untouched.
        seedCoveredPendingRow()
        publishSeededRow(hash = "hash-1")
        val originalFile = getOfflineFixtureFile()
        originalFile.parentFile?.mkdirs()
        originalFile.writeBytes(editedBody)
        check(originalFile.setLastModified(1_700_050_000L * 1000)) { "the fixture edit must carry its own mtime" }
        // PRECONDITION (RESUME-caf1 item 2): the seeded publication is the REAL one —
        // basis, bytes and recorded path all describe the original location before any move.
        FileController.getRealmInstance(userDrive).use { realm ->
            val seededRow = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            check(seededRow.offlinePublishedPath == originalFile.path) {
                "fixture precondition: the publication must record the original path"
            }
            check(seededRow.offlineContentDigest == sha256Hex(bodyV1)) { "fixture precondition: the recorded digest describes the seeded body" }
        }

        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")
        lastAnswerBody.set(bodyV1)
        stubDetails(
            hash = "hash-2",
            modifiedAtSec = 1_700_100_000L,
            size = bodyV1.size.toLong(),
            parentId = 800_501, // a REAL parent outside every root (valid metadata, not a clobber)
            path = "Other/fixture-outside/fixture-orch.bin", // the derived offline path moves with it
        )
        val manager = coveredManager()
        val result = runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }

        val movedFile = getOfflineFixtureFile() // re-derived from the REFRESHED row path
        assertTrue(
            result.toString().contains("Success"),
            "the withdrawn work completes silently (result=$result, bodyRequests=${bodyRequests.get()}, " +
                "row=${offlineRowDiagnostics()})",
        )
        assertEquals(1L, bodyRequests.get(), "the staged transfer fetched the body once before the withdrawal")
        assertTrue(originalFile.exists() && originalFile.readBytes().contentEquals(editedBody), "the ORIGINAL published bytes stand")
        assertEquals(1_700_050_000L, originalFile.lastModified().div(1000L), "the original bytes keep the edit's mtime")
        assertFalse(movedFile.exists(), "the moved-out location never received a copy")
        assertEquals(
            emptyList<String>(),
            movedFile.parentFile?.list()?.sorted(),
            "no staging leftover remains at the moved-out location",
        )
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertEquals(800_501, row.parentId, "the refresh really moved the row out")
            assertEquals("Other/fixture-outside/fixture-orch.bin", row.path, "the row carries the moved path")
            assertTrue(row.isOffline, "the previous publication's state stands")
            assertEquals("hash-1", row.serverContentHash, "no new identity was bound to the discarded staging")
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest, "the recorded basis is intact")
            assertEquals(1_700_000_000L, row.offlineBasisModifiedAt, "the recorded basis metadata is intact")
            assertEquals(originalFile.path, row.offlinePublishedPath, "the published path still records the ORIGINAL location")
        }
    }

    @Test
    @DisplayName("pending root-owned work moved out during the refresh never becomes an accidental direct pin")
    fun pendingRootOwnedWorkMovedOutDuringRefreshNeverBecomesAnAccidentalPin() {
        // Schedule C: a PENDING root file (no copy, no basis) whose row moves out during the
        // details refresh. The staged transfer runs, but the publication point finds the
        // root ownership stale and withdraws — nothing publishes, nothing is written at
        // either location. The bulk chain's pending clear (the real onFinish schedule) then
        // leaves NO offline signal at all: the file is not a pin, was never one.
        seedCoveredPendingRow() // pending, ownership recorded, NO basis, NO bytes
        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")
        lastAnswerBody.set(bodyV1)
        stubDetails(
            hash = "hash-1",
            modifiedAtSec = 1_700_100_000L,
            size = bodyV1.size.toLong(),
            parentId = 800_501,
            path = "Other/fixture-outside/fixture-orch.bin",
        )
        val manager = coveredManager()
        val result = runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }

        val movedFile = getOfflineFixtureFile()
        assertTrue(
            result.toString().contains("Success"),
            "the withdrawn work completes silently (result=$result, bodyRequests=${bodyRequests.get()}, " +
                "row=${offlineRowDiagnostics()})",
        )
        assertEquals(1L, bodyRequests.get(), "the staged transfer fetched a body before the withdrawal")
        assertFalse(movedFile.exists(), "the moved-out location never received a copy")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertFalse(row.isOffline, "nothing was published: the moved-out file is not an offline copy")
            assertEquals(0L, row.offlineBasisModifiedAt, "no basis was recorded")
            assertNull(row.offlineContentDigest, "no digest was recorded")
            assertNull(row.serverContentHash, "no identity was bound")
            assertNull(row.offlinePublishedPath, "no published path was recorded")
        }
        // The real pending clear (BulkDownloadWorker.onFinish) then leaves no intent either.
        FileController.getRealmInstance(userDrive).use { realm ->
            FileController.markFileAsOffline(customRealm = realm, fileId = fileId, isMarkedAsOffline = false)
        }
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(
                !row.isOffline && !row.isMarkedAsOffline && !row.isDirectlyPinned,
                "no offline signal survives: not a direct pin, not a resurrection",
            )
            assertTrue(row.offlineOwnedByRoot, "the recorded ownership stands until a withdrawal releases it (none ran here)")
        }
    }

    @Test
    @DisplayName("the REAL bulk worker's finish cleanup keeps root-owned edited bytes the refresh moved out")
    fun realWorkerOnFinishCleanupKeepsRootOwnedEditedBytes() {
        // Schedule B: execute preserved the edited copy through the root-owned early return
        // (lastDownloadedFile set); the refresh clobbered the row's parent, so the cleanup's
        // coverage walk is dead — and File.isIntactFile compares LENGTH ONLY, so this
        // different-length edit reaches the delete branch exactly when the ownership
        // protection is missing. The REAL worker runs the whole schedule: its manager
        // executes the file and its onFinish clears the pending markers and cleans up.
        seedCoveredPendingRow()
        publishSeededRow(hash = "hash-1")
        val offlineFile = getOfflineFixtureFile()
        offlineFile.parentFile?.mkdirs()
        offlineFile.writeBytes(editedBodyOtherLength)
        check(offlineFile.setLastModified(1_700_050_000L * 1000)) { "the fixture edit must carry its own mtime" }
        lastAnswerBody.set(bodyV1)
        stubDetails(hash = "hash-2", modifiedAtSec = 1_700_100_000L, size = bodyV1.size.toLong())

        // The worker resolves its file list over the DEFAULT realm drive/user — point it at
        // the fixture drive for this case (the production worker carries the same ids in its
        // input data).
        every { AccountUtils.currentUserId } returns userDrive.userId
        every { AccountUtils.currentDriveId } returns userDrive.driveId
        val worker = TestListenableWorkerBuilder<BulkDownloadWorker>(context)
            .setInputData(
                workDataOf(
                    BulkDownloadWorker.FOLDER_ID to coveredFolderId,
                    BaseDownloadWorker.USER_ID to userDrive.userId,
                    BaseDownloadWorker.DRIVE_ID to userDrive.driveId,
                ),
            )
            .build()

        val workResult = runBlocking { worker.doWork() }

        assertTrue(workResult.toString().contains("Success"), "the bulk chain completed (result=$workResult, row=${offlineRowDiagnostics()})")
        assertEquals(0L, bodyRequests.get(), "the preserved copy consumed no body request")
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(editedBodyOtherLength), "the finish cleanup did not delete the preserved bytes")
        assertEquals(
            1_700_050_000L,
            offlineFile.lastModified().div(1000L),
            "the edit's mtime is untouched (the cleanup's delete branch was reachable: length differs from the refreshed size)",
        )
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertEquals(0, row.parentId, "the refresh clobbered the parent: the cleanup's coverage walk is dead")
            assertFalse(row.isMarkedAsOffline, "the finish schedule cleared the pending marker")
            assertTrue(row.isOffline, "the published copy keeps its persistent state")
            assertTrue(row.offlineOwnedByRoot, "the recorded ownership is what protected the bytes through the cleanup")
        }
    }

    @Test
    @DisplayName("a cancelled REAL worker's finish cleanup keeps the chain's preserved copy")
    fun cancelledRealWorkerCleanupKeepsThePreservedCopy() {
        // The real cancellation schedule: a two-file chain — file A preserved via the
        // root-owned early return (the manager's lastDownloadedFile), file B parked mid
        // staged transfer — the worker cancelled in flight. doWork's completion path still
        // runs onFinish (pending clear + cleanup), and the cleanup must NOT dispose of A's
        // preserved bytes (they are root-owned; the recorded ownership survives the cancelled
        // chain and the refreshed row state).
        seedCoveredPendingRow()
        publishSeededRow(hash = "hash-1")
        val fileAFixture = getOfflineFixtureFile()
        fileAFixture.parentFile?.mkdirs()
        fileAFixture.writeBytes(editedBodyOtherLength)
        check(fileAFixture.setLastModified(1_700_050_000L * 1000)) { "the fixture edit must carry its own mtime" }

        // File B: a second pending child of the root with NO copy — its staged transfer parks.
        // Its name and mtime both order it AFTER file A under the worker's sort, so the chain
        // really reaches the preserved file first.
        val fileIdB = 900_402
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${fileIdB}_${userDrive.driveId}").apply {
                    id = fileIdB
                    driveId = userDrive.driveId
                    name = "fixture-zb.bin"
                    path = "Private/fixture-zb.bin"
                    lastModifiedAt = 1_699_000_000L
                    parentId = coveredFolderId
                    isMarkedAsOffline = true
                    isFromUploads = false
                    offlineOwnedByRoot = true
                }
            }
        }
        val bodyB = "fixture-body-b".toByteArray()
        lastAnswerBody.set(bodyB)
        // Per-file details: A keeps its same-path clobber (preservation branch), B stays
        // inside the root (its staged transfer is the park point).
        every { ApiRepository.getFileDetailsWithHash(any(), any()) } answers {
            detailsRequests.incrementAndGet()
            servedDetailIds.add(firstArg<File>().id)
            val requestedId = firstArg<File>().id
            if (requestedId == fileId) {
                detailsPayload(hash = "hash-2", modifiedAtSec = 1_700_100_000L, size = bodyV1.size.toLong())
            } else {
                // The response carries FILE B's OWN id: updateExistingFile writes the
                // response over the row the ID selects — B's metadata under A's id would
                // overwrite A's row (RESUME-caf1 item 2, javap-independent source defect).
                detailsPayload(
                    hash = "hash-b",
                    modifiedAtSec = 1_699_000_000L,
                    size = bodyB.size.toLong(),
                    responseFileId = fileIdB,
                    parentId = coveredFolderId,
                    name = "fixture-zb.bin",
                    path = "Private/fixture-zb.bin",
                )
            }
        }
        parkWhenServingBody = bodyB

        every { AccountUtils.currentUserId } returns userDrive.userId
        every { AccountUtils.currentDriveId } returns userDrive.driveId
        val worker = TestListenableWorkerBuilder<BulkDownloadWorker>(context)
            .setInputData(
                workDataOf(
                    BulkDownloadWorker.FOLDER_ID to coveredFolderId,
                    BaseDownloadWorker.USER_ID to userDrive.userId,
                    BaseDownloadWorker.DRIVE_ID to userDrive.driveId,
                ),
            )
            .build()

        var parked = false
        var unwound = false
        runBlocking {
            val job = launch { runCatching { worker.doWork() } }
            try {
                // SUSPENDING wait, not CountDownLatch.await: the child runs on THIS
                // runBlocking event loop — a blocking await() parks the loop's only thread
                // and the child could never even start (RESUME-caf1 item 2). delay yields,
                // the loop dispatches the child, B's transfer parks (its own cancellable
                // delay inside the transport stub), and the poll observes the latch.
                parked = withTimeoutOrNull(20_000) {
                    while (bodyParkReached.count > 0) delay(10)
                    true
                } ?: false
            } finally {
                // UNCONDITIONAL cleanup: even a failed or timed-out wait cancels and joins
                // the worker, so no parked transfer can outlive the case.
                job.cancel()
                unwound = withTimeoutOrNull(20_000) {
                    job.join()
                    true
                } ?: false
            }
        }
        assertTrue(parked, "file B's staged transfer really parked mid-flight (bounded suspending wait)")
        assertTrue(unwound, "the cancelled worker unwound in bounded time")
        // Chain-order evidence: file A was processed FIRST, then B reached its OWN transport
        // (one parked body) — the cancellation hit a chain that had already preserved A.
        assertEquals(listOf(fileId, fileIdB), servedDetailIds, "A processed first; each file's refresh requested its own row")
        assertTrue(bodyRequests.get() == 1L, "only file B's transfer reached the real transport boundary")

        assertTrue(fileAFixture.exists() && fileAFixture.readBytes().contentEquals(editedBodyOtherLength), "the cancellation cleanup did not delete file A's preserved bytes")
        assertEquals(1_700_050_000L, fileAFixture.lastModified().div(1000L), "the edit's mtime is untouched")
        FileController.getRealmInstance(userDrive).use { realm ->
            val rowA = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(rowA.isOffline && rowA.offlineOwnedByRoot, "file A's publication and ownership stand")
            assertFalse(rowA.isMarkedAsOffline, "the finish schedule cleared the pending markers")
            assertEquals("fixture-orch.bin", rowA.name, "B's response never overwrote A's row (each response carried its own id)")
            assertEquals(0, rowA.parentId, "A's row state is exactly the preservation fixture's clobber, nothing else")
            val rowB = realm.where(File::class.java).equalTo(File::id.name, fileIdB).findFirst()!!
            assertTrue(!rowB.isOffline && !rowB.isMarkedAsOffline, "file B published nothing (withdrawn by cancellation)")
            assertFalse(realm.where(File::class.java).equalTo(File::id.name, fileIdB).findFirst()!!.getOfflineFile(context, userDrive.userId)!!.exists(), "file B never received a copy")
        }
    }

    @Test
    @DisplayName("the baseline offline sync skips a moved-out root-owned copy instead of deleting on FILE_DELETE")
    fun baselineSyncSkipsTheMovedOutRootOwnedCopy() {
        // The SyncOfflineUtils guards use the same current-tree predicate before their
        // delete/upload/download effects: with only that predicate, a root-owned copy whose
        // row moved out of the subtree would be baseline-acted on (here: a FILE_DELETE
        // activity would delete the preserved bytes) before root maintenance ever runs. The
        // recorded ownership keeps the skip alive. The FULL baseline entry runs — only the
        // network boundaries are injected.
        seedCoveredPendingRow()
        publishSeededRow(hash = "hash-1")
        val offlineFile = getOfflineFixtureFile()
        offlineFile.parentFile?.mkdirs()
        offlineFile.writeBytes(editedBodyOtherLength)
        check(offlineFile.setLastModified(1_700_050_000L * 1000)) { "the fixture edit must carry its own mtime" }

        seedFolderRow(id = 800_402, name = "fixture-inner", path = "Private/fixture-orch-folder/inner", parentId = coveredFolderId)
        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.parentId = 800_402
                transactionRealm.where(File::class.java).equalTo(File::id.name, 800_402).findFirst()?.parentId = 800_501
            }
        }

        mockkObject(DriveInfosController)
        // The drive list is built INSIDE an answer, never as a `returns` concrete-collection
        // value: the Gallery failure already demonstrated that a collection-returning stub
        // can wedge mock-advice recording (fbc284a39 dropped the last such stub).
        every { DriveInfosController.getDrives(any(), any(), any(), any()) } answers {
            arrayListOf(Drive(objectId = "fixture-drive").apply { id = userDrive.driveId })
        }
        every { ApiRepository.getFilesLastActivities(any(), any()) } answers {
            activityRequests.incrementAndGet()
            ApiResponse(
                result = ApiResponseStatus.SUCCESS,
                data = listOf(LastFileAction(fileId = fileId, lastAction = FileActivityType.FILE_DELETE)),
            )
        }
        // The root cadence riding the same entry is out of scope here: its per-file refresh
        // is covered by the other cases in this class.
        coEvery { OfflineFolderSyncUtils.syncOfflineFolderRoots(any()) } returns Unit
        every { AccountUtils.currentUserId } returns userDrive.userId
        every { AccountUtils.currentDriveId } returns userDrive.driveId

        runBlocking { SyncOfflineUtils.startSyncOffline(context) }

        // The activities seam was really reached: a no-op execution (an exception before the
        // guards, a drives read failure) must not pass this case.
        assertTrue(activityRequests.get() >= 1L, "the baseline sync really consulted the activities seam")
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(editedBodyOtherLength), "the FILE_DELETE activity did not delete the root-owned bytes")
        assertEquals(1_700_050_000L, offlineFile.lastModified().div(1000L), "the edit's mtime is untouched")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.isOffline, "the copy's persistent state stands")
            assertTrue(row.offlineOwnedByRoot, "the recorded ownership kept the baseline skip alive")
        }
    }

    @Test
    @DisplayName("a never-root direct pin's stale queued work keeps its baseline semantics")
    fun neverRootDirectPinStaleWorkKeepsBaselineSemantics() {
        // Control: no root anywhere, the user pinned the file themselves (no recorded
        // ownership), an intact copy already on disk, and a stale queued download. The
        // baseline "already there" branch keeps the bytes without a body request, the
        // recorded ownership stays unset, and the finish cleanup keeps an intact copy.
        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${fileId}_${userDrive.driveId}").apply {
                    id = fileId
                    driveId = userDrive.driveId
                    name = "fixture-orch.bin"
                    path = "Other/fixture-outside/fixture-orch.bin"
                    lastModifiedAt = 1_700_000_000L
                    parentId = 800_501
                    size = bodyV1.size.toLong()
                    isOffline = true
                    isDirectlyPinned = true
                    isMarkedAsOffline = true
                    isFromUploads = false
                    offlineBasisModifiedAt = 1_700_000_000L
                    offlineBasisSize = bodyV1.size.toLong()
                    offlineContentDigest = sha256Hex(bodyV1)
                    serverContentHash = "hash-pin"
                }
            }
        }
        val offlineFile = getOfflineFixtureFile()
        offlineFile.parentFile?.mkdirs()
        offlineFile.writeBytes(bodyV1)
        check(offlineFile.setLastModified(1_700_000_000L * 1000)) { "the intact copy must carry the basis mtime" }
        lastAnswerBody.set(bodyV2) // must never be fetched

        stubDetails(hash = "hash-pin", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong(), parentId = 800_501)
        val manager = coveredManager()
        val result = runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }
        manager.cleanLastDownloadedFile() // the finish-cleanup schedule for the same transfer

        assertTrue(result.toString().contains("Success"), "the direct pin's stale work completes (result=$result)")
        assertEquals(0L, bodyRequests.get(), "the baseline kept the intact copy without a body request")
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(bodyV1), "the intact copy stands")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.isDirectlyPinned, "the direct pin stands")
            assertFalse(row.offlineOwnedByRoot, "never-root work records no root ownership")
            assertEquals("hash-pin", row.serverContentHash, "the pin's accepted identity stands")
        }
    }

    @Test
    @DisplayName("an explicit direct pin suppresses the recorded root ownership: the pin's own baseline applies")
    fun explicitDirectPinSuppressesRecordedRootOwnership() {
        // Explicit direct retention: the root queued this file once (ownership recorded),
        // the user pinned it DIRECTLY and the row now lives outside every root. The pin —
        // not the root contract — governs: the baseline refresh behavior applies (a stale
        // copy is replaced by the changed remote), which is only reachable because the
        // recorded ownership is suppressed by the pin.
        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${fileId}_${userDrive.driveId}").apply {
                    id = fileId
                    driveId = userDrive.driveId
                    name = "fixture-orch.bin"
                    path = "Other/fixture-outside/fixture-orch.bin"
                    lastModifiedAt = 1_700_020_000L // the row's metadata is NEWER than the copy: stale-looking
                    parentId = 800_501
                    size = bodyV1.size.toLong()
                    isOffline = true
                    isDirectlyPinned = true
                    isMarkedAsOffline = true
                    isFromUploads = false
                    offlineBasisModifiedAt = 1_700_020_000L
                    offlineBasisSize = bodyV1.size.toLong()
                    offlineContentDigest = sha256Hex(bodyV1)
                    serverContentHash = "hash-1"
                    offlineOwnedByRoot = true // recorded when the root queued it, before the pin
                }
            }
        }
        val offlineFile = getOfflineFixtureFile()
        offlineFile.parentFile?.mkdirs()
        offlineFile.writeBytes(editedBodyOtherLength)
        check(offlineFile.setLastModified(1_700_000_000L * 1000)) { "the copy carries its own older mtime" }
        lastAnswerBody.set(bodyV2)

        // CONSISTENT valid metadata (RESUME-caf1 item 2): the response's parent AND path
        // must describe the row's actual location — a default Private/... path here would
        // rewrite the row to another directory and the refresh would RELOCATE instead of
        // replace in place, which is not what this control proves.
        stubDetails(
            hash = "hash-2",
            modifiedAtSec = 1_700_100_000L,
            size = bodyV1.size.toLong(),
            parentId = 800_501,
            path = "Other/fixture-outside/fixture-orch.bin",
        )
        val manager = coveredManager()
        val result = runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }

        assertTrue(result.toString().contains("Success"), "the pin's refresh completed (result=$result, row=${offlineRowDiagnostics()})")
        assertEquals(1L, bodyRequests.get(), "the baseline refresh fetched the changed remote once")
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(bodyV2), "the pin's baseline replaced the stale copy with the changed remote")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.isDirectlyPinned, "the pin stands")
            assertTrue(row.isOffline, "the refreshed copy is the pin's offline copy")
            assertEquals("Other/fixture-outside/fixture-orch.bin", row.path, "the replacement happened IN PLACE (no relocation was involved)")
            assertEquals(sha256Hex(bodyV2), row.offlineContentDigest, "the refreshed basis was recorded")
            assertEquals(offlineFile.path, row.offlinePublishedPath, "the recorded path is the same location")
        }
    }

    @Test
    @DisplayName("overlapping roots: the copy survives the first owner's unmark and falls only when the last releases")
    fun overlappingRootsKeepTheCopyUntilEveryOwnerReleases() {
        // Nested roots: root2 inside root1's subtree; the file sits in root2. Unmarking
        // root1 keeps the copy (another root owns it — and the recorded ownership survives);
        // only the LAST owner's unmark releases a clean copy through the checked deletion.
        seedCoveredPendingRow()
        publishSeededRow(hash = "hash-1")
        val offlineFile = getOfflineFixtureFile()
        offlineFile.parentFile?.mkdirs()
        offlineFile.writeBytes(bodyV1)
        check(offlineFile.setLastModified(1_700_000_000L * 1000)) { "the clean copy carries the basis mtime" }

        seedFolderRow(id = 800_601, name = "fixture-root2", path = "Private/fixture-orch-folder/root2", parentId = coveredFolderId)
        seedRootRegistry(folderId = 800_601)
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.parentId = 800_601
            }
        }

        val firstUnmark = runBlocking { OfflineFolderSyncUtils.unmarkFolderOffline(context, folderRow(coveredFolderId), userDrive) }
        assertTrue(firstUnmark, "the first unmark completes")
        FileController.getRealmInstance(userDrive).use { realm ->
            assertNull(
                realm.where(OfflineFolderRoot::class.java).equalTo(OfflineFolderRoot::folderId.name, coveredFolderId).findFirst(),
                "root1's registration is gone",
            )
            assertNotNull(
                realm.where(OfflineFolderRoot::class.java).equalTo(OfflineFolderRoot::folderId.name, 800_601).findFirst(),
                "root2 still owns the subtree",
            )
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.isOffline && row.offlineOwnedByRoot, "the copy and its recorded ownership survive the first unmark")
            assertTrue(offlineFile.exists(), "the copy's bytes survive the first unmark")
        }

        val secondUnmark = runBlocking { OfflineFolderSyncUtils.unmarkFolderOffline(context, folderRow(800_601), userDrive) }
        assertTrue(secondUnmark, "the last unmark completes")
        FileController.getRealmInstance(userDrive).use { realm ->
            assertEquals(0L, realm.where(OfflineFolderRoot::class.java).count(), "no root remains")
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(!row.isOffline && !row.isMarkedAsOffline && !row.offlineOwnedByRoot, "the last withdrawal released everything, ownership included")
            assertFalse(offlineFile.exists(), "the clean copy was released through the checked deletion")
        }
    }

    @Test
    @DisplayName("a withdrawal after the LAST pre-publication read publishes nothing and preserves a late local edit")
    fun withdrawalAtPublicationPreservesTheUnmarkPostconditionAndALateLocalEdit() {
        // Published covered copy, intact. The server moved on (new hash, unchanged
        // metadata): the maintenance chain reaches stagedRedownload and decides REPLACE_BYTES.
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_100L, hash = "hash-v1")
        stubDetails(hash = "hash-v2", modifiedAtSec = 1_700_000_100L, size = bodyV2.size.toLong())
        lastAnswerBody.set(bodyV2)

        // Gate placed AFTER the last pre-publication read (live row + existing-file digest)
        // and BEFORE the first mutation: the offline intent is withdrawn THERE, and a late
        // local edit lands at the final path — bytes nobody this code may destroy.
        var publicationWindowReached = false
        OfflineFolderSyncUtils.prePublicationGate = {
            publicationWindowReached = true
            FileController.getRealmInstance(userDrive).use { realm ->
                realm.executeTransaction { transactionRealm ->
                    transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.apply {
                        isOffline = false
                        isMarkedAsOffline = false
                        isDirectlyPinned = false
                        offlineBasisModifiedAt = 0L
                        offlineBasisSize = null
                        offlineContentDigest = null
                        serverContentHash = null
                        offlinePublishedPath = null
                    }
                }
            }
            getOfflineFixtureFile().writeBytes(editedBody)
        }
        try {
            refresh()
        } finally {
            OfflineFolderSyncUtils.prePublicationGate = null
        }

        assertTrue(publicationWindowReached, "the publication window was actually reached (the gate is not a silent no-op)")
        val offlineFile = getOfflineFixtureFile()
        assertTrue(offlineFile.exists(), "the final path keeps the late local edit")
        assertTrue(offlineFile.readBytes().contentEquals(editedBody), "the late edit was never replaced by the staged body")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(!row.isOffline, "no offline flag resurrection at publication")
            assertTrue(!row.isMarkedAsOffline, "no pending-marker resurrection at publication")
            assertEquals(0L, row.offlineBasisModifiedAt, "no basis resurrection at publication")
            assertNull(row.offlineContentDigest, "no digest resurrection at publication")
            assertNull(row.serverContentHash, "no identity resurrection at publication")
        }
    }

    @Test
    @DisplayName("an unmark racing the publication window serializes: defined order, no interleaved state")
    fun unmarkRacingThePublicationWindowSerializesIntoADefinedOrder() {
        // A root-covered, published, intact copy. The server moved on (new hash, unchanged
        // metadata): the maintenance chain reaches stagedRedownload and decides
        // REPLACE_BYTES — it enters the publication critical section.
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_100L, hash = "hash-v1")
        val folderId = 800_402
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${folderId}_${userDrive.driveId}").apply {
                    id = folderId
                    driveId = userDrive.driveId
                    name = "fixture-orch-folder"
                    path = "Private/fixture-orch-folder"
                    type = "dir"
                }
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.parentId = folderId
                transactionRealm.createObject(
                    OfflineFolderRoot::class.java,
                    OfflineFolderRoot.buildUid(folderId, userDrive.driveId, userDrive.userId),
                ).apply {
                    this.folderId = folderId
                    driveId = userDrive.driveId
                    userId = userDrive.userId
                }
            }
        }
        stubDetails(hash = "hash-v2", modifiedAtSec = 1_700_000_100L, size = bodyV2.size.toLong())
        lastAnswerBody.set(bodyV2)

        // The gate fires INSIDE the publication critical section (the pass holds the lock):
        // a concurrent unmark launched HERE can only wait on the same lock — it cannot
        // interleave between the final intent read, the rename and the basis record. The
        // defined orders are "withdrawn first -> nothing published" or "published first ->
        // the unmark removes the published copy afterwards"; either way no mixed state.
        var publicationWindowReached = false
        var unmarkJob: kotlinx.coroutines.Job? = null
        OfflineFolderSyncUtils.prePublicationGate = {
            publicationWindowReached = true
            unmarkJob = kotlinx.coroutines.CoroutineScope(kotlinx.coroutines.Dispatchers.IO).launch {
                OfflineFolderSyncUtils.unmarkFolderOffline(
                    context,
                    File(id = folderId, driveId = userDrive.driveId),
                    userDrive,
                )
            }
        }
        try {
            refresh()
        } finally {
            OfflineFolderSyncUtils.prePublicationGate = null
        }
        runBlocking { unmarkJob?.join() }

        assertTrue(publicationWindowReached, "the publication window was actually reached (the gate is not a silent no-op)")
        assertTrue(bodyRequests.get() == 1L, "exactly one body transfer happened for the publication")
        // BOTH filesystem bytes AND persisted state, after the serialized unmark: the
        // published copy was removed by the user's unmark (intact against its own fresh
        // basis), the intent is gone, and nothing was resurrected or left half-withdrawn.
        val offlineFile = getOfflineFixtureFile()
        assertTrue(!offlineFile.exists(), "the unmark deleted the (intact) published copy after the publication completed")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(!row.isOffline, "the unmark cleared the offline flag")
            assertTrue(!row.isMarkedAsOffline, "the unmark cleared the pending marker")
            assertTrue(!row.isDirectlyPinned, "the unmark cleared the direct pin")
            assertEquals(0L, row.offlineBasisModifiedAt, "the unmark cleared the basis")
            assertNull(row.offlineContentDigest, "the unmark cleared the digest")
        }
        val rootLeft = FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(OfflineFolderRoot::class.java).count()
        }
        assertEquals(0L, rootLeft, "the confirmed unmark released the root")
    }

    @Test
    @DisplayName("a local edit landing inside the coordinated publication window is re-judged under the lock")
    fun localEditInsideThePublicationWindowIsRejudgedUnderTheCoordination() {
        // Published covered copy, intact. The server moved on (new hash, unchanged
        // metadata): the maintenance chain reaches stagedRedownload and decides
        // REPLACE_BYTES from its PRE-LOCK preview evidence.
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_100L, hash = "hash-v1")
        stubDetails(hash = "hash-v2", modifiedAtSec = 1_700_000_100L, size = bodyV2.size.toLong())
        lastAnswerBody.set(bodyV2)

        // The gate fires INSIDE the publication critical section (before the in-lock
        // evidence reads): the edit lands there with the intent KEPT — stale pre-lock
        // evidence must not authorize the rename; the in-lock content revalidation
        // re-judges the copy as edited and preserves it.
        var publicationWindowReached = false
        OfflineFolderSyncUtils.prePublicationGate = {
            publicationWindowReached = true
            getOfflineFixtureFile().writeBytes(editedBody)
        }
        try {
            refresh()
        } finally {
            OfflineFolderSyncUtils.prePublicationGate = null
        }

        assertTrue(publicationWindowReached, "the publication window was actually reached (the gate is not a silent no-op)")
        val offlineFile = getOfflineFixtureFile()
        assertTrue(offlineFile.readBytes().contentEquals(editedBody), "the in-window edit was never replaced by the staged body")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.isOffline, "the intent was never withdrawn")
            assertEquals(1_700_000_100L, row.offlineBasisModifiedAt, "the accepted basis stands untouched")
            assertEquals("hash-v1", row.serverContentHash, "no identity re-stamp from the aborted publication")
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest, "the recorded digest still describes the preserved basis")
        }
        assertTrue(bodyRequests.get() == 1L, "exactly one body transfer happened; nothing re-fetched over the edit")
    }

    /** The publication window was actually reached (the gate is not a silent no-op). */
    @Test
    @DisplayName("a mid-transfer failure discards the owned staging and publishes nothing")
    fun midTransferFailureDiscardsTheOwnedStaging() {
        seedCoveredPendingRow()
        val offlineFile = getOfflineFixtureFile()

        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())
        coEvery { DownloadOfflineFileManager.downloadFileResponseAsync(any(), any(), any()) } answers {
            throw CancellationException("transfer aborted mid-flight")
        }
        val manager = DownloadOfflineFileManager(
            userDrive = userDrive,
            filesCount = 1,
            downloadWorker = mockk(relaxed = true),
            notificationManagerCompat = mockk(relaxed = true),
        )
        // Cancellation propagates out of execute (the same .cancellable() semantics as the
        // baseline transfer path) — the invariant under test is what it leaves behind.
        val thrown = runCatching {
            runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }
        }.exceptionOrNull()

        assertTrue(thrown is CancellationException, "a mid-transfer cancellation propagates (worker semantics)")
        assertTrue(!offlineFile.exists(), "the final path was never written")
        assertTrue(
            offlineFile.parentFile?.listFiles().isNullOrEmpty(),
            "the owned staging was discarded: nothing lingers next to the (missing) copy",
        )
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(!row.isOffline, "a failed transfer publishes no offline status")
            assertNull(row.offlineContentDigest, "a failed transfer records no basis")
        }
    }

    @Test
    @DisplayName("foreign bytes at the renamed location are never blessed or replaced")
    fun foreignBytesAtTheRenamedLocationAreNeverBlessedOrReplaced() {
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_000L, hash = "hash-1")
        val oldOfflineFile = getOfflineFixtureFile()
        lastAnswerBody.set(bodyV2) // must never be fetched or published over the foreign bytes

        // The server renames the file; ANOTHER producer's bytes occupy the new location.
        // They are an adversarial same-stat lookalike of the accepted publication: right
        // size, right mtime, wrong content — only the digest can tell them apart.
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.apply {
                    name = "fixture-orch-renamed.bin"
                    path = "Private/fixture-orch-renamed.bin"
                }
            }
        }
        val newOfflineFile = getOfflineFixtureFile()
        newOfflineFile.parentFile?.mkdirs()
        newOfflineFile.writeBytes(editedBody)
        check(newOfflineFile.setLastModified(1_700_000_000L * 1000)) { "fixture bytes must look same-stat intact" }

        // The server content is unchanged but its timestamp moved: without the digest guard
        // this metadata-only adoption would re-stamp the foreign bytes as the accepted
        // publication (and repoint the recorded path over them).
        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_500L, size = bodyV1.size.toLong())
        refresh()

        assertEquals(0L, bodyRequests.get(), "no body is ever fetched for a metadata-only decision")
        assertTrue(newOfflineFile.readBytes().contentEquals(editedBody), "the foreign bytes were NOT replaced")
        assertTrue(oldOfflineFile.exists() && oldOfflineFile.readBytes().contentEquals(bodyV1), "the recorded copy stays at its recorded path, preserved")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertEquals(oldOfflineFile.path, row.offlinePublishedPath, "the recorded path was not repointed over foreign bytes")
            assertEquals("hash-1", row.serverContentHash, "no new identity was adopted for unverified bytes")
            assertEquals(1_700_000_000L, row.offlineBasisModifiedAt, "the basis was not advanced onto foreign bytes")
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest, "the basis still describes the preserved publication")
        }
    }

    @Test
    @DisplayName("a moved-out root-owned row's intact by-id cache bytes are never published at the moved-out location (R1)")
    fun cacheMoveEntryDeclinesMovedOutRootWorkAndPreservesCacheBytes() {
        // FHNW-caf1 R1: the cache-move entry is a publication path the staged refusal never
        // sees — the by-id cache file survives a row move, so a moved-out root-owned row's
        // previously-viewed bytes are intact there; moving them would fabricate offline
        // retention at the moved-out location through the accepted-publication seam. The
        // authority is decided INSIDE the move seam, immediately before its first
        // irreversible effect; on a decline the cache bytes are preserved where they are.
        // REAL worker path (the bulk chain is the schedule's driver).
        seedCoveredPendingRow()
        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")
        // The user previously VIEWED the file: intact by-id cache bytes exist.
        val cacheFile = File(id = fileId, driveId = userDrive.driveId).getCacheFile(context, userDrive)
        cacheFile.parentFile?.mkdirs()
        cacheFile.writeBytes(bodyV1)
        check(cacheFile.setLastModified(1_700_000_000L * 1000)) { "the cache bytes must be intact against the served metadata" }
        lastAnswerBody.set(bodyV1) // must never be fetched

        stubDetails(
            hash = "hash-1",
            modifiedAtSec = 1_700_000_000L,
            size = bodyV1.size.toLong(),
            parentId = 800_501, // valid metadata: a REAL parent outside every root
            path = "Other/fixture-outside/fixture-orch.bin",
        )
        every { AccountUtils.currentUserId } returns userDrive.userId
        every { AccountUtils.currentDriveId } returns userDrive.driveId
        val worker = TestListenableWorkerBuilder<BulkDownloadWorker>(context)
            .setInputData(
                workDataOf(
                    BulkDownloadWorker.FOLDER_ID to coveredFolderId,
                    BaseDownloadWorker.USER_ID to userDrive.userId,
                    BaseDownloadWorker.DRIVE_ID to userDrive.driveId,
                ),
            )
            .build()

        val workResult = runBlocking { worker.doWork() }

        assertTrue(workResult.toString().contains("Success"), "the declined cache-move completes silently (result=$workResult, row=${offlineRowDiagnostics()})")
        assertEquals(0L, bodyRequests.get(), "nothing was transferred: the move was declined before any effect")
        assertTrue(cacheFile.exists() && cacheFile.readBytes().contentEquals(bodyV1), "the cache bytes were PRESERVED at the cache path (not moved-then-deleted)")
        val movedFile = getOfflineFixtureFile() // re-derived from the REFRESHED row path
        assertFalse(movedFile.exists(), "the moved-out location never received a copy")
        // The staging shape: the seam copies the cache bytes to a staging temp OUTSIDE the
        // authority and renames inside the hold — a decline leaves no staging behind at the
        // (absent) moved-out parent directory.
        assertTrue(
            movedFile.parentFile?.list().orEmpty().none { it.startsWith("offline_staging_") },
            "no staging leftover at the moved-out location",
        )
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertEquals(800_501, row.parentId, "the details refresh really moved the row out")
            assertFalse(row.isOffline, "no offline status was fabricated at the moved-out location")
            assertEquals(0L, row.offlineBasisModifiedAt, "no basis was recorded")
            assertNull(row.offlineContentDigest, "no digest was recorded")
            assertNull(row.offlinePublishedPath, "no published path was recorded")
            assertFalse(row.isMarkedAsOffline, "the worker's finish cleared the pending marker")
            assertTrue(row.offlineOwnedByRoot, "the recorded claim stands for the maintenance release (R2)")
        }
    }

    @Test
    @DisplayName("a coordinated cached tree move committed before the publication declines BEFORE the rename: previous bytes preserved")
    fun coordinatedTreeMoveBeforePublicationDeclinesBeforeTheRenameAndPreservesPreviousBytes() {
        // RESUME-b528-2212 item 1 (replaces the contradictory in-window injection case): the
        // cached tree writers JOIN the commit authority, so the writer-wins order is proven
        // at the REAL writer entry point — FileController.updateFile commits the move BEFORE
        // the publication starts, and the publication's in-hold fresh authority check
        // declines BEFORE the first filesystem effect. The original path is captured BEFORE
        // the move and the moved target derived AFTER it (the two are asserted distinct —
        // getOfflineFixtureFile re-reads the live row, so one variable can never name both).
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_100L, hash = "hash-v1")
        val folderId = 800_401
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${folderId}_${userDrive.driveId}").apply {
                    id = folderId
                    driveId = userDrive.driveId
                    name = "fixture-orch-folder"
                    path = "Private/fixture-orch-folder"
                    type = "dir"
                }
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.apply {
                    parentId = folderId
                    offlineOwnedByRoot = true
                }
                transactionRealm.createObject(
                    OfflineFolderRoot::class.java,
                    OfflineFolderRoot.buildUid(folderId, userDrive.driveId, userDrive.userId),
                ).apply {
                    this.folderId = folderId
                    driveId = userDrive.driveId
                    userId = userDrive.userId
                }
            }
        }
        val originalFile = getOfflineFixtureFile() // captured BEFORE the move
        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")

        // THE COORDINATED WRITER, at its real entry point: the cached tree move commits
        // through FileController.updateFile (a commit-authority participant), before the
        // publication ever starts.
        FileController.updateFile(fileId, userDrive = userDrive) { row ->
            row.parentId = 800_501
            row.path = "Other/fixture-outside/fixture-orch.bin"
        }
        val movedFile = getOfflineFixtureFile() // derived AFTER the move
        check(originalFile.path != movedFile.path) { "fixture: the move really changed the derived path" }

        lastAnswerBody.set(bodyV2)
        stubDetails(hash = "hash-v2", modifiedAtSec = 1_700_000_100L, size = bodyV1.size.toLong())

        var publicationWindowReached = false
        var renamePointReached = false
        OfflineFolderSyncUtils.prePublicationGate = { publicationWindowReached = true }
        OfflineFolderSyncUtils.preRenameGate = { renamePointReached = true }
        try {
            refresh()
        } finally {
            OfflineFolderSyncUtils.prePublicationGate = null
            OfflineFolderSyncUtils.preRenameGate = null
        }

        assertTrue(publicationWindowReached, "the publication window was actually reached (the gate is not a silent no-op)")
        assertFalse(renamePointReached, "the writer-wins refusal preceded the rename point entirely")
        assertTrue(bodyRequests.get() == 1L, "the staged transfer fetched the body once (the decline is not a skipped transfer)")
        assertTrue(originalFile.exists() && originalFile.readBytes().contentEquals(bodyV1), "the PREVIOUS bytes were preserved at the original path")
        assertEquals(1_700_000_100L, originalFile.lastModified().div(1000L), "the previous copy's mtime stands")
        assertFalse(movedFile.exists(), "the moved-out location never received a copy")
        // Staging cleanup at the ACTUAL staging directory (the original copy's parent): an
        // absent parent's list() returns null, so the assertion uses an empty-array default.
        assertTrue(
            originalFile.parentFile?.list().orEmpty().none { it.startsWith("offline_staging_") },
            "the owned staging was discarded: nothing lingers next to the preserved copy",
        )
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.isOffline, "the previous publication's state stands untouched")
            assertEquals("hash-v1", row.serverContentHash, "no new identity was recorded")
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest, "the recorded basis is intact")
            assertEquals(1_700_000_100L, row.offlineBasisModifiedAt, "the recorded basis metadata is intact")
            assertEquals(originalFile.path, row.offlinePublishedPath, "the recorded path still describes the preserved bytes")
            assertTrue(row.offlineOwnedByRoot, "the recorded claim stands for the releases")
        }
    }

    @Test
    @DisplayName("a coordinated pin writer is EXCLUDED from the final publication window: publication wins, the writer observes the completed operation")
    fun coordinatedPinWriterIsExcludedFromTheFinalPublicationWindow() {
        // RESUME-b528-2212 item 1, publication-wins order, positive contention at the REAL
        // writer entry point: while the publication holds the commit authority (the
        // pre-rename gate proves the hold with another thread's failing probe), the REAL
        // FileController.setDirectlyPinned launched from inside the window can only wait —
        // its commit lands AFTER the publication's record, and the final row carries BOTH
        // effects coherently (the published basis and the writer's pin).
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_100L, hash = "hash-v1")
        val folderId = 800_403
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${folderId}_${userDrive.driveId}").apply {
                    id = folderId
                    driveId = userDrive.driveId
                    name = "fixture-orch-folder"
                    path = "Private/fixture-orch-folder"
                    type = "dir"
                }
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.apply {
                    parentId = folderId
                    offlineOwnedByRoot = true
                }
                transactionRealm.createObject(
                    OfflineFolderRoot::class.java,
                    OfflineFolderRoot.buildUid(folderId, userDrive.driveId, userDrive.userId),
                ).apply {
                    this.folderId = folderId
                    driveId = userDrive.driveId
                    userId = userDrive.userId
                }
            }
        }
        stubDetails(hash = "hash-v2", modifiedAtSec = 1_700_000_100L, size = bodyV2.size.toLong())
        lastAnswerBody.set(bodyV2)
        every { AccountUtils.currentUserId } returns userDrive.userId
        every { AccountUtils.currentDriveId } returns userDrive.driveId

        val probeSawFreeAuthority = java.util.concurrent.atomic.AtomicBoolean(true)
        val writerAtEntry = java.util.concurrent.CountDownLatch(1)
        val writerCommitted = java.util.concurrent.CountDownLatch(1)
        var writerError: Throwable? = null
        var writerRef: Thread? = null
        var gateReached = false
        var writerQueuedWhileHoldActive = false
        var pinLandedInsideHold = false
        OfflineFolderSyncUtils.preRenameGate = {
            gateReached = true
            // Positive held-evidence: ANOTHER thread cannot take the authority while the
            // publication is inside its window (bounded, asserted).
            val probe = Thread { probeSawFreeAuthority.set(OfflineCommitAuthority.tryProbeFromOtherThread()) }
            probe.start()
            probe.join(5_000)
            check(!probe.isAlive) { "the authority probe thread must finish (bounded)" }
            check(!probeSawFreeAuthority.get()) { "the authority must be HELD while the publication is in its window" }
            // The REAL coordinated writer, from its real entry point: it signals arrival,
            // then calls the real setter — and the gate WAITS (bounded) until the writer is
            // provably QUEUED at the real authority acquisition while the hold is active
            // (FHNW-1189 E1: the entry latch alone cannot show the writer reached the
            // acquisition — it may be descheduled before it — and a different thread's
            // probe shows only that A lock is held, not that THIS writer contended).
            writerRef = Thread {
                try {
                    writerAtEntry.countDown()
                    FileController.setDirectlyPinned(fileId = fileId, isDirectlyPinned = true)
                    writerCommitted.countDown()
                } catch (t: Throwable) {
                    writerError = t
                    writerCommitted.countDown()
                }
            }.also { it.start() }
            val queuedDeadline = System.currentTimeMillis() + 10_000
            while (!OfflineCommitAuthority.hasQueuedWriter(writerRef!!) && System.currentTimeMillis() < queuedDeadline) {
                Thread.sleep(10)
            }
            check(OfflineCommitAuthority.hasQueuedWriter(writerRef!!)) {
                "the real pin writer must QUEUE at the authority while the publication hold is active"
            }
            writerQueuedWhileHoldActive = true
            // The forbidden interleaving cannot complete: while the writer waits at the
            // acquisition, its pin is NOT in the row.
            OfflineFolderSyncUtils.certifiedFreshRealm(userDrive).use { realm ->
                val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
                check(!row.isDirectlyPinned) { "the queued pin writer must not commit inside the publication's hold" }
                pinLandedInsideHold = row.isDirectlyPinned
            }
        }
        try {
            refresh()
        } finally {
            OfflineFolderSyncUtils.preRenameGate = null
            // Unconditional join AFTER the hold released (never inside it): the writer's
            // completion is awaited with a bound, its failure is rethrown, the thread never
            // leaks.
            writerRef?.let { writer ->
                check(writerAtEntry.await(10, java.util.concurrent.TimeUnit.SECONDS)) { "the writer reached its real entry point" }
                check(writerCommitted.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
                    "the coordinated writer completed only after the publication released the authority"
                }
                writer.join(10_000)
                check(!writer.isAlive) { "the pin writer thread must terminate (bounded)" }
            }
            writerError?.let { throw it }
        }

        assertTrue(gateReached, "the pre-rename point was actually reached (the gate is not a silent no-op)")
        assertTrue(writerQueuedWhileHoldActive, "the real writer PROVABLY contended at the authority acquisition while the hold was active")
        assertFalse(pinLandedInsideHold, "the pin never landed inside the publication window")
        val offlineFile = getOfflineFixtureFile()
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(bodyV2), "the publication's bytes stand at the final path")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.isOffline, "the publication recorded the offline status")
            assertEquals("hash-v2", row.serverContentHash, "the publication recorded the new identity")
            assertEquals(sha256Hex(bodyV2), row.offlineContentDigest, "the publication recorded the accepted digest")
            assertTrue(row.isDirectlyPinned, "the writer's pin commit landed AFTER the publication (both effects present, defined order)")
        }
    }

    @Test
    @DisplayName("a COMPLETE maintenance walk discharges a moved-out owned row (bytes preserved); an INCOMPLETE walk releases nothing")
    fun maintenanceReleaseDischargesMovedOutOwnedRowsAfterACompleteWalkAndPreservesBytes() {
        // FHNW-caf1 R2: normal maintenance must discharge the recorded claim outside every
        // root after a COMPLETE walk (bytes preserved — the remote-move contract), while an
        // INCOMPLETE walk proves nothing and must not release. The rows the walk DID list
        // are excluded: remote presence inside a root keeps the contract whatever the
        // cached chain looks like.
        seedCoveredPendingRow()
        publishSeededRow(hash = "hash-1")
        val offlineFile = getOfflineFixtureFile()
        offlineFile.parentFile?.mkdirs()
        offlineFile.writeBytes(editedBodyOtherLength) // a moved-out EDIT: preserved, never deleted
        check(offlineFile.setLastModified(1_700_050_000L * 1000)) { "the edit carries its own mtime" }

        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")
        // The cached tree update: the row moves out of the root's subtree BEFORE the pass.
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.apply {
                    parentId = 800_501
                    path = "Other/fixture-outside/fixture-orch.bin"
                }
            }
        }

        mockkObject(DriveInfosController)
        // The drive list is built INSIDE an answer at invocation time, never as a `returns`
        // concrete-collection value: the Gallery failure already demonstrated that a
        // collection-returning stub can wedge mock-advice recording (fbc284a39 dropped the
        // last such stub).
        every { DriveInfosController.getDrives(any(), any(), any(), any()) } answers {
            arrayListOf(Drive(objectId = "fixture-drive").apply { id = userDrive.driveId })
        }
        // PHASE 1 — INCOMPLETE walk: the remote listing FAILS; absence is proven by nothing.
        coEvery { ApiRepository.getFolderFiles(any(), any(), any(), any(), any()) } answers {
            CursorApiResponse(result = ApiResponseStatus.ERROR, data = null)
        }
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.offlineOwnedByRoot, "an INCOMPLETE walk releases nothing: absence was not proven")
            assertTrue(row.isOffline, "the copy's state stands")
        }
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(editedBodyOtherLength), "the edited bytes stand")

        // PHASE 2 — COMPLETE walk: the root's remote listing holds nothing (the file moved
        // away remotely); absence is proven, the claim is discharged, bytes PRESERVED.
        coEvery { ApiRepository.getFolderFiles(any(), any(), any(), any(), any()) } answers {
            CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = emptyList<File>(), cursor = null, hasMore = false)
        }
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }

        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertFalse(row.offlineOwnedByRoot, "the COMPLETE walk discharged the recorded claim")
            assertFalse(row.isOffline, "the moved-out copy's offline state was released")
            assertFalse(row.isMarkedAsOffline, "the pending marker was cleared too")
            assertEquals(0L, row.offlineBasisModifiedAt, "the basis was released")
            assertNull(row.offlineContentDigest, "the digest was released")
            assertNotNull(realm.where(OfflineFolderRoot::class.java).findFirst(), "the root itself is NOT unmarked by maintenance")
        }
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(editedBodyOtherLength), "the release PRESERVED the bytes (the remote-move contract)")
        assertEquals(1_700_050_000L, offlineFile.lastModified().div(1000L), "the edit's mtime is untouched")
        assertEquals(0L, bodyRequests.get(), "no body was fetched: a release preserves, it never re-downloads")
    }

    @Test
    @DisplayName("another root's FAILED walk disables the pass-wide discharge: its owned rows stand untouched (R2 sweep scope)")
    fun failedWalkOfAnotherRootDisablesThePassDischargeAndKeepsItsOwnedRows() {
        // RESUME-b528-2212 item 2: root A completes EMPTY while root B — whose cached
        // ancestor chain moved out (X's cached row sits outside B) — FAILS its walk. The
        // old per-root discharge judged every other root from the cache and would have
        // cleared X's ownership from A's completion alone; completing A proves nothing
        // about B. The pass-wide union discharge requires EVERY registered root's walk to
        // complete: a failed walk disables the whole release phase.
        seedCoveredPendingRow()
        // X (the primary fixture row) is claimed+pending under root B, with its cached row
        // moved OUT of B's subtree (the chain gap that made the cache-based judgment fail).
        val rootBFolder = 800_402
        seedFolderRow(id = rootBFolder, name = "fixture-root-b", path = "Private/fixture-root-b")
        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(
                    OfflineFolderRoot::class.java,
                    OfflineFolderRoot.buildUid(rootBFolder, userDrive.driveId, userDrive.userId),
                ).apply {
                    this.folderId = rootBFolder
                    driveId = userDrive.driveId
                    userId = userDrive.userId
                }
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.apply {
                    parentId = 800_501 // the cached chain no longer reaches B: the gap
                    path = "Other/fixture-outside/fixture-orch.bin"
                }
            }
        }
        val offlineFile = getOfflineFixtureFile()
        check(!offlineFile.exists()) { "fixture precondition: pending work has no bytes yet" }

        mockkObject(DriveInfosController)
        every { DriveInfosController.getDrives(any(), any(), any(), any()) } answers {
            arrayListOf(Drive(objectId = "fixture-drive").apply { id = userDrive.driveId })
        }
        // Root A (the primary covered folder) completes EMPTY; root B's listing FAILS.
        coEvery { ApiRepository.getFolderFiles(any(), any(), any(), any(), any()) } answers {
            if (thirdArg<Int>() == rootBFolder) {
                CursorApiResponse(result = ApiResponseStatus.ERROR, data = null)
            } else {
                CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = emptyList<File>(), cursor = null, hasMore = false)
            }
        }
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }

        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.offlineOwnedByRoot, "B's failed walk disabled the discharge: the recorded claim stands")
            assertTrue(row.isMarkedAsOffline, "the pending marker stands: no release cleared it")
            assertFalse(row.isOffline, "the pending row was never published (its own precondition)")
        }
        assertFalse(offlineFile.exists(), "nothing was written at any location")
        assertNotNull(
            FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(OfflineFolderRoot::class.java)
                    .equalTo(OfflineFolderRoot::folderId.name, rootBFolder)
                    .findFirst()
            },
            "root B stays registered",
        )
    }

    @Test
    @DisplayName("a root registered during the pass is a missing walk: the discharge is skipped entirely (R2 sweep scope)")
    fun registryChangeDuringThePassSkipsTheDischarge() {
        // RESUME-b528-2212 item 2, the concurrent-registration guard: the pass's snapshot
        // holds one root; a SECOND root registers (through the REAL coordinated insert)
        // while the first root's walk runs. The second root's walk has not happened — a
        // missing relevant walk — so the in-hold registry re-read diverges from the
        // snapshot and the discharge is skipped: claimed rows keep their contract until a
        // pass whose walks cover the whole registry.
        seedCoveredPendingRow()
        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")
        // The claimed row's cached chain is moved out of its root (it would be discharged
        // by a completion-only judgment).
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.apply {
                    parentId = 800_501
                    path = "Other/fixture-outside/fixture-orch.bin"
                }
            }
        }
        val midPassRegisteredFolder = 800_404

        mockkObject(DriveInfosController)
        every { DriveInfosController.getDrives(any(), any(), any(), any()) } answers {
            arrayListOf(Drive(objectId = "fixture-drive").apply { id = userDrive.driveId })
        }
        var injected = false
        coEvery { ApiRepository.getFolderFiles(any(), any(), any(), any(), any()) } answers {
            if (!injected) {
                injected = true
                // The REAL coordinated registration, at its real entry point shape: the
                // commit authority + the registry transaction (markFolderOffline's insert).
                OfflineCommitAuthority.withCommitAuthority {
                    FileController.getRealmInstance(userDrive).use { realm ->
                        realm.executeTransaction { mutableRealm ->
                            mutableRealm.createObject(
                                OfflineFolderRoot::class.java,
                                OfflineFolderRoot.buildUid(midPassRegisteredFolder, userDrive.driveId, userDrive.userId),
                            ).apply {
                                folderId = midPassRegisteredFolder
                                driveId = userDrive.driveId
                                userId = userDrive.userId
                            }
                        }
                    }
                }
            }
            CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = emptyList<File>(), cursor = null, hasMore = false)
        }
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }

        assertEquals(2L, FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(OfflineFolderRoot::class.java).count()
        }, "the mid-pass registration really happened")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.offlineOwnedByRoot, "a missing relevant walk skipped the discharge: the claim stands")
            assertTrue(row.isMarkedAsOffline, "the pending marker stands")
        }
    }

    @Test
    @DisplayName("a root withdrawn mid-walk marks nothing: the marking transaction re-checks the registry under the authority")
    fun walkMarksNothingWhenTheRootIsWithdrawnMidWalk() {
        // RESUME-b528-2212 item 1, the dying-root resurrection closure: the walk's marking
        // transaction joins the commit authority and re-checks the registry INSIDE its own
        // transaction. The coordinated withdrawal deletes the root row while the walk is in
        // flight; the page's marking must then mark and claim nothing.
        seedCoveredPendingRow()
        // Reset the seeded provenance: the case proves the WALK would have marked it.
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.apply {
                    isMarkedAsOffline = false
                    offlineOwnedByRoot = false
                }
            }
        }
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            check(!row.isMarkedAsOffline && !row.offlineOwnedByRoot) { "fixture precondition: unmarked, unclaimed" }
        }

        mockkObject(DriveInfosController)
        every { DriveInfosController.getDrives(any(), any(), any(), any()) } answers {
            arrayListOf(Drive(objectId = "fixture-drive").apply { id = userDrive.driveId })
        }
        var withdrawn = false
        coEvery { ApiRepository.getFolderFiles(any(), any(), any(), any(), any()) } answers {
            if (!withdrawn) {
                withdrawn = true
                // The REAL coordinated withdrawal of the root row (the unmark closer's
                // deletion shape), while the walk is in flight.
                OfflineCommitAuthority.withCommitAuthority {
                    FileController.getRealmInstance(userDrive).use { realm ->
                        realm.executeTransaction { mutableRealm ->
                            mutableRealm.where(OfflineFolderRoot::class.java).findAll().deleteAllFromRealm()
                        }
                    }
                }
            }
            CursorApiResponse(
                result = ApiResponseStatus.SUCCESS,
                data = listOf(
                    File(id = fileId, driveId = userDrive.driveId).apply {
                        initUid() // listing rows carry their primary key in production
                        name = "fixture-orch.bin"
                        path = "Private/fixture-orch.bin"
                        type = "file"
                        lastModifiedAt = 1_700_000_000L
                        size = bodyV1.size.toLong()
                        parentId = coveredFolderId
                    },
                ),
                cursor = null,
                hasMore = false,
            )
        }
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }

        assertEquals(0L, FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(OfflineFolderRoot::class.java).count()
        }, "the mid-walk withdrawal really happened")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertFalse(row.isMarkedAsOffline, "a withdrawn root's walk marks nothing")
            assertFalse(row.offlineOwnedByRoot, "a withdrawn root's walk claims nothing")
            assertFalse(row.isOffline, "no offline status was fabricated")
        }
    }

    @Test
    @DisplayName("a walk ADOPTS an already-offline copy's ownership; the move-out then releases at the unmark with bytes preserved")
    fun adoptedOfflineCopyAcquiresOwnershipAtWalkAndReleasesAtUnmarkAfterMoveOut() {
        // FHNW-caf1 R6 + RESUME-caf1 item 3a: the acquisition is proven through the REAL
        // walk (never by seeding the flag): a published, non-pinned copy inside a newly
        // marked root's subtree records the root's ownership at the walk's visit — the
        // previously-offline cohort the brand-new-pending branch never stamped. Full
        // lifecycle: real-flow acquisition -> cached move-out -> unmark release.
        seedFolderRow(id = coveredFolderId, name = "fixture-orch-folder", path = "Private/fixture-orch-folder")
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_000L, hash = "hash-1")
        val offlineFile = getOfflineFixtureFile()
        offlineFile.writeBytes(editedBodyOtherLength)
        check(offlineFile.setLastModified(1_700_050_000L * 1000)) { "the edit carries its own mtime" }
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.parentId = coveredFolderId
            }
            val seeded = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            check(!seeded.offlineOwnedByRoot) { "fixture precondition: no ownership before the walk" }
            check(seeded.isOffline && !seeded.isDirectlyPinned) { "fixture precondition: published, non-pinned" }
        }

        // The REAL walk (markFolderOffline): the listing serves the file; saveRemoteFiles
        // and the marking transaction are production code.
        coEvery { ApiRepository.getFolderFiles(any(), any(), any(), any(), any()) } answers {
            CursorApiResponse(
                result = ApiResponseStatus.SUCCESS,
                data = listOf(
                    File(id = fileId, driveId = userDrive.driveId).apply {
                        initUid() // listing rows carry their primary key in production
                        name = "fixture-orch.bin"
                        path = "Private/fixture-orch.bin"
                        type = "file"
                        lastModifiedAt = 1_700_000_000L
                        size = bodyV1.size.toLong()
                        parentId = coveredFolderId
                    },
                ),
                cursor = null,
                hasMore = false,
            )
        }
        val marked = runBlocking { OfflineFolderSyncUtils.markFolderOffline(context, folderRow(coveredFolderId), userDrive) }
        assertTrue(marked, "the walk completed")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.offlineOwnedByRoot, "the walk ADOPTED the already-offline copy: ownership recorded by the real flow")
            assertFalse(row.isMarkedAsOffline, "the copy was not re-marked pending (its own contract stands)")
            assertTrue(row.isOffline, "the copy's own offline state stands")
            assertNotNull(row.offlinePublishedPath, "the recorded publication path was carried through the adoption")
            assertNotNull(realm.where(OfflineFolderRoot::class.java).findFirst(), "the root was registered")
        }

        // The cached tree update: the row moves out of the root's subtree.
        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.apply {
                    parentId = 800_501
                    path = "Other/fixture-outside/fixture-orch.bin"
                }
            }
        }

        // The unmark: the moved-out adopted copy is beyond the subtree — the sweep releases
        // it. The EDITED bytes are preserved; the claim and the offline state are released.
        val unmarkConfirmed = runBlocking { OfflineFolderSyncUtils.unmarkFolderOffline(context, folderRow(coveredFolderId), userDrive) }
        assertTrue(unmarkConfirmed, "the unmark completed (nothing refused)")
        FileController.getRealmInstance(userDrive).use { realm ->
            assertEquals(0L, realm.where(OfflineFolderRoot::class.java).count(), "the root row is removed")
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertFalse(row.offlineOwnedByRoot, "the adopted claim was released by the beyond-subtree sweep")
            assertFalse(row.isOffline, "the offline state was released")
            assertFalse(row.isMarkedAsOffline, "the pending marker was cleared")
        }
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(editedBodyOtherLength), "the EDITED bytes were preserved (never deleted by the release)")
        assertEquals(1_700_050_000L, offlineFile.lastModified().div(1000L), "the edit's mtime is untouched")
    }

    @Test
    @DisplayName("a real unmark completing during the cache copy declines BEFORE the rename: cache and destination preserved")
    fun realWithdrawalDuringTheCacheCopyDeclinesBeforeTheRenameAndPreservesBoth() {
        // FHNW-1189 A2 schedule 1: the staging copy runs OUTSIDE the authority; a REAL
        // unmark (coordinated writer) completes in exactly that span and clears EVERY
        // intent signal — a row that only checked stale-ownership would read "not stale"
        // (owned=false) and rename, discovering the withdrawal only at the record
        // (the forbidden move-then-delete repair). The pre-effect gate must refuse BEFORE
        // the rename: the destination is never created, the cache bytes stay.
        seedCoveredPendingRow()
        val cacheFile = File(id = fileId, driveId = userDrive.driveId).getCacheFile(context, userDrive)
        cacheFile.parentFile?.mkdirs()
        cacheFile.writeBytes(bodyV1)
        check(cacheFile.setLastModified(1_700_000_000L * 1000)) { "the cache bytes must be intact against the served metadata" }
        lastAnswerBody.set(bodyV1) // must never be fetched
        stubDetails(
            hash = "hash-1",
            modifiedAtSec = 1_700_000_000L,
            size = bodyV1.size.toLong(),
            parentId = coveredFolderId, // the row stays covered until the unmark clears it
        )
        every { AccountUtils.currentUserId } returns userDrive.userId
        every { AccountUtils.currentDriveId } returns userDrive.driveId

        DownloadOfflineFileManager.preCacheMovePublicationGate = {
            // The REAL withdrawal, at its real entry point, completing while the staging
            // copy has already run and the hold has not started (the gate runs OUTSIDE the
            // authority, so a bounded blocking wait is safe here).
            val unmarked = runBlocking {
                OfflineFolderSyncUtils.unmarkFolderOffline(context, folderRow(coveredFolderId), userDrive)
            }
            check(unmarked) { "fixture unmark must confirm" }
        }
        val worker = TestListenableWorkerBuilder<BulkDownloadWorker>(context)
            .setInputData(
                workDataOf(
                    BulkDownloadWorker.FOLDER_ID to coveredFolderId,
                    BaseDownloadWorker.USER_ID to userDrive.userId,
                    BaseDownloadWorker.DRIVE_ID to userDrive.driveId,
                ),
            )
            .build()
        try {
            val workResult = runBlocking { worker.doWork() }

            assertTrue(workResult.toString().contains("Success"), "the declined cache-move completes silently (result=$workResult, row=${offlineRowDiagnostics()})")
            assertEquals(0L, bodyRequests.get(), "nothing was transferred")
            assertTrue(cacheFile.exists() && cacheFile.readBytes().contentEquals(bodyV1), "the cache bytes were PRESERVED")
            val destination = getOfflineFixtureFile()
            assertFalse(destination.exists(), "the rename never happened: the destination was never created")
            assertTrue(
                destination.parentFile?.list().orEmpty().none { it.startsWith("offline_staging_") },
                "no staging leftover at the destination directory",
            )
            FileController.getRealmInstance(userDrive).use { realm ->
                val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
                assertFalse(row.isOffline, "the withdrawn offline status was not resurrected")
                assertFalse(row.isMarkedAsOffline, "the unmark cleared the pending marker")
                assertFalse(row.offlineOwnedByRoot, "the unmark cleared the recorded claim")
                assertEquals(0L, row.offlineBasisModifiedAt, "no basis was published")
                assertNull(row.offlineContentDigest, "no digest was recorded")
                assertNull(row.offlinePublishedPath, "no published path was recorded")
                assertEquals(0L, realm.where(OfflineFolderRoot::class.java).count(), "the real unmark completed")
            }
        } finally {
            DownloadOfflineFileManager.preCacheMovePublicationGate = null
        }
    }

    @Test
    @DisplayName("an occupied destination whose bytes the row does not own is never replaced by the cache move")
    fun occupiedEditedDestinationIsNeverReplacedByTheCacheMove() {
        // FHNW-1189 A2 gap 2: rename(2) replaces silently — bytes that arrived at the
        // destination during the staging copy (here: an edited/unknown occupant) must be
        // judged BEFORE the effect; the refusal preserves BOTH sides.
        seedCoveredPendingRow()
        val cacheFile = File(id = fileId, driveId = userDrive.driveId).getCacheFile(context, userDrive)
        cacheFile.parentFile?.mkdirs()
        cacheFile.writeBytes(bodyV1)
        check(cacheFile.setLastModified(1_700_000_000L * 1000)) { "the cache bytes must be intact against the served metadata" }
        lastAnswerBody.set(bodyV1) // must never be fetched
        stubDetails(
            hash = "hash-1",
            modifiedAtSec = 1_700_000_000L,
            size = bodyV1.size.toLong(),
            parentId = coveredFolderId,
        )
        every { AccountUtils.currentUserId } returns userDrive.userId
        every { AccountUtils.currentDriveId } returns userDrive.driveId

        DownloadOfflineFileManager.preCacheMovePublicationGate = {
            // The occupant arrives during the copy (the row is still pending: no basis,
            // no recorded path — the bytes can never be this row's accepted publication).
            val destination = getOfflineFixtureFile()
            destination.parentFile?.mkdirs()
            destination.writeBytes(editedBodyOtherLength)
            check(destination.setLastModified(1_700_050_000L * 1000)) { "the occupant carries its own mtime" }
        }
        val worker = TestListenableWorkerBuilder<BulkDownloadWorker>(context)
            .setInputData(
                workDataOf(
                    BulkDownloadWorker.FOLDER_ID to coveredFolderId,
                    BaseDownloadWorker.USER_ID to userDrive.userId,
                    BaseDownloadWorker.DRIVE_ID to userDrive.driveId,
                ),
            )
            .build()
        try {
            val workResult = runBlocking { worker.doWork() }

            assertTrue(workResult.toString().contains("Success"), "the refused cache-move completes silently (result=$workResult, row=${offlineRowDiagnostics()})")
            assertEquals(0L, bodyRequests.get(), "nothing was transferred")
            assertTrue(cacheFile.exists() && cacheFile.readBytes().contentEquals(bodyV1), "the cache bytes were PRESERVED")
            val destination = getOfflineFixtureFile()
            assertTrue(destination.exists() && destination.readBytes().contentEquals(editedBodyOtherLength), "the occupant's bytes were NOT replaced")
            FileController.getRealmInstance(userDrive).use { realm ->
                val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
                assertFalse(row.isOffline, "no offline status was published over the occupant")
                assertTrue(row.isMarkedAsOffline, "the row's own pending intent stands (only the publication was refused)")
                assertEquals(0L, row.offlineBasisModifiedAt, "no basis was recorded")
                assertNull(row.offlineContentDigest, "no digest was recorded")
                assertNull(row.offlinePublishedPath, "no path was recorded over the occupant")
            }
            assertTrue(
                destination.parentFile?.list().orEmpty().none { it.startsWith("offline_staging_") },
                "no staging leftover at the destination directory",
            )
        } finally {
            DownloadOfflineFileManager.preCacheMovePublicationGate = null
        }
    }

    @Test
    @DisplayName("a real publication completing during the cache copy is never downgraded by the stale cache bytes")
    fun aPublicationDuringTheCacheCopyIsNeverReplacedByTheStaleCacheBytes() {
        // FHNW-1189 A2 schedule 2: a competing publication completes between the
        // generation capture and the hold (the whole copy span). The hold's generation
        // certification refuses BEFORE the rename: the recorded publication's bytes and
        // basis stand, the cache body never replaces them, and no stale digest is stamped
        // as the accepted publication.
        seedCoveredPendingRow()
        val cacheFile = File(id = fileId, driveId = userDrive.driveId).getCacheFile(context, userDrive)
        cacheFile.parentFile?.mkdirs()
        cacheFile.writeBytes(bodyV1)
        check(cacheFile.setLastModified(1_700_000_000L * 1000)) { "the cache bytes must be intact against the served metadata" }
        lastAnswerBody.set(bodyV1) // must never be fetched
        stubDetails(
            hash = "hash-1",
            modifiedAtSec = 1_700_000_000L,
            size = bodyV1.size.toLong(),
            parentId = coveredFolderId,
        )
        every { AccountUtils.currentUserId } returns userDrive.userId
        every { AccountUtils.currentDriveId } returns userDrive.driveId

        DownloadOfflineFileManager.preCacheMovePublicationGate = {
            // A REAL coordinated publication lands at the destination mid-copy, through
            // the real record seam (bytes + basis + identity).
            val destination = getOfflineFixtureFile()
            destination.parentFile?.mkdirs()
            destination.writeBytes(editedBodyOtherLength)
            check(destination.setLastModified(1_700_000_000L * 1000)) { "the published bytes carry the basis mtime" }
            val published = runBlocking {
                OfflineFolderSyncUtils.recordAcceptedPublication(
                    userDrive,
                    FileController.getRealmInstance(userDrive).use { realm ->
                        realm.copyFromRealm(realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!, 0)
                    },
                    destination,
                    acceptedServerHash = "hash-live",
                )
            }
            check(published == OfflineFolderSyncUtils.PublicationOutcome.PUBLISHED) { "fixture: the competing publication must record (got $published)" }
        }
        val worker = TestListenableWorkerBuilder<BulkDownloadWorker>(context)
            .setInputData(
                workDataOf(
                    BulkDownloadWorker.FOLDER_ID to coveredFolderId,
                    BaseDownloadWorker.USER_ID to userDrive.userId,
                    BaseDownloadWorker.DRIVE_ID to userDrive.driveId,
                ),
            )
            .build()
        try {
            val workResult = runBlocking { worker.doWork() }

            assertTrue(workResult.toString().contains("Success"), "the refused cache-move completes silently (result=$workResult, row=${offlineRowDiagnostics()})")
            assertEquals(0L, bodyRequests.get(), "nothing was transferred")
            assertTrue(cacheFile.exists() && cacheFile.readBytes().contentEquals(bodyV1), "the cache bytes were PRESERVED")
            val destination = getOfflineFixtureFile()
            assertTrue(destination.exists() && destination.readBytes().contentEquals(editedBodyOtherLength), "the competing publication's bytes stand")
            FileController.getRealmInstance(userDrive).use { realm ->
                val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
                assertTrue(row.isOffline, "the competing publication's offline status stands")
                assertEquals("hash-live", row.serverContentHash, "the recorded identity was not downgraded")
                assertEquals(sha256Hex(editedBodyOtherLength), row.offlineContentDigest, "the recorded digest still describes the published bytes — never the stale cache body")
            }
        } finally {
            DownloadOfflineFileManager.preCacheMovePublicationGate = null
        }
    }

    @Test
    @DisplayName("a retained reader's thread still sees a pre-acquisition commit under the certified fresh read")
    fun retainedReaderSeesAPreAcquisitionCommitUnderTheCertifiedFreshRead() {
        // FHNW-1189 B, corrected by RESUME-962c item 3 / FHNW-962c P2-4: the former test
        // called certifiedFreshRealm ITSELF before the production seam, pre-refreshing away
        // the very production omission it claimed to exclude. The production entries are now
        // exercised DIRECTLY on the stale retained thread — no test-level certification in
        // between — and the premise (the retained view predates the writer's commit) stays a
        // separate helper-only check.
        // COVERAGE QUALIFICATION (RESUME-33dd item 3): the STALE-SEAM exercise is the RECORD
        // leg (step 4) — recordAcceptedPublicationInHold is called on the thread whose
        // retained view is stale, and the production certification is what observes the
        // post-commit state. The ADOPTION leg (step 5) CANNOT reach a stale retained view by
        // construction: refreshOfflineFile's own certified snapshot at entry refreshes any
        // retained instance on the calling thread before anything else, and the adoption's
        // decision read runs on Dispatchers.IO, where a fresh instance is served — so that
        // leg pins the OBSERVABLE decline through the real maintenance entry (writer-wins,
        // the setLastModified effect never runs), NOT stale-seam exclusion; the decision
        // read's own certification there is uniform-discipline defense-in-depth.
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_100L, hash = "hash-v1")
        val offlineFile = getOfflineFixtureFile()
        val retained = java.util.concurrent.atomic.AtomicReference<Realm>()
        val executor = java.util.concurrent.Executors.newSingleThreadExecutor()
        try {
            // 1. The retained reader: an instance pinned to the CURRENT view, left open.
            executor.submit {
                val realm = FileController.getRealmInstance(userDrive)
                realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()
                retained.set(realm)
            }.get(10, java.util.concurrent.TimeUnit.SECONDS)

            // 2. The REAL coordinated writer commits on another thread: the offline intent
            // is withdrawn through the production entry the file list uses.
            FileController.updateIsOfflineForFiles(listOf(fileId), userDrive = userDrive, isOffline = false)

            // 3. PREMISE ONLY (helper-level, no production entry yet): the retained view
            //    predates the writer's commit — it still shows the withdrawn intent as live.
            executor.submit {
                val staleRow = retained.get()!!.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
                check(staleRow.isOffline) { "fixture premise: the retained view predates the writer's commit" }
            }.get(10, java.util.concurrent.TimeUnit.SECONDS)

            // 4. THE PRODUCTION RECORD SEAM, called directly on the stale retained thread:
            //    its own certified read observes the pre-acquisition withdrawal and declines
            //    BEFORE any record (writer-wins, no filesystem effect, nothing recorded).
            executor.submit {
                val outcome = OfflineCommitAuthority.withCommitAuthority {
                    OfflineFolderSyncUtils.recordAcceptedPublicationInHold(
                        userDrive,
                        File(id = fileId, driveId = userDrive.driveId),
                        offlineFile,
                        acceptedServerHash = null,
                        acceptedDigest = sha256Hex(bodyV1),
                        acceptedLength = offlineFile.length(),
                    )
                }
                check(outcome == OfflineFolderSyncUtils.PublicationOutcome.INTENT_WITHDRAWN) {
                    "the publication must decline on the pre-acquisition withdrawal (got $outcome)"
                }
            }.get(15, java.util.concurrent.TimeUnit.SECONDS)

            // 5. THE PRODUCTION MAINTENANCE ENTRY (submitted on the same executor thread,
            //    though its decision read runs on Dispatchers.IO — see the qualification
            //    above): a hash-identical timestamp change routes to the METADATA_ONLY
            //    adoption — the certified chain observes the withdrawal and declines BEFORE
            //    the setLastModified effect. This pins the decline-path control through the
            //    real entry, not a stale-seam exercise.
            stubDetails(hash = "hash-v1", modifiedAtSec = 1_700_000_500L, size = bodyV1.size.toLong())
            executor.submit {
                runBlocking { OfflineFolderSyncUtils.refreshOfflineFile(context, userDrive, fileId) }
            }.get(15, java.util.concurrent.TimeUnit.SECONDS)
        } finally {
            runCatching { executor.submit { retained.get()?.close() }.get(5, java.util.concurrent.TimeUnit.SECONDS) }
            executor.shutdownNow()
        }
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertFalse(row.isOffline, "the withdrawal stands")
            assertTrue(offlineFile.exists(), "the declined publication recorded nothing and destroyed nothing")
            assertEquals(1_700_000_100L * 1000, offlineFile.lastModified(), "the adoption's timestamp effect NEVER ran on the stale thread")
            assertEquals(1_700_000_100L, row.offlineBasisModifiedAt, "no basis was re-recorded")
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest, "the basis evidence stands for the preserved bytes")
            assertEquals("hash-v1", row.serverContentHash, "no identity was re-recorded")
        }
    }

    @Test
    @DisplayName("a coordinated pin writer queues at the WITHDRAWAL's hold: the withdrawal completes, the pin lands after")
    fun coordinatedPinWriterIsExcludedFromTheWithdrawalHold() {
        // FHNW-1189 E1, pin-versus-withdrawal: the real setDirectlyPinned is launched
        // inside a withdrawal's per-file hold and PROVABLY queues at the real authority
        // acquisition (hasQueuedWriter) — it cannot resurrect intent inside the hold; the
        // withdrawal completes, and the pin commits afterwards, observing the completed
        // operation.
        seedCoveredPendingRow()
        val writerAtEntry = java.util.concurrent.CountDownLatch(1)
        val writerCommitted = java.util.concurrent.CountDownLatch(1)
        var writerError: Throwable? = null
        var writerRef: Thread? = null
        var writerQueuedWhileHoldActive = false
        OfflineFolderSyncUtils.preWithdrawalDecisionGate = {
            writerRef = Thread {
                try {
                    writerAtEntry.countDown()
                    FileController.setDirectlyPinned(fileId = fileId, isDirectlyPinned = true)
                    writerCommitted.countDown()
                } catch (t: Throwable) {
                    writerError = t
                    writerCommitted.countDown()
                }
            }.also { it.start() }
            val queuedDeadline = System.currentTimeMillis() + 10_000
            while (!OfflineCommitAuthority.hasQueuedWriter(writerRef!!) && System.currentTimeMillis() < queuedDeadline) {
                Thread.sleep(10)
            }
            check(OfflineCommitAuthority.hasQueuedWriter(writerRef!!)) {
                "the real pin writer must QUEUE at the authority while the withdrawal hold is active"
            }
            writerQueuedWhileHoldActive = true
            // The forbidden interleaving cannot complete: the pin is not in the row while
            // the withdrawal decides.
            OfflineFolderSyncUtils.certifiedFreshRealm(userDrive).use { realm ->
                check(!realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!.isDirectlyPinned) {
                    "the queued pin writer must not commit inside the withdrawal's hold"
                }
            }
        }
        var unmarkConfirmed = false
        try {
            unmarkConfirmed = runBlocking {
                OfflineFolderSyncUtils.unmarkFolderOffline(context, folderRow(coveredFolderId), userDrive)
            }
        } finally {
            OfflineFolderSyncUtils.preWithdrawalDecisionGate = null
            // Unconditional join AFTER the hold released (never inside it).
            writerRef?.let { writer ->
                check(writerAtEntry.await(10, java.util.concurrent.TimeUnit.SECONDS)) { "the writer reached its real entry point" }
                check(writerCommitted.await(10, java.util.concurrent.TimeUnit.SECONDS)) {
                    "the coordinated writer completed only after the withdrawal released the authority"
                }
                writer.join(10_000)
                check(!writer.isAlive) { "the pin writer thread must terminate (bounded)" }
            }
            writerError?.let { throw it }
        }

        assertTrue(unmarkConfirmed, "the unmark completed")
        assertTrue(writerQueuedWhileHoldActive, "the real writer PROVABLY contended at the authority acquisition while the hold was active")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.isDirectlyPinned, "the pin commit landed AFTER the withdrawal (both effects present, defined order)")
            assertFalse(row.isOffline, "the withdrawal cleared the offline state before the pin landed")
            assertFalse(row.isMarkedAsOffline, "the withdrawal cleared the pending marker")
            assertFalse(row.offlineOwnedByRoot, "the withdrawal released the recorded claim")
        }
        assertEquals(0L, FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(OfflineFolderRoot::class.java).count()
        }, "the confirmed unmark released the root")
    }

    @Test
    @DisplayName("a missing root folder row recovered from the authoritative remote answer: the release defers while it is unproven and converges on the walked pass")
    fun missingRootFolderRowIsRecoveredFromTheAuthoritativeRemoteAnswer() {
        // FHNW-1189 A3, cache loss, corrected by RESUME-962c item 2: the former recovery
        // called updateExistingFile — a refresh-only lever whose insert runs inside
        // getFileById(...)?.let — so an ABSENT row made the recovery a silent no-op that
        // still reported success. The coordinated INSERT lever actually creates the row.
        // And per RESUME-962c item 5 (FHNW-962c P2-5), the recovered root is UNPROVEN this
        // pass: the whole release defers (the former "the completed root's release still
        // ran" expectation WAS the premature-release schedule), then converges on the pass
        // that really walks the recovered root.
        seedCoveredPendingRow()
        val rootBFolder = 800_402
        seedRootRegistry(rootBFolder) // registry row WITHOUT a cached folder row (cache loss)
        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())

        mockkObject(DriveInfosController)
        every { DriveInfosController.getDrives(any(), any(), any(), any()) } answers {
            arrayListOf(Drive(objectId = "fixture-drive").apply { id = userDrive.driveId })
        }
        val walkedFolderIds = java.util.Collections.synchronizedList(mutableListOf<Int>())
        coEvery { ApiRepository.getFolderFiles(any(), any(), any(), any(), any()) } answers {
            walkedFolderIds.add(thirdArg())
            CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = emptyList<File>(), cursor = null, hasMore = false)
        }
        // The authoritative remote answer for the missing folder: LIVE (recovered).
        every { ApiRepository.getFileDetails(any(), any()) } answers {
            ApiResponse(
                result = ApiResponseStatus.SUCCESS,
                data = File(id = rootBFolder, driveId = userDrive.driveId).apply {
                    initUid()
                    name = "fixture-root-b"
                    path = "Private/fixture-root-b"
                    type = "dir"
                },
            )
        }
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }

        val recoveredRow = FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(File::class.java).equalTo(File::id.name, rootBFolder).findFirst()
        }
        assertNotNull(recoveredRow, "the cached root folder row was RECOVERED from the remote details (the insert-side lever really creates it)")
        assertNotNull(
            FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(OfflineFolderRoot::class.java).equalTo(OfflineFolderRoot::folderId.name, rootBFolder).findFirst()
            },
            "the registration was NOT withdrawn on a missing cache row",
        )
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.offlineOwnedByRoot, "the release DEFERRED while the recovered root is unproven (the all-complete gate)")
            assertTrue(row.isMarkedAsOffline, "the pending intent stands through the deferral")
        }
        assertEquals(0L, bodyRequests.get(), "a deferral preserves: nothing was re-downloaded")

        // PASS 2: the recovered row is really WALKED, and with BOTH walks complete the
        // deferred release applies (the covered root's listing omits the row — remotely
        // absent from every proven root).
        walkedFolderIds.clear()
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }

        assertTrue(walkedFolderIds.contains(rootBFolder), "the following pass walks the recovered root")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertFalse(row.offlineOwnedByRoot, "the deferred release converged once both walks completed")
            assertFalse(row.isOffline, "the released row's intent stands cleared")
            assertFalse(row.isMarkedAsOffline, "the released row's marker stands cleared")
        }
        assertEquals(0L, bodyRequests.get(), "a release preserves: nothing was re-downloaded")
    }

    @Test
    @DisplayName("a listing writer racing the recovery preserves the NEWER row: the in-transaction absence recheck")
    fun aListingWriterRacingTheRecoveryPreservesTheNewerRow() {
        // RESUME-33dd item 1: the absence premise is established BEFORE the remote details
        // request, outside any hold — while the recovery waits, a real listing writer may
        // recreate the row with newer cache state and child links. The former lever wrote
        // insertOrUpdate unconditionally: the detached details snapshot would have
        // overwritten that newer row app-writer-wins. Now the insert RECHECKS absence
        // inside the authoritative write transaction and preserves the row that appeared.
        // (The absent-row positive control is the case above.)
        seedCoveredPendingRow()
        val rootBFolder = 800_403
        seedRootRegistry(rootBFolder) // registry row WITHOUT a cached folder row (cache loss)
        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())

        // THE RACING LISTING WRITER, on the real seam that recreates a missing folder row
        // (saveRemoteFiles with no local proxy copies the folder row in and links its
        // children): it runs INSIDE the details request, BEFORE the detached (older)
        // answer returns, so the recovery's hold opens on a row that already exists with
        // NEWER state.
        val racingChildRow = File(id = 800_404, driveId = userDrive.driveId).apply {
            initUid()
            name = "child-of-b.bin"
            path = "Private/fixture-root-b/child-of-b.bin"
            type = "file"
            parentId = rootBFolder
            lastModifiedAt = 1_700_000_050L
            size = bodyV1.size.toLong()
        }
        val listingRenamedRoot = File(id = rootBFolder, driveId = userDrive.driveId).apply {
            initUid()
            name = "fixture-root-b-renamed"
            path = "Private/fixture-root-b-renamed"
            type = "dir"
        }
        mockkObject(DriveInfosController)
        every { DriveInfosController.getDrives(any(), any(), any(), any()) } answers {
            arrayListOf(Drive(objectId = "fixture-drive").apply { id = userDrive.driveId })
        }
        val walkedFolderIds = java.util.Collections.synchronizedList(mutableListOf<Int>())
        coEvery { ApiRepository.getFolderFiles(any(), any(), any(), any(), any()) } answers {
            walkedFolderIds.add(thirdArg())
            CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = emptyList<File>(), cursor = null, hasMore = false)
        }
        every { ApiRepository.getFileDetails(any(), any()) } answers {
            FileController.getRealmInstance(userDrive).use { realm ->
                FileController.saveRemoteFiles(
                    realm = realm,
                    localFolderProxy = null,
                    remoteFolder = listingRenamedRoot,
                    apiResponse = CursorApiResponse(
                        result = ApiResponseStatus.SUCCESS,
                        data = arrayListOf(racingChildRow),
                        cursor = null,
                        hasMore = false,
                    ),
                    isFirstPage = true,
                    isCompleteFolder = true,
                )
            }
            ApiResponse(
                result = ApiResponseStatus.SUCCESS,
                data = File(id = rootBFolder, driveId = userDrive.driveId).apply {
                    initUid()
                    name = "fixture-root-b"
                    path = "Private/fixture-root-b"
                    type = "dir"
                },
            )
        }
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }

        FileController.getRealmInstance(userDrive).use { realm ->
            val preserved = realm.where(File::class.java).equalTo(File::id.name, rootBFolder).findFirst()!!
            assertEquals(
                "fixture-root-b-renamed",
                preserved.name,
                "the racing listing writer's NEWER row survived: the detached details snapshot never overwrote it",
            )
            assertTrue(preserved.children.any { it.id == 800_404 }, "the racing writer's child link survived")
            assertNotNull(realm.where(File::class.java).equalTo(File::id.name, 800_404).findFirst(), "the racing writer's child row exists")
        }
        assertNotNull(
            FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(OfflineFolderRoot::class.java).equalTo(OfflineFolderRoot::folderId.name, rootBFolder).findFirst()
            },
            "the registration stands",
        )
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.offlineOwnedByRoot, "the release DEFERRED while the preserved root is unproven (the all-complete gate)")
        }
        assertEquals(0L, bodyRequests.get(), "a deferral preserves: nothing was re-downloaded")

        // PASS 2: the preserved row is really WALKED and the deferred release converges —
        // the preserved cache state is fully functional, not merely present.
        walkedFolderIds.clear()
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }
        assertTrue(walkedFolderIds.contains(rootBFolder), "the following pass walks the PRESERVED row")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertFalse(row.offlineOwnedByRoot, "the deferred release converged once both walks completed")
        }
        assertEquals(0L, bodyRequests.get(), "a release preserves: nothing was re-downloaded")
    }

    @Test
    @DisplayName("an OLD pass parked across a same-id re-registration defers: the successor's own evidence completes the churn")
    fun anOldPassParkedAcrossASameIdReregistrationDefersTheRelease() {
        // RESUME-33dd item 2, the ACTUAL old-pass/new-registration ABA — the serial
        // unmark -> re-mark -> pass control is the ordinary lifecycle, not this schedule.
        // The old pass captures the registration and X's claim; while it is parked in its
        // refresh span (a real suspension window), a REAL unmark+re-mark replaces the
        // registration with the SAME folder id and re-acquires X through the successor's
        // own marking walk; the old pass's completed listing omits X and predates that
        // walk. The former gate compared folder-id SETS: the successor's identical set
        // passed, and the stale union would have discharged a claim the successor had just
        // re-acquired. The gate now compares REGISTRATION LIFETIMES (markedAt is stamped
        // at every mark): pass 1 defers; the successor's OWN pass judges X from current
        // evidence. The listing schedule below IS the remote churn (X away, X back, X
        // away again) — every answer a real listing at that moment.
        seedCoveredPendingRow()
        stubDetails(
            hash = "hash-1",
            modifiedAtSec = 1_700_000_050L,
            size = bodyV1.size.toLong(),
            responseFileId = 800_405,
            name = "fixture-y.bin",
            path = "Private/fixture-orch-folder/fixture-y.bin",
        )

        val remoteXRow = File(id = fileId, driveId = userDrive.driveId).apply {
            initUid()
            name = "fixture-orch.bin"
            path = "Private/fixture-orch.bin"
            type = "file"
            parentId = coveredFolderId
            lastModifiedAt = 1_700_000_000L
            size = bodyV1.size.toLong()
        }
        val remoteYRow = File(id = 800_405, driveId = userDrive.driveId).apply {
            initUid()
            name = "fixture-y.bin"
            path = "Private/fixture-orch-folder/fixture-y.bin"
            type = "file"
            parentId = coveredFolderId
            lastModifiedAt = 1_700_000_050L
            size = bodyV1.size.toLong()
        }
        lastAnswerBody.set(bodyV1)
        mockkObject(DriveInfosController)
        every { DriveInfosController.getDrives(any(), any(), any(), any()) } answers {
            arrayListOf(Drive(objectId = "fixture-drive").apply { id = userDrive.driveId })
        }
        val walkCalls = java.util.concurrent.atomic.AtomicInteger()
        coEvery { ApiRepository.getFolderFiles(any(), any(), any(), any(), any()) } answers {
            when (walkCalls.incrementAndGet()) {
                1 -> CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = arrayListOf(remoteYRow), cursor = null, hasMore = false)
                2 -> CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = arrayListOf(remoteXRow, remoteYRow), cursor = null, hasMore = false)
                else -> CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = arrayListOf(remoteYRow), cursor = null, hasMore = false)
            }
        }
        // THE PARKED SPAN: pass 1 refreshes its one listed file (Y) — a basis-less pending
        // row routes straight to the staged redownload, whose body transport is a real
        // suspension window (the deliverBodyAfterUnmark precedent). INSIDE it, the real
        // unmark+re-mark replace the registration and re-acquire X.
        val raced = java.util.concurrent.atomic.AtomicBoolean()
        coEvery { DownloadOfflineFileManager.downloadFileResponseAsync(any(), any(), any()) } coAnswers {
            if (raced.compareAndSet(false, true)) {
                assertTrue(OfflineFolderSyncUtils.unmarkFolderOffline(context, folderRow(coveredFolderId), userDrive), "the unmark confirmed")
                assertTrue(OfflineFolderSyncUtils.markFolderOffline(context, folderRow(coveredFolderId), userDrive), "the re-mark confirmed")
            }
            bodyRequests.incrementAndGet()
            fakeBodyResponse(lastAnswerBody.get())
        }

        // PASS 1: the old pass walks A (omits X), publishes Y across the raced
        // re-registration, then reaches the discharge hold. The lifetime map now DIFFERS
        // (the successor's markedAt) — the whole release defers, and X's re-acquired
        // claim STANDS (the former id-set gate would have discharged it here).
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }
        FileController.getRealmInstance(userDrive).use { realm ->
            val xRow = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(xRow.offlineOwnedByRoot, "the stale-union ABA discharge never ran: X's re-acquired claim stands (the lifetime gate deferred)")
            assertTrue(xRow.isMarkedAsOffline, "X's pending intent stands through the deferral")
            assertEquals(1L, realm.where(OfflineFolderRoot::class.java).count(), "the successor registration stands")
        }
        assertTrue(bodyRequests.get() >= 1, "Y's publication crossed the parked span (the race actually ran)")

        // PASS 2: the successor is captured at ITS start; its own walk omits X again —
        // with CURRENT evidence the discharge applies: the bounded churn completes (claim
        // and marker clear; no bytes ever existed to decide about), the deferral was not
        // immortal. Y — the mid-pass claim the successor's own walk owns — is kept.
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }
        FileController.getRealmInstance(userDrive).use { realm ->
            val xRow = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertFalse(xRow.offlineOwnedByRoot, "the successor's own current evidence completes the churn: the deferral was bounded, not immortal")
            assertFalse(xRow.isMarkedAsOffline, "the pending marker stands cleared")
            assertEquals(1L, realm.where(OfflineFolderRoot::class.java).count(), "the successor registration stands")
            val yRow = realm.where(File::class.java).equalTo(File::id.name, 800_405).findFirst()!!
            assertTrue(yRow.offlineOwnedByRoot, "the mid-pass claim the successor's walk owns was never judged by the old pass (kept)")
            assertTrue(yRow.isOffline, "Y published across the race and stands")
        }
    }

    @Test
    @DisplayName("an inconclusive missing-root answer defers the release; the definitive answer converges on the subsequent passes")
    fun inconclusiveMissingRootDoesNotFreezeTheCompletedRootsRemoteDeleteRelease() {
        // FHNW-1189 A3 + RESUME-962c item 5 (FHNW-962c P2-5): an inconclusive answer keeps
        // the registration and DEFERS the whole release (the all-complete gate — the
        // completed root's listing alone no longer releases anything: the former "the
        // completed root's release still ran" expectation WAS the premature-release
        // schedule the reviewer confirmed). The deferral is bounded: the definitive answer
        // withdraws the registration, and the subsequent pass — whose registry snapshot
        // matches again — releases both the deferred remote-delete evidence AND the ended
        // contract's orphaned claims. The reviewer's later byte-loss tail was WITHDRAWN
        // (FHNW-REVIEW-962c-ADDENDUM): no byte-loss assertion is made here; the pinned harm
        // is the claim/intent loss and its deferral-convergence.
        seedCoveredPendingRow()
        val rootBFolder = 800_402
        seedRootRegistry(rootBFolder)
        // A row that might belong to root B (its cached subtree is gone): claimed+pending.
        val maybeOwnedListId = 800_701
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${maybeOwnedListId}_${userDrive.driveId}").apply {
                    id = maybeOwnedListId
                    driveId = userDrive.driveId
                    name = "fixture-maybe-owned.bin"
                    path = "Private/fixture-root-b/fixture-maybe-owned.bin"
                    type = "file"
                    parentId = rootBFolder
                    isMarkedAsOffline = true
                    offlineOwnedByRoot = true
                    isFromUploads = false
                }
            }
        }
        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())

        mockkObject(DriveInfosController)
        every { DriveInfosController.getDrives(any(), any(), any(), any()) } answers {
            arrayListOf(Drive(objectId = "fixture-drive").apply { id = userDrive.driveId })
        }
        coEvery { ApiRepository.getFolderFiles(any(), any(), any(), any(), any()) } answers {
            CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = emptyList<File>(), cursor = null, hasMore = false)
        }
        var detailsDefinitive = false
        every { ApiRepository.getFileDetails(any(), any()) } answers {
            if (detailsDefinitive) {
                ApiResponse(
                    result = ApiResponseStatus.ERROR,
                    data = null,
                    error = com.infomaniak.core.network.models.ApiError(code = "object_not_found"),
                )
            } else {
                ApiResponse(
                    result = ApiResponseStatus.ERROR,
                    data = null,
                    error = com.infomaniak.core.network.models.ApiError(exception = com.infomaniak.core.network.models.exceptions.NetworkException()),
                )
            }
        }

        // PASS 1: inconclusive — the registration stands, no row is fabricated, and the
        // whole release DEFERS (both roots' claims and intents stand).
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }
        assertNotNull(
            FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(OfflineFolderRoot::class.java).equalTo(OfflineFolderRoot::folderId.name, rootBFolder).findFirst()
            },
            "an inconclusive answer never withdraws the registration",
        )
        assertNull(
            FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(File::class.java).equalTo(File::id.name, rootBFolder).findFirst()
            },
            "an inconclusive answer never fabricates the cached row",
        )
        FileController.getRealmInstance(userDrive).use { realm ->
            val maybeOwned = realm.where(File::class.java).equalTo(File::id.name, maybeOwnedListId).findFirst()!!
            assertTrue(maybeOwned.offlineOwnedByRoot, "the unproven root's possible row keeps its claim")
            assertTrue(maybeOwned.isMarkedAsOffline, "the unproven root's possible row keeps its intent")
            val covered = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(covered.offlineOwnedByRoot, "the completed root's release DEFERRED (the all-complete gate — the P2-5 discipline)")
            assertTrue(covered.isMarkedAsOffline, "the covered row's intent stands through the deferral")
        }

        // PASS 2: the definitive answer withdraws B's registration alone; the pass's
        // registry snapshot no longer matches, so the release still defers (conservative).
        detailsDefinitive = true
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }
        assertNull(
            FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(OfflineFolderRoot::class.java).equalTo(OfflineFolderRoot::folderId.name, rootBFolder).findFirst()
            },
            "the definitive answer withdrew the registration",
        )
        assertTrue(
            FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(File::class.java).equalTo(File::id.name, maybeOwnedListId).findFirst()!!.offlineOwnedByRoot
            },
            "the ended contract's claims stand through the withdrawal pass (they release on the subsequent pass)",
        )

        // PASS 3: the registry snapshot matches again ({A}), its walk completes, and the
        // discharge releases A's remotely-deleted row AND B's orphaned claims.
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }
        FileController.getRealmInstance(userDrive).use { realm ->
            val covered = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertFalse(covered.offlineOwnedByRoot, "the deferred release converged")
            assertFalse(covered.isMarkedAsOffline, "the covered row's intent cleared")
            val maybeOwned = realm.where(File::class.java).equalTo(File::id.name, maybeOwnedListId).findFirst()!!
            assertFalse(maybeOwned.offlineOwnedByRoot, "the ended contract's orphaned claim released through the subsequent pass")
            assertFalse(maybeOwned.isMarkedAsOffline, "its intent cleared with it")
        }
        assertEquals(0L, bodyRequests.get(), "every stage preserves: nothing was re-downloaded")
    }

    @Test
    @DisplayName("a definitive remote answer (object_not_found) withdraws the missing root's registration with bytes preserved")
    fun definitiveRemoteAnswerWithdrawsTheMissingRootRegistrationWithBytesPreserved() {
        // FHNW-1189 A3, definitive deletion: the authoritative answer is the API's
        // object_not_found — the ONE non-trash signal that may withdraw the registration.
        // The withdrawal deletes ONLY the registry row: the claims release through the
        // existing scope policy on a later pass, and the BYTES ARE PRESERVED.
        seedCoveredPendingRow()
        val rootBFolder = 800_402
        seedRootRegistry(rootBFolder)
        // A claimed row with REAL bytes under the definitively-gone root.
        val ownedId = 800_702
        val ownedBytes = bodyV1
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${ownedId}_${userDrive.driveId}").apply {
                    id = ownedId
                    driveId = userDrive.driveId
                    name = "fixture-owned.bin"
                    path = "Private/fixture-root-b/fixture-owned.bin"
                    type = "file"
                    parentId = rootBFolder
                    isOffline = true
                    offlineOwnedByRoot = true
                    offlineBasisModifiedAt = 1_700_000_000L
                    offlineBasisSize = ownedBytes.size.toLong()
                    offlineContentDigest = sha256Hex(ownedBytes)
                    isFromUploads = false
                }
            }
        }
        val ownedFile = JavaFile(
            File.getOfflineFolder(context),
            "${userDrive.userId}/${userDrive.driveId}/Private/fixture-root-b/fixture-owned.bin",
        )
        ownedFile.parentFile?.mkdirs()
        ownedFile.writeBytes(ownedBytes)
        check(ownedFile.setLastModified(1_700_000_000L * 1000)) { "the owned bytes carry the basis mtime" }
        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())

        mockkObject(DriveInfosController)
        every { DriveInfosController.getDrives(any(), any(), any(), any()) } answers {
            arrayListOf(Drive(objectId = "fixture-drive").apply { id = userDrive.driveId })
        }
        coEvery { ApiRepository.getFolderFiles(any(), any(), any(), any(), any()) } answers {
            CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = emptyList<File>(), cursor = null, hasMore = false)
        }
        every { ApiRepository.getFileDetails(any(), any()) } answers {
            ApiResponse(
                result = ApiResponseStatus.ERROR,
                data = null,
                error = com.infomaniak.core.network.models.ApiError(code = "object_not_found"),
            )
        }
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }

        assertNull(
            FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(OfflineFolderRoot::class.java).equalTo(OfflineFolderRoot::folderId.name, rootBFolder).findFirst()
            },
            "the definitive answer withdrew the registration",
        )
        assertNotNull(
            FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(OfflineFolderRoot::class.java).equalTo(OfflineFolderRoot::folderId.name, coveredFolderId).findFirst()
            },
            "the other root's registration stands",
        )
        assertTrue(ownedFile.exists() && ownedFile.readBytes().contentEquals(ownedBytes), "the BYTES ARE PRESERVED (the registration withdrawal never deletes)")
        FileController.getRealmInstance(userDrive).use { realm ->
            val owned = realm.where(File::class.java).equalTo(File::id.name, ownedId).findFirst()!!
            assertTrue(owned.offlineOwnedByRoot, "the claim releases through the existing scope policy on a later pass (conservative this pass)")
            assertTrue(owned.isOffline, "the claimed row's state stands untouched this pass")
        }

        // SUBSEQUENT PASS (RESUME-962c item 2b): the ended contract's claims release with
        // bytes preserved once the registry snapshot matches again — the convergence the
        // withdrawal promised, now proven instead of assumed.
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }

        assertTrue(ownedFile.exists() && ownedFile.readBytes().contentEquals(ownedBytes), "the bytes are STILL preserved through the convergence pass")
        FileController.getRealmInstance(userDrive).use { realm ->
            val owned = realm.where(File::class.java).equalTo(File::id.name, ownedId).findFirst()!!
            assertFalse(owned.offlineOwnedByRoot, "the ended contract's claim released through the subsequent pass")
            assertFalse(owned.isOffline, "the released row's intent cleared with its claim")
            val covered = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertFalse(covered.offlineOwnedByRoot, "the covered root's own deferred release converged too")
            assertFalse(covered.isMarkedAsOffline, "the covered row's pending marker cleared with it")
        }
    }

    @Test
    @DisplayName("the SAME row's claim survives a real release and re-acquisition (unmark -> re-mark -> pass): the serial lifecycle control")
    fun sameRowClaimSurvivesReleaseAndReacquisitionThroughRealEntryPoints() {
        // FHNW-962c P3-6: previousClaimedIds cannot distinguish releasing and re-acquiring
        // the SAME id under a replaced registration, and the former case inserted a NEW
        // fixture row instead of exercising that schedule. This pins the same-row lifecycle
        // through the REAL entry points: the unmark releases the byte-less claim (the
        // closer), the re-mark's walk re-acquires the SAME row through its adoption marking,
        // and the maintenance pass KEEPS the re-acquired claim — the row is listed by the
        // very walk that owns it, so the discharge never judges it from the old evidence
        // (the all-complete gate closes the mid-pass variant: an incomplete re-marking walk
        // defers the whole release). SCOPE (RESUME-33dd item 2): this is the ORDINARY
        // serial lifecycle control — its last pass starts with the NEW registration and
        // lists the row. The actual old-pass/new-registration MID-PASS schedule (the old
        // pass parked across a same-id re-registration, its union predating the successor's
        // marking walk) is pinned by anOldPassParkedAcrossASameIdReregistrationDefersTheRelease.
        seedCoveredPendingRow()
        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())
        val remoteRow = File(id = fileId, driveId = userDrive.driveId).apply {
            initUid()
            name = "fixture-orch.bin"
            path = "Private/fixture-orch.bin"
            type = "file"
            parentId = coveredFolderId
            lastModifiedAt = 1_700_000_000L
            size = bodyV1.size.toLong()
        }
        lastAnswerBody.set(bodyV1)
        mockkObject(DriveInfosController)
        every { DriveInfosController.getDrives(any(), any(), any(), any()) } answers {
            arrayListOf(Drive(objectId = "fixture-drive").apply { id = userDrive.driveId })
        }
        coEvery { ApiRepository.getFolderFiles(any(), any(), any(), any(), any()) } answers {
            CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = arrayListOf(remoteRow), cursor = null, hasMore = false)
        }

        // REAL release: the unmark's byte-less closer clears the SAME row's claim.
        runBlocking { OfflineFolderSyncUtils.unmarkFolderOffline(context, folderRow(coveredFolderId), userDrive) }
        assertEquals(0L, FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(OfflineFolderRoot::class.java).count()
        }, "the unmark withdrew the registration")
        FileController.getRealmInstance(userDrive).use { realm ->
            val released = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertFalse(released.offlineOwnedByRoot, "the unmark released the row's claim")
            assertFalse(released.isMarkedAsOffline, "the unmark cleared the pending marker")
        }

        // REAL re-acquisition: the re-mark's walk re-claims the SAME row id.
        runBlocking { OfflineFolderSyncUtils.markFolderOffline(context, folderRow(coveredFolderId), userDrive) }
        assertEquals(1L, FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(OfflineFolderRoot::class.java).count()
        }, "the re-mark re-registered the same folder")
        FileController.getRealmInstance(userDrive).use { realm ->
            val reacquired = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(reacquired.offlineOwnedByRoot, "the walk's adoption re-claimed the SAME row")
            assertTrue(reacquired.isMarkedAsOffline, "the re-marked row is pending again")
        }

        // The maintenance pass: the re-acquired claim is LISTED by its own walk — the
        // discharge keeps it (the same id in previousClaimedIds does not hurt).
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }
        FileController.getRealmInstance(userDrive).use { realm ->
            val kept = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(kept.offlineOwnedByRoot, "the same-row re-acquired claim SURVIVES the pass (listed by its own walk)")
            assertTrue(kept.isMarkedAsOffline, "the re-acquired pending marker stands")
        }
    }

    @Test
    @DisplayName("a row moved to an UNPROVEN root mid-pass keeps claim, intent, basis and bytes: the partial pass defers, the complete pass keeps")
    fun moveOutToUnprovenRootDuringPartialPassKeepsClaimIntentAndBytes() {
        // FHNW-962c P2-5, the confirmed schedule: X sits in A's cached previousSubtree; the
        // remote moves X to B during the pass; A's complete listing omits X; B's walk
        // FAILS. The former partial-pass release cleared X's claim and intent from A's
        // evidence alone (X's cached chain still reached A, so the unproven-root keep-check
        // could not see B). Now the partial pass DEFERS, and the complete pass KEEPS X
        // because B's listing LISTS it. The reviewer's later byte-loss tail was WITHDRAWN
        // (FHNW-REVIEW-962c-ADDENDUM): this pins the retained claim/intent/basis and the
        // preserved bytes, not any byte-loss schedule.
        seedCoveredPendingRow()
        publishSeededRow(hash = "hash-v1")
        val rootBFolder = 800_403
        seedFolderRow(id = rootBFolder, name = "fixture-root-b", path = "Private/fixture-root-b")
        seedRootRegistry(rootBFolder)
        val offlineFile = getOfflineFixtureFile()
        val movedRow = File(id = fileId, driveId = userDrive.driveId).apply {
            initUid()
            name = "fixture-orch.bin"
            path = "Private/fixture-root-b/fixture-orch.bin"
            type = "file"
            parentId = rootBFolder
            lastModifiedAt = 1_700_000_000L
            size = bodyV1.size.toLong()
        }
        lastAnswerBody.set(bodyV1)
        var bWalkSucceeds = false
        mockkObject(DriveInfosController)
        every { DriveInfosController.getDrives(any(), any(), any(), any()) } answers {
            arrayListOf(Drive(objectId = "fixture-drive").apply { id = userDrive.driveId })
        }
        coEvery { ApiRepository.getFolderFiles(any(), any(), any(), any(), any()) } answers {
            when (thirdArg<Int>()) {
                coveredFolderId -> CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = emptyList<File>(), cursor = null, hasMore = false)
                rootBFolder -> if (bWalkSucceeds) {
                    CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = arrayListOf(movedRow), cursor = null, hasMore = false)
                } else {
                    CursorApiResponse(result = ApiResponseStatus.ERROR, data = null, cursor = null, hasMore = false)
                }
                else -> CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = emptyList<File>(), cursor = null, hasMore = false)
            }
        }
        stubDetails(hash = "hash-v1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())

        // PASS 1: A completes (omits X), B FAILS — the pass defers and X keeps EVERYTHING.
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.offlineOwnedByRoot, "the claim stands (the partial pass released nothing)")
            assertTrue(row.isOffline, "the intent stands")
            assertEquals(1_700_000_000L, row.offlineBasisModifiedAt, "the basis stands")
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest, "the accepted digest stands")
        }
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(bodyV1), "the bytes stand at the recorded path")

        // PASS 2: B's walk completes and LISTS X (under B) — X is kept by the listing
        // evidence itself: no release, no re-download. (The maintenance refresh relocates
        // the accepted copy to the moved row's fresh location — the documented relocation.)
        bWalkSucceeds = true
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }
        val relocatedFile = JavaFile(
            File.getOfflineFolder(context),
            "${userDrive.userId}/${userDrive.driveId}/Private/fixture-root-b/fixture-orch.bin",
        )
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.offlineOwnedByRoot, "the claim stands: B's complete listing keeps the moved row")
            assertTrue(row.isOffline, "the intent stands through the complete pass")
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest, "the accepted digest was not re-written")
        }
        assertTrue(relocatedFile.exists() && relocatedFile.readBytes().contentEquals(bodyV1), "the accepted bytes were relocated to the moved row's location, preserved")
        assertFalse(offlineFile.exists(), "the old location no longer holds a second copy (a rename, not a copy)")
        assertEquals(0L, bodyRequests.get(), "no re-download churn in either pass")
    }

    @Test
    @DisplayName("the LAST root's definitive-gone answer converges: the empty-registry pass releases the orphaned claims, bytes and pins preserved")
    fun soleRootDefinitiveGoneConvergesThroughTheEmptyRegistryPass() {
        // RESUME-962c item 2b: with a SINGLE root, the definitive answer withdraws the only
        // registration and the promised "later pass" release must actually happen — the
        // former pass exited at roots.isEmpty, so the orphaned claims were immortal. The
        // empty-registry pass now runs the discharge: the orphaned claims release with
        // BYTES PRESERVED, the pinned row's own contract stands, and a third pass is stable.
        val rootBFolder = 800_403
        seedRootRegistry(rootBFolder)
        val ownedId = 800_704
        val pinnedId = 800_705
        val ownedBytes = bodyV1
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${ownedId}_${userDrive.driveId}").apply {
                    id = ownedId
                    driveId = userDrive.driveId
                    name = "fixture-sole-owned.bin"
                    path = "Private/fixture-sole-root/fixture-sole-owned.bin"
                    type = "file"
                    parentId = rootBFolder
                    isOffline = true
                    offlineOwnedByRoot = true
                    offlineBasisModifiedAt = 1_700_000_000L
                    offlineBasisSize = ownedBytes.size.toLong()
                    offlineContentDigest = sha256Hex(ownedBytes)
                    isFromUploads = false
                }
                transactionRealm.createObject(File::class.java, "${pinnedId}_${userDrive.driveId}").apply {
                    id = pinnedId
                    driveId = userDrive.driveId
                    name = "fixture-sole-pinned.bin"
                    path = "Private/fixture-sole-root/fixture-sole-pinned.bin"
                    type = "file"
                    parentId = rootBFolder
                    isOffline = true
                    isDirectlyPinned = true
                    offlineOwnedByRoot = true
                    isFromUploads = false
                }
            }
        }
        val ownedFile = JavaFile(
            File.getOfflineFolder(context),
            "${userDrive.userId}/${userDrive.driveId}/Private/fixture-sole-root/fixture-sole-owned.bin",
        )
        ownedFile.parentFile?.mkdirs()
        ownedFile.writeBytes(ownedBytes)
        check(ownedFile.setLastModified(1_700_000_000L * 1000)) { "the owned bytes carry the basis mtime" }
        every { ApiRepository.getFileDetails(any(), any()) } answers {
            ApiResponse(
                result = ApiResponseStatus.ERROR,
                data = null,
                error = com.infomaniak.core.network.models.ApiError(code = "object_not_found"),
            )
        }
        mockkObject(DriveInfosController)
        every { DriveInfosController.getDrives(any(), any(), any(), any()) } answers {
            arrayListOf(Drive(objectId = "fixture-drive").apply { id = userDrive.driveId })
        }

        // PASS 1: the definitive answer withdraws the ONLY registration; the claims stand
        // (the release deliberately defers to the pass whose evidence is complete).
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }
        assertEquals(0L, FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(OfflineFolderRoot::class.java).count()
        }, "the sole registration was withdrawn")
        assertTrue(ownedFile.exists() && ownedFile.readBytes().contentEquals(ownedBytes), "the bytes were never touched by the withdrawal")
        assertTrue(
            FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(File::class.java).equalTo(File::id.name, ownedId).findFirst()!!.offlineOwnedByRoot
            },
            "the claim stands through the withdrawal pass",
        )

        // PASS 2: the EMPTY registry no longer exits early — the discharge releases the
        // orphaned claims (bytes preserved); the pinned row's own contract stands.
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }
        assertTrue(ownedFile.exists() && ownedFile.readBytes().contentEquals(ownedBytes), "the bytes survive the convergence pass")
        FileController.getRealmInstance(userDrive).use { realm ->
            val owned = realm.where(File::class.java).equalTo(File::id.name, ownedId).findFirst()!!
            assertFalse(owned.offlineOwnedByRoot, "the orphaned claim released through the empty-registry pass")
            assertFalse(owned.isOffline, "the released row's intent cleared with its claim")
            val pinned = realm.where(File::class.java).equalTo(File::id.name, pinnedId).findFirst()!!
            assertTrue(pinned.isDirectlyPinned, "the direct pin stands (the discharge skips pinned rows)")
            assertTrue(pinned.offlineOwnedByRoot, "the pinned row's claim stands for the unpin's own contract")
        }

        // PASS 3: stable — nothing further changes.
        runBlocking { OfflineFolderSyncUtils.syncOfflineFolderRoots(context) }
        assertTrue(ownedFile.exists(), "the bytes still stand after the stable pass")
        assertFalse(
            FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(File::class.java).equalTo(File::id.name, ownedId).findFirst()!!.offlineOwnedByRoot
            },
            "the released state is stable",
        )
    }

    @Test
    @DisplayName("an occupied destination holding a PRE-EXISTING same-stat edit is never replaced by the cache move")
    fun occupiedSameStatEditedDestinationIsNeverReplacedByTheCacheMove() {
        // RESUME-962c item 4 (FHNW-962c P2-3): the destination classification compared only
        // the recorded path, mtime and optional size — an already-edited copy carrying the
        // ORIGINAL size and mtime (a pre-existing same-stat edit, not an external edit
        // racing the hold) was therefore replaceable. The destination's BYTES are now
        // digested outside the hold and must equal the row's accepted digest; the edited
        // bytes and the cache bytes are BOTH preserved.
        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${fileId}_${userDrive.driveId}").apply {
                    id = fileId
                    driveId = userDrive.driveId
                    name = "fixture-orch.bin"
                    path = "Other/fixture-outside/fixture-orch.bin"
                    lastModifiedAt = 1_700_000_000L
                    parentId = 800_501
                    size = bodyV1.size.toLong()
                    isOffline = true
                    isFromUploads = false
                    offlineBasisModifiedAt = 1_700_000_000L
                    offlineBasisSize = bodyV1.size.toLong()
                    offlineContentDigest = sha256Hex(bodyV1)
                    serverContentHash = "hash-1"
                }
            }
        }
        // The row's own accepted publication at the recorded location...
        val destination = getOfflineFixtureFile()
        destination.parentFile?.mkdirs()
        destination.writeBytes(bodyV1)
        check(destination.setLastModified(1_700_000_000L * 1000)) { "the accepted copy carries the basis mtime" }
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()
                    ?.offlinePublishedPath = destination.path
            }
        }
        // ...then edited IN PLACE with the ORIGINAL stat restored (same length, same mtime).
        destination.writeBytes(editedBody)
        check(destination.setLastModified(1_700_000_000L * 1000)) { "the edit restored the basis mtime" }
        // An intact cache copy awaits publication (the cache-move's source).
        val cacheFile = File(id = fileId, driveId = userDrive.driveId).getCacheFile(context, userDrive)
        cacheFile.parentFile?.mkdirs()
        cacheFile.writeBytes(bodyV1)
        check(cacheFile.setLastModified(1_700_000_000L * 1000)) { "the cache bytes must be intact against the served metadata" }
        lastAnswerBody.set(bodyV2) // must never be fetched
        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong(), parentId = 800_501)
        val manager = coveredManager()
        val result = runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }

        assertTrue(result.toString().contains("Success"), "the refused cache-move completes silently (result=$result, row=${offlineRowDiagnostics()})")
        assertEquals(0L, bodyRequests.get(), "nothing was transferred")
        assertTrue(destination.exists() && destination.readBytes().contentEquals(editedBody), "the same-stat edited bytes were NOT replaced")
        assertTrue(cacheFile.exists() && cacheFile.readBytes().contentEquals(bodyV1), "the cache bytes were PRESERVED")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.isOffline, "the row's own offline status stands")
            assertEquals(1_700_000_000L, row.offlineBasisModifiedAt, "no basis was re-recorded over the edited bytes")
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest, "the recorded digest still describes the ACCEPTED content, not the edit")
            assertEquals("hash-1", row.serverContentHash, "no identity was re-recorded")
        }
        assertTrue(
            destination.parentFile?.list().orEmpty().none { it.startsWith("offline_staging_") },
            "no staging leftover at the destination directory",
        )
    }

    @Test
    @DisplayName("the row's own intact accepted publication at the destination IS the refreshable occupant (positive control)")
    fun intactAcceptedDestinationIsRefreshedByTheCacheMove() {
        // The same entry, positive control: the destination holds the row's OWN accepted
        // bytes (digest-verified) — the baseline refresh proceeds, the record stands, and
        // the cache file is CONSUMED by the rename (the reachability evidence: the effect
        // actually ran, through the real entry).
        seedFolderRow(id = 800_501, name = "fixture-outside", path = "Other/fixture-outside")
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.createObject(File::class.java, "${fileId}_${userDrive.driveId}").apply {
                    id = fileId
                    driveId = userDrive.driveId
                    name = "fixture-orch.bin"
                    path = "Other/fixture-outside/fixture-orch.bin"
                    lastModifiedAt = 1_700_000_000L
                    parentId = 800_501
                    size = bodyV1.size.toLong()
                    isOffline = true
                    isFromUploads = false
                    offlineBasisModifiedAt = 1_700_000_000L
                    offlineBasisSize = bodyV1.size.toLong()
                    offlineContentDigest = sha256Hex(bodyV1)
                    serverContentHash = "hash-1"
                }
            }
        }
        // The INTACT occupant: the row's own accepted bytes with the basis stat.
        val destination = getOfflineFixtureFile()
        destination.parentFile?.mkdirs()
        destination.writeBytes(bodyV1)
        check(destination.setLastModified(1_700_000_000L * 1000)) { "the accepted copy carries the basis mtime" }
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { transactionRealm ->
                transactionRealm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()
                    ?.offlinePublishedPath = destination.path
            }
        }
        val cacheFile = File(id = fileId, driveId = userDrive.driveId).getCacheFile(context, userDrive)
        cacheFile.parentFile?.mkdirs()
        cacheFile.writeBytes(bodyV1)
        check(cacheFile.setLastModified(1_700_000_000L * 1000)) { "the cache bytes must be intact against the served metadata" }
        lastAnswerBody.set(bodyV2) // must never be fetched
        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong(), parentId = 800_501)
        val manager = coveredManager()
        val result = runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }

        assertTrue(result.toString().contains("Success"), "the refresh completes (result=$result, row=${offlineRowDiagnostics()})")
        assertEquals(0L, bodyRequests.get(), "the refresh consumed the intact cache copy — no transfer")
        assertFalse(cacheFile.exists(), "the cache file was consumed by the rename (the move REACHED its effect)")
        assertTrue(destination.exists() && destination.readBytes().contentEquals(bodyV1), "the destination holds the published bytes")
        FileController.getRealmInstance(userDrive).use { realm ->
            val row = realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
            assertTrue(row.isOffline, "the offline status stands")
            assertEquals(destination.path, row.offlinePublishedPath, "the record names the destination")
            assertEquals(1_700_000_000L, row.offlineBasisModifiedAt, "the basis stands")
            assertEquals(sha256Hex(bodyV1), row.offlineContentDigest, "the digest describes the published bytes")
            assertNull(row.serverContentHash, "the cache-move's record stores an UNKNOWN identity (cache bytes are unproven against the remote)")
        }
    }

    private fun folderRow(folderId: Int): File = FileController.getRealmInstance(userDrive).use { realm ->
        realm.copyFromRealm(realm.where(File::class.java).equalTo(File::id.name, folderId).findFirst()!!, 0)
    }

    @AfterEach
    fun tearDown() {
        unmockkAll()
    }

    @AfterAll
    fun cleanup() {
        // ONLY the synthetic fixture realm and the synthetic offline directory are deleted —
        // never any other realm, never any owner data.
        runCatching { Realm.deleteRealm(FileController.getRealmConfiguration(userDrive)) }
        runCatching { offlineRoot.deleteRecursively() }
    }

    private fun sha256Hex(bytes: ByteArray): String {
        val digest = java.security.MessageDigest.getInstance("SHA-256")
        return digest.digest(bytes).joinToString("") { "%02x".format(it) }
    }
}
