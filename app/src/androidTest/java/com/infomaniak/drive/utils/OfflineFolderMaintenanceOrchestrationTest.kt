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
import com.infomaniak.core.network.models.ApiResponseStatus
import com.infomaniak.core.network.models.ApiResponse
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.OfflineFolderRoot
import com.infomaniak.drive.data.models.UserDrive
import io.mockk.MockKAnnotations
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.unmockkAll
import io.realm.Realm
import kotlinx.coroutines.runBlocking
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
    private val offlineRoot = JavaFile(
        File.getOfflineFolder(context),
        "${userDrive.userId}/${userDrive.driveId}",
    )

    private val bodyV1 = "fixture-body-v1".toByteArray()
    private val bodyV2 = "fixture-body-v2-with-different-length".toByteArray()
    private val editedBody = "fixture-EDITED!".toByteArray() // same length as bodyV1: a same-stat edit

    private val detailsRequests = AtomicLong(0)
    private val bodyRequests = AtomicLong(0)

    @BeforeAll
    fun initRealm() {
        Realm.init(context)
    }

    @BeforeEach
    fun setUp() {
        MockKAnnotations.init(this)
        detailsRequests.set(0)
        bodyRequests.set(0)
        runCatching { Realm.deleteRealm(FileController.getRealmConfiguration(userDrive)) }
        runCatching { offlineRoot.deleteRecursively() }

        mockkObject(ApiRepository)
        mockkObject(AccountUtils)
        coEvery { AccountUtils.getHttpClient(any(), any(), any(), any()) } returns OkHttpClient()
        // The body transport is injected; staging, digest, atomic rename and the basis seam
        // below it stay REAL. saveRemoteData is deliberately NOT mocked.
        mockkObject(DownloadOfflineFileManager.Companion)
        coEvery { DownloadOfflineFileManager.downloadFileResponseAsync(any(), any(), any()) } answers {
            bodyRequests.incrementAndGet()
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

    private fun detailsResponse(hash: String?, modifiedAtSec: Long, size: Long?): ApiResponse<File> {
        detailsRequests.incrementAndGet()
        return ApiResponse(
            result = ApiResponseStatus.SUCCESS,
            data = File(id = fileId, driveId = userDrive.driveId).apply {
                name = "fixture-orch.bin"
                path = "Private/fixture-orch.bin"
                lastModifiedAt = modifiedAtSec
                this.size = size
                serverContentHash = hash
            },
        )
    }

    private fun stubDetails(hash: String?, modifiedAtSec: Long, size: Long?) {
        every { ApiRepository.getFileDetailsWithHash(any(), any()) } returns detailsResponse(hash, modifiedAtSec, size)
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
                    lastModifiedAt = 1_700_000_000L
                    parentId = folderId
                    isMarkedAsOffline = true
                    isOffline = false
                    isFromUploads = false
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

    private fun getOfflineFixtureFile(): JavaFile {
        val row = FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()!!
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
    @DisplayName("a deleted local copy with an unchanged remote is restored with exactly one body")
    fun deletedCopyIsRestoredWithExactlyOneBody() {
        seedAcceptedPublication(bodyV1, modifiedAtSec = 1_700_000_000L, hash = "hash-1")
        val offlineFile = getOfflineFixtureFile()
        lastAnswerBody.set(bodyV1)

        // The user (or storage cleanup) removes the local bytes; the row keeps its basis.
        check(offlineFile.delete()) { "fixture setup: the copy must be removable" }

        // Pass 1: MISSING -> one staged body republishes the copy.
        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())
        refresh()
        assertEquals(1L, bodyRequests.get(), "a missing copy costs exactly one body")
        assertTrue(offlineFile.exists() && offlineFile.readBytes().contentEquals(bodyV1), "the copy is restored")

        // Pass 2: intact again -> details only, no repeated body.
        refresh()
        assertEquals(2L, detailsRequests.get())
        assertEquals(1L, bodyRequests.get(), "no repeated body for the restored copy")
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

    @Test
    @DisplayName("a covered copy's initial download publishes through the staged path (owned staging, atomic rename)")
    fun coveredInitialDownloadPublishesThroughTheStagedPath() {
        seedCoveredPendingRow()
        val offlineFile = getOfflineFixtureFile()
        lastAnswerBody.set(bodyV1)

        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())
        val manager = DownloadOfflineFileManager(
            userDrive = userDrive,
            filesCount = 1,
            downloadWorker = mockk(relaxed = true),
            notificationManagerCompat = mockk(relaxed = true),
        )
        val result = runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }

        assertTrue(result.toString().contains("Success"), "the initial covered download succeeds")
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

        stubDetails(hash = "hash-1", modifiedAtSec = 1_700_000_000L, size = bodyV1.size.toLong())
        // The user unmarks the root WHILE the transfer is in flight: the body is delivered
        // only after the unmark completed.
        coEvery { DownloadOfflineFileManager.downloadFileResponseAsync(any(), any(), any()) } coAnswers {
            val unmarked = OfflineFolderSyncUtils.unmarkFolderOffline(context, folderRow(800_401), userDrive)
            check(unmarked) { "fixture unmark must confirm" }
            fakeBodyResponse(lastAnswerBody.get())
        }
        val manager = DownloadOfflineFileManager(
            userDrive = userDrive,
            filesCount = 1,
            downloadWorker = mockk(relaxed = true),
            notificationManagerCompat = mockk(relaxed = true),
        )
        val result = runBlocking { manager.execute(context, File(id = fileId, driveId = userDrive.driveId)) { _, _ -> } }

        assertTrue(result.toString().contains("Success"), "withdrawn work completes silently (no retry loop)")
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
