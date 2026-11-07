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
