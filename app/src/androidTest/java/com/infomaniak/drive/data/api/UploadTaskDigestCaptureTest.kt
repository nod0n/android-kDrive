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
package com.infomaniak.drive.data.api

import androidx.test.core.app.ApplicationProvider
import com.infomaniak.core.network.models.ApiResponse
import com.infomaniak.core.network.models.ApiResponseStatus
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.upload.UploadSegment
import com.infomaniak.drive.data.models.upload.UploadSession
import com.infomaniak.drive.data.models.upload.ValidChunks
import com.infomaniak.drive.data.sync.PhotoTrashController
import com.infomaniak.drive.utils.AccountUtils
import io.mockk.MockKAnnotations
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.spyk
import io.mockk.unmockkAll
import kotlinx.coroutines.runBlocking
import okhttp3.Interceptor
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Protocol
import okhttp3.Response
import okhttp3.ResponseBody
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.security.MessageDigest
import java.util.Date
import java.util.concurrent.CopyOnWriteArrayList
import java.util.concurrent.atomic.AtomicReference

/**
 * THE ACTUAL UPLOAD-TO-RECORDPAIRING WIRING (androidTest instrumentation): these tests drive
 * the REAL [UploadTask] transfer paths — direct, empty, fresh-chunked and resumed-chunked —
 * with only the network and the realm boundary faked (an OkHttp interceptor that consumes the
 * request body exactly as a server would, and a mocked [PhotoTrashController] record call so
 * no production realm is ever touched), and assert what [PhotoTrashController.recordPairing]
 * ACTUALLY receives:
 *  - a direct upload passes the digest of the bytes the transfer read;
 *  - an empty upload passes the empty-body identity;
 *  - chunked uploads (fresh AND resumed) pass NO stream digest — the pairing then binds
 *    through recordPairing's validated server-to-source comparison against the response's
 *    hash, or stays unbound.
 *
 * Fully disposable and account-free: synthetic constants only — NO owner data, NO account,
 * NO real network, NO production realm write.
 */
@Suppress("SameParameterValue")
class UploadTaskDigestCaptureTest {

    private val context = ApplicationProvider.getApplicationContext<android.content.Context>()
    private val userId = 971
    private val driveId = 771
    private val createdRemoteId = 900_701
    private val uploadUri = "content://fixture/digest-capture"
    private val pairingHash = "pairing-hash"

    private val contentA = "fixture-upload-content-A".toByteArray()
    private val chunkedContent = ByteArray(5 * 1024 * 1024 + 1_024) { index -> (index % 251).toByte() }

    private val transferredBodies = CopyOnWriteArrayList<ByteArray>()
    private val recordedDigest = AtomicReference<String?>(null)
    private val recordedRemoteFile = AtomicReference<File?>(null)

    /**
     * The bytes the mocked MediaStore currently serves. EVERY openInputStream returns a NEW
     * stream over this content — exactly what a real resolver does and what the chunked
     * transfer requires (each chunk opens its own stream and skips ahead).
     */
    private val resolverContent = AtomicReference(contentA)

    /** The app context with its ContentResolver replaced by the fixture resolver. */
    private lateinit var mockContext: android.content.Context

    @BeforeEach
    fun setUp() {
        MockKAnnotations.init(this)
        transferredBodies.clear()
        recordedDigest.set(null)
        recordedRemoteFile.set(null)
        resolverContent.set(contentA)

        val mockResolver = mockk<android.content.ContentResolver>()
        every { mockResolver.openInputStream(any()) } answers { ByteArrayInputStream(resolverContent.get()) }
        mockContext = spyk(context)
        every { mockContext.contentResolver } returns mockResolver

        mockkObject(ApiRepository)
        mockkObject(AccountUtils)
        every { AccountUtils.currentUserId } returns userId
        coEvery { AccountUtils.getHttpClient(any(), any(), any(), any()) } returns OkHttpClient()

        // FAILURE-PATH ISOLATION: the transfer paths catch failures (e.g. a vanished source)
        // and call real UploadFile cleanup methods, which would open the PRODUCTION Sync
        // realm. Any such access must fail the test loudly here instead.
        mockkObject(UploadFile.Companion)
        every { UploadFile.getRealmInstance() } throws
            AssertionError("UploadTaskDigestCaptureTest must never open the production Sync realm")

        mockkObject(PhotoTrashController)
        every {
            PhotoTrashController.recordPairing(any(), any(), any(), any(), any())
        } answers {
            recordedRemoteFile.set(args[2] as File?)
            recordedDigest.set(args[3] as String?)
            Unit
        }

        every { UploadFile.uploadFinished(any()) } returns Unit
    }

    /** Fakes the transfer: consumes the request body (which reads the upload's digesting
     *  stream, exactly as a server would) and answers with a canned success body. */
    private fun fakeTransferInterceptor(responseJson: String): Interceptor = Interceptor { chain ->
        val request = chain.request()
        val buffer = okio.Buffer()
        request.body?.writeTo(buffer)
        transferredBodies.add(buffer.readByteArray())
        Response.Builder()
            .request(request)
            .protocol(Protocol.HTTP_1_1)
            .code(200)
            .message("OK")
            .body(ResponseBody.create("application/json".toMediaType(), responseJson))
            .build()
    }

    private fun fakeOkHttpClient(): OkHttpClient = OkHttpClient.Builder()
        .addInterceptor(fakeTransferInterceptor(createdFileJson()))
        .build()

    private fun createdFileJson(): String =
        """{"result":"success","data":{"id":$createdRemoteId,"drive_id":$driveId,"hash":"$pairingHash"}}"""

    private fun uploadFile(fileSize: Long): UploadFile = spyk(
        UploadFile(
            uri = uploadUri,
            userId = userId,
            driveId = driveId,
            fileName = "fixture-digest.jpg",
            fileSize = fileSize,
            fileModifiedAt = Date(1_700_000_000_000L),
        )
    ).also { uploadFile ->
        every { uploadFile.okHttpClient } returns fakeOkHttpClient()
    }

    private fun runUpload(uploadFile: UploadFile): Boolean = runBlocking {
        UploadTask(
            context = mockContext,
            uploadFile = uploadFile,
            setProgress = ::progressSink,
            notificationManagerCompat = mockk(relaxed = true),
            uploadNotificationBuilder = mockk(relaxed = true),
        ).start()
    }

    private suspend fun progressSink(data: androidx.work.Data) = Unit

    @Test
    @DisplayName("a direct sync upload passes the digest of the bytes the transfer actually read")
    fun directUploadPassesTheTransferredBytesDigest() {
        assertTrue(runUpload(uploadFile(fileSize = contentA.size.toLong())))

        assertEquals(1, transferredBodies.size, "exactly one request carried the body")
        assertEquals(contentA.toList(), transferredBodies.single().toList(), "the transfer read the local source bytes")
        val digest = recordedDigest.get()
        assertNotNull(digest, "the transfer-bound digest reached recordPairing")
        assertEquals(sha256Hex(contentA), digest, "the digest is of the bytes the upload read, not a later sample")
        assertEquals(createdRemoteId, recordedRemoteFile.get()?.id, "the created server file was passed through")
    }

    @Test
    @DisplayName("an empty upload passes the empty-body identity, not a later sample")
    fun emptyUploadPassesTheEmptyBodyIdentity() {
        coEvery { ApiRepository.uploadEmptyFile(any()) } answers {
            ApiResponse(result = ApiResponseStatus.SUCCESS, data = File(id = createdRemoteId, driveId = driveId))
        }

        assertTrue(runUpload(uploadFile(fileSize = 0L)))

        assertEquals(0, transferredBodies.size, "an empty upload transfers no body")
        assertEquals(sha256Hex(ByteArray(0)), recordedDigest.get(), "the empty-body identity IS the uploaded content identity")
        assertEquals(createdRemoteId, recordedRemoteFile.get()?.id)
    }

    @Test
    @DisplayName("a fresh chunked upload passes NO stream digest; the finish-session file is passed through")
    fun freshChunkedUploadPassesNoStreamDigest() {
        resolverContent.set(chunkedContent)
        coEvery { ApiRepository.startUploadSession(any(), any(), any()) } answers {
            ApiResponse(
                result = ApiResponseStatus.SUCCESS,
                data = UploadSession.StartUploadSession(
                    directoryId = null,
                    directoryPath = null,
                    fileName = "fixture-digest.jpg",
                    uploadHost = "https://fake-upload-host",
                    file = File(id = createdRemoteId, driveId = driveId),
                    token = "session-token",
                    isSuccess = true,
                ),
            )
        }
        coEvery { ApiRepository.finishSession(any(), any(), any()) } answers {
            ApiResponse(
                result = ApiResponseStatus.SUCCESS,
                data = UploadSession(
                    file = File(id = createdRemoteId, driveId = driveId).apply { serverContentHash = pairingHash },
                    token = "session-token",
                    isSuccess = true,
                ),
            )
        }
        val uploadFile = uploadFile(fileSize = chunkedContent.size.toLong())
        every { uploadFile.updateUploadToken(any(), any()) } answers { uploadFile.uploadToken = secondArg<String>() }

        assertTrue(runUpload(uploadFile))

        assertTrue(transferredBodies.size >= 2, "the chunks were transferred")
        assertEquals(chunkedContent.toList(), transferredBodies.concatenated().toList(), "every chunk came from the local source")
        assertNull(recordedDigest.get(), "parallel per-chunk streams compose to no whole-body digest: null, never a sample")
        assertEquals(createdRemoteId, recordedRemoteFile.get()?.id, "the finish-session response's file is the pairing identity")
    }

    @Test
    @DisplayName("a resumed chunked upload passes NO stream digest and skips the already-valid chunks")
    fun resumedChunkedUploadPassesNoStreamDigest() {
        resolverContent.set(chunkedContent)
        val chunkConfig = FileChunkSizeManager().computeChunkConfig(fileSize = chunkedContent.size.toLong())
        coEvery { ApiRepository.getValidChunks(any(), any(), any()) } answers {
            ApiResponse(
                result = ApiResponseStatus.SUCCESS,
                data = ValidChunks(
                    expectedChunksCount = chunkConfig.totalChunks,
                    uploadedChunkCount = 1,
                    failedChunks = 0L,
                    expectedSize = chunkedContent.size.toLong(),
                    uploadedSize = chunkConfig.fileChunkSize,
                    chunks = arrayListOf(
                        UploadSegment(number = 1, status = UploadSegment.ChunkStatus.OK, createdAt = Date(0), size = chunkConfig.fileChunkSize.toInt(), hash = "fixture")
                    ),
                ),
            )
        }
        coEvery { ApiRepository.finishSession(any(), any(), any()) } answers {
            ApiResponse(
                result = ApiResponseStatus.SUCCESS,
                data = UploadSession(
                    file = File(id = createdRemoteId, driveId = driveId).apply { serverContentHash = pairingHash },
                    token = "session-token",
                    isSuccess = true,
                ),
            )
        }
        val uploadFile = uploadFile(fileSize = chunkedContent.size.toLong())
        uploadFile.uploadToken = "session-token"
        uploadFile.uploadHost = "https://fake-upload-host"

        assertTrue(runUpload(uploadFile))

        // Chunk 1 was already valid and is NOT re-read; only the remaining chunk(s) transferred.
        assertTrue(transferredBodies.isNotEmpty())
        assertNull(recordedDigest.get(), "a resumed run never re-reads a previous run's chunks: no whole-body digest exists")
        assertEquals(createdRemoteId, recordedRemoteFile.get()?.id)
    }

    @Test
    @DisplayName("non-sync uploads never reach recordPairing: the digest work stays scoped to sync")
    fun nonSyncUploadsNeverReachRecordPairing() {
        val manualFile = uploadFile(fileSize = contentA.size.toLong()).also { it.type = UploadFile.Type.CLOUD_STORAGE.name }

        assertTrue(runUpload(manualFile))

        assertEquals(1, transferredBodies.size)
        assertNull(recordedDigest.get(), "recordPairing is never called for non-sync uploads")
        assertNull(recordedRemoteFile.get())
    }

    @AfterEach
    fun tearDown() {
        unmockkAll()
    }

    private fun List<ByteArray>.concatenated(): ByteArray =
        flatMap { it.toList() }.toByteArray()

    private fun sha256Hex(bytes: ByteArray): String =
        MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
}
