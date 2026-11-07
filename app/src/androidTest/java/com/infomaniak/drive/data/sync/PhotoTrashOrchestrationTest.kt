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
package com.infomaniak.drive.data.sync

import androidx.test.core.app.ApplicationProvider
import com.infomaniak.core.network.models.ApiResponse
import com.infomaniak.core.network.models.ApiResponseStatus
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.PhotoPairing
import com.infomaniak.drive.data.models.SyncSettings
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.RealmModules
import io.mockk.MockKAnnotations
import io.mockk.coEvery
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.spyk
import io.mockk.unmockkAll
import io.realm.Realm
import io.realm.RealmConfiguration
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.ByteArrayInputStream
import java.util.Date
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import android.database.Cursor

/**
 * THE REAL PHOTO TRASH ORCHESTRATION (androidTest instrumentation — the row store is Realm,
 * a native engine that cannot open on the JVM; this is why the test lives here).
 *
 * Unlike the JVM decision tests (which pin the pure contract only), this drives the ACTUAL
 * production chain: [PhotoTrashController.recordPairing]'s pairing-time baseline and content
 * binding, [runLocalReconciliation]'s rules call, the DESTRUCTIVE-BOUNDARY digest
 * re-verification and the [ApiRepository.deleteFile] seam, and the cloud->device consent
 * arm/expire lifecycle — with ONLY the platform boundaries injected (the mocked MediaStore
 * cursor/streams and the mocked API); realm rows, digests, state transitions and request
 * counts are real. Request counts are OBSERVED at the network boundary, not simulated.
 *
 * Fully disposable and account-free: a synthetic upload realm (deleted in cleanup), a mocked
 * ContentResolver, synthetic constants only — NO owner data, NO account, NO real network.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PhotoTrashOrchestrationTest {

    private val context = ApplicationProvider.getApplicationContext<android.content.Context>()
    private val userId = 951
    private val driveId = 751
    private val remoteFileId = 900_501
    private val pairingUri = "content://fixture/photo-trash-orch"

    private val contentA = "fixture-photo-content-A".toByteArray()
    private val contentB = "fixture-photo-content-B".toByteArray() // same length: a same-stat replacement

    private val pairingHash = "pairing-hash"

    /** The bytes the mocked MediaStore currently serves through openInputStream. */
    private val resolverContent = AtomicReference(contentA)

    /** The mocked MediaStore row's trashed flag (size/mtime never change: the adversarial case). */
    private val mediaTrashed = AtomicBoolean(false)
    private val mediaSize = contentA.size.toLong()
    private val mediaModifiedSec = 1_700_000_000L

    /** The fresh server details the mocked API currently serves. */
    private val remoteTrashed = AtomicBoolean(false)
    private val remoteHash = AtomicReference(pairingHash)
    private val deleteRequests = AtomicLong(0)

    private val fixtureName = "photoTrashOrch-${UUID.randomUUID()}"
    private val config = RealmConfiguration.Builder()
        .schemaVersion(UploadMigration.DB_VERSION)
        .modules(RealmModules.SyncFilesModule())
        .name(fixtureName)
        .directory(context.cacheDir)
        .build()

    private lateinit var mockContext: android.content.Context

    @BeforeAll
    fun initRealm() {
        Realm.init(context)
    }

    @BeforeEach
    fun setUp() {
        MockKAnnotations.init(this)
        runCatching { Realm.deleteRealm(config) }
        resolverContent.set(contentA)
        mediaTrashed.set(false)
        remoteTrashed.set(false)
        remoteHash.set(pairingHash)
        deleteRequests.set(0)

        val mockResolver = mockk<android.content.ContentResolver>()
        every { mockResolver.query(any(), any(), any(), any(), any()) } answers { mediaCursor(mediaTrashed.get()) }
        every { mockResolver.openInputStream(any()) } answers { ByteArrayInputStream(resolverContent.get()) }
        mockContext = spyk(context)
        every { mockContext.contentResolver } returns mockResolver

        mockkObject(ApiRepository)
        mockkObject(AccountUtils)
        every { AccountUtils.currentUserId } returns userId
        coEvery { AccountUtils.getHttpClient(any(), any(), any(), any()) } returns OkHttpClient()
        mockkObject(UploadFile.Companion)
        every { UploadFile.getAppSyncSettings() } returns SyncSettings(
            userId = userId,
            driveId = driveId,
            photoTrashSyncEnabled = true,
        )
        every { ApiRepository.deleteFile(any()) } answers {
            deleteRequests.incrementAndGet()
            ApiResponse(result = ApiResponseStatus.SUCCESS)
        }
        every { ApiRepository.getFileDetailsWithHash(any(), any()) } answers {
            ApiResponse(
                result = ApiResponseStatus.SUCCESS,
                data = File(id = remoteFileId, driveId = driveId).apply {
                    if (remoteTrashed.get()) status = "trashed"
                    lastModifiedAt = 1_700_000_000L
                    size = contentA.size.toLong()
                    serverContentHash = remoteHash.get()
                },
            )
        }
    }

    private fun mediaCursor(trashed: Boolean): Cursor = mockk(relaxed = true) {
        every { moveToFirst() } returns true
        every { getLong(0) } returns mediaSize
        every { getLong(1) } returns mediaModifiedSec
        every { getInt(2) } returns if (trashed) 1 else 0
    }

    private fun realm(): Realm = Realm.getInstance(config)

    private fun seedPairing(
        state: String = PhotoPairing.State.ACTIVE.name,
        baselineTrashed: Boolean? = false,
        lastObservedTrashed: Boolean = false,
        localDigest: String? = sha256Hex(contentA),
        remoteHashValue: String? = pairingHash,
    ): PhotoPairing = realm().use { realm ->
        realm.executeTransaction { mutableRealm ->
            mutableRealm.createObject(PhotoPairing::class.java, pairingUri)?.apply {
                userId = this@PhotoTrashOrchestrationTest.userId
                driveId = this@PhotoTrashOrchestrationTest.driveId
                remoteFileId = this@PhotoTrashOrchestrationTest.remoteFileId
                fileName = "fixture-orch.jpg"
                localSizeAtPairing = mediaSize
                localModifiedAtPairing = mediaModifiedSec * 1_000L
                localContentDigest = localDigest
                remoteContentHash = remoteHashValue
                this.baselineTrashed = baselineTrashed
                this.lastObservedTrashed = lastObservedTrashed
                this.state = state
            }
        }
        realm.where(PhotoPairing::class.java)
            .equalTo(PhotoPairing::uri.name, pairingUri)
            .findFirst()!!
            .let { realm.copyFromRealm(it, 0) }
    }

    private fun row(): PhotoPairing = realm().use { realm ->
        realm.where(PhotoPairing::class.java)
            .equalTo(PhotoPairing::uri.name, pairingUri)
            .findFirst()!!
            .let { realm.copyFromRealm(it, 0) }
    }

    private fun uploadFile() = UploadFile(
        uri = pairingUri,
        userId = userId,
        driveId = driveId,
        fileName = "fixture-orch.jpg",
        fileSize = mediaSize,
        fileModifiedAt = Date(mediaModifiedSec * 1_000L),
    )

    @Test
    @DisplayName("a trash after pairing and before the first pass is a transition, not a baseline")
    fun trashAfterPairingBeforeTheFirstPassPropagates() {
        // Pairing binds the activation baseline and the content digest at the upload moment.
        realm().use { pairingRealm ->
            PhotoTrashController.recordPairing(
                context = mockContext,
                uploadFile = uploadFile(),
                remoteFile = File(id = remoteFileId, driveId = driveId).apply { serverContentHash = pairingHash },
                uploadedContentDigest = sha256Hex(contentA),
                customRealm = pairingRealm,
            )
        }
        val seeded = row()
        assertEquals(false, seeded.baselineTrashed, "the baseline was captured at pairing, not lazily")
        assertEquals(false, seeded.lastObservedTrashed)
        assertEquals(sha256Hex(contentA), seeded.localContentDigest, "the content binding was recorded")
        assertEquals(pairingHash, seeded.remoteContentHash, "the remote basis was recorded")
        assertEquals(mediaSize, seeded.localSizeAtPairing, "the local basis is bound from the same producer the pass reads")
        assertEquals(mediaModifiedSec * 1_000L, seeded.localModifiedAtPairing)

        // The user trashes the photo AFTER pairing and BEFORE the first maintenance pass:
        // content and metadata are unchanged (a MediaStore trash moves no bytes).
        mediaTrashed.set(true)
        val propagated = runBlocking { realm().use { PhotoTrashController.runLocalReconciliation(mockContext, it) } }

        assertEquals(1, propagated, "the transition propagated exactly once")
        assertEquals(1L, deleteRequests.get(), "the server trash was called exactly once")
        val reconciled = row()
        assertTrue(reconciled.lastLocalTrashReconciledAt > 0L)
        assertTrue(reconciled.lastObservedTrashed)
        assertEquals(PhotoPairing.State.ACTIVE.name, reconciled.state)
    }

    @Test
    @DisplayName("a stored DENIED row becomes eligible after a remote restore and re-arms on re-delete")
    fun deniedRowBecomesEligibleAfterRestoreAndReDelete() {
        seedPairing(state = PhotoPairing.State.DENIED.name)

        // The denial is respected while the remote stays trashed.
        remoteTrashed.set(true)
        assertNull(runBlocking { realm().use { PhotoTrashController.onRemoteTrashEvent(mockContext, remoteFileId, driveId, it) } })
        assertEquals(PhotoPairing.State.DENIED.name, row().state, "the denial stands")

        // The remote restore EXPIRES the denial: the row is eligible again.
        remoteTrashed.set(false)
        assertNull(runBlocking { realm().use { PhotoTrashController.onRemoteTrashEvent(mockContext, remoteFileId, driveId, it) } })
        assertEquals(PhotoPairing.State.ACTIVE.name, row().state, "ClearPending expired the stored denial")
        assertEquals(0L, row().remoteTrashPendingSince)

        // A re-delete then arms a FRESH consent.
        remoteTrashed.set(true)
        val armed = runBlocking { realm().use { PhotoTrashController.onRemoteTrashEvent(mockContext, remoteFileId, driveId, it) } }
        assertNotNull(armed, "the re-delete arms a consent again")
        assertTrue(row().remoteTrashPendingSince > 0L)
    }

    @Test
    @DisplayName("a late baseline capture of a trashed item is a visible inconclusive, never silent history")
    fun lateBaselineCaptureOfATrashedItemMarksInconclusive() {
        // A row left pending (its pairing-time observation was unavailable), and the item is
        // ALREADY trashed when the late capture finally observes it.
        val pending = seedPairing(baselineTrashed = null)
        mediaTrashed.set(true)

        val baseline = realm().use { pairingRealm ->
            PhotoTrashController.captureActivationBaseline(mockContext, pending, pairingRealm)
        }

        assertTrue(baseline)
        val captured = row()
        assertEquals(true, captured.baselineTrashed)
        assertTrue(captured.lastObservedTrashed)
        assertEquals(PhotoPairing.State.INCONCLUSIVE.name, captured.state, "a late TRUE baseline is visibly inconclusive, not silent history")
    }

    @Test
    @DisplayName("an unbound upload digest never propagates and never calls the server trash")
    fun unboundDigestNeverPropagates() {
        // Chunked upload whose pairing-time sample failed: NO content binding.
        seedPairing(localDigest = null)
        mediaTrashed.set(true)

        val propagated = runBlocking { realm().use { PhotoTrashController.runLocalReconciliation(mockContext, it) } }

        assertEquals(0, propagated, "unknown evidence preserves: nothing propagated")
        assertEquals(0L, deleteRequests.get(), "the destructive API was never called")
        assertEquals(PhotoPairing.State.INCONCLUSIVE.name, row().state, "the unbound pairing is visibly inconclusive")
    }

    @Test
    @DisplayName("a same-metadata URI replacement is caught at the destructive boundary")
    fun sameMetadataReplacementIsCaughtAtTheBoundary() {
        // The local URI now holds DIFFERENT bytes with the SAME size and mtime (a same-stat
        // replacement): every metadata check passes, only the boundary digest can tell.
        seedPairing()
        mediaTrashed.set(true)
        resolverContent.set(contentB)

        val propagated = runBlocking { realm().use { PhotoTrashController.runLocalReconciliation(mockContext, it) } }

        assertEquals(0, propagated, "replaced content never gained trash authority")
        assertEquals(0L, deleteRequests.get(), "the destructive API was never called")
        val conflicted = row()
        assertEquals(PhotoPairing.State.CONFLICT_LOCAL_CHANGED.name, conflicted.state, "the replacement is a visible conflict")
        assertTrue(conflicted.lastObservedTrashed)
    }

    @AfterEach
    fun tearDown() {
        unmockkAll()
    }

    @AfterAll
    fun cleanup() {
        // ONLY the synthetic fixture realm is deleted — never any other realm, never any
        // owner data.
        runCatching { Realm.deleteRealm(config) }
    }

    private fun sha256Hex(bytes: ByteArray): String {
        val digest = java.security.MessageDigest.getInstance("SHA-256")
        return digest.digest(bytes).joinToString("") { "%02x".format(it) }
    }
}
