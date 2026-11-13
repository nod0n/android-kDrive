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
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okhttp3.Call
import okhttp3.Callback
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
import java.io.ByteArrayInputStream
import java.util.Date
import java.util.UUID
import java.util.concurrent.CountDownLatch
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger
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

    /** A STRICTLY CANONICAL server hash token (the official example's exact shape): the
     *  parse ladder must accept it, and the rules compare server-vs-server equality only. */
    private val pairingHash = "xxh3:00ec7bec63e548df"

    /** The bytes the mocked MediaStore currently serves through openInputStream. */
    private val resolverContent = AtomicReference(contentA)

    /** The mocked MediaStore row's trashed flag (size/mtime never change: the adversarial case). */
    private val mediaTrashed = AtomicBoolean(false)
    private val mediaRowPresent = AtomicBoolean(true)
    private val mediaSize = contentA.size.toLong()
    private val mediaModifiedSec = 1_700_000_000L

    /** The fresh server details the mocked API currently serves. */
    private val remoteTrashed = AtomicBoolean(false)
    private val remoteHash = AtomicReference(pairingHash)
    private val deleteRequests = AtomicLong(0)

    /** The live opt-in value the mocked settings serve (flippable mid-pass). */
    private val syncSettingsEnabled = AtomicBoolean(true)

    /** The live activation epoch the mocked settings serve (bumped to simulate an activation). */
    private val settingsActivationEpoch = AtomicLong(0L)

    /**
     * Mid-IO interleaving is established with EXPLICIT capture/release latches at the
     * production remote-details seam — never with Thread.sleep: a blocking sleep in the
     * runBlocking event loop never yields to a child that inherited that loop, so the
     * mutation could run entirely before the pass even started.
     */
    private var parkAtRemoteQuery = false
    private lateinit var remoteQueryCapture: CountDownLatch
    private lateinit var remoteQueryRelease: CountDownLatch

    /** How many times the pass actually REACHED the gated remote-details boundary. */
    private val remoteQueryEntered = AtomicInteger(0)

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
        mediaRowPresent.set(true)
        remoteTrashed.set(false)
        remoteHash.set(pairingHash)
        deleteRequests.set(0)
        syncSettingsEnabled.set(true)
        settingsActivationEpoch.set(0L)
        parkAtRemoteQuery = false
        remoteQueryCapture = CountDownLatch(1)
        remoteQueryRelease = CountDownLatch(1)
        remoteQueryEntered.set(0)

        val mockResolver = mockk<android.content.ContentResolver>()
        every { mockResolver.query(any(), any(), any(), any(), any()) } answers { mediaCursor(mediaTrashed.get(), mediaRowPresent.get()) }
        every { mockResolver.openInputStream(any()) } answers { ByteArrayInputStream(resolverContent.get()) }
        mockContext = spyk(context)
        every { mockContext.contentResolver } returns mockResolver

        mockkObject(ApiRepository)
        mockkObject(AccountUtils)
        every { AccountUtils.currentUserId } returns userId
        coEvery { AccountUtils.getHttpClient(any(), any(), any(), any()) } returns OkHttpClient()
        mockkObject(UploadFile.Companion)
        every { UploadFile.getAppSyncSettings() } answers {
            SyncSettings(
                userId = userId,
                driveId = driveId,
                photoTrashSyncEnabled = syncSettingsEnabled.get(),
                photoTrashActivationEpoch = settingsActivationEpoch.get(),
            )
        }
        coEvery { ApiRepository.trashPairedPhoto(any(), any(), any()) } answers {
            val issue = thirdArg<(() -> Boolean) -> Boolean>()
            if (issue { deleteRequests.incrementAndGet(); true }) ApiResponse(result = ApiResponseStatus.SUCCESS)
            else null
        }
        val revision = PhotoTrashController.beginActivationWrite()
        try {
            PhotoTrashController.noteActivationWritten(userId, 0L, true, revision)
        } finally {
            PhotoTrashController.endActivationWrite()
        }
        every { ApiRepository.getFileDetailsWithHash(any(), any()) } answers {
            remoteQueryGate()
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

    /**
     * The remote-query gate at the REAL production IO boundary. When a test arms
     * [parkAtRemoteQuery], the parked pass first PROVES it reached the seam (its row list
     * and pairing-time observation were captured BEFORE this point — the mutation may only
     * begin after the capture latch fires) and then stays suspended until the test
     * releases it. A latch timeout fails OPEN so a broken park can never wedge the suite.
     */
    private fun remoteQueryGate() {
        remoteQueryEntered.incrementAndGet()
        if (parkAtRemoteQuery) {
            remoteQueryCapture.countDown()
            remoteQueryRelease.await(15, TimeUnit.SECONDS)
        }
    }

    /**
     * Drives the REAL pass on an IO dispatcher (async WITHOUT a dispatcher inherits this
     * runBlocking event loop — the parent's blocking sleep would never let it start, so
     * the "interleaving" could happen entirely before the pass began). The test blocks
     * until the pass has REACHED the production remote-details seam, performs the mutation
     * against the LIVE stores, releases the suspended pass, and returns its result.
     */
    private fun runPassWithMidIoMutation(mutation: () -> Unit): Int = runBlocking {
        parkAtRemoteQuery = true
        val pass = async(Dispatchers.IO) { PhotoTrashController.runLocalReconciliation(mockContext, config) }
        assertTrue(
            remoteQueryCapture.await(15, TimeUnit.SECONDS),
            "the pass must reach the remote-details seam (old row captured) before the mutation",
        )
        mutation()
        remoteQueryRelease.countDown()
        pass.await()
    }

    private fun mediaCursor(trashed: Boolean, rowPresent: Boolean = true): Cursor = mockk(relaxed = true) {
        every { moveToFirst() } returns rowPresent
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

    private fun uploadFile(uri: String = pairingUri) = UploadFile(
        uri = uri,
        userId = userId,
        driveId = driveId,
        fileName = "fixture-orch.jpg",
        fileSize = mediaSize,
        fileModifiedAt = Date(mediaModifiedSec * 1_000L),
    )

    private fun useRealTrashDispatch(beforeCall: () -> Unit = {}, onEnqueue: (Call, Callback, Request) -> Unit) {
        val client = mockk<OkHttpClient>()
        val call = mockk<Call>()
        lateinit var request: Request
        every { client.newCall(any()) } answers {
            request = firstArg()
            beforeCall()
            call
        }
        every { call.cancel() } returns Unit
        every { call.enqueue(any()) } answers {
            deleteRequests.incrementAndGet()
            onEnqueue(call, firstArg(), request)
        }
        coEvery { AccountUtils.getHttpClient(userId, null, any(), any()) } returns client
        coEvery { ApiRepository.trashPairedPhoto(any(), any(), any()) } coAnswers { callOriginal() }
    }

    private fun successfulTrashResponse(request: Request) = Response.Builder().request(request)
        .protocol(Protocol.HTTP_1_1).code(200).message("OK")
        .body("{\"result\":\"success\"}".toResponseBody()).build()

    private fun writeFixtureSettings(enabled: Boolean) {
        syncSettingsEnabled.set(enabled)
        realm().use { realm ->
            UploadFile.setAppSyncSettings(
                SyncSettings(userId = userId, driveId = driveId, photoTrashSyncEnabled = enabled),
                customRealm = realm,
            )
        }
    }

    @Test
    @DisplayName("completed opt-out, rebinding, activation and account changes after validation each prevent actual enqueue")
    fun invalidationsAfterValidationPreventActualEnqueue() {
        val currentUser = AtomicInteger(userId)
        every { AccountUtils.currentUserId } answers { currentUser.get() }
        val invalidations = listOf<() -> Unit>(
            { writeFixtureSettings(false) },
            {
                PhotoTrashController.recordPairing(
                    mockContext, uploadFile(),
                    File(id = remoteFileId, driveId = driveId).apply { serverContentHash = pairingHash },
                    sha256Hex(contentA), config,
                )
            },
            { writeFixtureSettings(false); writeFixtureSettings(true) },
            { AccountUtils.updateCurrentScope { currentUser.set(userId + 1); true } },
            {
                AccountUtils.updateCurrentScope { currentUser.set(userId + 1); true }
                AccountUtils.updateCurrentScope { currentUser.set(userId); true }
            },
        )
        invalidations.forEachIndexed { index, invalidate ->
            realm().use { realm -> realm.executeTransaction { it.deleteAll() } }
            currentUser.set(userId)
            settingsActivationEpoch.set(0L)
            writeFixtureSettings(true)
            // First activation of a fresh settings row is epoch1; the pass and pairing agree.
            settingsActivationEpoch.set(1L)
            seedPairing().also {
                realm().use { realm -> realm.executeTransaction {
                    it.where(PhotoPairing::class.java).findFirst()!!.activationEpoch = 1L
                } }
            }
            mediaTrashed.set(true)
            deleteRequests.set(0)
            useRealTrashDispatch(beforeCall = invalidate) { call, callback, request ->
                callback.onResponse(call, successfulTrashResponse(request)) // a broken gate fails, never hangs
            }
            val propagated = runBlocking { PhotoTrashController.runLocalReconciliation(mockContext, config) }
            assertEquals(0, propagated, "invalidation $index completed after the old row checks")
            assertEquals(0L, deleteRequests.get(), "invalidation $index must prevent Call.enqueue itself")
        }
    }

    @Test
    @DisplayName("actual enqueue precedes a queued invalidation without holding the gate over the response wait")
    fun enqueueWinsBeforeInvalidationButDoesNotWaitForItsResponse() = runBlocking {
        seedPairing()
        mediaTrashed.set(true)
        val queued = CountDownLatch(1)
        val writerEntered = CountDownLatch(1)
        val writerDone = CountDownLatch(1)
        val responseCallback = AtomicReference<Callback>()
        val responseCall = AtomicReference<Call>()
        val responseRequest = AtomicReference<Request>()
        val writer = Thread {
            writerEntered.countDown()
            PhotoTrashController.removeAllPairings(config)
            writeFixtureSettings(false)
            writerDone.countDown()
        }
        useRealTrashDispatch { call, callback, request ->
            responseCall.set(call)
            responseCallback.set(callback)
            responseRequest.set(request)
            writer.start()
            assertTrue(writerEntered.await(5, TimeUnit.SECONDS))
            val deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5)
            while (writer.state != Thread.State.WAITING && System.nanoTime() < deadline) Thread.yield()
            assertEquals(Thread.State.WAITING, writer.state)
            assertTrue(writer.stackTrace.any { it.methodName == "beginBindingWrite" }, "the real invalidation waits at the issue gate")
            assertEquals(1L, writerDone.count)
            queued.countDown()
        }
        val pass = async(Dispatchers.IO) { PhotoTrashController.runLocalReconciliation(mockContext, config) }
        try {
            assertTrue(queued.await(5, TimeUnit.SECONDS))
            assertTrue(writerDone.await(5, TimeUnit.SECONDS), "invalidation finishes while the issued request still awaits its response")
            assertFalse(pass.isCompleted)
            responseCallback.get().onResponse(responseCall.get(), successfulTrashResponse(responseRequest.get()))
            assertEquals(1, withTimeout(5_000) { pass.await() }, "the already-issued request may succeed after disable")
            assertEquals(1L, deleteRequests.get())
        } finally {
            pass.cancelAndJoin()
            if (writer.state != Thread.State.NEW) writer.join(5_000)
            assertFalse(writer.isAlive, "the invalidation must finish before fixture teardown")
        }
    }

    @Test
    @DisplayName("a trash after pairing and before the first pass is a transition, not a baseline")
    fun trashAfterPairingBeforeTheFirstPassPropagates() {
        // Pairing binds the activation baseline and the content digest at the upload moment.
        PhotoTrashController.recordPairing(
            context = mockContext,
            uploadFile = uploadFile(),
            remoteFile = File(id = remoteFileId, driveId = driveId).apply { serverContentHash = pairingHash },
            uploadedContentDigest = sha256Hex(contentA),
            customRealmConfig = config,
        )
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
        val propagated = runBlocking { PhotoTrashController.runLocalReconciliation(mockContext, config) }

        assertEquals(1, propagated, "the transition propagated exactly once")
        assertEquals(1L, deleteRequests.get(), "the server trash was called exactly once")
        val reconciled = row()
        assertTrue(reconciled.lastLocalTrashReconciledAt > 0L)
        assertTrue(reconciled.lastObservedTrashed)
        assertEquals(PhotoPairing.State.ACTIVE.name, reconciled.state)
    }

    @Test
    @DisplayName("a stored DENIED row expires on the pass's own restore observation and re-arms on re-delete")
    fun deniedRowExpiresThroughTheRealCatchUpPathAndReArms() {
        seedPairing(state = PhotoPairing.State.DENIED.name)

        // The denial is respected while the remote stays trashed: the pass neither re-asks
        // nor clears it.
        remoteTrashed.set(true)
        assertEquals(0, runBlocking { PhotoTrashController.runLocalReconciliation(mockContext, config) })
        assertEquals(PhotoPairing.State.DENIED.name, row().state, "the denial stands")

        // The remote restore has NO MQTT event (the app only ever receives FILE_TRASH): the
        // EXPIRY happens on the pass's own observation that the remote is no longer trashed.
        remoteTrashed.set(false)
        assertEquals(0, runBlocking { PhotoTrashController.runLocalReconciliation(mockContext, config) })
        assertEquals(PhotoPairing.State.ACTIVE.name, row().state, "the pass expired the stored denial")
        assertEquals(0L, row().remoteTrashPendingSince)

        // A re-delete (delivered through the REAL FILE_TRASH event path) arms a FRESH consent.
        remoteTrashed.set(true)
        val armed = runBlocking { PhotoTrashController.onRemoteTrashEvent(mockContext, remoteFileId, driveId, config) }
        assertNotNull(armed, "the re-delete arms a consent again")
        assertTrue(row().remoteTrashPendingSince > 0L)
    }

    @Test
    @DisplayName("a pairing binds through validated server-to-source evidence, never an unconditional sample")
    fun pairingBindsThroughValidatedServerToSourceEvidence() {
        // Chunked upload (no stream digest): the created file's server hash is the REAL
        // documented format (xxh3: + 16 hex — the archived API evidence), and BOTH
        // receipts (the server-algorithm digest AND the local sha-256 receipt) are
        // computed from ONE stream pass over the same bytes.
        val serverTokenA = PhotoTrashServerHash.hashStreaming(ByteArrayInputStream(contentA))
        PhotoTrashController.recordPairing(
            context = mockContext,
            uploadFile = uploadFile(),
            remoteFile = File(id = remoteFileId, driveId = driveId).apply { serverContentHash = serverTokenA },
            uploadedContentDigest = null,
            customRealmConfig = config,
        )
        assertEquals(sha256Hex(contentA), row().localContentDigest, "the local receipt comes from the SAME pass that matched the server token")
        assertEquals(serverTokenA, row().remoteContentHash, "the remote basis is the server's own identity")

        // A same-metadata URI replacement BEFORE pairing: the fresh local read no longer
        // matches the server token — unknown evidence, NEVER a binding (the rejected
        // sample fallback would have recorded the replacement's digest as the identity).
        val replacedUri = "content://fixture/photo-trash-orch-replaced"
        resolverContent.set(contentB)
        PhotoTrashController.recordPairing(
            context = mockContext,
            uploadFile = uploadFile(uri = replacedUri),
            remoteFile = File(id = remoteFileId, driveId = driveId).apply { serverContentHash = serverTokenA },
            uploadedContentDigest = null,
            customRealmConfig = config,
        )
        assertNull(
            realm().use { realm ->
                realm.where(PhotoPairing::class.java).equalTo(PhotoPairing::uri.name, replacedUri).findFirst()!!.localContentDigest
            },
            "a replaced source records NO digest: unknown preserves",
        )

        // An UNSUPPORTED server algorithm (here: a bare sha-256 hex receipt — the app's
        // own algorithm is NOT a server identity): unknown preserves, never a binding
        // that was manufactured by comparing different algorithms.
        val shaUri = "content://fixture/photo-trash-orch-sha"
        PhotoTrashController.recordPairing(
            context = mockContext,
            uploadFile = uploadFile(uri = shaUri),
            remoteFile = File(id = remoteFileId, driveId = driveId).apply { serverContentHash = sha256Hex(contentA) },
            uploadedContentDigest = null,
            customRealmConfig = config,
        )
        assertNull(
            realm().use { realm ->
                realm.where(PhotoPairing::class.java).equalTo(PhotoPairing::uri.name, shaUri).findFirst()!!.localContentDigest
            },
            "a non-canonical server token records NO digest: unknown preserves",
        )
    }

    @Test
    @DisplayName("an absent or unreadable local row stays pending — never an assumed false baseline")
    fun absentLocalRowStaysPending() {
        // Pairing-time observation: the MediaStore row is ABSENT (query returns no row).
        mediaRowPresent.set(false)
        PhotoTrashController.recordPairing(
            context = mockContext,
            uploadFile = uploadFile(),
            remoteFile = File(id = remoteFileId, driveId = driveId).apply { serverContentHash = pairingHash },
            uploadedContentDigest = sha256Hex(contentA),
            customRealmConfig = config,
        )
        assertNull(row().baselineTrashed, "an absent row never starts as a present-and-not-trashed baseline")

        // Reactivation over an ALREADY-POPULATED baseline with the row still absent: the
        // old baseline must not survive as eligible through an observation that could not
        // actually see the item — the capture resets it to pending.
        realm().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java).equalTo(PhotoPairing::uri.name, pairingUri).findFirst()?.baselineTrashed = false
            }
        }
        mediaRowPresent.set(false)
        PhotoTrashController.captureActivationBaselines(mockContext, 0L, config)
        assertNull(row().baselineTrashed, "an unavailable reactivation observation resets the baseline to pending")
        assertEquals(PhotoPairing.State.ACTIVE.name, row().state, "no state promotion happens without a real observation")
    }

    @Test
    @DisplayName("an option switched off DURING the pass stops it: zero delete calls, no state writes")
    fun optionDisabledMidPassStopsBeforeTheDestructiveCall() {
        seedPairing()
        mediaTrashed.set(true)

        val propagated = runPassWithMidIoMutation {
            // The option is switched off WHILE the pass is suspended in its remote-details
            // IO (proven reached).
            syncSettingsEnabled.set(false)
        }

        assertEquals(0, propagated, "the unauthorized pass propagated nothing")
        assertEquals(0L, deleteRequests.get(), "the destructive API was never reached after the switch-off")
        assertEquals(PhotoPairing.State.ACTIVE.name, row().state, "no state was written by the unauthorized pass")
        assertEquals(0L, row().lastLocalTrashReconciledAt, "the reconciliation marker was never set")
        assertTrue(remoteQueryEntered.get() >= 1, "the pass actually reached the gated mid-IO boundary")
    }

    @Test
    @DisplayName("an item trashed while the option was off is inconclusive at activation, never a new transition")
    fun itemTrashedWhileOffIsInconclusiveAtActivation() {
        // Paired while the option was OFF (recordPairing runs for every sync upload):
        // baseline captured as not-trashed at pairing time.
        seedPairing(baselineTrashed = false, lastObservedTrashed = false)

        // The item is trashed while the option is still off; the user then enables the
        // option: the activation capture re-baselines EVERY row off the REAL MediaStore.
        mediaTrashed.set(true)
        PhotoTrashController.captureActivationBaselines(mockContext, 0L, config)
        val activated = row()
        assertEquals(true, activated.baselineTrashed, "the activation baseline is the real opt-in state")
        assertEquals(PhotoPairing.State.INCONCLUSIVE.name, activated.state, "a pre-activation trash is visibly inconclusive")

        // The first pass after enabling propagates NOTHING for it.
        val propagated = runBlocking { PhotoTrashController.runLocalReconciliation(mockContext, config) }
        assertEquals(0, propagated, "pre-activation history never becomes a post-activation transition")
        assertEquals(0L, deleteRequests.get(), "the destructive API was never called")
        assertEquals(PhotoPairing.State.INCONCLUSIVE.name, row().state)
    }

    @Test
    @DisplayName("an option enabled before a trash still propagates it after activation")
    fun enabledOptionBeforeTrashStillPropagatesAfterActivation() {
        seedPairing(baselineTrashed = false, lastObservedTrashed = false)

        // Activation re-baseline observes the item NOT trashed: no state change.
        PhotoTrashController.captureActivationBaselines(mockContext, 0L, config)
        assertEquals(PhotoPairing.State.ACTIVE.name, row().state)
        assertEquals(false, row().baselineTrashed)

        // A trash AFTER activation is a transition: the first pass propagates it.
        mediaTrashed.set(true)
        val propagated = runBlocking { PhotoTrashController.runLocalReconciliation(mockContext, config) }
        assertEquals(1, propagated, "a transition after activation propagates exactly once")
        assertEquals(1L, deleteRequests.get())
        assertTrue(row().lastLocalTrashReconciledAt > 0L)
    }

    @Test
    @DisplayName("a late baseline capture of a trashed item is a visible inconclusive, never silent history")
    fun lateBaselineCaptureOfATrashedItemMarksInconclusive() {
        // A row left pending (its pairing-time observation was unavailable), and the item is
        // ALREADY trashed when the late capture finally observes it.
        val pending = seedPairing(baselineTrashed = null)
        mediaTrashed.set(true)

        val baseline = PhotoTrashController.captureActivationBaseline(mockContext, pending, 0L, config)

        assertTrue(baseline)
        val captured = row()
        assertEquals(true, captured.baselineTrashed)
        assertTrue(captured.lastObservedTrashed)
        assertEquals(PhotoPairing.State.INCONCLUSIVE.name, captured.state, "a late TRUE baseline is visibly inconclusive, not silent history")
    }

    @Test
    @DisplayName("an unbound upload digest never propagates and never calls the server trash")
    fun unboundDigestNeverPropagates() {
        // Chunked upload with NO bound content identity (no stream digest, no validated
        // server-to-source match): NO content binding.
        seedPairing(localDigest = null)
        mediaTrashed.set(true)

        val propagated = runBlocking { PhotoTrashController.runLocalReconciliation(mockContext, config) }

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

        val propagated = runBlocking { PhotoTrashController.runLocalReconciliation(mockContext, config) }

        assertEquals(0, propagated, "replaced content never gained trash authority")
        assertEquals(0L, deleteRequests.get(), "the destructive API was never called")
        val conflicted = row()
        assertEquals(PhotoPairing.State.CONFLICT_LOCAL_CHANGED.name, conflicted.state, "the replacement is a visible conflict")
        assertTrue(conflicted.lastObservedTrashed)
    }

    @Test
    @DisplayName("a same-URI replacement between observation and effect is never trashed or mutated by the stale pass")
    fun staleObservationCannotTrashAReplacementPairing() {
        seedPairing()
        mediaTrashed.set(true)

        val propagated = runPassWithMidIoMutation {
            // While the pass is suspended in its remote IO (its OLD row already captured),
            // the user re-uploads the SAME uri: a NEW binding — new remote file, new
            // pairing moment, new content binding.
            PhotoTrashController.recordPairing(
                context = mockContext,
                uploadFile = uploadFile(),
                remoteFile = File(id = 900_502, driveId = driveId).apply { serverContentHash = pairingHash },
                uploadedContentDigest = sha256Hex(contentB),
                customRealmConfig = config,
            )
            // The stale pass's boundary re-read still verifies its OWN recorded digest
            // (contentA): the witness must catch the replacement, not the digest.
            resolverContent.set(contentA)
        }

        assertTrue(remoteQueryEntered.get() >= 1, "the pass reached the gated mid-IO boundary")
        assertEquals(0, propagated, "the stale pass propagated nothing")
        assertEquals(0L, deleteRequests.get(), "the stale pass never reached the destructive call")
        val replacement = row()
        assertEquals(900_502, replacement.remoteFileId, "the live row is the replacement binding")
        assertEquals(PhotoPairing.State.ACTIVE.name, replacement.state, "no stale write touched the replacement")
        assertEquals(0L, replacement.lastLocalTrashReconciledAt, "no stale reconciled marker on the replacement")
    }

    @Test
    @DisplayName("an OFF->ON activation between observation and effect is never bypassed by the in-flight pass")
    fun offOnActivationBetweenObservationAndEffectIsNeverBypassed() {
        seedPairing(baselineTrashed = false, lastObservedTrashed = false)
        mediaTrashed.set(true)

        val propagated = runPassWithMidIoMutation {
            // The user turns the option OFF->ON mid-pass: the activation capture re-baselines
            // EVERY row off the real MediaStore under a NEW epoch (every captured value can
            // come back identical — the epoch is what invalidates the in-flight pass, which
            // holds the PRE-toggle detached copy).
            settingsActivationEpoch.incrementAndGet()
            PhotoTrashController.captureActivationBaselines(mockContext, settingsActivationEpoch.get(), config)
        }

        assertTrue(remoteQueryEntered.get() >= 1, "the pass reached the gated mid-IO boundary")
        assertEquals(0, propagated, "the stale-baseline pass propagated nothing")
        assertEquals(0L, deleteRequests.get(), "the destructive API was never called by the stale pass")
        val activated = row()
        assertEquals(true, activated.baselineTrashed, "the activation re-baseline stands")
        assertEquals(1L, activated.activationEpoch, "the activation re-stamped the generation")
        assertEquals(PhotoPairing.State.INCONCLUSIVE.name, activated.state, "the activation's fresh-start protection is NOT bypassed")
        assertEquals(0L, activated.lastLocalTrashReconciledAt, "the stale pass left no reconciled marker")
    }

    @Test
    @DisplayName("a stale expiry cannot clear a replaced binding's fresh arming")
    fun staleExpiryCannotClearAReplacedBindingFreshArming() {
        seedPairing()
        realm().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java).equalTo(PhotoPairing::uri.name, pairingUri).findFirst()?.remoteTrashPendingSince = 1_000L
            }
        }
        mediaTrashed.set(false)
        remoteTrashed.set(false) // the stale pass will observe a restored remote

        runPassWithMidIoMutation {
            // The binding is replaced AND freshly armed (a new remote trash the old pass
            // knows nothing about) while the pass is suspended in its remote IO.
            PhotoTrashController.recordPairing(
                context = mockContext,
                uploadFile = uploadFile(),
                remoteFile = File(id = 900_502, driveId = driveId).apply { serverContentHash = pairingHash },
                uploadedContentDigest = sha256Hex(contentA),
                customRealmConfig = config,
            )
            realm().use { realm ->
                realm.executeTransaction { mutableRealm ->
                    mutableRealm.where(PhotoPairing::class.java).equalTo(PhotoPairing::uri.name, pairingUri).findFirst()?.remoteTrashPendingSince = 9_000L
                }
            }
        }

        assertTrue(remoteQueryEntered.get() >= 1, "the pass reached the gated mid-IO boundary")
        assertEquals(9_000L, row().remoteTrashPendingSince, "the stale expiry never cleared the replacement's fresh arming")
    }

    @Test
    @DisplayName("a row absent at pairing and visible later rides the content binding, never a false metadata conflict")
    fun absentRowMetadataBecomesVisibleWithoutFalseConflict() {
        // Pairing while the MediaStore row is ABSENT: baseline pending, NO metadata bound
        // (the -1 sentinel — never a different-producer fallback).
        mediaRowPresent.set(false)
        PhotoTrashController.recordPairing(
            context = mockContext,
            uploadFile = uploadFile(),
            remoteFile = File(id = remoteFileId, driveId = driveId).apply { serverContentHash = pairingHash },
            uploadedContentDigest = sha256Hex(contentA),
            customRealmConfig = config,
        )
        assertNull(row().baselineTrashed)
        assertEquals(-1L, row().localSizeAtPairing, "an unobservable row binds NO metadata")
        assertEquals(-1L, row().localModifiedAtPairing)

        // The row becomes VISIBLE, content unchanged: the late capture fills the baseline,
        // and the pass propagates on the content binding alone — no false metadata conflict.
        mediaRowPresent.set(true)
        PhotoTrashController.captureActivationBaselines(mockContext, 0L, config)
        assertEquals(false, row().baselineTrashed, "the late capture fills the pending baseline")
        mediaTrashed.set(true)
        assertEquals(1, runBlocking { PhotoTrashController.runLocalReconciliation(mockContext, config) }, "the unchanged visible row propagates")
        assertEquals(1L, deleteRequests.get())
        assertEquals(PhotoPairing.State.ACTIVE.name, row().state)

        // The CHANGED variant: a pairing recorded while absent, the row becomes visible with
        // REPLACED bytes (not trashed) — the late capture fills the baseline, then a trash
        // reaches the destructive boundary where the digest (not metadata) catches it.
        val replacedUri = "content://fixture/photo-trash-orch-absent-replaced"
        mediaRowPresent.set(false)
        mediaTrashed.set(false)
        resolverContent.set(contentB)
        PhotoTrashController.recordPairing(
            context = mockContext,
            uploadFile = uploadFile(uri = replacedUri),
            remoteFile = File(id = 900_503, driveId = driveId).apply { serverContentHash = pairingHash },
            uploadedContentDigest = sha256Hex(contentA), // the transfer carried A; the row now serves B
            customRealmConfig = config,
        )
        mediaRowPresent.set(true) // visible again, still serving B
        PhotoTrashController.captureActivationBaselines(mockContext, 0L, config)
        assertEquals(false, realm().use { realm ->
            realm.where(PhotoPairing::class.java).equalTo(PhotoPairing::uri.name, replacedUri).findFirst()!!.baselineTrashed
        }, "the visible row's baseline filled")
        mediaTrashed.set(true) // the user trashes the replaced item
        assertEquals(0, runBlocking { PhotoTrashController.runLocalReconciliation(mockContext, config) }, "replaced bytes never propagate")
        assertEquals(1L, deleteRequests.get(), "no additional destructive call")
        assertEquals(
            PhotoPairing.State.CONFLICT_LOCAL_CHANGED.name,
            realm().use { realm ->
                realm.where(PhotoPairing::class.java).equalTo(PhotoPairing::uri.name, replacedUri).findFirst()!!.state
            },
            "the boundary digest caught the replacement",
        )
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
