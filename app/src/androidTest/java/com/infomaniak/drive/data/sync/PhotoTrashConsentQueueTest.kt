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
import kotlinx.coroutines.runBlocking
import okhttp3.OkHttpClient
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
import java.util.concurrent.atomic.AtomicReference
import android.database.Cursor

/**
 * THE DURABLE SERIALIZED CONSENT QUEUE (androidTest instrumentation — the row store is
 * Realm, a native engine that cannot open on the JVM). The pending row state
 * ([PhotoPairing.remoteTrashPendingSince]) IS the queue: these tests drive
 * [PhotoTrashController.pollNextPendingConsent]'s launch-time revalidation (fresh remote
 * trash with an unchanged bound identity, unchanged local digest at the destructive
 * boundary, opt-in and account gates) and [PhotoTrashController.onRemoteTrashConsentResult]'s
 * request-keyed result application — the sequences the durable queue must survive (process
 * death with no collector, two simultaneous items, denial, duplicate/stale results, account
 * switch, opt-out, remote restore, local edit while waiting).
 *
 * Fully disposable and account-free: a synthetic upload realm (deleted in cleanup), a
 * mocked ContentResolver, synthetic constants only — NO owner data, NO account, NO real
 * network. The system dialog itself is UI (MainActivity); here the queue is exercised to
 * the point where MainActivity would launch it, and the result path beyond it.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PhotoTrashConsentQueueTest {

    private val context = ApplicationProvider.getApplicationContext<android.content.Context>()
    private val userId = 961
    private val driveId = 761
    private val remoteFileIdA = 900_601
    private val remoteFileIdB = 900_602
    private val uriA = "content://fixture/consent-queue-a"
    private val uriB = "content://fixture/consent-queue-b"
    private val stampA = 1_000L
    private val stampB = 2_000L

    private val contentA = "fixture-queue-content-A".toByteArray()
    private val contentB = "fixture-queue-content-B".toByteArray() // same length: same-stat replacement

    private val pairingHash = "pairing-hash"
    private val resolverContent = AtomicReference(contentA)
    private val mediaTrashed = AtomicBoolean(false)
    private val remoteTrashed = AtomicBoolean(true)
    private val remoteHash = AtomicReference(pairingHash)

    /**
     * Mid-IO interleaving is established with an EXPLICIT capture/release latch at the
     * production remote-details seam — never with Thread.sleep: a blocking sleep in the
     * runBlocking event loop never yields to a child that inherited that loop, so the
     * identity flip could run entirely before the poll even started.
     */
    private var parkAtRemoteQuery = false
    private lateinit var remoteQueryCapture: CountDownLatch
    private lateinit var remoteQueryRelease: CountDownLatch

    /** How many times the poll actually REACHED the gated remote-details boundary. */
    private val remoteQueryEntered = AtomicInteger(0)

    // A FRESH fixture realm per case (runtime evidence: a shared per-class file turned one
    // leaked open instance into duplicate-primary-key failures for every later case, because
    // setUp's delete could not remove a file an open instance still held).
    private lateinit var config: RealmConfiguration
    private lateinit var mockContext: android.content.Context

    @BeforeAll
    fun initRealm() {
        Realm.init(context)
    }

    @BeforeEach
    fun setUp() {
        MockKAnnotations.init(this)
        config = RealmConfiguration.Builder()
            .schemaVersion(UploadMigration.DB_VERSION)
            .modules(RealmModules.SyncFilesModule())
            .name("photoTrashConsentQueue-${UUID.randomUUID()}")
            .directory(context.cacheDir)
            .build()
        resolverContent.set(contentA)
        mediaTrashed.set(false)
        remoteTrashed.set(true)
        remoteHash.set(pairingHash)
        parkAtRemoteQuery = false
        remoteQueryCapture = CountDownLatch(1)
        remoteQueryRelease = CountDownLatch(1)
        remoteQueryEntered.set(0)

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
        every { ApiRepository.getFileDetailsWithHash(any(), any()) } answers {
            remoteQueryGate()
            val requested = firstArg<File>()
            ApiResponse(
                result = ApiResponseStatus.SUCCESS,
                data = File(id = requested.id, driveId = requested.driveId).apply {
                    if (remoteTrashed.get()) status = "trashed"
                    lastModifiedAt = 1_700_000_000L
                    size = contentA.size.toLong()
                    serverContentHash = remoteHash.get()
                },
            )
        }
    }

    /** A live account flip DURING the poll's remote IO must end the drain (softcheck #3):
     *  the parked poll first PROVES it reached the seam, then the identity flips, then the
     *  poll is released. A latch timeout fails OPEN so a broken park cannot wedge the suite. */
    private fun remoteQueryGate() {
        remoteQueryEntered.incrementAndGet()
        if (parkAtRemoteQuery) {
            remoteQueryCapture.countDown()
            remoteQueryRelease.await(15, TimeUnit.SECONDS)
        }
    }

    private fun mediaCursor(trashed: Boolean): Cursor = mockk(relaxed = true) {
        every { moveToFirst() } returns true
        every { getLong(0) } returns contentA.size.toLong()
        every { getLong(1) } returns 1_700_000_000L
        every { getInt(2) } returns if (trashed) 1 else 0
    }

    private fun realm(): Realm = Realm.getInstance(config)

    private fun seedArmedRow(uri: String, remoteFileId: Int, stamp: Long, state: String = PhotoPairing.State.ACTIVE.name) {
        realm().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.createObject(PhotoPairing::class.java, uri)?.apply {
                    userId = this@PhotoTrashConsentQueueTest.userId
                    driveId = this@PhotoTrashConsentQueueTest.driveId
                    this.remoteFileId = remoteFileId
                    fileName = "fixture-queue.jpg"
                    localSizeAtPairing = contentA.size.toLong()
                    localModifiedAtPairing = 1_700_000_000L * 1_000L
                    localContentDigest = sha256Hex(contentA)
                    remoteContentHash = pairingHash
                    baselineTrashed = false
                    lastObservedTrashed = false
                    this.state = state
                    remoteTrashPendingSince = stamp
                }
            }
        }
    }

    private fun row(uri: String): PhotoPairing = realm().use { realm ->
        realm.where(PhotoPairing::class.java)
            .equalTo(PhotoPairing::uri.name, uri)
            .findFirst()!!
            .let { realm.copyFromRealm(it, 0) }
    }

    /** The full scoped request identity MainActivity would have captured for a row. */
    private fun consentRequest(
        uri: String,
        remoteFileId: Int,
        stamp: Long,
        pairedAt: Long = 0L,
    ) = PhotoTrashController.ConsentRequest(
        uri = uri,
        userId = userId,
        driveId = driveId,
        remoteFileId = remoteFileId,
        pendingSince = stamp,
        pairedAt = pairedAt,
        activationEpoch = 0L,
        bindingGeneration = 0L,
    )

    @Test
    @DisplayName("armed requests survive a restart with no collector and surface oldest first")
    fun armedRequestsSurviveRestartAndSurfaceOldestFirst() {
        seedArmedRow(uriA, remoteFileIdA, stampA)
        seedArmedRow(uriB, remoteFileIdB, stampB)

        // No dialog was ever shown and no flow collector ever ran (process death): the
        // durable row state alone resurfaces the requests on the next app entry.
        val first = runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }
            ?: throw AssertionError("the durable queue must resurface armed requests after a restart")
        assertEquals(uriA, first.uri, "the OLDEST armed request is surfaced first")
        assertEquals(stampA, first.pendingSince, "the launch carries the consent stamp of that request")
    }

    @Test
    @DisplayName("two simultaneous items serialize: one dialog per completed result, oldest first")
    fun twoSimultaneousItemsSerializeOldestFirst() {
        seedArmedRow(uriA, remoteFileIdA, stampA)
        seedArmedRow(uriB, remoteFileIdB, stampB)

        assertEquals(uriA, runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }?.uri)
        PhotoTrashController.onRemoteTrashConsentResult(consentRequest(uriA, remoteFileIdA, stampA), granted = true, customRealmConfig = config)

        assertEquals(uriB, runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }?.uri, "the second item is only offered after the first result")
        PhotoTrashController.onRemoteTrashConsentResult(consentRequest(uriB, remoteFileIdB, stampB), granted = true, customRealmConfig = config)

        assertNull(runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }, "the queue drains to empty")
        assertEquals(0L, row(uriA).remoteTrashPendingSince)
        assertEquals(0L, row(uriB).remoteTrashPendingSince)
    }

    @Test
    @DisplayName("a declined first item is not re-offered; the second one is still approved")
    fun firstDeniedThenSecondApproved() {
        seedArmedRow(uriA, remoteFileIdA, stampA)
        seedArmedRow(uriB, remoteFileIdB, stampB)

        assertEquals(uriA, runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }?.uri)
        PhotoTrashController.onRemoteTrashConsentResult(consentRequest(uriA, remoteFileIdA, stampA), granted = false, customRealmConfig = config)
        assertEquals(PhotoPairing.State.DENIED.name, row(uriA).state, "the denial is recorded")

        val next = runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }
        assertEquals(uriB, next?.uri, "the denial does not block the second request")
        assertNotNull(next)
        PhotoTrashController.onRemoteTrashConsentResult(consentRequest(uriB, remoteFileIdB, stampB), granted = true, customRealmConfig = config)
        assertEquals(PhotoPairing.State.ACTIVE.name, row(uriB).state)
    }

    @Test
    @DisplayName("duplicate and stale results are dropped, never applied to a newer request")
    fun duplicateAndStaleResultsAreDropped() {
        seedArmedRow(uriA, remoteFileIdA, stampA)

        PhotoTrashController.onRemoteTrashConsentResult(consentRequest(uriA, remoteFileIdA, stampA), granted = true, customRealmConfig = config)
        val appliedAt = row(uriA).lastRemoteTrashAppliedAt
        assertTrue(appliedAt > 0L)

        // A DUPLICATE delivery of the same request — this time as a DENIAL: the stamp no
        // longer matches (the row was already consumed), so it must be dropped. A denial is
        // decisive regardless of clock granularity: a wrongly-applied duplicate would flip
        // the row ACTIVE->DENIED (two same-millisecond timestamps could not tell a repeated
        // grant apart).
        PhotoTrashController.onRemoteTrashConsentResult(consentRequest(uriA, remoteFileIdA, stampA), granted = false, customRealmConfig = config)
        assertEquals(PhotoPairing.State.ACTIVE.name, row(uriA).state, "the duplicate result did not re-apply")
        assertEquals(appliedAt, row(uriA).lastRemoteTrashAppliedAt, "the duplicate result did not re-stamp")

        // A re-arm, then a STALE result from the older request: dropped, the newer request
        // stays armed.
        realm().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java).equalTo(PhotoPairing::uri.name, uriA).findFirst()?.apply {
                    remoteTrashPendingSince = 5_000L
                }
            }
        }
        PhotoTrashController.onRemoteTrashConsentResult(consentRequest(uriA, remoteFileIdA, stampA), granted = false, customRealmConfig = config)
        assertEquals(5_000L, row(uriA).remoteTrashPendingSince, "the stale result did not touch the newer request")
        assertEquals(PhotoPairing.State.ACTIVE.name, row(uriA).state, "the stale denial was not recorded")

        // A MISMATCHED identity (right uri+stamp, WRONG remote file): dropped, never applied.
        PhotoTrashController.onRemoteTrashConsentResult(consentRequest(uriA, remoteFileIdB, 5_000L), granted = true, customRealmConfig = config)
        assertEquals(5_000L, row(uriA).remoteTrashPendingSince, "a wrong-pair result did not consume the request")
    }

    @Test
    @DisplayName("an account switch and an opt-out both gate the queue and its results")
    fun accountSwitchAndOptOutGateTheQueue() {
        seedArmedRow(uriA, remoteFileIdA, stampA)

        // Account switch: the queue serves only the CURRENT account's rows.
        every { AccountUtils.currentUserId } returns userId + 1
        assertNull(runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }, "another account's armed rows are not offered")
        PhotoTrashController.onRemoteTrashConsentResult(consentRequest(uriA, remoteFileIdA, stampA), granted = true, customRealmConfig = config)
        assertEquals(stampA, row(uriA).remoteTrashPendingSince, "a result from a switched account is dropped")

        // Back on the owner account, an opt-out stops the drain but keeps the row state.
        every { AccountUtils.currentUserId } returns userId
        every { UploadFile.getAppSyncSettings() } returns SyncSettings(
            userId = userId,
            driveId = driveId,
            photoTrashSyncEnabled = false,
        )
        assertNull(runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }, "an opt-out stops the queue")
        assertEquals(stampA, row(uriA).remoteTrashPendingSince, "the armed request is preserved until the option returns")
    }

    @Test
    @DisplayName("an account switched DURING the poll's remote IO ends the drain without launching")
    fun accountSwitchMidPollEndsTheDrain() {
        seedArmedRow(uriA, remoteFileIdA, stampA)

        val surfaced = runBlocking {
            parkAtRemoteQuery = true
            // An explicit IO dispatcher: a child inheriting the runBlocking event loop
            // could not even start while the parent blocked, and the flip must land
            // strictly INSIDE the poll's remote IO.
            val poll = async(Dispatchers.IO) { PhotoTrashController.pollNextPendingConsent(mockContext, config) }
            assertTrue(
                remoteQueryCapture.await(15, TimeUnit.SECONDS),
                "the poll must reach the remote-details seam before the identity flip",
            )
            every { AccountUtils.currentUserId } returns userId + 1
            remoteQueryRelease.countDown()
            poll.await()
        }

        assertNull(surfaced, "a drain that lost its account mid-flight launches nothing")
        assertEquals(stampA, row(uriA).remoteTrashPendingSince, "the request stays armed for the owner account")
        assertEquals(PhotoPairing.State.ACTIVE.name, row(uriA).state, "no state was written by the unauthorized drain")
        assertTrue(remoteQueryEntered.get() >= 1, "the poll actually reached the gated mid-IO boundary")
    }

    @Test
    @DisplayName("a binding replacement DURING the drain surfaces nothing (generation/stamp certification)")
    fun aBindingWriteDuringTheDrainSurfacesNothing() {
        seedArmedRow(uriA, remoteFileIdA, stampA)
        val pairedAtBefore = row(uriA).pairedAt

        val surfaced = runBlocking {
            parkAtRemoteQuery = true
            val poll = async(Dispatchers.IO) { PhotoTrashController.pollNextPendingConsent(mockContext, config) }
            assertTrue(
                remoteQueryCapture.await(15, TimeUnit.SECONDS),
                "the poll must reach the remote-details seam before the binding write",
            )
            // The REAL writer path: a replacement pairing for the SAME uri commits while
            // the poll is parked inside its remote IO. recordPairing bumps the binding
            // generation and the global write stamp BEFORE its transaction (pre-commit
            // invalidation), so the poll's read rows can no longer be certified against
            // the registry.
            PhotoTrashController.recordPairing(
                context = mockContext,
                uploadFile = UploadFile().apply {
                    uri = uriA
                    userId = userId
                    driveId = driveId
                    fileName = "fixture-queue.jpg"
                },
                remoteFile = File(id = remoteFileIdA, driveId = driveId).apply {
                    serverContentHash = "xxh3:0123456789abcdef"
                    lastModifiedAt = 1_700_000_000L
                    size = contentA.size.toLong()
                },
                uploadedContentDigest = null,
                customRealmConfig = config,
            )
            remoteQueryRelease.countDown()
            poll.await()
        }

        assertNull(surfaced, "a binding write during the drain uncertifies the read rows: nothing is handed out")
        assertTrue(remoteQueryEntered.get() >= 1, "the poll actually reached the gated mid-IO boundary")
        // The replacement itself landed through the real writer path (a NEW binding, a
        // strictly later pairing moment, starting unarmed); the drain surfaced nothing for
        // the OLD binding it had read.
        val replaced = row(uriA)
        assertNotNull(replaced.pairedAt, "the replacement binding was recorded")
        assertTrue(
            (replaced.pairedAt?.time ?: 0L) > (pairedAtBefore?.time ?: 0L),
            "the replacement's pairing moment is strictly past the previous binding's",
        )
        assertEquals(0L, replaced.remoteTrashPendingSince, "the replacement starts unarmed; no old request leaked through")
    }

    @Test
    @DisplayName("a binding write ALREADY in flight before the poll starts surfaces nothing")
    fun aBindingWriteAlreadyInFlightBeforeThePollSurfacesNothing() {
        seedArmedRow(uriA, remoteFileIdA, stampA)
        val generationBefore = PhotoTrashController.bindingGeneration(uriA)

        // The writer thread begins the REAL replacement: recordPairing marks itself
        // unsettled and pre-bumps the generation BEFORE its transaction, which then BLOCKS
        // on the write lock this thread holds — so the writer is in flight (bumped,
        // uncommitted) before the poll starts, the schedule the mid-drain regression could
        // not see. The holder instance is CLOSED here (use): a shared thread-local cached
        // instance left open pinned the pre-commit snapshot, so the replacement was
        // inspected through a stale read (runtime evidence: remoteTrashPendingSince was
        // 1000 after the writer had committed 0).
        val writerFailure = AtomicReference<Throwable?>(null)
        val writer = Thread {
            try {
                PhotoTrashController.recordPairing(
                    context = mockContext,
                    uploadFile = UploadFile().apply {
                        uri = uriA
                        userId = userId
                        driveId = driveId
                        fileName = "fixture-queue.jpg"
                    },
                    remoteFile = File(id = remoteFileIdA, driveId = driveId).apply {
                        serverContentHash = "xxh3:0123456789abcdef"
                        lastModifiedAt = 1_700_000_000L
                        size = contentA.size.toLong()
                    },
                    uploadedContentDigest = null,
                    customRealmConfig = config,
                )
            } catch (throwable: Throwable) {
                writerFailure.set(throwable)
            }
        }
        realm().use { holder ->
            holder.beginTransaction()
            try {
                writer.start()
                // Deterministic condition poll (no interleaving by timing): the writer's
                // pre-commit bump is the condition; only its visibility is awaited.
                val deadline = System.currentTimeMillis() + 15_000
                while (PhotoTrashController.bindingGeneration(uriA) == generationBefore && System.currentTimeMillis() < deadline) {
                    Thread.sleep(10)
                }
                assertTrue(
                    PhotoTrashController.bindingGeneration(uriA) > generationBefore,
                    "the in-flight writer pre-bumped the generation before its commit",
                )

                // The poll runs on a worker thread (this thread holds the fixture write lock):
                val surfaced = runBlocking {
                    val poll = async(Dispatchers.IO) { PhotoTrashController.pollNextPendingConsent(mockContext, config) }
                    poll.await()
                }
                assertNull(surfaced, "a drain that starts while a binding write is in flight surfaces nothing")
            } finally {
                // Releases the write lock; the replacement commits. A commit refusal must not
                // skip the close: runCatching, and the exception propagates after the close.
                runCatching { if (holder.isInTransaction) holder.commitTransaction() }
                    .onFailure { throw it }
            }
        }
        writer.join(15_000)
        assertFalse(writer.isAlive, "the replacement writer finished")
        assertNull(writerFailure.get(), "the replacement committed cleanly: ${writerFailure.get()}")
        assertTrue(PhotoTrashController.bindingGeneration(uriA) > generationBefore, "the replacement advanced the generation")
        // The holder instance is closed above: this read opens a FRESH instance with the
        // committed state, never the pinned pre-commit snapshot.
        val replaced = row(uriA)
        assertEquals(0L, replaced.remoteTrashPendingSince, "the replacement starts unarmed; no old request leaked through")
    }

    @Test
    @DisplayName("a remote restore while armed expires the request through the poll")
    fun remoteRestoreWhileArmedExpiresThroughThePoll() {
        seedArmedRow(uriA, remoteFileIdA, stampA)
        remoteTrashed.set(false)

        assertNull(runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }, "a restored remote asks for nothing")
        assertEquals(0L, row(uriA).remoteTrashPendingSince, "the stale request was cleared by the poll's own fresh observation")
    }

    @Test
    @DisplayName("a local edit while waiting is a conflict at launch time and never launches")
    fun localEditWhileWaitingNeverLaunches() {
        seedArmedRow(uriA, remoteFileIdA, stampA)
        // Same size/mtime, different bytes: every metadata check passes, only the
        // launch-time digest check can tell.
        resolverContent.set(contentB)

        assertNull(runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }, "replaced content never reaches the dialog")
        assertEquals(PhotoPairing.State.CONFLICT_LOCAL_CHANGED.name, row(uriA).state, "the replacement is a visible conflict")
        assertEquals(0L, row(uriA).remoteTrashPendingSince, "the request is consumed by the conflict")
    }

    @Test
    @DisplayName("an unbound digest while waiting is inconclusive and never launches")
    fun unboundDigestWhileWaitingNeverLaunches() {
        seedArmedRow(uriA, remoteFileIdA, stampA)
        // Chunked upload with no bound identity: the arming should never have happened, but
        // the launch-time gate is the last line regardless.
        realm().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java).equalTo(PhotoPairing::uri.name, uriA).findFirst()?.localContentDigest = null
            }
        }

        assertNull(runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }, "unknown evidence never reaches the dialog")
        assertEquals(PhotoPairing.State.INCONCLUSIVE.name, row(uriA).state)
        assertEquals(0L, row(uriA).remoteTrashPendingSince)
    }

    @Test
    @DisplayName("a replaced binding never receives an older request's result")
    fun aReplacedBindingNeverReceivesAnOlderRequestsResult() {
        seedArmedRow(uriA, remoteFileIdA, stampA)

        val surfaced = runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }
            ?: throw AssertionError("the armed request must surface")
        assertEquals(0L, surfaced.pairedAt, "the seeded binding has no pairing moment; the request carries the 0 sentinel")

        // The binding is REPLACED (a re-upload of the same uri) after the request was handed
        // out: a NEW pairing moment — the older request's answer must never apply to it.
        PhotoTrashController.recordPairing(
            context = mockContext,
            uploadFile = UploadFile(
                uri = uriA,
                userId = userId,
                driveId = driveId,
                fileName = "fixture-queue.jpg",
                fileSize = contentA.size.toLong(),
                fileModifiedAt = Date(1_700_000_000_000L),
            ),
            remoteFile = File(id = remoteFileIdA, driveId = driveId),
            uploadedContentDigest = sha256Hex(contentA),
            customRealmConfig = config,
        )
        PhotoTrashController.onRemoteTrashConsentResult(surfaced, granted = true, customRealmConfig = config)

        assertEquals(0L, row(uriA).lastRemoteTrashAppliedAt, "the older request's result never applied to the replacement binding")
    }

    @Test
    @DisplayName("an unreachable remote while polling keeps the request pending, untouched")
    fun unreachableRemoteWhilePollingKeepsTheRequestPending() {
        seedArmedRow(uriA, remoteFileIdA, stampA)
        every { ApiRepository.getFileDetailsWithHash(any(), any()) } returns ApiResponse(result = ApiResponseStatus.ERROR)

        assertNull(runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }, "an unreachable remote surfaces nothing")
        assertEquals(stampA, row(uriA).remoteTrashPendingSince, "the armed request stays pending for a later entry")
        assertEquals(PhotoPairing.State.ACTIVE.name, row(uriA).state, "a transient network failure never rewrites state")
        assertTrue(remoteQueryEntered.get() >= 1, "the poll actually reached the remote query boundary")
    }

    @Test
    @DisplayName("the REAL settings write seam publishes in commit order with the activation epoch")
    fun realSettingsWriteSeamPublishesInCommitOrder() {
        // Drives the PRODUCTION UploadFile.setAppSyncSettings against the fixture realm
        // (never the production one): the snapshot must converge to the LATEST commit at a
        // strictly growing revision, and the OFF->ON transition must advance the epoch —
        // the ordering properties the publish protocol exists for.
        val realmInstance = Realm.getInstance(config)
        val baseRevision = PhotoTrashController.currentActivationSnapshot?.revision ?: 0L
        try {
            UploadFile.setAppSyncSettings(
                SyncSettings(userId = userId, driveId = driveId, photoTrashSyncEnabled = true),
                customRealm = realmInstance,
            )
            with(PhotoTrashController.currentActivationSnapshot) {
                assertEquals(true, this?.optIn, "the enable is published")
                assertEquals(1L, this?.activationEpoch, "the first enable assigns epoch 1")
                assertEquals(userId, this?.userId, "the authority is scoped to the settings owner")
                assertTrue(this!!.revision > baseRevision)
            }

            UploadFile.setAppSyncSettings(
                SyncSettings(userId = userId, driveId = driveId, photoTrashSyncEnabled = false, photoTrashActivationEpoch = 1L),
                customRealm = realmInstance,
            )
            with(PhotoTrashController.currentActivationSnapshot) {
                assertEquals(false, this?.optIn, "the disable is published")
                assertEquals(1L, this?.activationEpoch, "a disable does not advance the epoch")
                assertTrue(this!!.revision > baseRevision + 0L, "revisions grow across writes")
            }
            val disableRevision = PhotoTrashController.currentActivationSnapshot!!.revision

            UploadFile.setAppSyncSettings(
                SyncSettings(userId = userId, driveId = driveId, photoTrashSyncEnabled = true, photoTrashActivationEpoch = 1L),
                customRealm = realmInstance,
            )
            with(PhotoTrashController.currentActivationSnapshot) {
                assertEquals(true, this?.optIn, "the re-enable is published")
                assertEquals(2L, this?.activationEpoch, "the OFF->ON transition advances the epoch")
                assertTrue(this!!.revision > disableRevision, "a later commit publishes at a higher revision")
            }
            assertEquals(
                2L,
                realmInstance.where(SyncSettings::class.java)
                    .equalTo(SyncSettings::userId.name, userId)
                    .findFirst()!!.photoTrashActivationEpoch,
                "the live row carries the transitioned epoch",
            )
        } finally {
            realmInstance.close()
            // The fixture realm rows are deleted with the fixture file in cleanup.
        }
    }

    @Test
    @DisplayName("binding generations advance with real writes and never repeat across a wipe")
    fun bindingGenerationsAdvanceAndNeverRepeatAcrossAWipe() {
        val genBefore = PhotoTrashController.bindingGeneration(uriA)
        PhotoTrashController.recordPairing(
            context = mockContext,
            uploadFile = UploadFile(
                uri = uriA,
                userId = userId,
                driveId = driveId,
                fileName = "fixture-queue.jpg",
                fileSize = contentA.size.toLong(),
                fileModifiedAt = Date(1_700_000_000_000L),
            ),
            remoteFile = File(id = remoteFileIdA, driveId = driveId),
            uploadedContentDigest = sha256Hex(contentA),
            customRealmConfig = config,
        )
        val genAfterWrite = PhotoTrashController.bindingGeneration(uriA)
        assertTrue(genAfterWrite > genBefore, "a REAL binding write (pre-commit invalidation) advanced the generation")

        // The wipe (feature off) must invalidate WITHOUT reusing any generation value: a
        // generation captured before the wipe can never recur after it (no ABA).
        val genOther = PhotoTrashController.bindingGeneration("content://fixture/never-paired")
        PhotoTrashController.removeAllPairings(customRealmConfig = config)
        val genAfterWipe = PhotoTrashController.bindingGeneration(uriA)
        assertTrue(genAfterWipe != genAfterWrite && genAfterWipe != genBefore, "the wiped world reuses no generation value")
        assertTrue(genAfterWipe != genOther, "the base invalidation moved every uri, not only the written one")

        PhotoTrashController.recordPairing(
            context = mockContext,
            uploadFile = UploadFile(
                uri = uriA,
                userId = userId,
                driveId = driveId,
                fileName = "fixture-queue.jpg",
                fileSize = contentA.size.toLong(),
                fileModifiedAt = Date(1_700_000_000_000L),
            ),
            remoteFile = File(id = remoteFileIdA, driveId = driveId),
            uploadedContentDigest = sha256Hex(contentA),
            customRealmConfig = config,
        )
        val genAfterRewrite = PhotoTrashController.bindingGeneration(uriA)
        assertTrue(genAfterRewrite > genAfterWipe, "a binding after the wipe advances strictly again")
        assertTrue(genAfterRewrite != genAfterWrite, "the post-wipe generation is not the pre-wipe value (no ABA)")
    }

    @AfterEach
    fun tearDown() {
        unmockkAll()
        // Leak report, not a silent swallow: an open instance makes deleteRealm throw, which
        // is exactly the stale-fixture corruption the per-case configs above exist to expose.
        if (this::config.isInitialized) {
            try {
                Realm.deleteRealm(config)
            } catch (leak: Throwable) {
                throw IllegalStateException("fixture realm leaked (an instance was still open): ${config.realmFileName}", leak)
            }
        }
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
