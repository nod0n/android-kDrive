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
import java.util.UUID
import java.util.concurrent.atomic.AtomicBoolean
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

    private val fixtureName = "photoTrashConsentQueue-${UUID.randomUUID()}"
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
        remoteTrashed.set(true)
        remoteHash.set(pairingHash)

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
        assertEquals(stampA, first.remoteTrashPendingSince, "the launch carries the consent stamp of that request")
    }

    @Test
    @DisplayName("two simultaneous items serialize: one dialog per completed result, oldest first")
    fun twoSimultaneousItemsSerializeOldestFirst() {
        seedArmedRow(uriA, remoteFileIdA, stampA)
        seedArmedRow(uriB, remoteFileIdB, stampB)

        assertEquals(uriA, runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }?.uri)
        PhotoTrashController.onRemoteTrashConsentResult(uriA, granted = true, expectedPendingSince = stampA, customRealmConfig = config)

        assertEquals(uriB, runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }?.uri, "the second item is only offered after the first result")
        PhotoTrashController.onRemoteTrashConsentResult(uriB, granted = true, expectedPendingSince = stampB, customRealmConfig = config)

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
        PhotoTrashController.onRemoteTrashConsentResult(uriA, granted = false, expectedPendingSince = stampA, customRealmConfig = config)
        assertEquals(PhotoPairing.State.DENIED.name, row(uriA).state, "the denial is recorded")

        val next = runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }
        assertEquals(uriB, next?.uri, "the denial does not block the second request")
        assertNotNull(next)
        PhotoTrashController.onRemoteTrashConsentResult(uriB, granted = true, expectedPendingSince = stampB, customRealmConfig = config)
        assertEquals(PhotoPairing.State.ACTIVE.name, row(uriB).state)
    }

    @Test
    @DisplayName("duplicate and stale results are dropped, never applied to a newer request")
    fun duplicateAndStaleResultsAreDropped() {
        seedArmedRow(uriA, remoteFileIdA, stampA)

        PhotoTrashController.onRemoteTrashConsentResult(uriA, granted = true, expectedPendingSince = stampA, customRealmConfig = config)
        val appliedAt = row(uriA).lastRemoteTrashAppliedAt
        assertTrue(appliedAt > 0L)

        // A DUPLICATE delivery of the same result: the stamp no longer matches (the row was
        // already consumed) — dropped, never re-applied.
        PhotoTrashController.onRemoteTrashConsentResult(uriA, granted = true, expectedPendingSince = stampA, customRealmConfig = config)
        assertEquals(appliedAt, row(uriA).lastRemoteTrashAppliedAt, "the duplicate result did not re-apply")

        // A re-arm, then a STALE result from the older request: dropped, the newer request
        // stays armed.
        realm().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java).equalTo(PhotoPairing::uri.name, uriA).findFirst()?.apply {
                    remoteTrashPendingSince = 5_000L
                }
            }
        }
        PhotoTrashController.onRemoteTrashConsentResult(uriA, granted = false, expectedPendingSince = stampA, customRealmConfig = config)
        assertEquals(5_000L, row(uriA).remoteTrashPendingSince, "the stale result did not touch the newer request")
        assertEquals(PhotoPairing.State.ACTIVE.name, row(uriA).state, "the stale denial was not recorded")
    }

    @Test
    @DisplayName("an account switch and an opt-out both gate the queue and its results")
    fun accountSwitchAndOptOutGateTheQueue() {
        seedArmedRow(uriA, remoteFileIdA, stampA)

        // Account switch: the queue serves only the CURRENT account's rows.
        every { AccountUtils.currentUserId } returns userId + 1
        assertNull(runBlocking { PhotoTrashController.pollNextPendingConsent(mockContext, config) }, "another account's armed rows are not offered")
        PhotoTrashController.onRemoteTrashConsentResult(uriA, granted = true, expectedPendingSince = stampA, customRealmConfig = config)
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
