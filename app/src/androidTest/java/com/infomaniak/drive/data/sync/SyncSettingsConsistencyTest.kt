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
import com.infomaniak.drive.data.models.PhotoPairing
import com.infomaniak.drive.data.models.SyncSettings
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.RealmModules
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.spyk
import io.mockk.unmockkAll
import io.realm.Realm
import io.realm.RealmConfiguration
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.Date
import java.util.UUID

/**
 * The settings-store consistency seams behind the confirmed P2 (FHNW-ce3a-N2-RESULT
 * C.2/C.3; the one confirmed P2 of FHNW-WHOLE-REVIEW-ce3a): the upload worker's per-media
 * write of a WHOLE detached settings copy resurrected deleted settings, re-enabled the
 * option from a stale snapshot and fabricated a fresh activation generation that
 * permanently blocked every prior-generation pairing; the settings UI's activation capture
 * derived its epoch from the stale UI snapshot instead of the live row.
 *
 * These cases exercise the REAL production seams — [UploadFile.advanceSyncLastSync] (the
 * scan-bookkeeping seam that replaced the worker's full detached write),
 * [UploadFile.nextActivationEpoch] (the derivation shared by the write seam and the
 * settings UI's activation capture), [UploadFile.setAppSyncSettings],
 * [UploadFile.removeAppSyncSettings] and
 * [PhotoTrashController.captureActivationBaselines] — against disposable per-case fixture
 * realms (the established harness: PhotoTrashActivationSeamTest /
 * PhotoTrashConsentQueueTest). The settings UI's call ORDER (derive from the live read,
 * stamp the saved object, capture, save) is source-traced; the derivation, capture and
 * save seams themselves run REAL here. Compilation implies no device run; every case is
 * offline and deterministic.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SyncSettingsConsistencyTest {

    private val context = ApplicationProvider.getApplicationContext<android.content.Context>()
    private val userId = 961
    private val driveId = 761
    private val syncFolder = 555

    private lateinit var config: RealmConfiguration

    @BeforeAll
    fun initRealm() {
        Realm.init(context)
    }

    @BeforeEach
    fun setUp() {
        config = RealmConfiguration.Builder()
            .schemaVersion(UploadMigration.DB_VERSION)
            .modules(RealmModules.SyncFilesModule())
            .name("syncSettingsConsistency-${UUID.randomUUID()}")
            .directory(context.cacheDir)
            .build()
    }

    @AfterEach
    fun tearDown() {
        unmockkAll()
        // Leak report, not a silent swallow: deleteRealm throws while an instance is open.
        try {
            Realm.deleteRealm(config)
        } catch (leak: Throwable) {
            throw IllegalStateException("fixture realm leaked (an instance was still open): ${config.realmFileName}", leak)
        }
    }

    /** The settings UI's mocked MediaStore observation: one readable, not-trashed item. */
    private fun routeMediaObservation(): android.content.Context {
        val mockResolver = mockk<android.content.ContentResolver>()
        every { mockResolver.query(any(), any(), any(), any(), any()) } answers {
            mockk(relaxed = true) {
                every { moveToFirst() } returns true
                every { getLong(0) } returns 10L
                every { getLong(1) } returns 1_700_000_000L // seconds; the reader multiplies by 1000
                every { getInt(2) } returns 0
            }
        }
        val mockContext = spyk(context)
        every { mockContext.contentResolver } returns mockResolver
        return mockContext
    }

    /**
     * Routes [UploadFile.removeAppSyncSettings]' default-realm acquisition to THIS case's
     * fixture realm (the seam itself — transaction, activation-write mark, fail-closed
     * mirror publication — runs REAL; [AccountUtils.currentUserId] is fixture-owned for
     * the publication and [getPairings]' user filter). Every other member stays real.
     */
    private fun routeDefaultRealmToFixture() {
        mockkObject(UploadFile.Companion)
        every { UploadFile.getRealmInstance() } answers { Realm.getInstance(config) }
        mockkObject(AccountUtils)
        every { AccountUtils.currentUserId } returns userId
    }

    private fun realm(): Realm = Realm.getInstance(config)

    private fun liveSettings(): SyncSettings? = realm().use { realm ->
        realm.where(SyncSettings::class.java)
            .equalTo(SyncSettings::userId.name, userId)
            .findFirst()
            ?.let { realm.copyFromRealm(it, 0) }
    }

    private fun pairing(uri: String): PhotoPairing? = realm().use { realm ->
        realm.where(PhotoPairing::class.java)
            .equalTo(PhotoPairing::uri.name, uri)
            .findFirst()
            ?.let { realm.copyFromRealm(it, 0) }
    }

    /** Direct durable seed of an exact live-row state (the established queue-test pattern). */
    private fun seedLiveSettings(settings: SyncSettings) {
        realm().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.copyToRealmOrUpdate(settings)
            }
        }
    }

    private fun seedPairing(uri: String, epoch: Long, stamp: Long = 1_000L) {
        realm().use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.createObject(PhotoPairing::class.java, uri)?.apply {
                    this.userId = this@SyncSettingsConsistencyTest.userId
                    driveId = this@SyncSettingsConsistencyTest.driveId
                    remoteFileId = 900_001
                    fileName = "fixture-consistency.jpg"
                    activationEpoch = epoch
                    localSizeAtPairing = 10L
                    localModifiedAtPairing = 1_700_000_000_000L
                    baselineTrashed = false
                    lastObservedTrashed = false
                    state = PhotoPairing.State.ACTIVE.name
                    remoteTrashPendingSince = stamp
                }
            }
        }
    }

    /** Runs [block] with one explicitly managed fixture realm (the seam does not close a customRealm). */
    private fun withFixtureRealm(block: (Realm) -> Unit) {
        realm().use(block)
    }

    @Test
    @DisplayName("worker bookkeeping on an absent row never resurrects settings")
    fun workerBookkeepingOnAnAbsentRowNeverResurrectsSettings() {
        // The scan's detached copy, read once before the cursor loop (enabled, epoch 2).
        seedLiveSettings(
            SyncSettings(
                userId = userId,
                driveId = driveId,
                syncFolder = syncFolder,
                lastSync = Date(1_000L),
                photoTrashSyncEnabled = true,
                photoTrashActivationEpoch = 2L,
            ),
        )

        routeDefaultRealmToFixture()
        // The durable state a concurrent disableAutoSync produces: the settings ROW is
        // deleted (the REAL deletion seam — its transaction and its fail-closed mirror
        // publication run).
        UploadFile.removeAppSyncSettings()
        assertNull(liveSettings(), "the deletion landed")

        // The worker's per-media bookkeeping against the deleted row: no resurrection.
        withFixtureRealm { fixtureRealm ->
            UploadFile.advanceSyncLastSync(
                userId = userId,
                driveId = driveId,
                syncFolder = syncFolder,
                scannedLastSync = Date(5_000L),
                customRealm = fixtureRealm,
            )
        }

        assertNull(
            liveSettings(),
            "the scan bookkeeping must not recreate settings the user's disable deleted",
        )
        // Durable absence stays the authoritative fail-closed evidence: the option reads
        // disabled for this user.
        assertFalse(UploadFile.getAppSyncSettings(config)?.photoTrashSyncEnabled ?: false)
    }

    @Test
    @DisplayName("an obsolete scan scope never advances a replacement configuration's watermark; same scope advances monotonically past a concurrent edit")
    fun scanWatermarkIsScopedAndMonotonic() {
        // The live row was EDITED while the scan held an older copy: new drive/folder, the
        // option re-affirmed, a new epoch, a watermark of its own, other settings flipped.
        seedLiveSettings(
            SyncSettings(
                userId = userId,
                driveId = driveId + 1,
                syncFolder = syncFolder + 1,
                lastSync = Date(9_000L),
                syncImmediately = true,
                syncVideo = true,
                deleteAfterSync = true,
                onlyWifiSyncMedia = true,
                photoTrashSyncEnabled = true,
                photoTrashActivationEpoch = 3L,
            ),
        )

        // The obsolete scan's scope (pre-edit drive/folder): must not move the replacement
        // configuration's watermark — media found under the old scope was never scanned for
        // the new configuration, and advancing the watermark would hide uploads from it.
        withFixtureRealm { fixtureRealm ->
            UploadFile.advanceSyncLastSync(
                userId = userId,
                driveId = driveId,
                syncFolder = syncFolder,
                scannedLastSync = Date(5_000L),
                customRealm = fixtureRealm,
            )
        }
        liveSettings()!!.let { live ->
            assertEquals(9_000L, live.lastSync.time, "an obsolete scope never advances the replacement watermark")
            assertEquals(driveId + 1, live.driveId, "the edited drive survived the scan write")
            assertEquals(syncFolder + 1, live.syncFolder, "the edited folder survived the scan write")
            assertEquals(3L, live.photoTrashActivationEpoch, "the edited epoch survived the scan write")
            assertTrue(live.photoTrashSyncEnabled, "the opt-in survived the scan write")
            assertTrue(
                live.syncVideo && live.deleteAfterSync && live.onlyWifiSyncMedia && live.syncImmediately,
                "the concurrent settings edit survived the scan write",
            )
        }

        // Same scope: the bookkeeping advances ONLY the watermark, past the concurrent edit.
        withFixtureRealm { fixtureRealm ->
            UploadFile.advanceSyncLastSync(
                userId = userId,
                driveId = driveId + 1,
                syncFolder = syncFolder + 1,
                scannedLastSync = Date(12_000L),
                customRealm = fixtureRealm,
            )
        }
        liveSettings()!!.let { live ->
            assertEquals(12_000L, live.lastSync.time, "same-scope bookkeeping advances the watermark monotonically")
            assertEquals(driveId + 1, live.driveId)
            assertEquals(syncFolder + 1, live.syncFolder)
            assertEquals(3L, live.photoTrashActivationEpoch, "the scan bookkeeping never touches the activation epoch")
            assertTrue(live.photoTrashSyncEnabled)
        }

        // Monotonicity: an OLDER candidate never rewinds the watermark.
        withFixtureRealm { fixtureRealm ->
            UploadFile.advanceSyncLastSync(
                userId = userId,
                driveId = driveId + 1,
                syncFolder = syncFolder + 1,
                scannedLastSync = Date(4_000L),
                customRealm = fixtureRealm,
            )
        }
        assertEquals(12_000L, liveSettings()!!.lastSync.time, "the watermark never rewinds")
    }

    @Test
    @DisplayName("capture/save deletion seam: a save landing after the row was deleted adopts the CAPTURED generation, not a fabricated one")
    fun saveAfterDeletionLandsTheCapturedGeneration() {
        // Live row DISABLED at generation 2, one pairing stamped 2 (the pre-activation state).
        seedLiveSettings(
            SyncSettings(
                userId = userId,
                driveId = driveId,
                syncFolder = syncFolder,
                lastSync = Date(1_000L),
                photoTrashSyncEnabled = false,
                photoTrashActivationEpoch = 2L,
            ),
        )
        seedPairing("content://fixture/consistency-deletion", epoch = 2L)

        val fixtureContext = routeMediaObservation()
        routeDefaultRealmToFixture()

        // The settings UI's activation save schedule (source-traced order): derive from the
        // LIVE row through the shared derivation, stamp the saved object, capture, save.
        val liveAtCapture = UploadFile.getAppSyncSettings(config)
        val activationEpoch = UploadFile.nextActivationEpoch(
            liveAtCapture,
            inputEpoch = 0L,
            inputEnabled = true,
        )
        val savedObject = SyncSettings(
            userId = userId,
            driveId = driveId,
            syncFolder = syncFolder,
            lastSync = Date(2_000L),
            photoTrashSyncEnabled = true,
        ).apply { photoTrashActivationEpoch = activationEpoch }
        PhotoTrashController.captureActivationBaselines(fixtureContext, activationEpoch, config)
        assertEquals(3L, activationEpoch, "a live disabled row at 2 derives the activation increment 3")
        assertEquals(
            3L,
            pairing("content://fixture/consistency-deletion")!!.activationEpoch,
            "the capture stamped the pairings with the derived generation",
        )

        // THE SEAM UNDER TEST: the settings row is DELETED between the capture and the save.
        UploadFile.removeAppSyncSettings()
        assertNull(liveSettings(), "the deletion landed between capture and save")

        withFixtureRealm { fixtureRealm ->
            UploadFile.setAppSyncSettings(savedObject, fixtureRealm)
        }

        val finalSettings = liveSettings()!!
        assertEquals(3L, finalSettings.photoTrashActivationEpoch, "the save lands the CAPTURED generation, not a fabricated fresh one")
        assertTrue(finalSettings.photoTrashSyncEnabled, "the user's opt-in intent is preserved")
        assertEquals(
            3L,
            pairing("content://fixture/consistency-deletion")!!.activationEpoch,
            "pairing and settings share one generation: the launch gate's epoch comparison can pass for fresh requests",
        )
    }

    @Test
    @DisplayName("a stale-UI save over an already-enabled live row re-baselines the CURRENT generation; a pre-save request's epoch stays invalid")
    fun staleUiSaveOverALiveEnabledRowRebaselinesTheCurrentGeneration() {
        // The live row was re-enabled OUTSIDE this UI's snapshot (the fabricated-revival
        // shape): enabled at generation 1, while the pairing still carries generation 2.
        seedLiveSettings(
            SyncSettings(
                userId = userId,
                driveId = driveId,
                syncFolder = syncFolder,
                lastSync = Date(1_000L),
                photoTrashSyncEnabled = true,
                photoTrashActivationEpoch = 1L,
            ),
        )
        seedPairing("content://fixture/consistency-reaffirm", epoch = 2L)

        val fixtureContext = routeMediaObservation()
        mockkObject(AccountUtils)
        every { AccountUtils.currentUserId } returns userId

        // The stale UI (old snapshot disabled) saves enabled: the capture derives from the
        // LIVE row — an already-enabled row is a RE-AFFIRMATION of the current generation.
        val liveAtCapture = UploadFile.getAppSyncSettings(config)
        val activationEpoch = UploadFile.nextActivationEpoch(
            liveAtCapture,
            inputEpoch = 0L,
            inputEnabled = true,
        )
        val savedObject = SyncSettings(
            userId = userId,
            driveId = driveId,
            syncFolder = syncFolder,
            lastSync = Date(2_000L),
            photoTrashSyncEnabled = true,
        ).apply { photoTrashActivationEpoch = activationEpoch }
        PhotoTrashController.captureActivationBaselines(fixtureContext, activationEpoch, config)
        assertEquals(1L, activationEpoch, "a live ENABLED row derives NO increment: the current generation stands")
        assertEquals(
            1L,
            pairing("content://fixture/consistency-reaffirm")!!.activationEpoch,
            "the re-affirmation re-baselines the pairing under the CURRENT generation",
        )
        // A request issued BEFORE the save carries the pairing's pre-save generation.
        val staleRequestEpoch = 2L

        withFixtureRealm { fixtureRealm ->
            UploadFile.setAppSyncSettings(savedObject, fixtureRealm)
        }

        val finalSettings = liveSettings()!!
        assertEquals(1L, finalSettings.photoTrashActivationEpoch, "the save keeps the live generation (no fabricated bump)")
        assertTrue(finalSettings.photoTrashSyncEnabled)
        assertEquals(1L, pairing("content://fixture/consistency-reaffirm")!!.activationEpoch)
        assertTrue(
            staleRequestEpoch != finalSettings.photoTrashActivationEpoch,
            "the pre-save request's generation no longer matches the persisted settings: the launch gate refuses it",
        )
    }
}
