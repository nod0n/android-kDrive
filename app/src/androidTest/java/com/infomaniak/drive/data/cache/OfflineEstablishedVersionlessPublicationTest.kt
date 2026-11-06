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
import androidx.test.core.app.ApplicationProvider
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.models.file.FileVersion
import com.infomaniak.drive.utils.DownloadOfflineFileManager
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.publicationGenerationUnresolved
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.readPublishedProvenance
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.writePublishedProvenance
import com.infomaniak.drive.utils.GenerationEstablishment
import com.infomaniak.drive.utils.IOFile
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.util.UUID

/**
 * THE ESTABLISHED-VERSIONLESS PUBLICATION REGRESSION (offline-established-versionless-
 * publication-fix-20261106.txt; the device diagnostic's finding): the publication's
 * both-versions-unknown arm downgraded an ESTABLISHED, hash-verified generation to
 * PUBLISHED_UNRESOLVED on the version-less rows (the app's own uploaded fixtures) — the
 * pending marker stayed, the batch failed, the bodies re-downloaded forever, and the
 * attempt observation never existed for the downgraded outcome so the no-spin bound could
 * not engage. TWO production boundaries are driven, never a mirrored predicate:
 *  - [DownloadOfflineFileManager.publicationGenerationUnresolved] — THE production decision
 *    the publication core invokes (the finite test seam the fix extracted); the device
 *    shape (Established + both versions unknown) must resolve, and every preserved arm
 *    (the coarse tier, the explicit flag, the versioned rows) keeps its own case;
 *  - [DownloadOfflineFileManager.writePublishedProvenance] — THE production v3 record
 *    writer: the fix relies on its contract — an Established version-less record KEEPS the
 *    exact verified server hash, carries the real local receipt, and NEVER invents a
 *    version number (0 stays 0); the coarse record force-zeroes the version and DROPS the
 *    server hash; a versioned record carries the real version.
 * No publication-core harness (the persisted app-context blocker stands); no gate, ladder,
 * marker or retry semantics exercised beyond these two boundaries. No Realm is opened (the
 * row models are unmanaged; the writer is file I/O over the sandbox registry). The coder
 * executes nothing — the orchestrator runs this on a device.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OfflineEstablishedVersionlessPublicationTest {

    companion object {
        private val baseContext: Context = ApplicationProvider.getApplicationContext()

        // THE RANDOMIZED SYNTHETIC SCOPE (per class load; a fresh band — the other
        // fixtures' scopes use the 9_1/9_2/9_3/9_5/9_8xx ranges). This fixture writes NO
        // Realm rows and NO shared registry: the provenance registry is rooted under THIS
        // sandbox's external-media dir (provenanceRoot derives from File.getOfflineFolder),
        // so the band is identity bookkeeping only.
        private val scopeBase =
            9_900_000 + ((UUID.randomUUID().leastSignificantBits % 50_000L).toInt() * 8)
        private val USER_ID = scopeBase + 1
        private val DRIVE_ID = scopeBase + 2
        private val userDrive = UserDrive(userId = USER_ID, driveId = DRIVE_ID)
        private val sandboxRoot = IOFile(baseContext.cacheDir, "offline-sandbox-${UUID.randomUUID()}")
        private val sandboxOffline = IOFile(sandboxRoot, "offline")
        private val sandboxContext: Context = object : ContextWrapper(baseContext) {
            override fun getExternalMediaDirs(): Array<out java.io.File> = arrayOf(sandboxOffline)
        }

        private val methodCounter = java.util.concurrent.atomic.AtomicInteger(0)

        /** This method's own byte file at a unique sandbox path (the registry key). */
        private fun seededTarget(payload: String): IOFile {
            val target = IOFile(
                sandboxOffline,
                "m${methodCounter.incrementAndGet()}-${UUID.randomUUID()}.bin",
            )
            target.parentFile?.mkdirs()
            target.writeText(payload)
            return target
        }

        /** This method's own synthetic row model (the record's identity fields). */
        private fun rowModel(target: IOFile, version: FileVersion?): File = File().apply {
            uid = "${target.name}_row"
            id = scopeBase + 100 + methodCounter.get()
            parentId = scopeBase + 3
            driveId = DRIVE_ID
            name = target.name
            path = "/fixture/${target.nameWithoutExtension}/"
            this.version = version
        }
    }

    // ===================================================================
    // THE SEAM — the production unresolved decision, exact device shape first
    // ===================================================================

    @Test
    @DisplayName("an Established hash-verified generation resolves despite both versions unknown")
    fun establishedGenerationResolvesDespiteUnknownVersions() {
        // THE EXACT DEVICE SHAPE (the loop's cause): the ladder verified the bytes, the
        // live row and the captured basis carry no version number — the outcome must be
        // RESOLVED (the marker clears, the batch succeeds, the loop ends).
        val established = GenerationEstablishment.Established(
            fresh = File(),
            serverHash = "xxh3:0123456789abcdef",
            localSha256 = "aa".repeat(32),
        )
        assertFalse(
            publicationGenerationUnresolved(
                explicitUnresolvedGeneration = false,
                establishment = established,
                liveRowExists = true,
                liveVersionNumber = null,
                casBasisVersionNumber = null,
            ),
            "a missing optional version number alone must never downgrade an Established exact-content publication",
        )
    }

    @Test
    @DisplayName("the coarse tier stays unresolved — coarse metadata alone never verifies")
    fun coarseTierStaysUnresolved() {
        // NO establishment at all (the promotions' shape): the arm fires in full.
        assertTrue(
            publicationGenerationUnresolved(
                explicitUnresolvedGeneration = false,
                establishment = null,
                liveRowExists = true,
                liveVersionNumber = null,
                casBasisVersionNumber = null,
            ),
            "a both-versions-unknown tie without any content proof stays unresolved",
        )
        // A COARSE establishment (metadata equality only): unresolved — the original
        // established==null semantics preserved (CoarseUnresolved included).
        assertTrue(
            publicationGenerationUnresolved(
                explicitUnresolvedGeneration = false,
                establishment = GenerationEstablishment.CoarseUnresolved(seenServerHash = null),
                liveRowExists = true,
                liveVersionNumber = null,
                casBasisVersionNumber = null,
            ),
            "coarse metadata equality alone never verifies a generation",
        )
    }

    @Test
    @DisplayName("the explicit unresolved flag decides regardless of the establishment")
    fun explicitFlagDecidesRegardless() {
        assertTrue(
            publicationGenerationUnresolved(
                explicitUnresolvedGeneration = true,
                establishment = GenerationEstablishment.Established(
                    fresh = File(),
                    serverHash = "xxh3:0123456789abcdef",
                    localSha256 = "bb".repeat(32),
                ),
                liveRowExists = true,
                liveVersionNumber = 7,
                casBasisVersionNumber = 7,
            ),
            "an explicitly unresolved publication stays unresolved in every shape",
        )
    }

    @Test
    @DisplayName("versioned rows resolve exactly as before the fix")
    fun versionedRowsResolveAsBefore() {
        assertFalse(
            publicationGenerationUnresolved(
                explicitUnresolvedGeneration = false,
                establishment = null,
                liveRowExists = true,
                liveVersionNumber = 3,
                casBasisVersionNumber = null,
            ),
            "a known live version already resolved the generation before the fix — unchanged",
        )
        assertFalse(
            publicationGenerationUnresolved(
                explicitUnresolvedGeneration = false,
                establishment = GenerationEstablishment.Established(
                    fresh = File(),
                    serverHash = "xxh3:0123456789abcdef",
                    localSha256 = "cc".repeat(32),
                ),
                liveRowExists = true,
                liveVersionNumber = 3,
                casBasisVersionNumber = 3,
            ),
            "a versioned Established publication was never unresolved — unchanged",
        )
    }

    // ===================================================================
    // THE V3 RECORD WRITER — the contract the fix relies on
    // ===================================================================

    @Test
    @DisplayName("an Established version-less record keeps the verified hash and never invents a version")
    fun establishedVersionlessRecordKeepsHashAndInventsNoVersion() {
        val target = seededTarget("fixture established versionless record payload")
        val localReceipt = UploadFile.sha256OrNull(target)!!
        val wrote = writePublishedProvenance(
            context = sandboxContext,
            target = target,
            file = rowModel(target, version = null),
            userDrive = userDrive,
            unresolvedRecord = false,
            // A synthetic canonical-shaped value STANDING IN for the establishment route's
            // verified server hash — the assertion is the writer's pass-through contract,
            // never a content claim.
            serverHash = "xxh3:0123456789abcdef",
            contentHashOverride = localReceipt,
        )
        assertTrue(wrote, "the Established version-less record must write")
        val record = readPublishedProvenance(sandboxContext, target)
        assertNotNull(record)
        assertEquals(
            0,
            record!!.version,
            "the record keeps the fresh row's own version — 0 when unknown, NEVER invented",
        )
        assertEquals(
            "xxh3:0123456789abcdef",
            record.serverHash,
            "the verified server hash is the v3 frame's identity — kept, never dropped",
        )
        assertEquals(localReceipt, record.contentHash, "the record carries the real local receipt")
        assertTrue(
            record.certifiesBytes(target),
            "the hash-certified record certifies the published bytes (the destructive-decision authority)",
        )
    }

    @Test
    @DisplayName("a coarse record force-zeroes the version and drops the server hash")
    fun coarseRecordForcesZeroAndDropsServerHash() {
        val target = seededTarget("fixture coarse unresolved record payload")
        val wrote = writePublishedProvenance(
            context = sandboxContext,
            target = target,
            file = rowModel(target, version = FileVersion(number = 4)),
            userDrive = userDrive,
            unresolvedRecord = true,
            serverHash = "xxh3:fedcba9876543210",
            contentHashOverride = null,
        )
        assertTrue(wrote, "the coarse record must write")
        val record = readPublishedProvenance(sandboxContext, target)
        assertNotNull(record)
        assertEquals(0, record!!.version, "the unresolved record's version is BY DESIGN zero")
        assertNull(
            record.serverHash,
            "an unresolved record drops the server hash — coarse metadata alone never claims verified content",
        )
        assertEquals(
            UploadFile.sha256OrNull(target),
            record.contentHash,
            "the computed local receipt still identifies the bytes",
        )
    }

    @Test
    @DisplayName("a versioned Established record carries the real version")
    fun versionedEstablishedRecordCarriesRealVersion() {
        val target = seededTarget("fixture versioned established record payload")
        val wrote = writePublishedProvenance(
            context = sandboxContext,
            target = target,
            file = rowModel(target, version = FileVersion(number = 5)),
            userDrive = userDrive,
            unresolvedRecord = false,
            serverHash = "xxh3:0123456789abcdef",
            contentHashOverride = UploadFile.sha256OrNull(target),
        )
        assertTrue(wrote, "the versioned Established record must write")
        val record = readPublishedProvenance(sandboxContext, target)
        assertNotNull(record)
        assertEquals(
            5,
            record!!.version,
            "the record carries the fresh row's real version — never zeroed by the establishment path",
        )
    }

    @AfterAll
    fun cleanup() {
        // ONLY this fixture's own state: the sandbox tree (the byte files AND the
        // sandbox-rooted provenance registry). Nothing else is touched.
        runCatching { sandboxRoot.deleteRecursively() }
    }
}
