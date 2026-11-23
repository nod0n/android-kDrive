/* Copyright (C) 2026 Infomaniak Network SA. SPDX-License-Identifier: GPL-3.0-or-later */
package com.infomaniak.drive.utils

import android.content.Context
import android.os.Build
import androidx.room.RoomDatabase
import com.infomaniak.core.auth.room.UserDatabase
import com.infomaniak.core.network.models.ApiResponse
import com.infomaniak.core.network.models.ApiResponseStatus
import com.infomaniak.core.network.models.ApiError
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.R
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.CursorApiResponse
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.cache.DriveInfosController
import com.infomaniak.drive.data.models.AppSettings
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.OfflineFolderRoot
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.models.drive.Drive
import io.mockk.*
import io.realm.*
import io.realm.internal.RealmCore
import kotlinx.coroutines.*
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Assertions.*
import splitties.init.appCtx
import java.nio.file.Files
import java.util.Date
import sun.misc.Unsafe

/** Actual unmark/mark/maintenance/walk and coordinated FileController writers, real files/digests.
 * Only Android/account metadata, HTTP replies and Realm IO are substituted. No real Realm-engine claim.
 * Eager synthetic account singletons require the existing F01 single-class worker isolation. */
@Tag("jvm-isolated")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class OfflineRootRacesTest {
    private val dir = Files.createTempDirectory("offline-root-races").toFile()
    private val context = mockk<Context>()
    private val realm = mockk<Realm>(relaxed = true)
    private val scope = UserDrive(7, 42)
    private val files = linkedMapOf<Int, File>()
    private val roots = linkedMapOf<String, OfflineFolderRoot>()
    private var listings: (Int, String?) -> CursorApiResponse<List<File>> = { _, _ -> page(emptyList()) }
    private var details: (File) -> ApiResponse<File> = { ApiResponse(result = ApiResponseStatus.ERROR, error = ApiError(code = "object_not_found")) }
    private var duringDigest: () -> Unit = {}
    private var digests = 0
    private lateinit var database: UserDatabase
    private lateinit var bytes: java.io.File
    private val realmContext = Class.forName("io.realm.BaseRealm").getDeclaredField("applicationContext").apply { isAccessible = true }
    private var oldContext: Any? = null
    private val manufacturer = Build::class.java.getDeclaredField("MANUFACTURER").apply { isAccessible = true }
    private var oldManufacturer: Any? = null
    private val unsafe = Unsafe::class.java.getDeclaredField("theUnsafe").apply { isAccessible = true }.get(null) as Unsafe

    @BeforeAll fun bootstrap() {
        every { context.filesDir } returns dir.resolve("files").apply { mkdirs() }
        every { context.cacheDir } returns dir.resolve("cache").apply { mkdirs() }
        every { context.externalMediaDirs } returns arrayOf(dir.resolve("media"))
        every { context.applicationContext } returns context
        every { context.packageName } returns "synthetic.root.races"
        every { context.getSharedPreferences(any(), any()) } returns mockk(relaxed = true)
        every { context.getString(R.string.EXPOSED_OFFLINE_DIR) } returns "offline_storage"
        mockkStatic("splitties.init.AppCtxKt")
        every { appCtx } returns context
        oldManufacturer = manufacturer.get(null)
        unsafe.putObjectVolatile(unsafe.staticFieldBase(manufacturer), unsafe.staticFieldOffset(manufacturer), "synthetic-root")
        mockkStatic(android.util.Log::class, android.provider.Settings.Secure::class)
        every { android.util.Log.i(any(), any(), any()) } returns 0
        every { android.util.Log.d(any(), any(), any()) } returns 0
        every { android.util.Log.e(any(), any(), any()) } returns 0
        every { context.contentResolver } returns mockk(relaxed = true)
        every { android.provider.Settings.Secure.getString(any(), any()) } returns "synthetic-device"
        oldContext = realmContext.get(null); realmContext.set(null, context)
        // Same reversible real-getter/bootstrap boundary as the accepted O08 host fixture.
        assertSame(context, Realm.getApplicationContext())
        val configuration = mockk<RealmConfiguration>()
        mockkStatic(Realm::class, RealmCore::class)
        every { Realm.getInstance(any<RealmConfiguration>()) } returns realm
        every { RealmCore.loadLibrary(any()) } just Runs
        mockkConstructor(RealmConfiguration.Builder::class)
        every { anyConstructed<RealmConfiguration.Builder>().build() } returns configuration
        val settings = mockk<RealmQuery<RealmModel>>(relaxed = true)
        every { realm.where(any<Class<RealmModel>>()) } returns settings
        every { settings.findFirst() } returns null
        Class.forName(AppSettings::class.java.name, true, AppSettings::class.java.classLoader)
        mockkObject(AppSettings.Companion)
        every { AppSettings.getAppSettings() } returns AppSettings(_currentUserId = 7, _currentDriveId = 42)
        mockkConstructor(RoomDatabase.Builder::class)
        every { anyConstructed<RoomDatabase.Builder<RoomDatabase>>().build() } answers {
            if (!::database.isInitialized) database = mockk(relaxed = true)
            database
        }
        mockkObject(AccountUtils)
        assertTrue(::database.isInitialized, "isolated Core database must be synthetic")
        every { AccountUtils.currentUserId } returns 7
        every { AccountUtils.currentDriveId } returns 42
        coEvery { AccountUtils.getHttpClient(any(), any()) } returns mockk(relaxed = true)
        mockkObject(FileController, DriveInfosController, ApiRepository, Utils, SentryLog)
        every { FileController.getRealmInstance(any()) } answers {
            val requested = firstArg<UserDrive?>()
            assertTrue(requested == null || (requested.userId == 7 && requested.driveId == 42))
            realm
        }
        every { DriveInfosController.getDrives(7) } returns arrayListOf(Drive(id = 42, userId = 7))
        every { Utils.enqueueBulkDownloadWorker(any(), any(), any()) } just Runs
        every { SentryLog.i(any(), any(), any()) } just Runs
        every { SentryLog.w(any(), any(), any()) } just Runs
        every { SentryLog.d(any(), any(), any()) } just Runs
        every { realm.where(File::class.java) } answers { query({ files.values.toList() }, { files.remove(it.id) }) }
        every { realm.where(OfflineFolderRoot::class.java) } answers { query({ roots.values.toList() }, { roots.remove(it.uid) }) }
        every { realm.executeTransaction(any()) } answers { firstArg<Realm.Transaction>().execute(realm) }
        every { realm.copyFromRealm(any<OfflineFolderRoot>(), any<Int>()) } answers {
            firstArg<OfflineFolderRoot>().let { OfflineFolderRoot(it.uid, it.folderId, it.driveId, it.userId, Date(it.markedAt.time)) }
        }
        every { realm.copyFromRealm(any<File>(), any<Int>()) } answers { firstArg() }
        every { realm.insertOrUpdate(any<RealmModel>()) } answers {
            when (val row = firstArg<RealmModel>()) {
                is File -> files[row.id] = row
                is OfflineFolderRoot -> roots[row.uid] = row
                else -> error("Unexpected host storage type")
            }
        }
        coEvery { ApiRepository.getFolderFiles(any(), 42, any(), any(), any()) } answers { listings(thirdArg(), arg(3)) }
        every { ApiRepository.getFileDetails(any(), any()) } answers { details(firstArg()) }
        mockkObject(OfflineFolderSyncUtils, recordPrivateCalls = true)
        every { OfflineFolderSyncUtils["computeDigest"](any<java.io.File>()) } answers {
            digests++; val result = callOriginal(); duringDigest(); result
        }
        println("ASTRA_BOOTSTRAP isolatedWorker=${System.getProperty("org.gradle.test.worker")} pid=${ProcessHandle.current().pid()}")
    }

    /** A small Realm IO double: comparisons/grouped OR/results/delete only; no root policy lives here. */
    private fun <T : RealmModel> query(rows: () -> List<T>, remove: (T) -> Unit): RealmQuery<T> {
        val q = mockk<RealmQuery<T>>()
        val terms = mutableListOf(mutableListOf<(T) -> Boolean>())
        fun value(row: T, name: String): Any? {
            val getter = if (name.startsWith("is")) name else "get" + name.replaceFirstChar(Char::uppercaseChar)
            return row.javaClass.getMethod(getter).invoke(row)
        }
        fun equal(name: String, expected: Any?, negated: Boolean = false) {
            terms.last().add { (value(it, name) == expected) != negated }
        }
        every { q.equalTo(any<String>(), any<Int>()) } answers { equal(firstArg(), secondArg()); q }
        every { q.equalTo(any<String>(), any<Long>()) } answers { equal(firstArg(), secondArg()); q }
        every { q.equalTo(any<String>(), any<Boolean>()) } answers { equal(firstArg(), secondArg()); q }
        every { q.equalTo(any<String>(), any<String>()) } answers { equal(firstArg(), secondArg()); q }
        every { q.notEqualTo(any<String>(), any<String>()) } answers { equal(firstArg(), secondArg(), true); q }
        every { q.beginGroup() } returns q; every { q.endGroup() } returns q
        every { q.or() } answers { terms.add(mutableListOf()); q }
        fun selected() = rows().filter { row -> terms.any { and -> and.all { it(row) } } }
        every { q.findFirst() } answers { selected().firstOrNull() }
        every { q.count() } answers { selected().size.toLong() }
        every { q.findAll() } answers {
            val result = mockk<RealmResults<T>>()
            every { result.iterator() } answers { selected().toMutableList().iterator() }
            every { result.size } answers { selected().size }
            every { result.deleteAllFromRealm() } answers { selected().forEach(remove); true }
            result
        }
        return q
    }
    private fun page(rows: List<File>, more: Boolean = false, cursor: String? = null) =
        CursorApiResponse(result = ApiResponseStatus.SUCCESS, data = rows, hasMore = more, cursor = cursor)
    private fun folder(id: Int, parent: Int = 0) = File(id = id, driveId = 42, name = "folder$id", type = "dir", parentId = parent, path = "/folder$id").apply { initUid() }
    private fun register(id: Int, at: Long = 1000) = OfflineFolderRoot(folderId = id, driveId = 42, userId = 7, markedAt = Date(at)).apply { initUid(); roots[uid] = this }
    private fun child() = files.getValue(83)
    @BeforeEach fun seed() {
        files.clear(); roots.clear(); digests = 0; duringDigest = {}
        listings = { _, _ -> page(emptyList()) }
        details = { ApiResponse(result = ApiResponseStatus.ERROR, error = ApiError(code = "object_not_found")) }
        files[10] = folder(10); files[20] = folder(20); files[15] = folder(15, 10)
        files[83] = File(id = 83, driveId = 42, parentId = 15, name = "fixture.bin", path = "/fixed/fixture.bin", size = 4, lastModifiedAt = 10).apply {
            initUid(); isOffline = true; isMarkedAsOffline = true; offlineOwnedByRoot = true
            offlineBasisModifiedAt = 10; offlineBasisSize = 4; offlineContentDigest = digest("BASE".toByteArray())
        }
        bytes = child().getOfflineFile(context, 7)!!; bytes.writeText("BASE"); bytes.setLastModified(10_000)
        child().offlinePublishedPath = bytes.absolutePath
        register(10)
    }
    private fun digest(bytes: ByteArray) = java.security.MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
    private fun moveAncestor() {
        val moved = folder(15, 20)
        FileController.updateExistingFile(moved, realm) // actual coordinated writer; stored X path is unchanged
        assertEquals(20, files.getValue(15).parentId)
    }
    @Test fun moveIntoOtherRootAfterInitialSnapshotBeforeCopyEvidencePreservesCopy() = runBlocking {
        register(20); var checks = 0
        assertTrue(OfflineFolderSyncUtils.unmarkFolderOffline(context, files.getValue(10), scope) {
            if (++checks == 2) moveAncestor() // all initial subtree snapshots completed; no per-copy evidence yet
        })
        assertEquals("BASE", bytes.readText()); assertTrue(child().isOffline); assertTrue(child().offlineOwnedByRoot)
        assertTrue(OfflineFolderSyncUtils.isRootCoveredCopy(child(), realm, scope))
        assertEquals("/fixed/fixture.bin", child().path)
    }
    @Test fun digestInvalidationRetryRechecksNewOtherRootCoverage() = runBlocking {
        register(20)
        duringDigest = { if (digests == 1) moveAncestor() }
        var checks = 0
        var retryCertified = false
        assertTrue(OfflineFolderSyncUtils.unmarkFolderOffline(context, files.getValue(10), scope) {
            if (++checks == 4) {
                // The subtree loop has finished, but the independent beyond-subtree sweep
                // has not started. This separates the retry from its later recertification.
                assertEquals(2, digests, "actual digest + invalidation + fresh retry")
                retryCertified = true
            }
        })
        assertTrue(retryCertified)
        assertEquals(3, digests, "the later ownership sweep independently recertifies the kept copy")
        assertEquals("BASE", bytes.readText()); assertTrue(child().isOffline)
        assertTrue(OfflineFolderSyncUtils.isRootCoveredCopy(child(), realm, scope))
    }
    @Test fun unchangedLastOwnerStillDeletesCertifiedCleanBytes() = runBlocking {
        assertTrue(OfflineFolderSyncUtils.unmarkFolderOffline(context, files.getValue(10), scope))
        assertFalse(bytes.exists()); assertFalse(child().isOffline); assertFalse(child().offlineOwnedByRoot)
    }
    @Test fun staticOverlapKeepsCopyAndIntent() = runBlocking {
        register(15)
        assertTrue(OfflineFolderSyncUtils.unmarkFolderOffline(context, files.getValue(10), scope))
        assertEquals("BASE", bytes.readText()); assertTrue(child().isOffline); assertTrue(child().offlineOwnedByRoot)
    }
    private fun missingResponse(trashed: Boolean, replace: Boolean) = runBlocking {
        files.remove(10); child().offlineBasisModifiedAt = 0 // unknown bytes: real unmark preserves; successor reacquires
        files.remove(15); child().parentId = 10
        val old = roots.values.single().markedAt.time
        var held = false
        listings = { _, _ -> page(listOf(child())) }
        details = { runBlocking {
            held = true
            if (replace) {
                val f = folder(10)
                assertTrue(FileController.upsertMissingRemoteFile(f, realm))
                assertTrue(OfflineFolderSyncUtils.unmarkFolderOffline(context, f, scope))
                assertTrue(OfflineFolderSyncUtils.markFolderOffline(context, f, scope, {}, {}))
                assertNotEquals(old, roots.values.single().markedAt.time)
            }
            if (trashed) ApiResponse(result = ApiResponseStatus.SUCCESS, data = folder(10).apply { status = "trashed" })
            else ApiResponse(result = ApiResponseStatus.ERROR, error = ApiError(code = "object_not_found"))
        } }
        OfflineFolderSyncUtils.syncOfflineFolderRoots(context)
        assertTrue(held)
        if (replace) {
            assertEquals(1, roots.size); assertNotEquals(old, roots.values.single().markedAt.time)
            assertTrue(child().isMarkedAsOffline); assertTrue(child().offlineOwnedByRoot)
            OfflineFolderSyncUtils.syncOfflineFolderRoots(context) // successor's actual later walk maintains the same child
            assertEquals(1, roots.size); assertTrue(child().isMarkedAsOffline); assertTrue(child().offlineOwnedByRoot)
        } else assertTrue(roots.isEmpty())
        assertEquals("BASE", bytes.readText())
    }
    @Test fun oldNotFoundCannotConsumeRemarkedSuccessor() = missingResponse(false, true)
    @Test fun oldTrashedCannotConsumeRemarkedSuccessor() = missingResponse(true, true)
    @Test fun unchangedLifetimeNotFoundWithdrawsRegistryOnly() = missingResponse(false, false)
    @Test fun unchangedLifetimeTrashedWithdrawsRegistryOnly() = missingResponse(true, false)
    private fun truncated(cursor: String?) = runBlocking {
        files.remove(15); child().parentId = 10
        var valid = false; val seen = mutableListOf<String?>()
        listings = { _, input -> seen += input; if (!valid && seen.size > 1)
            CursorApiResponse(result = ApiResponseStatus.ERROR, data = null)
            else if (!valid) page(emptyList(), true, cursor)
            else if (input == null) page(emptyList(), true, "opaque+/=unchanged") else page(listOf(child())) }
        OfflineFolderSyncUtils.syncOfflineFolderRoots(context)
        assertEquals(1, seen.size, "unusable continuation stops rather than guessing or looping")
        assertTrue(child().isOffline); assertTrue(child().isMarkedAsOffline); assertTrue(child().offlineOwnedByRoot)
        assertEquals(1, roots.size)
        // Direct mark returns the actual production walk outcome too (no helper/predicate mock).
        seen.clear()
        assertFalse(OfflineFolderSyncUtils.markFolderOffline(context, files.getValue(10), scope, {}, {}))
        child().offlineBasisModifiedAt = 0 // existing unknown bytes avoid an unrelated transfer during valid convergence
        valid = true; seen.clear()
        OfflineFolderSyncUtils.syncOfflineFolderRoots(context)
        assertEquals(listOf(null, "opaque+/=unchanged"), seen)
        assertTrue(child().offlineOwnedByRoot); assertTrue(child().isMarkedAsOffline); assertEquals("BASE", bytes.readText())
    }
    @Test fun moreWithoutCursorRetainsClaimAndLaterWalkConverges() = truncated(null)
    @Test fun moreWithBlankCursorRetainsClaimAndLaterWalkConverges() = truncated("  ")
    @Test fun trueTerminalOmissionReleasesIntentButPreservesBytes() = runBlocking {
        files.remove(15); child().parentId = 10
        OfflineFolderSyncUtils.syncOfflineFolderRoots(context)
        assertFalse(child().isOffline); assertFalse(child().isMarkedAsOffline); assertFalse(child().offlineOwnedByRoot)
        assertEquals("BASE", bytes.readText()); assertEquals(1, roots.size)
    }
    @AfterAll fun close() {
        realmContext.set(null, oldContext)
        unsafe.putObjectVolatile(unsafe.staticFieldBase(manufacturer), unsafe.staticFieldOffset(manufacturer), oldManufacturer)
        unmockkAll(); dir.deleteRecursively()
    }
}
