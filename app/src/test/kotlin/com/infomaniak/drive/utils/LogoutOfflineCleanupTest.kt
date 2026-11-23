/* Copyright (C) 2026 Infomaniak Network SA. SPDX-License-Identifier: GPL-3.0-or-later */
package com.infomaniak.drive.utils

import android.content.Context
import android.os.Build
import android.provider.Settings
import android.util.Log
import androidx.room.RoomDatabase
import com.infomaniak.core.auth.models.user.User
import com.infomaniak.core.auth.room.UserDatabase
import com.infomaniak.core.auth.room.UserDao
import com.infomaniak.core.inappreview.AppReviewSettingsRepository
import com.infomaniak.core.inappupdate.AppUpdateSettingsRepository
import com.infomaniak.drive.MainApplication
import com.infomaniak.drive.data.cache.DriveInfosController
import com.infomaniak.drive.data.documentprovider.CloudStorageProvider
import com.infomaniak.drive.data.models.AppSettings
import com.infomaniak.drive.data.models.UiSettings
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.drive.Drive
import io.mockk.clearMocks
import io.mockk.coEvery
import io.mockk.coVerify
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.mockkObject
import io.mockk.mockkStatic
import io.mockk.Runs
import io.mockk.unmockkAll
import io.realm.Realm
import io.realm.internal.RealmCore
import io.realm.RealmConfiguration
import io.realm.RealmModel
import io.realm.RealmQuery
import io.realm.RealmResults
import java.nio.file.Files
import sun.misc.Unsafe
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Assertions.assertTrue
import splitties.init.appCtx

/** Real removeUser/FileController/resetApp deletion; only Android/Realm/account metadata services are substituted. */
@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
@Tag("jvm-isolated")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class LogoutOfflineCleanupTest {
    private val root = Files.createTempDirectory("o08-cleanup").toFile()
    private val files = root.resolve("internal/files").apply { mkdirs() }
    private val cache = root.resolve("internal/cache").apply { mkdirs() }
    private val external = root.resolve("external/offline_storage/101/42/disposable.txt").apply {
        parentFile!!.mkdirs()
        writeText("synthetic public offline bytes; preserve after logout")
    }
    private val expected = external.readBytes()
    private val context = mockk<Context>()
    private val dao = mockk<UserDao>(relaxed = true)
    private lateinit var database: UserDatabase
    private var remaining = 1

    private val realmContextField = Class.forName("io.realm.BaseRealm")
        .getDeclaredField("applicationContext").apply { isAccessible = true }
    private var originalRealmContext: Any? = null
    private var realmContextPatched = false
    private val manufacturerField = Build::class.java.getDeclaredField("MANUFACTURER").apply { isAccessible = true }
    private var originalManufacturer: Any? = null
    private var manufacturerPatched = false
    private val unsafe = Unsafe::class.java.getDeclaredField("theUnsafe").apply { isAccessible = true }.get(null) as Unsafe

    @Suppress("DEPRECATION") // JDK-only write to final mockable-Android metadata; no new test framework.
    private fun setSyntheticManufacturer(value: Any?) {
        // Only the host Android stub's final metadata field; always restored in AfterAll.
        unsafe.putObjectVolatile(unsafe.staticFieldBase(manufacturerField), unsafe.staticFieldOffset(manufacturerField), value)
    }
    private val branchEvents = mutableListOf<String>()

    @BeforeAll
    fun isolatePlatformAndAccountMetadata() {
        Dispatchers.setMain(Dispatchers.Unconfined)
        // The mockable Android SDK has null build metadata; UiSettings reads this default.
        originalManufacturer = manufacturerField.get(null)
        if (originalManufacturer == null) {
            manufacturerPatched = true
            setSyntheticManufacturer("synthetic-o08")
            assertEquals("synthetic-o08", Build.MANUFACTURER)
        }
        every { context.filesDir } returns files
        every { context.cacheDir } returns cache
        every { context.applicationContext } returns context
        every { context.getSharedPreferences(any(), any()) } returns mockk(relaxed = true)
        every { context.packageName } returns "synthetic.o08"
        mockkStatic("splitties.init.AppCtxKt")
        every { appCtx } returns context
        mockkStatic(Log::class)
        every { Log.i(any(), any(), null) } returns 0
        mockkStatic(Settings.Secure::class)
        every { Settings.Secure.getString(any(), any()) } returns "synthetic-device"
        every { context.contentResolver } returns mockk(relaxed = true)

        // Prevent Room database creation: no real user/token storage or Application startup.
        mockkConstructor(RoomDatabase.Builder::class)
        every { anyConstructed<RoomDatabase.Builder<RoomDatabase>>().build() } answers {
            if (!::database.isInitialized) database = mockk(relaxed = true)
            every { database.userDao() } returns dao
            database
        }
        coEvery { dao.allUsers() } returns emptyList()
        coEvery { dao.findById(any()) } returns null
        val otherUser = mockk<User>(relaxed = true)
        every { otherUser.id } returns 202
        coEvery { dao.getFirst() } answers { if (remaining > 0) otherUser else null }
        coEvery { dao.deleteUserById(101) } answers { branchEvents += "coreUserRemoved" }
        coEvery { dao.userCount() } answers { remaining }

        // Realm's real getter reads this inherited static field. Do not instrument its superclass.
        originalRealmContext = realmContextField.get(null)
        realmContextField.set(null, context)
        realmContextPatched = true
        assertSame(context, Realm.getApplicationContext())
        println("O08_BOOTSTRAP realRealmContextGetter=synthetic")
        val realmRuntimeTypes = listOf("io.reactivex.Flowable", "org.reactivestreams.Publisher")
        val missingRuntimeTypes = realmRuntimeTypes.filter { name ->
            runCatching { Class.forName(name, false, Realm::class.java.classLoader) }.isFailure
        }
        assertTrue(missingRuntimeTypes.isEmpty(), "Realm reflective signature types must be present at runtime")
        Realm::class.java.declaredMethods
        requireNotNull(Realm::class.java.superclass).declaredMethods
        val realm = mockk<Realm>(relaxed = true)
        val configuration = mockk<RealmConfiguration>()
        mockkStatic(Realm::class, RealmCore::class)
        every { Realm.getInstance(any<RealmConfiguration>()) } returns realm
        every { RealmCore.loadLibrary(any()) } just Runs
        assertSame(realm, Realm.getInstance(configuration))
        assertSame(context, Realm.getApplicationContext())
        println("O08_BOOTSTRAP runtimeSignaturesAndIsolatedRealmFactory=true")
        mockkConstructor(RealmConfiguration.Builder::class)
        every { anyConstructed<RealmConfiguration.Builder>().build() } returns configuration
        // Use an erased query during eager metadata initialization, outside MockK's recorder.
        val settingsQuery = mockk<RealmQuery<RealmModel>>(relaxed = true)
        every { realm.where(any<Class<RealmModel>>()) } returns settingsQuery
        every { settingsQuery.findFirst() } returns null
        Class.forName(AppSettings::class.java.name, true, AppSettings::class.java.classLoader)
        println("O08_BOOTSTRAP appSettingsInitializedOutsideRecorder=true")

        mockkObject(AppSettings.Companion)
        every { AppSettings.getAppSettings() } returns AppSettings(_currentUserId = 101, _currentDriveId = 42)
        every { AppSettings.updateAppSettings(any(), any()) } just Runs
        every { AppSettings.resetAppSettings() } answers {
            branchEvents += "lastUserResetReached"
            assertFalse(files.resolve("kDrive-101-42.realm").exists())
        }
        mockkObject(MainApplication.Companion)
        every { MainApplication.userDataCleanableList } returns emptyList()

        // Account switching/profile/preferences are not filesystem deletion under test.
        AccountUtils.init()
        assertTrue(::database.isInitialized, "Core account singleton was initialized before this isolated fixture")
        // The inherited property is protected; inspect its real getter without changing production visibility.
        val accountDatabase = AccountUtils::class.java.getDeclaredMethod("getUserDatabase").apply {
            isAccessible = true
        }.invoke(AccountUtils)
        assertSame(database, accountDatabase, "AccountUtils must use this worker's synthetic database")
        println("O08_BOOTSTRAP worker=${System.getProperty("org.gradle.test.worker")} pid=${ProcessHandle.current().pid()}")
        mockkObject(MyKSuiteDataUtils)
        coEvery { MyKSuiteDataUtils.deleteData(any()) } just Runs
        mockkObject(UploadFile.Companion)
        every { UploadFile.getAppSyncSettings() } returns null
        mockkConstructor(UiSettings::class, AppReviewSettingsRepository::class, AppUpdateSettingsRepository::class)
        every { anyConstructed<UiSettings>().removeUiSettings() } just Runs
        coEvery { anyConstructed<AppReviewSettingsRepository>().clear() } just Runs
        coEvery { anyConstructed<AppUpdateSettingsRepository>().clear() } just Runs
        mockkObject(CloudStorageProvider.Companion)
        every { CloudStorageProvider.notifyRootsChanged() } just Runs

        // Keep real DriveInfosController.deleteDrives transaction, substituting only its Realm backend.
        mockkObject(DriveInfosController)
        every { DriveInfosController.getRealmInstance() } returns realm
        every { DriveInfosController.getDrive(any(), any(), any(), any(), any()) } returns null
        every { realm.executeTransaction(any<Realm.Transaction>()) } answers {
            branchEvents += "perUserCleanupReached"
            assertFalse(files.resolve("kDrive-101-42.realm").exists())
            assertFalse(files.resolve("kDrive-101-42.realm.lock").exists())
            assertFalse(files.resolve("kDrive-101-43.realm.management").exists())
            assertTrue(files.resolve("kDrive-202-42.realm").exists())
            firstArg<Realm.Transaction>().execute(realm)
        }
        val driveQuery = mockk<RealmQuery<Drive>>(relaxed = true)
        val drives = mockk<RealmResults<Drive>>(relaxed = true)
        every { realm.where(Drive::class.java) } returns driveQuery
        every { driveQuery.equalTo(Drive::userId.name, any<Int>()) } returns driveQuery
        every { driveQuery.findAll() } returns drives
        every { drives.iterator() } answers { mutableListOf<Drive>().iterator() }
    }

    @BeforeEach
    fun prepareCase() {
        files.mkdirs()
        cache.mkdirs()
        branchEvents.clear()
        clearMocks(dao, answers = false)
        AccountUtils.currentUserId = 101
    }

    @AfterAll
    fun closeSyntheticScope() {
        try {
            if (realmContextPatched) {
                realmContextField.set(null, originalRealmContext)
                assertSame(originalRealmContext, realmContextField.get(null))
                println("O08_BOOTSTRAP originalRealmContextRestored=true")
            }
        } finally {
            try {
                if (manufacturerPatched) {
                    setSyntheticManufacturer(originalManufacturer)
                    assertSame(originalManufacturer, manufacturerField.get(null))
                    println("O08_BOOTSTRAP originalManufacturerRestored=true")
                }
            } finally {
                unmockkAll()
                Dispatchers.resetMain()
                root.deleteRecursively()
            }
        }
    }

    private fun populateInternalFiles() {
        files.resolve("kDrive-101-42.realm").writeText("synthetic removed-user realm")
        files.resolve("kDrive-101-42.realm.lock").writeText("synthetic removed-user sidecar")
        files.resolve("kDrive-101-43.realm.management/nested").apply { mkdirs() }
            .resolve("fixture").writeText("synthetic removed-user management directory")
        files.resolve("kDrive-202-42.realm").writeText("synthetic other-user realm")
        files.resolve("bookkeeping").writeText("synthetic internal bookkeeping")
        cache.resolve("preview").writeText("synthetic preview")
    }

    @Test
    fun remainingUserRemovalDeletesOnlyInternalUserFilesAndKeepsExternalOfflineBytes() = runBlocking {
        remaining = 1
        populateInternalFiles()
        AccountUtils.removeUser(101)
        coVerify(exactly = 1) { dao.deleteUserById(101) }
        assertFalse(files.resolve("kDrive-101-42.realm").exists())
        assertFalse(files.resolve("kDrive-101-42.realm.lock").exists())
        assertFalse(files.resolve("kDrive-101-43.realm.management").exists())
        assertTrue(files.resolve("kDrive-202-42.realm").isFile)
        assertTrue(files.resolve("bookkeeping").isFile)
        assertTrue(cache.resolve("preview").isFile)
        assertArrayEquals(expected, external.readBytes())
        assertEquals(listOf("coreUserRemoved", "perUserCleanupReached"), branchEvents)
        println("O08_BRANCH remainingUser realRemoveUser=true perUserDeletion=true lastUserReset=false externalPreserved=true")
    }

    @Test
    fun lastUserRemovalDeletesInternalFilesAndCacheButKeepsExternalOfflineBytes() = runBlocking {
        remaining = 0
        populateInternalFiles()
        AccountUtils.removeUser(101)
        coVerify(exactly = 1) { dao.deleteUserById(101) }
        assertFalse(files.exists(), "real last-user resetApp must remove internal files")
        assertFalse(cache.exists(), "real last-user resetApp must remove cache")
        assertArrayEquals(expected, external.readBytes())
        assertEquals(listOf("coreUserRemoved", "perUserCleanupReached", "lastUserResetReached"), branchEvents)
        println("O08_BRANCH lastUser realRemoveUser=true perUserDeletion=true lastUserReset=true externalPreserved=true")
    }
}
