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
package com.infomaniak.drive.data.documentprovider

import android.content.ContentResolver
import android.content.Context
import android.content.ContextWrapper
import android.content.SharedPreferences
import android.content.pm.ProviderInfo
import android.graphics.Point
import android.net.Uri
import android.os.ParcelFileDescriptor
import android.os.SystemClock
import android.provider.DocumentsContract
import android.database.ContentObserver
import androidx.test.core.app.ApplicationProvider
import androidx.work.Data
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.core.network.models.ApiResponse
import com.infomaniak.core.network.models.ApiResponseStatus
import com.infomaniak.drive.R
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.cache.DriveInfosController
import com.infomaniak.drive.data.cache.DriveMigration
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.cache.FileMigration
import com.infomaniak.drive.data.cache.FolderFilesProvider
import com.infomaniak.drive.data.models.AppSettings
import com.infomaniak.drive.data.models.drive.Drive
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.sync.UploadMigration
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.IOFile
import com.infomaniak.drive.utils.RealmModules
import com.infomaniak.drive.utils.SyncUtils
import com.infomaniak.drive.utils.Utils
import io.mockk.coEvery
import io.mockk.clearMocks
import io.mockk.every
import io.mockk.just
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.mockkStatic
import io.mockk.runs
import io.mockk.unmockkObject
import io.mockk.unmockkStatic
import io.mockk.verify
import okhttp3.OkHttpClient
import io.realm.Realm
import io.realm.RealmConfiguration
import io.sentry.ScopeCallback
import io.sentry.Sentry
import io.sentry.SentryEvent
import io.sentry.protocol.SentryId
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.ExecutorCoroutineDispatcher
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeoutOrNull
import kotlin.coroutines.ContinuationInterceptor
import org.junit.After
import org.junit.AfterClass
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertNotNull
import org.junit.Assert.assertNull
import org.junit.Assert.assertTrue
import org.junit.Assert.fail
import org.junit.Before
import org.junit.BeforeClass
import org.junit.FixMethodOrder
import org.junit.Test
import org.junit.runners.MethodSorters
import java.io.File as PlatformFile
import java.io.IOException
import java.nio.file.Files
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger

/**
 * Focused in-process instrumentation for [CloudStorageProvider]'s REAL decision/effect
 * chain (P01-P05/PR01 territory). The provider is instantiated directly and bound with
 * [android.content.ContentProvider#attachInfo] — every call below runs the production
 * policy evaluation, realm routing, cache/offline file selection and the write-close
 * upload seam. In-process direct calls are NOT cross-process Binder evidence.
 *
 * ### Storage isolation (corrected per the root draft review AND the 0713 follow-up review)
 * `ApplicationProvider.getApplicationContext()` returns the TARGET app context, so the
 * harness never pretends to use test-private storage. Instead every exercised storage
 * route is explicitly redirected or bounded:
 * - The provider is bound with a wrapping [Context] whose `filesDir`, `cacheDir` and
 *   `externalMediaDirs` live under ONE exclusively owned per-CASE root
 *   ([Files.createTempDirectory] under the target cache, fixed prefix), created in
 *   every [setUp] and deleted in that case's [tearDown]. Creation is exclusive by
 *   construction: nothing pre-existing is ever reused, inspected-as-safe,
 *   refused-then-cleaned, or touched at all — a creation failure fails the run. The
 *   temp root (`cloud_storage_temp_files`), the read cache
 *   (`cloud_storage/<user>/<drive>`) and the offline folder probe all land in that
 *   owned namespace. The provider-disabled preferences do NOT: SharedPreferences has
 *   no directory seam, so they use per-case UNIQUE names in the app's REAL shared_prefs
 *   directory and are deleted and verified there by teardown. A root PRESERVED as
 *   evidence by a failed cleanup is never revisited: every later case creates its own
 *   new unique root and token, so no namespace is ever reused or cleared after a
 *   failed deletion.
 * - Realm file locations are redirected by stubbing ONLY the configuration seams
 *   ([FileController.getRealmConfiguration], [UploadFile.getRealmInstance]); the stubs
 *   mirror the production builders (same schema version, migrations, modules, file
 *   names) with the directory moved into the owned root. Realm itself, the schema, the
 *   migrations and every query/transaction run for real against those owned files.
 *
 * ### Captured boundaries (declared seams — no access policy, cache decision or upload
 * transaction is stubbed)
 * - [AccountUtils]: identity + network boundary. Users list is empty; every
 *   `getHttpClient` call throws, so remote details/downloads exercise the provider's
 *   real catch/fallback paths, never a transfer.
 * - [CloudStorageProvider.Companion.initRealm]: the provider's own repeated
 *   initialization boundary is captured (counted) so the suite never opens a
 *   default-realm handle through the provider — an open can create or migrate storage
 *   and its auxiliary files and leaks its handle in production; the earlier draft's
 *   "read-only" claim was unsupported and is withdrawn.
 * - [CloudStorageProvider.notifyProviderChanged] / [CloudStorageProvider.notifyRootsChanged]
 *   and the fixture context's [RecordingContentResolver] notifyChange override:
 *   change-notification boundaries are captured locally, so no unrelated provider
 *   observer is ever triggered by the suite. Cursor self-observer registration is NOT
 *   intercepted: the real AbstractCursor chain calls the hidden FOUR-argument
 *   registerContentObserver (root-verified against AOSP android-8.1.0_r1 and
 *   android-14.0.0_r1), which the compile SDK does not even reference and a subclass
 *   cannot override — the earlier draft's three-argument spy stubs never executed and
 *   are withdrawn here together with their "counted locally and never delivered"
 *   claims. The bounded residual: exercised cursors really register a TRANSIENT
 *   self-observer on the app's OWN authority for the cursor's lifetime, closed with
 *   the cursor. No Binder delivery to another process, no persisted-URI grant and no
 *   durable lock is executed or claimed.
 * - [SentryLog] and the provider's direct [Sentry] static calls: the telemetry
 *   boundary is captured (counted) — the logging wrapper, the thumbnail-generation
 *   failure (`Sentry.captureEvent`) and the write-close listener failure
 *   (`Sentry.captureException` with a scope callback). The suite never knowingly
 *   emits breadcrumbs or Sentry events; unstubbed Sentry statics keep their real
 *   behavior and no exercised path calls them.
 * - [AppSettings.Companion.getAppSettings]: the durable app-lock INPUT is a fixture
 *   object. The durable-settings store itself is NOT exercised: it lives in the real
 *   app's default Realm directory and has no test-only location seam (blocker +
 *   smallest production seam option recorded for the orchestrator). The provider's
 *   per-call re-read and the whole access policy stay real.
 * - [DriveInfosController.getRealmInstance]: the controller's only realm-open path,
 *   stubbed to a REAL empty drive-info Realm owned by the live case; the real
 *   getDrives body reads it, so root listing never opens the real DriveInfos cache
 *   and getDrives itself stays unrecorded (MockK's recording placeholder for a
 *   concrete java.util collection return redefines the ArrayList chain on this agent).
 * - [SyncUtils.syncImmediately]: the scheduler effect is captured (counted,
 *   thread-safe) at this seam. The app's WorkManager is already initialized in-process
 *   by the default initializer, so a test driver cannot be installed; capturing the
 *   call bounds the scheduler's downstream effects by construction (no worker is ever
 *   enqueued or run).
 *
 * ### App startup vs. provider initialization (traced separately, per the follow-up review)
 * `MainApplication.onCreate` ran BEFORE this suite: Sentry is configured, WorkManager
 * is default-initialized, and the default Realm is initialized against the real app
 * directory. That is the app's own startup state and predates every test; the suite
 * adds nothing to it. The provider's OWN initialization calls are captured at the
 * initRealm seam above, so the repeated `attachInfo`/`onCreate` per case adds no
 * default-realm access. One remaining declared app-process read: referencing the
 * [AppSettings] companion may perform its production class-load read of the real
 * AppSettings.realm (once).
 *
 * ### Explicitly NOT exercised
 * Real Binder/ContentResolver acquisition, persisted-URI grants, the system Files app,
 * remote mutation bodies (create/rename/copy/move success paths), thumbnail generation,
 * the queryRoots refresh coroutine (users are mocked empty), ChromeOS branches — and
 * the ASYNC FOLDER FILL for a real folder. Traced: `FolderFilesProvider
 * .getCloudStorageFiles` requires the transport (`getHttpClient`) BEFORE serving any
 * row, so under the declared no-transfer boundary the fill cannot complete offline,
 * and its failure path is an unhandled launch failure in the production `cloudScope`
 * (no CoroutineExceptionHandler; the synchronous branch is gated on the private
 * `isChromeOs` flag) — exercising it would crash the instrumentation process. The
 * child-cursor contract is therefore proven on the synchronous root path, and the
 * smallest production-seam option (an injectable exception boundary around the fill
 * launch, or an injectable offline branch gate) is recorded for the orchestrator. No
 * folder-fill coroutine is ever launched in this suite.
 *
 * ### Cleanup contract
 * [tearDown] cleanup is checked: each step's failure is collected and thrown as one
 * `AssertionError`; JUnit4 `RunAfters` aggregates that with any original test failure,
 * so cleanup damage is visible without masking the first error. Every step is gated on
 * the fixture state actually existing (a failed [setUp] leaves nothing to clean), and
 * [Realm.deleteRealm]'s Boolean return is verified together with the resulting file
 * state: a false return or a still-present file is a FAILED deletion, while a realm
 * this case never created is legitimately absent. The owned root is recursively erased
 * ONLY after every owned realm deleted cleanly — a failed deletion (an open handle or
 * an uncancelled job) PRESERVES the root in place as evidence and reports the failure
 * instead of erasing on top of it. Only owned namespaces are ever cleaned, and a
 * preserved root is never touched by a later case (each case creates its own new
 * unique root).
 *
 * ### Quiescence before destructive teardown
 * Every opened descriptor is closed by its own case (use/finally, even on assertion
 * failure), so the close-listener chain exists when teardown runs. Teardown FIRST
 * quiesces the provider's OWN scope — `private val cloudScope = CoroutineScope(...)`
 * (CloudStorageProvider.kt): the CoroutineScope(context) FACTORY adds a Job when the
 * context lacks one (root-verified against the cached kotlinx-coroutines-core-jvm
 * 1.11.0 bytecode), so the real handle exists and is reached test-owned via
 * reflection, no production change. Cancellation alone — or executor shutdownNow,
 * which is best-effort and waits for nothing — proves NOTHING: the barrier is
 * cancel-then-JOIN under a deadline. A close listener still QUEUED at cancel time
 * launches into an already-cancelled parent, so its body never runs (the production
 * listener's only pre-launch action is the exception check). The owned dispatcher is
 * closed only AFTER quiescence (thread-leak avoidance).
 *
 * A failed barrier — a timeout OR any error reaching the scope/Job — engages ONE
 * test-owned guard instead of trusting the next case to behave: the failing case's
 * state, fixtures, dispatcher and the global mocks are RETAINED (nothing deleted,
 * nothing restored), and every later setUp/tearDown in this class throws before
 * touching anything, so a possibly still-running late task can never be routed into a
 * fresh case's namespace or past restored production seams. The actual EXECUTION
 * BOUNDARY is stated plainly: a JUnit failure here does NOT terminate the entire
 * instrumentation suite — the runner can execute later classes, which would replace
 * or unmock these global seams, so the retained mocks are a within-class barrier
 * only, never a cross-class one. This class therefore requires a DEDICATED,
 * SINGLE-CLASS instrumentation invocation, and a failed or blocked teardown must end
 * that invocation with process termination CONFIRMED before any other class is run —
 * a precondition for any later orchestrator-approved run, not a device authorization
 * now. Within the class, the retained fixtures stay on disk until process exit. The
 * class teardown attempts exactly ONE bounded retry of the same barrier against the
 * retained provider — only a PROVEN completion (the join returns) restores the mocks;
 * a second timeout stays blocked and is never permission to restore production
 * routing. The original case failure is never swallowed: JUnit4 reports it for that
 * case, and each blocked or aborted path reports its own loud failure.
 */
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
class CloudStorageProviderTest {
    companion object {
        // Synthetic, exclusively owned namespaces — deliberately disjoint from every
        // other suite and from every real account id.
        private const val USER_ID = 4242
        private const val DRIVE_ID = 4243
        private const val PARENT_ID = 4244
        private const val FILE_ID = 4245
        private const val OTHER_DRIVE_ID = 99999
        private const val DRIVE_NAME = "FixtureDrive"
        private const val FILE_NAME = "providerx-fixture.bin"
        private const val MTIME_SECONDS = 1_700_000_000L
        private const val FIXTURE_ROOT_NAME = "providerx-fixture-cloud-storage"
        private const val FIXTURE_PREFS_PREFIX = "providerx-fixture-"
        // Mirrors CloudStorageProvider's private PREFS_NAME; the fixture wrapper renames
        // the SharedPreferences file so the real provider prefs are never touched.
        private const val PROVIDER_PREFS_NAME = "cloud_storage_provider"
        private const val NETWORK_BOUNDARY_MESSAGE = "provider-test: network boundary"
        private const val QUIESCE_TIMEOUT_MS = 15_000L
        private val FILE_BYTES = "providerx-fixture-bytes-0123456789".toByteArray()

        private val USER_DRIVE = UserDrive(USER_ID, DRIVE_ID)
        private val FILE_UID = "${FILE_ID}_${DRIVE_ID}"

        // The app-lock INPUT fixture. Mutating it drives the real per-call settings read
        // and the real policy; the durable AppSettings store is never written.
        private val appLockFixture = AppSettings().apply { _appSecurityEnabled = false }
        private val fileFixtureConfigs = ConcurrentHashMap<String, RealmConfiguration>()

        // Thread-safe effect counters: captured effects run on the provider's own
        // single-thread executor (the write-close chain) or on instrumentation threads,
        // never only on the test thread.
        private val syncImmediatelyCalls = AtomicInteger(0)
        private val initRealmCalls = AtomicInteger(0)
        private val telemetryCaptured = AtomicInteger(0)
        private val observerNotificationsCaptured = AtomicInteger(0)
        private val captureExceptionCalls = AtomicInteger(0)
        private val captureEventCalls = AtomicInteger(0)

        /** URIs whose change notification attempted to leave the process (recorded, not delivered). */
        private val notifiedUris = ConcurrentLinkedQueue<Uri>()

        private lateinit var appContext: Context

        /**
         * Everything ONE case owns. Null before the first case and between cases: setUp
         * clears it BEFORE its first throwing operation — a failed temp-dir creation
         * leaves it null, so teardown can never revisit a previous case's (possibly
         * evidence-preserved) root — and teardown consumes it only after quiescence.
         * Exactly one state at a time; no registry.
         */
        private var caseState: CaseState? = null

        private val currentCase: CaseState
            get() = caseState ?: error(
                "no live per-case state (setUp not run, failed before publication, or already consumed)",
            )

        // The guarded quiescence-failure state. Set ONLY by a failed completion barrier
        // (a timeout, or any error reaching the scope/Job). While it is set, the retained
        // case state, its fixtures, the owned dispatcher and the global mocks are kept
        // EXACTLY as left: every later setUp/tearDown throws before touching anything and
        // the class teardown restores no mock until ONE bounded retry PROVES completion —
        // a possibly still-running late task must never be routed into a fresh case's
        // namespace or past restored production seams.
        private var retainedQuiescenceBlocker: String? = null
        private var retainedProvider: CloudStorageProvider? = null

        private fun retainedTeardownMessage(): String =
            "provider work could NOT be proven quiescent — the per-case state, its fixtures, the " +
                "owned dispatcher and the global mocks are deliberately RETAINED (nothing deleted, " +
                "nothing restored) and every later setUp/tearDown is blocked; the class teardown may " +
                "attempt ONE bounded retry and only a PROVEN completion restores the mocks. Retained " +
                "blocker: $retainedQuiescenceBlocker"

        @JvmStatic
        @BeforeClass
        fun boot() {
            appContext = ApplicationProvider.getApplicationContext()

            Realm.init(appContext) // no-op when the app process already initialized Realm

            mockkObject(AccountUtils)
            every { AccountUtils.getAllUsersSync() } returns emptyList()
            coEvery { AccountUtils.getHttpClient(any()) } throws IOException(NETWORK_BOUNDARY_MESSAGE)
            coEvery { AccountUtils.getHttpClient(any(), any()) } throws IOException(NETWORK_BOUNDARY_MESSAGE)
            every { AccountUtils.init() } just runs

            mockkObject(AppSettings.Companion)
            every { AppSettings.getAppSettings() } answers { appLockFixture }

            mockkObject(UploadFile.Companion)
            // Routed through the LIVE per-case state on purpose: a request outside a
            // live case (a late task after teardown consumed the state) fails loudly
            // here instead of silently opening a realm in another case's namespace.
            every { UploadFile.getRealmInstance() } answers {
                Realm.getInstance(syncFixtureConfig(currentCase.ownedRoot))
            }

            mockkObject(FileController)
            every { FileController.getRealmConfiguration(any()) } answers {
                val drive = it.invocation.args[0] as UserDrive? ?: UserDrive()
                val key = if (drive.sharedWithMe) "shares-${drive.userId}" else "${drive.userId}-${drive.driveId}"
                fileFixtureConfigs.getOrPut(key) { buildFileFixtureConfig(drive, currentCase.ownedRoot) }
            }

            mockkObject(DriveInfosController)
            // Drive-metadata input: the REAL getDrives body runs against a REAL empty
            // drive-info Realm owned by the live case; getRealmInstance is the
            // controller's only realm-open path, so this one seam captures all of its
            // storage. getDrives is deliberately left unrecorded — MockK's recording
            // placeholder for a concrete java.util collection return redefines the
            // ArrayList class chain on this agent (the recorded 4daa failure).
            every { DriveInfosController.getRealmInstance() } answers {
                Realm.getInstance(driveInfosFixtureConfig(currentCase.ownedRoot))
            }

            mockkObject(SyncUtils)
            every {
                SyncUtils.run { any<Context>().syncImmediately(any<Data>(), any<Boolean>()) }
            } answers { syncImmediatelyCalls.incrementAndGet(); Unit }

            // The provider's own repeated initialization boundary: captured so no
            // test-case onCreate ever opens a default-realm handle (an open can
            // create/migrate storage and its auxiliary files, and leaks its handle in
            // production). The app's own startup initialization predates this suite and
            // is declared in the class KDoc — the two are distinct effects.
            mockkObject(CloudStorageProvider.Companion)
            coEvery {
                CloudStorageProvider.Companion.run { any<Context>().initRealm() }
            } answers { initRealmCalls.incrementAndGet(); Unit }
            // Global observer-notification boundary: captured, never delivered.
            every { CloudStorageProvider.notifyProviderChanged() } answers {
                observerNotificationsCaptured.incrementAndGet(); Unit
            }
            every { CloudStorageProvider.notifyRootsChanged() } answers {
                observerNotificationsCaptured.incrementAndGet(); Unit
            }

            // Telemetry boundaries: captured so the suite never knowingly emits
            // breadcrumbs or Sentry events. SentryLog covers the app's logging wrapper;
            // the provider's two DIRECT io.sentry.Sentry calls (the thumbnail-generation
            // failure and the write-close listener failure) are captured at the static
            // seam as counted no-ops returning an empty id. Unstubbed Sentry statics
            // keep their real behavior; no exercised path calls them.
            mockkObject(SentryLog)
            every { SentryLog.v(any(), any(), any()) } answers { telemetryCaptured.incrementAndGet(); Unit }
            every { SentryLog.d(any(), any(), any()) } answers { telemetryCaptured.incrementAndGet(); Unit }
            every { SentryLog.i(any(), any(), any()) } answers { telemetryCaptured.incrementAndGet(); Unit }
            every { SentryLog.w(any(), any(), any()) } answers { telemetryCaptured.incrementAndGet(); Unit }
            every { SentryLog.e(any(), any(), any(), any()) } answers { telemetryCaptured.incrementAndGet(); Unit }
            every { SentryLog.wtf(any(), any(), any(), any()) } answers { telemetryCaptured.incrementAndGet(); Unit }

            mockkStatic(Sentry::class)
            every { Sentry.captureException(any<Throwable>(), any<ScopeCallback>()) } answers {
                captureExceptionCalls.incrementAndGet()
                SentryId.EMPTY_ID
            }
            every { Sentry.captureEvent(any<SentryEvent>()) } answers {
                captureEventCalls.incrementAndGet()
                SentryId.EMPTY_ID
            }
        }

        @JvmStatic
        @AfterClass
        fun unboot() {
            // Guarded failure path: while a quiescence failure is retained, restoring the
            // global mocks (production routing) is FORBIDDEN until work is proven done.
            // The class teardown attempts exactly ONE bounded retry of the same
            // cancel-then-join barrier against the retained provider: only a PROVEN
            // completion (the join returns) authorizes the unmock below; a second timeout
            // stays blocked — it is never permission to restore production routing — and
            // the retained fixtures stay in place as evidence.
            retainedQuiescenceBlocker?.let { blocker ->
                val retained = retainedProvider
                    ?: throw AssertionError(
                        "class teardown blocked: a quiescence failure is retained but no provider was " +
                            "retained to retry against — mocks are NOT restored. Retained blocker: $blocker",
                    )
                val scope = try {
                    CloudStorageProvider::class.java.getDeclaredField("cloudScope")
                        .apply { isAccessible = true }
                        .get(retained) as CoroutineScope
                } catch (failure: Throwable) {
                    throw AssertionError(
                        "class teardown retry could not reach the retained provider's scope — mocks are " +
                            "NOT restored and the retained fixtures stay in place: $failure",
                        failure,
                    )
                }
                val job = scope.coroutineContext[Job]
                    ?: throw AssertionError(
                        "class teardown retry found no Job on the retained scope (the CoroutineScope(context)" +
                            "-factory counterevidence no longer holds) — mocks are NOT restored. " +
                            "Retained blocker: $blocker",
                    )
                val timedOut = runBlocking {
                    withTimeoutOrNull(QUIESCE_TIMEOUT_MS) {
                        job.cancelAndJoin()
                        false
                    } ?: true
                }
                if (timedOut) {
                    throw AssertionError(
                        "SECOND quiescence timeout at class teardown — this class stays BLOCKED (mocks are " +
                            "NOT restored, the retained fixtures stay in place); this is not permission to " +
                            "restore production routing. Original blocker: $blocker " +
                            "(job active=${job.isActive} cancelled=${job.isCancelled} completed=${job.isCompleted})",
                    )
                }
                // Proven completion: no late task can run any more, so restoring the mocks
                // is safe. The retained fixture root stays on disk as the recorded evidence
                // of the anomaly; the owned dispatcher is closed now that work is quiesced.
                (scope.coroutineContext[ContinuationInterceptor] as? ExecutorCoroutineDispatcher)?.close()
                retainedQuiescenceBlocker = null
                retainedProvider = null
                caseState = null
            }
            unmockkStatic(Sentry::class)
            unmockkObject(SentryLog)
            unmockkObject(CloudStorageProvider.Companion)
            unmockkObject(SyncUtils)
            unmockkObject(DriveInfosController)
            unmockkObject(FileController)
            unmockkObject(UploadFile.Companion)
            unmockkObject(AppSettings.Companion)
            unmockkObject(AccountUtils)
        }

        /**
         * Mirrors [FileController]'s production configuration builder with one changed
         * input: the directory. Schema version, migration, modules and file naming are
         * the production values.
         */
        private fun buildFileFixtureConfig(drive: UserDrive, root: IOFile): RealmConfiguration {
            val dbName =
                if (drive.sharedWithMe) "kDrive-${drive.userId}-shares.realm"
                else "kDrive-${drive.userId}-${drive.driveId}.realm"
            return RealmConfiguration.Builder()
                .schemaVersion(FileMigration.DB_VERSION)
                .migration(FileMigration())
                .modules(RealmModules.LocalFilesModule())
                .name(dbName)
                .directory(root)
                .build()
        }

        /** Mirrors [UploadFile]'s production Sync.realm configuration, owned directory. */
        private fun syncFixtureConfig(root: IOFile): RealmConfiguration = RealmConfiguration.Builder()
            .schemaVersion(UploadMigration.DB_VERSION)
            .migration(UploadMigration())
            .modules(RealmModules.SyncFilesModule())
            .name("Sync.realm")
            .directory(root)
            .build()

        /** Mirrors [DriveInfosController]'s production DrivesInfos.realm configuration,
         *  owned directory. */
        private fun driveInfosFixtureConfig(root: IOFile): RealmConfiguration = RealmConfiguration.Builder()
            .schemaVersion(DriveMigration.DB_VERSION)
            .migration(DriveMigration())
            .modules(RealmModules.DriveFilesModule())
            .name("DrivesInfos.realm")
            .directory(root)
            .build()

        private fun rootDocId() = "$USER_ID"
        private fun folderDocId() = "$USER_ID/${DRIVE_NAME}@$DRIVE_ID/$PARENT_ID"
        private fun fileDocId() = "$USER_ID/${DRIVE_NAME}@$DRIVE_ID/$FILE_ID"
        private fun driveDocId() = "$USER_ID/${DRIVE_NAME}@$DRIVE_ID/${Utils.ROOT_ID}"
        private fun otherDriveDocId(fileId: Int) = "$USER_ID/Other@$OTHER_DRIVE_ID/$fileId"

        private fun cacheFile(): IOFile =
            IOFile(IOFile(currentCase.ownedSub("cache"), "cloud_storage/$USER_ID/$DRIVE_ID"), FILE_ID.toString())

        private fun tempFile(): IOFile =
            IOFile(IOFile(currentCase.ownedSub("files"), "cloud_storage_temp_files/$PARENT_ID"), FILE_NAME)
    }

    /**
     * Per-case owned namespaces and handles, published as one state object: setUp fills
     * each field only after its creating operation succeeds, so teardown consumes and
     * cleans exactly what exists. The immutable [runToken] names every per-case
     * preference file. A failed quiescence RETAINS the current state (the guard) instead
     * of letting any teardown consume it, so a late task keeps routing into its own
     * namespace until the class teardown proves completion.
     */
    private class CaseState(val runToken: String) {
        lateinit var ownedRoot: IOFile
        lateinit var fixtureContext: Context
        lateinit var recordingResolver: RecordingContentResolver
        val hasOwnedRoot get() = ::ownedRoot.isInitialized
        fun fixturePrefsName(name: String): String = "$FIXTURE_PREFS_PREFIX$runToken-$name"
        fun ownedSub(name: String): IOFile = IOFile(ownedRoot, name).apply { mkdirs() }
    }

    /**
     * Redirects every filesystem root the exercised provider paths reach into the owned
     * per-case fixture namespace, renames SharedPreferences under the per-case unique
     * token so the real provider prefs file is never opened, and serves a recording
     * ContentResolver so change notifications attempted by exercised provider code are
     * captured in-process and never reach the system or unrelated observers.
     * `applicationContext` deliberately still resolves to the base app: exercised paths
     * use the direct accessors below, and the one production use of the static app
     * context (`notifyProviderChanged`) is captured at the companion seam.
     */
    private class FixtureContext(base: Context) : ContextWrapper(base) {
        override fun getFilesDir(): PlatformFile? = currentCase.ownedSub("files")
        override fun getCacheDir(): PlatformFile? = currentCase.ownedSub("cache")
        override fun getExternalMediaDirs(): Array<PlatformFile>? = arrayOf(currentCase.ownedSub("external-media"))
        override fun getContentResolver(): ContentResolver = currentCase.recordingResolver
        override fun getSharedPreferences(name: String, mode: Int): SharedPreferences =
            // SharedPreferences has no directory seam: the per-case UNIQUE name is the
            // only isolation, and these files live in the app's REAL shared_prefs
            // directory (not the owned root) — teardown deletes and verifies them there.
            super.getSharedPreferences(currentCase.fixturePrefsName(name), mode)
    }

    /**
     * Captures change notifications at the external-observation boundary: exercised
     * provider code notifies through the fixture context's resolver, so the attempted
     * notification is recorded locally and nothing is delivered system-wide. Cursor
     * self-observer registration (the real `setNotificationUri`/close path, which calls
     * the hidden four-argument registerContentObserver) is NOT intercepted — see the
     * class KDoc's captured-boundaries note for the bounded residual.
     */
    private class RecordingContentResolver(context: Context) : ContentResolver(context) {
        override fun notifyChange(uri: Uri, observer: ContentObserver?) {
            notifiedUris.add(uri)
        }
    }

    private lateinit var provider: CloudStorageProvider

    @Before
    fun setUp() {
        // Guarded failure path: a retained quiescence failure blocks EVERY later case
        // before anything is reset — the retained state, its fixtures and the global
        // mocks stay exactly as left, so a possibly still-running late task can never be
        // routed into this fresh case's namespace. Every later case of THIS class fails
        // loudly with the retained blocker (no cross-class suite abort is claimed — see
        // the class KDoc's execution-boundary note); the class teardown's single bounded
        // retry is the only way out.
        retainedQuiescenceBlocker?.let { blocker ->
            throw AssertionError(
                "SUITE BLOCKED by a retained quiescence failure from an earlier case — this setUp " +
                    "deliberately clears NOTHING. Retained blocker: $blocker",
            )
        }
        // The per-case state is cleared BEFORE the first throwing operation: if temp-dir
        // creation fails, teardown finds null and can never revisit a previous case's
        // (possibly evidence-preserved) root. The fresh state is published as soon as the
        // exclusively created root exists, so a later setup failure still leaves THIS
        // case's own namespace consumable by its teardown.
        caseState = null
        fileFixtureConfigs.clear()
        appLockFixture._appSecurityEnabled = false
        syncImmediatelyCalls.set(0)
        initRealmCalls.set(0)
        telemetryCaptured.set(0)
        observerNotificationsCaptured.set(0)
        captureExceptionCalls.set(0)
        captureEventCalls.set(0)
        notifiedUris.clear()

        val state = CaseState(UUID.randomUUID().toString())
        // Exclusively owned per-CASE namespace: the standard temp-directory API creates a
        // brand-new unique directory (or fails loudly) — nothing pre-existing is ever
        // reused, inspected as safe, refused-then-cleaned, or touched at all. A root
        // preserved as evidence by a previous case's failed cleanup is never revisited:
        // every later case creates its own new unique root and token.
        state.ownedRoot = Files.createTempDirectory(
            appContext.cacheDir.toPath(),
            "$FIXTURE_ROOT_NAME-",
        ).toFile()
        // Published IMMEDIATELY after the exclusive root exists — BEFORE the fixture
        // context/resolver constructors: if either throws, teardown still finds THIS
        // case's owned root and consumes it, instead of leaving the namespace stranded.
        caseState = state
        state.fixtureContext = FixtureContext(appContext)
        state.recordingResolver = RecordingContentResolver(appContext)

        // Fail fast on a non-clearable owned subtree instead of silently reproducing
        // order contamination (the checked-isolation rule).
        clearOwnedSubtree(IOFile(state.ownedSub("files"), "cloud_storage_temp_files"))
        clearOwnedSubtree(IOFile(state.ownedSub("cache"), "cloud_storage"))
        provider = CloudStorageProvider()
        provider.attachInfo(state.fixtureContext, ProviderInfo().apply {
            authority = appContext.getString(R.string.CLOUD_STORAGE_AUTHORITY)
            // Mirror production manifest requirements so real
            // DocumentsProvider.attachInfo runs its checks.
            exported = true
            grantUriPermissions = true
            readPermission = android.Manifest.permission.MANAGE_DOCUMENTS
            writePermission = android.Manifest.permission.MANAGE_DOCUMENTS
        })
        setProviderDisabled(false)
        seedFileRealm()
    }

    @After
    fun tearDown() {
        // Guarded failure path: a retained quiescence failure (from an earlier case, or
        // from THIS case's own barrier below) leaves everything exactly as left — no
        // quiesce, no consumption, no deletion on top of it. This case's own barrier
        // failure enters the guard inside quiesceProviderScope and aborts before any
        // destructive step the same way; the thrown error is aggregated with any original
        // test failure by JUnit4's RunAfters, so the original failure is never swallowed.
        retainedQuiescenceBlocker?.let { blocker ->
            throw AssertionError(
                "tearDown blocked by a retained quiescence failure — nothing is consumed or deleted. " +
                    "Retained blocker: $blocker",
            )
        }
        // QUIESCENCE FIRST — while every mock and the fixture ownership are still fully
        // valid: a late provider task can then complete normally into the owned fixtures
        // instead of failing against consumed state. A barrier failure throws before ANY
        // destructive step below: nothing is deleted, no global mock is restored and the
        // preserved namespace stays in place as evidence — the destructive steps never
        // run while a late, interrupt-ignoring task could still execute.
        if (::provider.isInitialized) quiesceProviderScope()

        // Consume the per-case state: nothing below may mistake a previous case's
        // namespace for this one's, and the next case starts from a clean slate.
        val state = caseState
        caseState = null
        if (state == null) return // setUp failed before publication: nothing was created

        val cleanupFailures = mutableListOf<String>()

        fun checked(label: String, block: () -> Unit) {
            try {
                block()
            } catch (throwable: Throwable) {
                cleanupFailures += "$label: $throwable"
            }
        }

        // In-memory fixture state only — the durable app-lock settings store is not touched.
        appLockFixture._appSecurityEnabled = false

        // Realm deletions are gated: the owned root is recursively erased ONLY if every
        // owned realm deleted cleanly. Realm.deleteRealm returns a Boolean: a false
        // return or a file still present afterwards is a FAILED deletion and never
        // authorizes recursive deletion; a realm this case never created may
        // legitimately be absent and is not a failure — the resulting state is verified,
        // not the return value alone. A failed deletion (an open handle or an uncancelled
        // job still holds the file) PRESERVES the root in place as evidence and reports
        // the failure — it never erases on top of the failed deletion. Later cases never
        // touch a preserved root: they create their own exclusively created unique roots.
        var allOwnedRealmsDeleted = true
        if (state.hasOwnedRoot) {
            val ownedRealmConfigs = fileFixtureConfigs.values +
                syncFixtureConfig(state.ownedRoot) +
                driveInfosFixtureConfig(state.ownedRoot)
            ownedRealmConfigs.forEach { config ->
                val realmFile = PlatformFile(config.path)
                checked("delete owned fixture realm ${realmFile.name}") {
                    try {
                        if (!realmFile.exists()) return@checked // never created by this case — legitimate
                        val deleted = Realm.deleteRealm(config)
                        check(deleted && !realmFile.exists()) {
                            "deleteRealm reported deleted=$deleted but ${realmFile.absolutePath} still exists"
                        }
                    } catch (throwable: Throwable) {
                        allOwnedRealmsDeleted = false
                        throw throwable
                    }
                }
            }
            if (allOwnedRealmsDeleted) {
                checked("delete owned fixture root") {
                    check(state.ownedRoot.deleteRecursively() && !state.ownedRoot.exists()) {
                        "owned fixture root not removable: ${state.ownedRoot}"
                    }
                }
            } else {
                cleanupFailures += "owned fixture root PRESERVED as evidence after a failed realm deletion " +
                    "(an open handle or uncancelled job may hold it): ${state.ownedRoot}"
            }
        }
        checked("delete owned fixture preferences") {
            // Synchronous flush barrier (android-15 framework source): the provider
            // setter writes via apply(), whose disk write is queued, and
            // deleteSharedPreferences does not wait for queued writes — one flushing
            // late could recreate the deleted file. A no-op commit() blocks until all
            // outstanding async writes for this instance are on disk before returning.
            // Grounded in the framework source; not asserted as this fixture's
            // historical cause.
            val flushed = appContext
                .getSharedPreferences(state.fixturePrefsName(PROVIDER_PREFS_NAME), Context.MODE_PRIVATE)
                .edit()
                .commit()
            check(flushed) {
                "owned fixture preferences flush barrier failed (commit=false) — deletion skipped, file preserved"
            }
            val deleted = appContext.deleteSharedPreferences(state.fixturePrefsName(PROVIDER_PREFS_NAME))
            // Verify the resulting state on the real filesystem (the standard
            // dataDir/shared_prefs layout — the per-case UNIQUE name is the only
            // isolation SharedPreferences offers) instead of trusting only the API's
            // return value: a file this case never created is legitimately absent; one
            // that still exists after the call is a failed deletion. The XML and its
            // write-time backup (.bak) are both checked: deleteSharedPreferences
            // removes both, so either one lingering is a failed deletion.
            val prefsFile = PlatformFile(
                appContext.applicationInfo.dataDir,
                "shared_prefs/${state.fixturePrefsName(PROVIDER_PREFS_NAME)}.xml",
            )
            val prefsBackupFile = PlatformFile(prefsFile.path + ".bak")
            check(!prefsFile.exists() && !prefsBackupFile.exists()) {
                "owned fixture preferences not removable (deleteSharedPreferences=$deleted): " +
                    "$prefsFile / $prefsBackupFile"
            }
        }

        // JUnit4 RunAfters aggregates this WITH the original test failure, so cleanup
        // damage is reported without masking the first error.
        if (cleanupFailures.isNotEmpty()) {
            fail("checked cleanup failures (the original failure, if any, is preserved): $cleanupFailures")
        }
    }

    /**
     * The completion barrier for this case's provider work. The production scope is
     * `private val cloudScope = CoroutineScope(CoroutineName + single-thread executor
     * dispatcher)` (CloudStorageProvider.kt): the CoroutineScope(context) FACTORY adds
     * a Job when the context lacks one — root-verified against the cached
     * kotlinx-coroutines-core-jvm 1.11.0 bytecode — so the real handle exists and is
     * reached here TEST-OWNED via reflection; no production change. Cancellation alone,
     * or executor shutdownNow (best-effort, waits for nothing), is NOT proof of
     * completion: the barrier is cancel-then-join under a deadline. A close listener
     * still QUEUED at cancel time launches into an already-cancelled parent, so its
     * body never runs (the production listener's only pre-launch action is the
     * exception check — the store/schedule/delete chain lives inside the coroutine).
     * The owned dispatcher is closed only AFTER quiescence, to avoid leaking the
     * per-case executor thread.
     *
     * EVERY failure mode — an error reaching the scope/Job (reflection, a missing Job)
     * as well as a timeout — routes through ONE guarded failure path: the failing
     * case's state, fixtures and the global mocks are RETAINED (see the companion
     * guard), and the thrown error aborts this tearDown before any destructive step.
     * A dispatcher-close failure after a proven join cannot leave work running; it
     * propagates un-guarded as an ordinary teardown failure.
     */
    private fun quiesceProviderScope() {
        val scope: CoroutineScope
        val job: Job
        try {
            scope = CloudStorageProvider::class.java.getDeclaredField("cloudScope")
                .apply { isAccessible = true }
                .get(provider) as CoroutineScope
            job = scope.coroutineContext[Job] ?: error(
                "the provider scope exposes no Job — the CoroutineScope(context)-factory " +
                    "counterevidence no longer holds and no bounded safe teardown exists " +
                    "without a production seam",
            )
        } catch (failure: Throwable) {
            retainQuiescenceFailure(
                "the completion barrier could not be reached (scope reflection or Job lookup): $failure",
            )
            throw AssertionError(retainedTeardownMessage(), failure)
        }
        val timedOut: Boolean
        try {
            timedOut = runBlocking {
                withTimeoutOrNull(QUIESCE_TIMEOUT_MS) {
                    job.cancelAndJoin()
                    false
                } ?: true
            }
        } catch (failure: Throwable) {
            retainQuiescenceFailure("the join barrier itself failed: $failure")
            throw AssertionError(retainedTeardownMessage(), failure)
        }
        if (timedOut) {
            retainQuiescenceFailure(
                "provider work did not quiesce within ${QUIESCE_TIMEOUT_MS}ms " +
                    "(job active=${job.isActive} cancelled=${job.isCancelled} completed=${job.isCompleted})",
            )
            throw AssertionError(retainedTeardownMessage())
        }
        (scope.coroutineContext[ContinuationInterceptor] as? ExecutorCoroutineDispatcher)?.close()
    }

    /** Records the FIRST barrier failure (detail + provider) as the retained guard state. */
    private fun retainQuiescenceFailure(detail: String) {
        if (retainedQuiescenceBlocker == null) {
            retainedQuiescenceBlocker = detail
            retainedProvider = provider
        }
    }

    // ---- the access gate ----------------------------------------------------------

    @Test
    fun anAppLockEnabledBlocksEveryGuardedEntryPoint() {
        appLockFixture._appSecurityEnabled = true
        try {
            val lockError = appContext.getString(R.string.cloudStorageAppLockError)

            assertSecurityException(fileDocId()) { provider.queryDocument(it, null).use { } }
            assertSecurityException(fileDocId()) { provider.openDocument(it, "r", null) }
            assertSecurityException(fileDocId()) { provider.openDocumentThumbnail(it, Point(64, 64), null) }
            assertSecurityException(folderDocId()) {
                provider.createDocument(it, DocumentsContract.Document.MIME_TYPE_DIR, "x")
            }
            assertSecurityException(fileDocId()) { provider.deleteDocument(it) }
            val renamed = assertSecurityException(fileDocId()) { provider.renameDocument(it, "y") }
            assertEquals(lockError, renamed.message)
            assertSecurityException(fileDocId()) { provider.copyDocument(it, folderDocId()) }
            assertTwoDocumentSecurityException(fileDocId(), folderDocId()) { source, target ->
                provider.moveDocument(source, folderDocId(), target)
            }

            provider.queryChildDocuments(folderDocId(), null as Array<out String>?, null as String?).use { cursor ->
                assertEquals(0, cursor.count)
                assertEquals(lockError, cursor.extras?.getString(DocumentsContract.EXTRA_ERROR))
            }
            provider.querySearchDocuments(rootDocId(), "query", null).use { cursor ->
                assertEquals(0, cursor.count)
                assertEquals(lockError, cursor.extras?.getString(DocumentsContract.EXTRA_ERROR))
            }
            // Denied-by-false contract on the relationship check (no exception, by design).
            assertFalse(provider.isChildDocument(folderDocId(), fileDocId()))
            provider.queryRoots(null).use { cursor ->
                // Identity is mocked to an empty user list: the blocked path is reachable
                // and non-throwing; email redaction itself is only vacuously covered.
                assertEquals(0, cursor.count)
            }
        } finally {
            appLockFixture._appSecurityEnabled = false
        }
    }

    @Test
    fun anAppLockTransitionReopensTheGateThroughTheRealPerCallRead() {
        provider.queryDocument(fileDocId(), null).use { cursor ->
            assertEquals(1, cursor.count)
        }
        appLockFixture._appSecurityEnabled = true
        try {
            assertSecurityException(fileDocId()) { provider.queryDocument(it, null).use { } }
        } finally {
            appLockFixture._appSecurityEnabled = false
        }
        provider.queryDocument(fileDocId(), null).use { cursor ->
            assertEquals(1, cursor.count)
        }
    }

    @Test
    fun aProviderOptOutBlocksWhileAppLockIsOffAndAppLockWinsOverIt() {
        val optOutError = appContext.getString(R.string.fileProviderExtensionError)
        val lockError = appContext.getString(R.string.cloudStorageAppLockError)

        setProviderDisabled(true)
        val optOutException = assertSecurityException(fileDocId()) { provider.queryDocument(it, null).use { } }
        assertEquals(optOutError, optOutException.message)

        setProviderDisabled(false)
        provider.queryDocument(fileDocId(), null).use { cursor -> assertEquals(1, cursor.count) }

        setProviderDisabled(true)
        appLockFixture._appSecurityEnabled = true
        try {
            val lockException = assertSecurityException(fileDocId()) { provider.queryDocument(it, null).use { } }
            assertEquals(lockError, lockException.message)
        } finally {
            appLockFixture._appSecurityEnabled = false
            setProviderDisabled(false)
        }
    }

    @Test
    fun searchWithoutABrowsedDriveParentReportsTheRealContractError() {
        // Order-independence: perform one real root query first so the provider's
        // browsed-parent state points at a root id (DriveInfos are the real empty owned
        // drive-info Realm input),
        // then assert the real contract error for a search without a browsed drive.
        provider.queryChildDocuments(rootDocId(), null as Array<out String>?, null as String?).use { }
        provider.querySearchDocuments(rootDocId(), "query", null).use { cursor ->
            assertEquals(
                appContext.getString(R.string.cloudStorageQuerySearchImpossibleSelectDrive),
                cursor.extras?.getString(DocumentsContract.EXTRA_ERROR),
            )
        }
    }

    // ---- scoped queries ------------------------------------------------------------

    @Test
    fun sharedChildrenKeepTheDocumentUsersRealmAndTransportScope() {
        val originalUser = AccountUtils.currentUserId
        val originalDrive = AccountUtils.currentDriveId
        val documentScope = UserDrive(USER_ID, -1, sharedWithMe = true)
        every { AccountUtils.currentUserId } returns USER_ID + 1
        every { AccountUtils.currentDriveId } returns OTHER_DRIVE_ID
        mockkObject(FolderFilesProvider)
        var reached = false
        every { FolderFilesProvider.getCloudStorageFiles(any(), any(), any(), any(), any(), any(), any()) } answers {
            val realm = firstArg<Realm>()
            assertEquals(documentScope, thirdArg<UserDrive>())
            assertEquals(FileController.getRealmConfiguration(documentScope).path, realm.path)
            reached = true
        }
        try {
            // The synchronous production fill seam is also called by queryChildDocuments;
            // no asynchronous provider job or real transport is started here.
            provider.loadSharedWithMeChildren("$USER_ID/-2", File.SortType.NAME_AZ) { }
            assertTrue(reached)
        } finally {
            unmockkObject(FolderFilesProvider)
            every { AccountUtils.currentUserId } returns originalUser
            every { AccountUtils.currentDriveId } returns originalDrive
        }
    }

    @Test
    fun copyAndMoveRejectCollidingIdsFromOtherScopesBeforeTransport() {
        mockkObject(ApiRepository)
        every { ApiRepository.duplicateFile(any(), any(), any()) } answers { error("unexpected copy dispatch") }
        every { ApiRepository.moveFile(any(), any(), any()) } answers { error("unexpected move dispatch") }
        try {
            val foreignTargets = listOf(
                otherDriveDocId(PARENT_ID),
                "${USER_ID + 1}/${DRIVE_NAME}@$DRIVE_ID/$PARENT_ID",
                "$USER_ID/-2/${DRIVE_NAME}@$DRIVE_ID/$PARENT_ID",
            )
            foreignTargets.forEach { target ->
                assertSecurityException(fileDocId()) { provider.copyDocument(it, target) }
                assertSecurityException(fileDocId()) { provider.moveDocument(it, folderDocId(), target) }
                assertSecurityException(fileDocId()) { provider.moveDocument(it, target, folderDocId()) }
            }
            verify(exactly = 0) { ApiRepository.duplicateFile(any(), any(), any()) }
            verify(exactly = 0) { ApiRepository.moveFile(any(), any(), any()) }
        } finally {
            unmockkObject(ApiRepository)
        }
    }

    @Test
    fun sameScopeCopyAndMoveUseTheDocumentsClientAndRows() {
        val client = mockk<OkHttpClient>()
        val copiedId = FILE_ID + 10
        mockkObject(ApiRepository)
        coEvery { AccountUtils.getHttpClient(USER_ID) } returns client
        every { ApiRepository.duplicateFile(any(), any(), client) } answers {
            assertEquals(FILE_ID, firstArg<File>().id)
            assertEquals(DRIVE_ID, firstArg<File>().driveId)
            ApiResponse(result = ApiResponseStatus.SUCCESS, data = File(id = copiedId, driveId = DRIVE_ID))
        }
        every { ApiRepository.moveFile(any(), any(), client) } answers {
            assertEquals(FILE_ID, firstArg<File>().id)
            assertEquals(DRIVE_ID, firstArg<File>().driveId)
            assertEquals(PARENT_ID, secondArg<File>().id)
            ApiResponse(result = ApiResponseStatus.SUCCESS)
        }
        try {
            val copiedDocumentId = provider.copyDocument(fileDocId(), folderDocId())
            assertEquals("$USER_ID/${DRIVE_NAME}@$DRIVE_ID/$copiedId", copiedDocumentId)
            assertFalse("a successful copy has a new identity", copiedDocumentId == fileDocId())
            provider.moveDocument(fileDocId(), folderDocId(), folderDocId())
            verify(exactly = 1) {
                ApiRepository.duplicateFile(any(), PARENT_ID, client)
            }
            verify(exactly = 1) {
                ApiRepository.moveFile(any(), any(), client)
            }
        } finally {
            coEvery { AccountUtils.getHttpClient(USER_ID) } throws IOException(NETWORK_BOUNDARY_MESSAGE)
            unmockkObject(ApiRepository)
        }
    }

    @Test
    fun copiedIdentityKeepsTheCompleteSharedTargetNamespace() {
        val scope = USER_DRIVE.copy(sharedWithMe = true)
        val rows = FileController.getRealmInstance(USER_DRIVE).use { realm ->
            realm.copyFromRealm(realm.where(File::class.java).findAll())
        }
        FileController.getRealmInstance(scope).use { realm ->
            realm.executeTransaction { it.copyToRealm(rows) }
        }
        val client = mockk<OkHttpClient>()
        val copiedId = FILE_ID + 10
        val prefix = "$USER_ID/-2/${DRIVE_NAME}@$DRIVE_ID"
        mockkObject(ApiRepository)
        coEvery { AccountUtils.getHttpClient(USER_ID) } returns client
        every { ApiRepository.duplicateFile(any(), PARENT_ID, client) } returns
            ApiResponse(result = ApiResponseStatus.SUCCESS, data = File(id = copiedId, driveId = DRIVE_ID))
        try {
            val result = provider.copyDocument("$prefix/$FILE_ID", "$prefix/$PARENT_ID")
            assertEquals("$prefix/$copiedId", result)
            assertFalse(result == "$prefix/$FILE_ID")
            verify(exactly = 1) { ApiRepository.duplicateFile(any(), PARENT_ID, client) }
        } finally {
            coEvery { AccountUtils.getHttpClient(USER_ID) } throws IOException(NETWORK_BOUNDARY_MESSAGE)
            unmockkObject(ApiRepository)
        }
    }

    @Test
    fun successfulCopyWithoutAValidNewIdentityIsRejectedWithoutReplay() {
        val client = mockk<OkHttpClient>()
        mockkObject(ApiRepository)
        coEvery { AccountUtils.getHttpClient(USER_ID) } returns client
        val invalidCopies = listOf(
            null,
            File(id = 0, driveId = DRIVE_ID),
            File(id = -1, driveId = DRIVE_ID),
            File(id = Utils.ROOT_ID, driveId = DRIVE_ID),
            File(id = FILE_ID, driveId = DRIVE_ID),
            File(id = FILE_ID + 10, driveId = OTHER_DRIVE_ID),
        )
        try {
            invalidCopies.forEach { returnedFile ->
                clearMocks(ApiRepository, answers = false)
                every { ApiRepository.duplicateFile(any(), PARENT_ID, client) } returns
                    ApiResponse(result = ApiResponseStatus.SUCCESS, data = returnedFile)
                try {
                    provider.copyDocument(fileDocId(), folderDocId())
                    fail("missing or invalid new identity must not return a document")
                } catch (_: IllegalStateException) {
                    // The already-issued mutation must never be repeated to obtain its ID.
                }
                verify(exactly = 1) { ApiRepository.duplicateFile(any(), PARENT_ID, client) }
            }
        } finally {
            coEvery { AccountUtils.getHttpClient(USER_ID) } throws IOException(NETWORK_BOUNDARY_MESSAGE)
            unmockkObject(ApiRepository)
        }
    }

    @Test
    fun copyAndMoveRejectAFileAsTheTargetBeforeTransport() {
        mockkObject(ApiRepository)
        try {
            try {
                provider.copyDocument(fileDocId(), fileDocId())
                fail("copy target must be a folder")
            } catch (_: IllegalArgumentException) { }
            try {
                provider.moveDocument(fileDocId(), folderDocId(), fileDocId())
                fail("move target must be a folder")
            } catch (_: IllegalArgumentException) { }
            verify(exactly = 0) { ApiRepository.duplicateFile(any(), any(), any()) }
            verify(exactly = 0) { ApiRepository.moveFile(any(), any(), any()) }
        } finally {
            unmockkObject(ApiRepository)
        }
    }

    @Test
    fun aScopedQueryServesSeededRowsAndKeepsOtherScopesEmpty() {
        provider.queryDocument(fileDocId(), null).use { cursor ->
            assertEquals(1, cursor.count)
            assertTrue(cursor.moveToFirst())
            assertEquals(
                FILE_NAME,
                cursor.getString(cursor.getColumnIndexOrThrow(DocumentsContract.Document.COLUMN_DISPLAY_NAME))
            )
            assertEquals(
                FILE_BYTES.size.toLong(),
                cursor.getLong(cursor.getColumnIndexOrThrow(DocumentsContract.Document.COLUMN_SIZE))
            )
        }
        // A stale/wrong scope never leaks another drive's rows.
        provider.queryDocument(otherDriveDocId(FILE_ID), null).use { cursor ->
            assertEquals(0, cursor.count)
        }
        provider.queryDocument(fileDocId().substringBeforeLast('/') + "/${FILE_ID + 1}", null).use { cursor ->
            assertEquals(0, cursor.count)
        }
    }

    @Test
    fun childDocumentsServeTheRootContractSynchronouslyOffline() {
        // TRACED per the 0713 follow-up review: the asynchronous fill for a REAL folder
        // cannot complete under this harness's declared no-transfer boundary —
        // FolderFilesProvider.getCloudStorageFiles requires the transport (getHttpClient)
        // BEFORE serving any row, and its failure path is an unhandled launch failure in
        // the production cloudScope (no CoroutineExceptionHandler; the synchronous branch
        // is gated on the private isChromeOs flag), which would crash the instrumentation
        // process. The harness therefore proves the REAL offline child-cursor contract on
        // the synchronous root path — virtual containers, loading extras and notification
        // registration — and never launches a folder-fill coroutine at all. The smallest
        // production-seam option (an injectable exception boundary around the fill
        // launch, or an injectable offline branch gate) is recorded for the orchestrator.
        provider.queryChildDocuments(rootDocId(), null as Array<out String>?, null as String?).use { cursor ->
            // DriveInfos are the real empty owned drive-info Realm: no drive rows, only
            // the two virtual containers the provider itself adds, synchronously.
            assertEquals(2, cursor.count)
            assertTrue(cursor.extras?.getBoolean(DocumentsContract.EXTRA_LOADING) == true)
            assertEquals(
                DocumentCursor.createUri(appContext, rootDocId()),
                cursor.notificationUri,
            )
        }
        // Bounded residual (deliberately NOT asserted): the cursor's self-observer
        // registration is really performed — the hidden four-argument
        // registerContentObserver of the real AbstractCursor chain cannot be intercepted
        // by this harness — as a TRANSIENT observer on the app's OWN authority, closed
        // with the cursor. No delivery to another process, no persisted grant and no
        // durable lock is claimed or counted here.
    }

    @Test
    fun childRelationshipFollowsTheScopedContract() {
        assertTrue(provider.isChildDocument(folderDocId(), fileDocId()))
        assertFalse(provider.isChildDocument(otherDriveDocId(PARENT_ID), fileDocId()))
        assertFalse(provider.isChildDocument(folderDocId(), driveDocId()))
    }

    // ---- scoped read streams -------------------------------------------------------

    @Test
    fun aReadStreamServesTheFreshCacheWithoutAnyNetwork() {
        val cache = cacheFile()
        cache.parentFile?.mkdirs()
        cache.writeBytes(FILE_BYTES)
        assertTrue(cache.setLastModified(MTIME_SECONDS * 1_000))

        val pfd = provider.openDocument(fileDocId(), "r", null)
        assertNotNull(pfd)
        ParcelFileDescriptor.AutoCloseInputStream(pfd!!).use { stream ->
            assertTrue(stream.readBytes().contentEquals(FILE_BYTES))
        }
        assertTrue(cache.exists())
        assertTrue(cache.readBytes().contentEquals(FILE_BYTES))
    }

    @Test
    fun aStaleCacheIsNotServedAndTheFailedDownloadLeavesItUntouched() {
        val cache = cacheFile()
        cache.parentFile?.mkdirs()
        val staleBytes = "stale-providerx-bytes".toByteArray()
        cache.writeBytes(staleBytes)
        assertTrue(cache.setLastModified((MTIME_SECONDS - 1_000) * 1_000))

        assertNull(provider.openDocument(fileDocId(), "r", null))
        assertTrue(cache.exists())
        assertTrue(cache.readBytes().contentEquals(staleBytes))
    }

    // ---- the pending write-close boundary ------------------------------------------

    @Test
    fun theWriteCloseBoundaryStoresThePendingUploadOnlyAfterClose() {
        val cache = cacheFile()
        cache.parentFile?.mkdirs()
        cache.writeBytes("stale-cache-to-be-deleted".toByteArray())

        // Before the descriptor exists, nothing may be stored or scheduled.
        assertNull("nothing is stored before the write descriptor exists", storedUpload())
        assertEquals(0, syncImmediatelyCalls.get())

        val descriptor = provider.openDocument(fileDocId(), "w", null)
        assertNotNull(descriptor)
        val stream = ParcelFileDescriptor.AutoCloseOutputStream(descriptor!!)
        try {
            stream.write(FILE_BYTES)
            // The pending-open boundary, observed while the descriptor is STILL open:
            // the close coroutine cannot have started, so nothing may be stored or
            // scheduled yet.
            assertNull("nothing is stored while the write descriptor is still open", storedUpload())
            assertEquals(
                "nothing is scheduled while the write descriptor is still open",
                0,
                syncImmediatelyCalls.get(),
            )
        } finally {
            stream.close() // triggers the production close listener exactly once
        }

        // Bounded observable completion of the close coroutine: dropping the old cache
        // is its LAST effect (store -> schedule -> delete, in that order inside the same
        // coroutine), so observing the cache gone — together with the row and the
        // scheduling signal — bounds the whole chain before the final assertions and
        // the checked cleanup. If the chain dies midway, this fails loudly instead of
        // letting cleanup erase the evidence.
        assertTrue(
            "the close coroutine did not complete within the deadline (old cache not dropped)",
            awaitUntil(15_000) { !cache.exists() },
        )
        val stored = storedUpload()
        assertNotNull(stored)
        assertEquals(DRIVE_ID, stored!!.driveId)
        assertEquals(USER_ID, stored.userId)
        assertEquals(PARENT_ID, stored.remoteFolder)
        assertEquals(FILE_BYTES.size.toLong(), stored.fileSize)
        assertEquals(UploadFile.Type.CLOUD_STORAGE.name, stored.type)
        assertTrue(
            "the upload source must stay in the owned temp namespace",
            stored.uri.contains("cloud_storage_temp_files/$PARENT_ID"),
        )
        assertEquals("the real scheduling seam must have been called exactly once", 1, syncImmediatelyCalls.get())
        assertTrue(tempFile().exists()) // the upload source is preserved
        assertFalse(cache.exists()) // the old cache is dropped (completion already observed)
    }

    // ---- helpers -------------------------------------------------------------------

    /**
     * Detached copy: the managed row is copied inside the live Realm before it closes,
     * so assertions never touch a closed Realm's objects.
     */
    private fun storedUpload(): UploadFile? = UploadFile.getRealmInstance().use { realm ->
        realm.where(UploadFile::class.java)
            .equalTo(UploadFile::fileName.name, FILE_NAME)
            .equalTo(UploadFile::type.name, UploadFile.Type.CLOUD_STORAGE.name)
            .findFirst()?.let { realm.copyFromRealm(it, 0) }
    }

    /** Owned renamed preferences only; the real provider prefs file is never opened. */
    private fun setProviderDisabled(disabled: Boolean) {
        CloudStorageProvider.setDisabled(currentCase.fixtureContext, disabled)
        assertEquals(disabled, CloudStorageProvider.isDisabled(currentCase.fixtureContext))
    }

    private fun seedFileRealm() {
        val config = FileController.getRealmConfiguration(USER_DRIVE) // stubbed → owned fixture file
        // No pre-deletion: the per-case root was created empty moments ago, so the
        // fixture file cannot pre-exist (fail-don't-clean rule).
        Realm.getInstance(config).use { realm ->
            realm.executeTransaction {
                it.copyToRealm(
                    File(
                        uid = "${PARENT_ID}_${DRIVE_ID}",
                        id = PARENT_ID,
                        parentId = Utils.ROOT_ID,
                        driveId = DRIVE_ID,
                        name = "FixtureFolder",
                        type = "dir",
                        lastModifiedAt = MTIME_SECONDS,
                    )
                )
                it.copyToRealm(
                    File(
                        uid = FILE_UID,
                        id = FILE_ID,
                        parentId = PARENT_ID,
                        driveId = DRIVE_ID,
                        name = FILE_NAME,
                        type = "file",
                        lastModifiedAt = MTIME_SECONDS,
                        size = FILE_BYTES.size.toLong(),
                    )
                )
            }
        }
    }

    private fun clearOwnedSubtree(dir: IOFile?) {
        if (dir == null || !dir.exists()) return
        if (!(dir.deleteRecursively() && !dir.exists())) {
            fail("owned fixture subtree not clearable before the run: $dir")
        }
    }

    private fun awaitUntil(timeoutMs: Long = 10_000, condition: () -> Boolean): Boolean {
        val deadline = SystemClock.elapsedRealtime() + timeoutMs
        while (SystemClock.elapsedRealtime() < deadline) {
            if (condition()) return true
            Thread.sleep(50)
        }
        return condition()
    }

    private fun assertSecurityException(documentId: String, block: (String) -> Unit): SecurityException {
        try {
            block(documentId)
        } catch (exception: SecurityException) {
            return exception
        }
        fail("expected SecurityException for $documentId")
        throw IllegalStateException("unreachable")
    }

    private fun assertTwoDocumentSecurityException(
        firstDocumentId: String,
        secondDocumentId: String,
        block: (String, String) -> Unit,
    ): SecurityException {
        try {
            block(firstDocumentId, secondDocumentId)
        } catch (exception: SecurityException) {
            return exception
        }
        fail("expected SecurityException for $firstDocumentId / $secondDocumentId")
        throw IllegalStateException("unreachable")
    }
}
