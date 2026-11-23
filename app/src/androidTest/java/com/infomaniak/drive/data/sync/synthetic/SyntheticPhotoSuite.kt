/*
 * Copyright (C) 2026 Infomaniak Network SA.
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.infomaniak.drive.data.sync.synthetic

import android.app.Instrumentation
import android.content.Context
import android.content.ContentResolver
import android.content.ContentValues
import android.net.Uri
import android.os.Bundle
import androidx.test.internal.runner.TestExecutor
import androidx.test.core.app.ApplicationProvider
import androidx.test.internal.runner.listener.InstrumentationResultPrinter
import androidx.test.platform.app.InstrumentationRegistry
import com.infomaniak.core.auth.backup.RestoreFromBackupManager
import com.infomaniak.core.auth.room.UserDatabase
import com.infomaniak.core.common.getAndroidId
import com.infomaniak.core.network.NetworkConfiguration
import com.infomaniak.core.network.api.ApiController
import com.infomaniak.core.network.networking.HttpClientConfig
import com.infomaniak.drive.data.api.FileDeserialization
import com.infomaniak.drive.data.models.File
import io.realm.Realm
import io.mockk.coEvery
import io.mockk.every
import io.mockk.isMockKMock
import io.mockk.mockk
import io.mockk.mockkStatic
import io.mockk.spyk
import io.mockk.unmockkStatic
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.first
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import org.json.JSONObject
import org.json.JSONArray
import org.junit.runner.Description
import org.junit.runner.Request
import org.junit.runner.notification.Failure
import org.junit.runner.notification.RunListener
import org.junit.runners.model.RunnerBuilder
import org.junit.jupiter.api.Test
import okhttp3.Interceptor
import splitties.init.injectAsAppCtx
import java.io.IOException

/** Loaded only AFTER the parent verified UID/plain Application/APK identity. */
object SyntheticPhotoSuite {
    @JvmStatic
    fun run(host: Instrumentation, context: Context, input: Bundle): Bundle {
        val entry = input.getString("syntheticEntry")!!
        val events = SyntheticPhotoContract.Events(entry)
        val expected = SyntheticPhotoContract.methods(entry).size
        var phase = "REGISTRY"
        var completed = false
        var emptyAccountSdk = false
        var result = Bundle()
        var diagnostic: JSONArray? = null
        try {
            // These are the registry and AppCtx copies in THIS verified main/test loader,
            // not the parent instrumentation's unrelated library singleton instances.
            val args = Bundle().apply {
                putString("class", SyntheticPhotoContract.className(entry))
                putString("disableAnalytics", "true")
                putString("runnerBuilder", "de.mannodermaus.junit5.AndroidJUnit5Builder")
            }
            InstrumentationRegistry.registerInstance(host, args)
            phase = "APPLICATION_PROVIDER_CONTEXT_IDENTITY"
            val fixtureVisibleContext = ApplicationProvider.getApplicationContext<Context>()
            SyntheticPhotoContract.require(fixtureVisibleContext === context && context.applicationContext === context)
            phase = "SAME_LOADER_CODE_ORIGINS"
            SyntheticPhotoContract.require(context.javaClass.classLoader === context.classLoader)
            SyntheticPhotoContract.require(SyntheticPhotoSuite::class.java.classLoader === context.classLoader)
            SyntheticPhotoContract.require(Class.forName("io.mockk.MockKKt", false, context.classLoader).classLoader === context.classLoader)
            phase = "APP_CONTEXT"
            context.injectAsAppCtx()
            phase = "REALM"
            // Before loading either test class: their RealmConfiguration fields are eager.
            // Realm's default directory and all module configs now live in this run only.
            Realm.init(context)
            phase = "NETWORK_METADATA"
            // No MainApplication/AccountUtils.init/AuthConfiguration/Sentry/WorkManager.
            // This supplies only formatting metadata needed by the intercepted upload.
            // Calling NetworkConfiguration.init would post a real UI toast, so don't.
            NetworkConfiguration.appId = "com.infomaniak.drive.test"
            NetworkConfiguration.appVersionCode = 0
            NetworkConfiguration.appVersionName = "synthetic-photo"
            // A stray default Core client must fail before network IO. The fixed tests'
            // explicit fake transfer clients/intercepted API seams remain unchanged.
            HttpClientConfig.customInterceptors = listOf(Interceptor { throw IOException("SYNTHETIC_NETWORK_REJECTED") })
            ApiController.init(arrayListOf(File::class.java to FileDeserialization()))
            phase = "EMPTY_ACCOUNT_SDK"
            // CredentialManager's superclass initializes these SDK singletons before
            // MockK can replace AccountUtils. They see only a newly created, empty Room
            // DB in this run. Never initialize AccountUtils' live account/bootstrap.
            mockkStatic("com.infomaniak.core.common.AndroidIdKt")
            try {
                coEvery { getAndroidId() } returns "synthetic-photo"
                runBlocking {
                    withTimeout(10_000) {
                        withContext(Dispatchers.IO) {
                            SyntheticPhotoContract.require(UserDatabase.getDatabase().userDao().userCount() == 0)
                        }
                        RestoreFromBackupManager.instance.state.first { it == RestoreFromBackupManager.State.Settled }
                    }
                }
            } finally {
                unmockkStatic("com.infomaniak.core.common.AndroidIdKt")
            }
            emptyAccountSdk = true
            phase = "RESOLVER_MOCK_CREATE"
            // A strict mock has no provider transport. Unknown operations fail instead
            // of reaching MediaStore; only the fixtures' own later mocks supply data.
            val deniedResolver = mockk<ContentResolver>()
            phase = "RESOLVER_STUB_QUERY"
            every { deniedResolver.query(any(), any(), any(), any(), any()) } throws SecurityException("SYNTHETIC_RESOLVER_REJECTED")
            phase = "RESOLVER_STUB_OPEN"
            every { deniedResolver.openInputStream(any()) } throws SecurityException("SYNTHETIC_RESOLVER_REJECTED")
            phase = "RESOLVER_STUB_INSERT"
            every { deniedResolver.insert(any(), any<ContentValues>()) } throws SecurityException("SYNTHETIC_RESOLVER_REJECTED")
            phase = "RESOLVER_STUB_UPDATE"
            every { deniedResolver.update(any(), any(), any(), any()) } throws SecurityException("SYNTHETIC_RESOLVER_REJECTED")
            phase = "RESOLVER_STUB_DELETE"
            every { deniedResolver.delete(any(), any(), any()) } throws SecurityException("SYNTHETIC_RESOLVER_REJECTED")
            phase = "RESOLVER_MOCK_IDENTITY"
            SyntheticPhotoContract.require(isMockKMock(deniedResolver))
            phase = "RESOLVER_REFLECTION_INSTALL"
            context.javaClass.getMethod("installFixtureResolver", ContentResolver::class.java)
                .apply { isAccessible = true }.invoke(context, deniedResolver)
            val deniedUri = Uri.parse("content://synthetic.invalid/blocked")
            val rejectedOperations = listOf<() -> Any?>(
                { context.contentResolver.query(deniedUri, null, null, null, null) },
                { context.contentResolver.openInputStream(deniedUri) },
                { context.contentResolver.insert(deniedUri, ContentValues()) },
                { context.contentResolver.update(deniedUri, ContentValues(), null, null) },
                { context.contentResolver.delete(deniedUri, null, null) },
            )
            val probePhases = listOf("RESOLVER_PROBE_QUERY", "RESOLVER_PROBE_OPEN", "RESOLVER_PROBE_INSERT", "RESOLVER_PROBE_UPDATE", "RESOLVER_PROBE_DELETE")
            rejectedOperations.forEachIndexed { index, operation ->
                phase = probePhases[index]
                val rejection = runCatching { operation() }.exceptionOrNull()
                if (rejection !is SecurityException) {
                    if (rejection != null) throw rejection
                    throw IllegalStateException("SYNTHETIC_DENIAL_NOT_ENFORCED")
                }
            }
            // Exercise the actual formerly failing spy-recording boundary before JUnit.
            phase = "RESOLVER_SPY_CREATE"
            val contextSpy = spyk(context)
            phase = "RESOLVER_SPY_MOCK_IDENTITY"
            SyntheticPhotoContract.require(isMockKMock(contextSpy, regular = false, spy = true))
            phase = "RESOLVER_SPY_RECORD"
            every { contextSpy.contentResolver } returns deniedResolver
            phase = "RESOLVER_SPY_IDENTITY"
            SyntheticPhotoContract.require(contextSpy.contentResolver === deniedResolver)
            phase = "FIXED_INVENTORY"
            val type = Class.forName(SyntheticPhotoContract.className(entry), true, context.classLoader)
            SyntheticPhotoContract.inventory(entry, type.declaredMethods.filter { it.isAnnotationPresent(Test::class.java) }
                .map { it.name }.toTypedArray())
            phase = "JUNIT_BUILDER"
            // The existing builder is androidTestRuntimeOnly, just as for the standard
            // runner's fixed runnerBuilder argument; no new compile dependency is needed.
            val builder = Class.forName("de.mannodermaus.junit5.AndroidJUnit5Builder", true, context.classLoader)
                .getDeclaredConstructor().newInstance() as RunnerBuilder
            val runner = checkNotNull(builder.runnerForClass(type))
            phase = "JUNIT_DISCOVERY"
            val leaves = mutableListOf<String>()
            fun visit(description: Description) {
                if (description.children.isEmpty()) {
                    val bare = SyntheticPhotoContract.bare(entry, description.className, description.methodName)
                    SyntheticPhotoContract.require(bare != null)
                    leaves += bare!!
                } else description.children.forEach(::visit)
            }
            visit(runner.description)
            SyntheticPhotoContract.inventory(entry, leaves.toTypedArray())
            val audit = object : RunListener() {
                override fun testStarted(d: Description) = events.start(d.className, d.methodName)
                override fun testFinished(d: Description) = events.finish(d.className, d.methodName)
                override fun testFailure(f: Failure) = events.fail(f.description.className, f.description.methodName)
                override fun testIgnored(d: Description) = events.skip(d.className, d.methodName, true)
                override fun testAssumptionFailure(f: Failure) = events.skip(f.description.className, f.description.methodName, false)
            }
            phase = "JUNIT_EXECUTION"
            // Keep actual Jupiter execution and AndroidX started/finished/fail/skip output.
            // The audit checks completeness; it never emits a substitute PASS/status event.
            result = TestExecutor.Builder(host)
                .addRunListener(InstrumentationResultPrinter())
                .addRunListener(audit)
                .build().execute(Request.runner(runner))
            phase = "EMPTY_ACCOUNT_POSTCHECK"
            runBlocking {
                withContext(Dispatchers.IO) {
                    SyntheticPhotoContract.require(UserDatabase.getDatabase().userDao().userCount() == 0)
                }
            }
            phase = "SUMMARY"
            completed = events.complete()
        } catch (failure: Throwable) {
            // Only code identities and source-owned frames: never messages, values or paths.
            diagnostic = JSONArray()
            var cause: Throwable? = failure
            repeat(8) {
                val current = cause ?: return@repeat
                val frames = JSONArray()
                current.stackTrace.filter { it.className.startsWith("com.infomaniak.drive.data.sync.synthetic.") }
                    .take(24).forEach { frame ->
                        frames.put(JSONObject().put("class", frame.className).put("method", frame.methodName).put("line", frame.lineNumber))
                    }
                val kind = when {
                    current.javaClass.name != "io.mockk.MockKException" -> "NOT_MOCKK"
                    current.message?.startsWith("Missing mocked calls inside every") == true -> "MISSING_MOCKED_CALLS"
                    current.message?.startsWith("Failed matching mocking signature") == true -> "SIGNATURE_MISMATCH"
                    current.message?.startsWith("no answer found") == true -> "UNSTUBBED_CALL"
                    else -> "OTHER_MOCKK"
                }
                diagnostic!!.put(JSONObject().put("class", current.javaClass.name).put("kind", kind).put("frames", frames))
                cause = current.cause.takeUnless { it === current }
            }
            result.putString("syntheticFailure", "FIXED_SYNTHETIC_REJECTED")
            result.putString("syntheticPhase", phase)
        }
        val summary = JSONObject().apply {
            put("schema", 1)
            put("entry", entry)
            put("expected", expected)
            put("started", events.started())
            put("finished", events.finished())
            put("passed", events.passed())
            put("failed", events.failed())
            put("skipped", events.skipped())
            put("invalidEvents", events.invalid())
            put("completed", completed)
            put("emptySyntheticAccountSdk", emptyAccountSdk)
            put("phase", phase)
            diagnostic?.let { put("diagnostic", it) }
        }.toString()
        result.putString("syntheticResult", summary)
        result.putBoolean("syntheticCompleted", completed)
        return result
    }
}
