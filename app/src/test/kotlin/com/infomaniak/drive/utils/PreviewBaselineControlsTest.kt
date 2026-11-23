/* Copyright (C) 2026 Infomaniak Network SA. SPDX-License-Identifier: GPL-3.0-or-later */
package com.infomaniak.drive.utils

import android.content.Context
import android.net.Uri
import com.infomaniak.core.auth.networking.AuthHttpClientProvider
import com.infomaniak.core.network.networking.DefaultHttpClientProvider
import com.infomaniak.core.network.networking.HttpUtils
import com.infomaniak.drive.R
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkObject
import io.mockk.mockkStatic
import io.mockk.unmockkAll
import io.realm.RealmList
import java.nio.file.Files
import java.util.Collections
import java.util.concurrent.CountDownLatch
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeout
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import okhttp3.Headers
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrl
import okhttp3.OkHttpClient
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue

/** Real preview/cache/routes/HTTP/body copy; only platform metadata, Uri and client providers are substituted. */
@OptIn(ExperimentalCoroutinesApi::class, com.infomaniak.core.network.networking.ManualAuthorizationRequired::class)
class PreviewBaselineControlsTest {
    private val root = Files.createTempDirectory("n4-preview").toFile()
    private val server = MockWebServer()
    private val requests = Collections.synchronizedList(mutableListOf<Pair<String, HttpUrl>>())
    private val clients = mutableListOf<OkHttpClient>()
    private val main = Executors.newSingleThreadExecutor { Thread(it, "n4-main") }.asCoroutineDispatcher()
    private val context = mockk<Context>()
    private val scope = UserDrive(userId = 7, driveId = 42)
    private val pdf = "%PDF-1.4\nsynthetic converted document\n%%EOF".toByteArray()

    @BeforeEach
    fun setup() {
        server.start()
        Dispatchers.setMain(main)
        every { context.cacheDir } answers { assertFalse(Thread.currentThread().name == "n4-main"); root }
        every { context.filesDir } answers { assertFalse(Thread.currentThread().name == "n4-main"); root.resolve("exposed") }
        every { context.getString(R.string.EXPOSED_PUBLIC_SHARE_DIR) } returns "public-share"
        mockkObject(AuthHttpClientProvider, DefaultHttpClientProvider, HttpUtils)
        every { AuthHttpClientProvider.authOkHttpClient } returns client("auth")
        every { DefaultHttpClientProvider.okHttpClient } returns client("public")
        every { HttpUtils.getHeaders(any()) } returns Headers.Builder().build()
        // Android Uri has no executable implementation in local JVM tests. Keep production route selection real.
        mockkStatic(Uri::class)
        every { Uri.parse(any()) } answers {
            val builder = firstArg<String>().toHttpUrl().newBuilder()
            val uri = mockk<Uri>()
            val androidBuilder = mockk<Uri.Builder>()
            every { uri.buildUpon() } returns androidBuilder
            every { androidBuilder.appendQueryParameter(any(), any()) } answers {
                builder.addQueryParameter(firstArg(), secondArg()); androidBuilder
            }
            every { androidBuilder.toString() } answers { builder.build().toString() }
            uri
        }
    }

    private fun client(marker: String) = OkHttpClient.Builder().retryOnConnectionFailure(false).addInterceptor { chain ->
        val request = chain.request()
        requests.add(marker to request.url)
        chain.proceed(request.newBuilder().url(server.url(request.url.encodedPath +
            (request.url.encodedQuery?.let { "?$it" } ?: ""))).build())
    }.build().also { clients.add(it) }

    @AfterEach
    fun teardown() {
        unmockkAll()
        Dispatchers.resetMain()
        main.close()
        clients.forEach { it.dispatcher.executorService.shutdownNow(); it.connectionPool.evictAll() }
        server.shutdown()
        root.deleteRecursively()
    }

    private fun file(office: Boolean = false) = File(id = 83, driveId = 42, name = "synthetic.pdf", lastModifiedAt = 1).apply {
        size = 9
        if (office) supportedBy = RealmList("onlyoffice")
    }

    @Test
    fun originalAndConvertedCachesStayDistinctAndConvertedBytesAreNotValidatedAsOriginal() = runBlocking {
        val file = file(office = true)
        val original = withContext(Dispatchers.IO) { file.getCacheFile(context, scope) }
        val converted = withContext(Dispatchers.IO) { file.getConvertedPdfCache(context, scope) }
        original.writeText("ORIGINAL!")
        converted.writeBytes(pdf)
        original.setLastModified(2000)
        converted.setLastModified(2000)
        assertNotEquals(original.canonicalPath, converted.canonicalPath)
        val result = withContext(Dispatchers.Main) { file.convertToIOFile(context, scope, true, {}) }
        assertEquals(converted, result)
        assertArrayEquals(pdf, result.readBytes())
        file.supportedBy = null
        val nativeResult = withContext(Dispatchers.Main) { file.convertToIOFile(context, scope, false, {}) }
        assertEquals(original, nativeResult)
        assertEquals("ORIGINAL!", nativeResult.readText())
        assertEquals(0, server.requestCount)
    }

    @Test
    fun authenticatedConversionAndNativePdfUseCorrectProductionRoutes() = runBlocking {
        repeat(2) { server.enqueue(MockResponse().setHeader("Content-Type", "application/pdf").setBody(String(pdf))) }
        val office = file(true)
        val previous = withContext(Dispatchers.IO) { office.getConvertedPdfCache(context, scope) }
        previous.writeText("old-converted-preview")
        previous.setLastModified(0)
        val converted = withContext(Dispatchers.Main) { office.convertToIOFile(context, scope, true, {}) }
        assertArrayEquals(pdf, converted.readBytes())
        assertEquals(previous, converted)
        assertEquals(listOf("83"), requireNotNull(converted.parentFile).listFiles()!!.map { it.name })
        val native = file().apply { id = 84 }
        val original = withContext(Dispatchers.Main) { native.convertToIOFile(context, scope, true, {}) }
        assertArrayEquals(pdf, original.readBytes())
        assertEquals(listOf("auth", "auth"), requests.map { it.first })
        assertEquals("/2/drive/42/files/83/download", requests[0].second.encodedPath)
        assertEquals("pdf", requests[0].second.queryParameter("as"))
        assertEquals("/2/drive/42/files/84/download", requests[1].second.encodedPath)
        assertNull(requests[1].second.queryParameter("as"))
        assertTrue(converted.toPath().startsWith(root.resolve("converted_pdf").toPath()))
        assertTrue(original.toPath().startsWith(root.resolve("cloud_storage").toPath()))
    }

    @Test
    fun publicShareUsesPublicRouteAndUnauthenticatedClient() = runBlocking {
        server.enqueue(MockResponse().setHeader("Content-Type", "application/pdf").setBody(String(pdf)))
        val public = file().apply { publicShareUuid = "synthetic-share" }
        val output = withContext(Dispatchers.Main) { public.convertToIOFile(context, scope, true, {}) }
        assertEquals(root.resolve("exposed/public-share/synthetic.pdf"), output)
        assertArrayEquals(pdf, output.readBytes())
        assertEquals("public", requests.single().first)
        assertEquals("/2/app/42/share/synthetic-share/files/83/download", requests.single().second.encodedPath)
        assertNull(requests.single().second.queryParameter("as"))
    }

    @Test
    fun cancellationDuringStreamingDoesNotPublishStaleBytesOrReadOnMain() = runBlocking {
        val started = CountDownLatch(1)
        val release = CountDownLatch(1)
        val gate = AtomicBoolean()
        val completed = AtomicBoolean()
        val progressSamples = java.util.concurrent.atomic.AtomicInteger()
        val output = root.resolve("previous.pdf").apply { writeText("previous-valid-preview") }
        val previous = output.readBytes()
        server.enqueue(MockResponse().setHeader("Content-Type", "application/pdf").setBody("x".repeat(2 * 1024 * 1024)))
        val job = launch(Dispatchers.Main) {
            downloadFile(output, file(), true, { progress ->
                progressSamples.incrementAndGet()
                assertFalse(Thread.currentThread().name == "n4-main", "stream read/progress on Main")
                if (gate.compareAndSet(false, true)) {
                    started.countDown()
                    check(release.await(10, TimeUnit.SECONDS))
                }
                if (progress == 100) completed.set(true)
            }, false)
        }
        try {
            assertTrue(withContext(Dispatchers.IO) { started.await(10, TimeUnit.SECONDS) }, "actual streaming did not start")
            job.cancel()
        } finally { release.countDown() }
        withTimeout(10000) { job.join() }
        assertTrue(job.isCancelled)
        assertArrayEquals(previous, output.readBytes(), "cancelled stream replaced an existing valid preview")
        assertFalse(completed.get(), "cancelled stream published completion")
        assertEquals(1, progressSamples.get(), "copy continued reading after cancellation")
        assertEquals(listOf("previous.pdf"), root.listFiles()!!.map { it.name })
    }
}
