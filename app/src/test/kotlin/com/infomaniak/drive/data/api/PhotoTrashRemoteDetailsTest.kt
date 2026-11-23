/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2026 Infomaniak Network SA
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.infomaniak.drive.data.api

import com.google.gson.JsonDeserializer
import com.google.gson.JsonParser
import com.infomaniak.core.auth.networking.AuthHttpClientProvider
import com.infomaniak.core.network.api.ApiController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.PhotoPairing
import com.infomaniak.drive.data.sync.PhotoTrashRules
import io.mockk.every
import io.mockk.mockkObject
import io.mockk.unmockkObject
import java.io.IOException
import java.util.Collections
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withTimeout
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.mockwebserver.MockResponse
import okhttp3.mockwebserver.MockWebServer
import okhttp3.mockwebserver.SocketPolicy
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DynamicTest.dynamicTest
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestFactory

/** Only Android header construction is substituted; routes, client, HTTP status and File decoding are real. */
class PhotoTrashRemoteDetailsTest {
    private lateinit var server: MockWebServer
    private lateinit var client: OkHttpClient
    private lateinit var originalGson: com.google.gson.Gson
    private val requests = Collections.synchronizedList(mutableListOf<okhttp3.HttpUrl>())
    private val target = File(id = 31, driveId = 42)
    private val hash = "xxh3:974db6253cceaf8d"
    private val body get() = """{"result":"success","data":{"id":31,"drive_id":42,"type":"file","status":"trashed","size":99,"last_modified_at":1700000000,"hash":"$hash"}}"""

    fun setup() {
        requests.clear()
        server = MockWebServer().apply { start() }
        originalGson = ApiController.gson
        ApiController.init(arrayListOf(File::class.java to FileDeserialization()))
        mockkObject(AuthHttpClientProvider)
        every { AuthHttpClientProvider.authOkHttpClient } throws AssertionError("captured account client required")
        mockkObject(ApiController)
        every { ApiController.createRequest(any(), any(), any()) } answers {
            Request.Builder().url(firstArg<String>()).get().build()
        }
        client = OkHttpClient.Builder().retryOnConnectionFailure(false).addInterceptor { chain ->
            val request = chain.request()
            requests.add(request.url)
            chain.proceed(request.newBuilder().header("X-Synthetic-Account", "captured").url(server.url(request.url.encodedPath + "?" + request.url.encodedQuery)).build())
        }.build()
    }

    fun teardown() {
        ApiController.gson = originalGson
        unmockkObject(ApiController)
        unmockkObject(AuthHttpClientProvider)
        client.dispatcher.executorService.shutdownNow()
        client.connectionPool.evictAll()
        server.shutdown()
    }

    private fun scenario(block: suspend CoroutineScope.() -> Unit) = runBlocking {
        setup()
        try { block() } finally { teardown() }
    }

    private suspend fun observe(): File? = ApiRepository.getPhotoTrashRemoteFile(target, client)

    @Test
    fun authoritative404ThenExactHashBearingTrashCanBeObserved() = scenario {
        server.enqueue(MockResponse().setResponseCode(404).setBody("""{"result":"success","data":null}"""))
        server.enqueue(MockResponse().setBody(body))
        val file = observe()
        assertEquals(hash, file?.serverContentHash)
        assertEquals(2, server.requestCount)
        assertEquals(listOf("/3/drive/42/files/31", "/3/drive/42/trash/31"), requests.map { it.encodedPath })
        assertTrue(requests.all { it.queryParameter("with")!!.split(',').contains("hash") })
        repeat(2) { assertEquals("captured", server.takeRequest(2, TimeUnit.SECONDS)!!.getHeader("X-Synthetic-Account")) }
        assertEquals(PhotoTrashRules.RemoteDecision.RequestConsent, decide(file))
    }
    private fun decide(file: File?, bound: Boolean = true, denied: Boolean = false, pending: Long = 0L): PhotoTrashRules.RemoteDecision =
        PhotoTrashRules.decideRemotePropagation(
            pending, denied, PhotoTrashRules.LocalObservation(true, false, 99, 1000),
            PhotoTrashRules.RemoteObservation(file != null, file?.isTrashed() == true, file?.size,
                file?.lastModifiedAt, file?.serverContentHash, file),
            99, 1000, bound, hash,
        )

    @Test
    fun ordinary200HasNoFallbackAndRemoteRestoreClearsPendingDenial() = scenario {
        server.enqueue(MockResponse().setBody(body.replace("trashed", "ok")))
        val file = observe()
        assertEquals(31, file?.id)
        assertEquals(42, file?.driveId)
        assertEquals(PhotoTrashRules.RemoteDecision.ClearPending, decide(file, denied = true, pending = 50))
        assertEquals(1, server.requestCount)
    }

    @Test
    fun matchingTrashCannotOverrideDeniedOrUnboundLocalOrOutstandingConsent() = scenario {
        server.enqueue(MockResponse().setResponseCode(404))
        server.enqueue(MockResponse().setBody(body))
        val file = observe()
        assertEquals(PhotoTrashRules.RemoteDecision.Inconclusive, decide(file, bound = false))
        assertEquals(PhotoTrashRules.RemoteDecision.None, decide(file, denied = true))
        assertEquals(PhotoTrashRules.RemoteDecision.None, decide(file, pending = 50))
    }

    @Test
    fun differentCanonicalTrashHashIsAConflictNotConsent() = scenario {
        server.enqueue(MockResponse().setResponseCode(404))
        server.enqueue(MockResponse().setBody(body.replace(hash, "xxh3:0000000000000001")))
        assertEquals(PhotoTrashRules.RemoteDecision.Conflict(PhotoPairing.State.CONFLICT_REMOTE_CHANGED), decide(observe()))
    }

    @Test
    fun bodyErrorClaiming404OnHTTP200CannotSelectFallback() = scenario {
        server.enqueue(MockResponse().setBody("""{"result":"error","error":{"code":404,"description":"not found"}}"""))
        assertNull(observe())
        assertEquals(1, server.requestCount)
    }

    @TestFactory
    fun ordinaryNon404StatusesNeverSelectFallback() = listOf(204, 301, 400, 401, 403, 429, 500, 503).map { code ->
        dynamicTest("ordinary HTTP $code never selects trash") {
            scenario {
                // Even a success-shaped body must not override the wire status.
                server.enqueue(MockResponse().setResponseCode(code).setBody(body))
                assertNull(observe())
                assertEquals(1, server.requestCount)
            }
        }
    }

    @TestFactory
    fun fallbackMustItselfReturnHTTP200() = listOf(204, 400, 401, 403, 404, 500, 503).map { code ->
        dynamicTest("trash HTTP $code is inconclusive") {
            scenario {
                server.enqueue(MockResponse().setResponseCode(404))
                server.enqueue(MockResponse().setResponseCode(code).setBody(body))
                assertNull(observe())
                assertEquals(2, server.requestCount)
            }
        }
    }

    @TestFactory
    fun malformedOrWrongTrashIdentityNeverAuthorizes() = listOf(
        "missing id" to ("id" to null), "null id" to ("id" to "null"),
        "wrong id" to ("id" to "32"), "string id" to ("id" to "\"31\""),
        "fraction id" to ("id" to "31.0"), "missing drive" to ("drive_id" to null),
        "wrong drive" to ("drive_id" to "43"), "string drive" to ("drive_id" to "\"42\""),
        "missing type" to ("type" to null), "directory" to ("type" to "\"dir\""),
        "missing status" to ("status" to null), "restored" to ("status" to "\"ok\""),
        "substring status" to ("status" to "\"not_trashed\""),
        "missing size" to ("size" to null), "null size" to ("size" to "null"),
        "negative size" to ("size" to "-1"), "string size" to ("size" to "\"99\""),
        "overflow size" to ("size" to "9223372036854775808"),
        "missing mtime" to ("last_modified_at" to null),
        "missing hash" to ("hash" to null), "null hash" to ("hash" to "null"),
        "empty hash" to ("hash" to "\"\""), "wrong algorithm" to ("hash" to "\"sha256:00\""),
        "numeric hash" to ("hash" to "123"), "object hash" to ("hash" to "{}"),
    ).map { (label, mutation) ->
        dynamicTest("trash refuses $label") {
            scenario {
                val changed = JsonParser.parseString(body).asJsonObject
                changed.getAsJsonObject("data").apply {
                    if (mutation.second == null) remove(mutation.first)
                    else add(mutation.first, JsonParser.parseString(mutation.second))
                }
                server.enqueue(MockResponse().setResponseCode(404))
                server.enqueue(MockResponse().setBody(changed.toString()))
                assertNull(observe())
                assertEquals(2, server.requestCount)
            }
        }
    }

    @TestFactory
    fun wrongOrdinaryScopeAlsoRefusesWithoutFallback() = listOf("id", "drive_id", "type", "status", "size").map { field ->
        dynamicTest("ordinary refuses missing $field without fallback") {
            scenario {
                val changed = JsonParser.parseString(body).asJsonObject
                changed.getAsJsonObject("data").remove(field)
                server.enqueue(MockResponse().setBody(changed.toString()))
                assertNull(observe())
                assertEquals(1, server.requestCount)
            }
        }
    }

    @TestFactory
    fun wrongReturnedOrdinaryIdentityNeverAuthorizes() = listOf("id" to "32", "drive_id" to "43", "type" to "\"dir\"").map { (field, value) ->
        dynamicTest("ordinary refuses wrong $field without fallback") {
            scenario {
                val changed = JsonParser.parseString(body).asJsonObject
                changed.getAsJsonObject("data").add(field, JsonParser.parseString(value))
                server.enqueue(MockResponse().setBody(changed.toString()))
                assertNull(observe())
                assertEquals(1, server.requestCount)
            }
        }
    }

    @TestFactory
    fun malformedEnvelopeIsNotTrashProof() = listOf("", "not JSON", "null", "[]",
        """{"result":"success"}""", """{"result":"success","data":null}""",
        """{"result":"error","data":{}}""").mapIndexed { index, invalid ->
        dynamicTest("invalid trash envelope $index") {
            scenario {
                server.enqueue(MockResponse().setResponseCode(404))
                server.enqueue(MockResponse().setBody(invalid))
                assertNull(observe())
            }
        }
    }

    @Test
    fun ordinaryNetworkFailureCannotSelectFallback() = scenario {
        client = client.newBuilder().addInterceptor { throw IOException("synthetic") }.build()
        assertNull(observe())
        assertEquals(1, requests.size)
        assertEquals(0, server.requestCount)
    }

    @Test
    fun ordinaryReadTimeoutCannotSelectFallback() = scenario {
        client = client.newBuilder().readTimeout(100, TimeUnit.MILLISECONDS).build()
        server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE))
        assertNull(observe())
        assertEquals(1, server.requestCount)
    }

    @TestFactory
    fun cancellationPropagatesOnEitherRoute() = listOf(false, true).map { fallback ->
        dynamicTest("cancel ${if (fallback) "trash" else "ordinary"} call") {
            scenario {
                if (fallback) server.enqueue(MockResponse().setResponseCode(404))
                server.enqueue(MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE))
                withTimeout(5000) {
                    val job = async(Dispatchers.IO) { observe(); error("cancelled query returned a value") }
                    repeat(if (fallback) 2 else 1) { assertTrue(server.takeRequest(2, TimeUnit.SECONDS) != null) }
                    job.cancelAndJoin()
                    assertTrue(job.isCancelled)
                }
                assertEquals(if (fallback) 2 else 1, server.requestCount)
            }
        }
    }

    @TestFactory
    fun cancellationDuringDecodeCannotPublishAnObservationEvenOnIO() = listOf(false, true).map { fallback ->
        dynamicTest("cancel during ${if (fallback) "trash" else "ordinary"} decoding") {
            scenario {
                if (fallback) server.enqueue(MockResponse().setResponseCode(404))
                server.enqueue(MockResponse().setBody(body))
                lateinit var job: Deferred<Unit>
                val delegate = ApiController.gson
                ApiController.gson = delegate.newBuilder().registerTypeAdapter(File::class.java, JsonDeserializer<File> { json, type, _ ->
                    job.cancel()
                    delegate.fromJson<File>(json, type)
                }).create()
                val returned = AtomicBoolean(false)
                job = async(Dispatchers.IO, start = CoroutineStart.LAZY) {
                    observe()
                    returned.set(true)
                }
                job.start()
                withTimeout(5000) { job.join() }
                assertTrue(job.isCancelled)
                assertTrue(!returned.get(), "cancelled decode must not return an observation")
            }
        }
    }

}
