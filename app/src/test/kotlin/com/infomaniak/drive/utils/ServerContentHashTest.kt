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
package com.infomaniak.drive.utils

import kotlinx.coroutines.CancellationException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertInstanceOf
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream

/**
 * JVM tests for [ServerContentHash]'s STRICT codec grammar, its cancellation hook contract
 * and its stream-ownership contract. The known-answer coverage lives in
 * [ServerContentHashKatTest]; here the parse ladder, the canonical rendering, the hook
 * sequencing, the abort semantics and the caller-owned stream lifetime are pinned.
 */
class ServerContentHashTest {

    // ------------------------------------------------------------- THE PARSE LADDER

    @Test
    fun `absent parse for null and empty`() {
        assertInstanceOf(ServerContentHash.Parsed.Absent::class.java, ServerContentHash.parse(null))
        assertInstanceOf(ServerContentHash.Parsed.Absent::class.java, ServerContentHash.parse(""))
    }

    @Test
    fun `supported parse for the exact canonical form`() {
        val parsed = ServerContentHash.parse("xxh3:0123456789abcdef")
        assertInstanceOf(ServerContentHash.Parsed.Supported::class.java, parsed)
        assertEquals("xxh3:0123456789abcdef", (parsed as ServerContentHash.Parsed.Supported).canonical)
    }

    @Test
    fun `unsupported parse preserves the raw token verbatim`() {
        listOf(
            "md5:0123456789abcdef",
            "xxh3:", // the prefix alone
            "xxh3:0123456789abcde", // 15 digits
            "xxh3:0123456789abcdeff", // 17 digits
            "xxh3:0123456789ABCDE", // uppercase
            "xxh3:0123456789abcdeg", // non-hex digit
            "xxh3: 123456789abcde", // whitespace
            "  xxh3:0123456789abcdef", // leading whitespace — never trimmed
        ).forEach { raw ->
            val parsed = ServerContentHash.parse(raw)
            assertInstanceOf(ServerContentHash.Parsed.Unsupported::class.java, parsed, raw)
            assertEquals(raw, (parsed as ServerContentHash.Parsed.Unsupported).raw, raw)
        }
    }

    @Test
    fun `canonical rendering is the unsigned value padded to sixteen lowercase digits`() {
        assertEquals("xxh3:0000000000000000", ServerContentHash.canonical(0UL))
        assertEquals("xxh3:ffffffffffffffff", ServerContentHash.canonical(ULong.MAX_VALUE))
        // The padding pins the UNSIGNED rendering: a bare signed hex formatter would
        // print "ff" here.
        assertEquals("xxh3:00000000000000ff", ServerContentHash.canonical(255UL))
        assertEquals("xxh3:8000000000000000", ServerContentHash.canonical(0x8000000000000000uL))
    }

    // ------------------------------------------------- THE CANCELLATION HOOK CONTRACT

    /** The hook runs once before the first read, once after EVERY successful read, and once
     * after the final read — N successful chunks produce exactly the event sequence
     * hook, (read, hook)×N, hook. */
    @Test
    fun `hook fires before the first read, after every chunk, and after the last`() {
        val payload = ByteArray(10) { it.toByte() }
        listOf(1, 3, 4, 10, 64).forEach { chunk ->
            val events = mutableListOf<String>()
            val token = ServerContentHash.hashStreaming(
                EventInputStream(ByteArrayInputStream(payload), chunk, events),
                onCheck = { events.add("hook") },
            )
            assertTrue(token.startsWith("xxh3:"))
            val chunks = (payload.size + chunk - 1) / chunk
            val expected = buildList {
                add("hook")
                repeat(chunks) {
                    add("read")
                    add("hook")
                }
                add("hook")
            }
            assertEquals(expected, events, "chunk $chunk")
        }
    }

    /** A cancellation thrown by the hook aborts the hash MID-STREAM (2-byte delivery: the
     * abort lands on the after-chunk call, with bytes still unread) and propagates. */
    @Test
    fun `cancellation from the hook aborts the streaming hash`() {
        val events = mutableListOf<String>()
        val exception = assertThrows(CancellationException::class.java) {
            ServerContentHash.hashStreaming(
                EventInputStream(ByteArrayInputStream(ByteArray(100)), 2, events),
                onCheck = {
                    events.add("hook")
                    if (events.count { it == "hook" } == 3) throw CancellationException("cancelled mid-hash")
                },
            )
        }
        assertEquals("cancelled mid-hash", exception.message)
        // The abort landed on the third hook call — after two delivered chunks (4 of the
        // 100 payload bytes), with the payload far from exhausted: a genuine mid-stream
        // abort, not a post-computation one.
        assertEquals(listOf("hook", "read", "hook", "read", "hook"), events)
    }

    // --------------------------------------------------- THE STREAM-OWNERSHIP CONTRACT

    @Test
    fun `the stream is not closed by hashStreaming on success`() {
        val stream = CloseCountingInputStream(ByteArrayInputStream(ByteArray(32)))
        ServerContentHash.hashStreaming(stream)
        assertEquals(0, stream.closeCount, "the caller owns the stream lifetime")
    }

    @Test
    fun `the stream is not closed by hashStreaming on a hook cancellation`() {
        val stream = CloseCountingInputStream(ByteArrayInputStream(ByteArray(32)))
        assertThrows(CancellationException::class.java) {
            ServerContentHash.hashStreaming(stream, onCheck = { throw CancellationException("cancelled") })
        }
        assertEquals(0, stream.closeCount, "the caller owns the stream lifetime even on abort")
    }

    /** A read failure is an honest IOException, never a silent digest. */
    @Test
    fun `an IOException from the stream propagates`() {
        assertThrows(IOException::class.java) {
            ServerContentHash.hashStreaming(FailingInputStream())
        }
    }

    private class EventInputStream(
        private val source: ByteArrayInputStream,
        private val maxChunk: Int,
        private val events: MutableList<String>,
    ) : InputStream() {
        override fun read(): Int = source.read()
        override fun read(b: ByteArray, off: Int, len: Int): Int {
            val n = source.read(b, off, minOf(len, maxChunk))
            if (n >= 0) events.add("read") // the EOF read is not a delivered chunk
            return n
        }
    }

    private class CloseCountingInputStream(private val source: ByteArrayInputStream) : InputStream() {
        var closeCount = 0
            private set

        override fun read(): Int = source.read()
        override fun read(b: ByteArray, off: Int, len: Int): Int = source.read(b, off, len)
        override fun close() {
            closeCount++
            source.close()
        }
    }

    private class FailingInputStream : InputStream() {
        override fun read(): Int = throw IOException("disk gone")
        override fun read(b: ByteArray, off: Int, len: Int): Int = throw IOException("disk gone")
    }
}
