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

import com.appmattus.crypto.internal.xxh3.xxh3ReferenceData
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.InputStream

/**
 * GENUINE XXH3-64 known-answer tests for [ServerContentHash.hashStreaming] — every expected
 * digest is TRANSCRIBED from pinned upstream reference sources, never computed by the
 * implementation under test.
 *
 * VECTOR PROVENANCE (all pinned at appmattus/crypto commit d53fa42addbb188117c6006b475fcb7afc
 * cdba7d; the URLs and SHA256s are recorded in the vendor-reference TEST-VECTOR-PROVENANCE.json):
 *  - The Dynatrace reference table (XXH3ReferenceData.kt — Copyright 2022 Dynatrace LLC /
 *    translation Appmattus Limited, Apache-2.0) is copied VERBATIM into the test tree at
 *    com/appmattus/crypto/internal/xxh3/XXH3ReferenceData.kt (sha256 663c05ee800de0ef4fa748328
 *    ab0d25f2cf27490bfe69e1881c4ffe1483cc0db — identical to the pinned source; its license
 *    headers are preserved in the file itself). 2011 entries; each entry's `hash0` is the
 *    DEFAULT-SEED XXH3-64 digest of its hex `input`; the per-entry `seed`/`hash1` (seeded
 *    digest) fields are deliberately NOT exercised — this codec is default-seed only.
 *  - The official xxHash sanity table (seed-0 rows only) from the upstream
 *    XXH3_64Test.kt (Apache-2.0), with the EXACT upstream pseudorandom buffer generator
 *    reproduced verbatim (its values must not be changed).
 *
 * COMPARISON SHAPE: the codec emits "xxh3:" + sixteen LOWERCASE hex digits (the unsigned
 * 64-bit value). The upstream expected values are 64-bit LONGS (the reference data) or
 * UPPERCASE hex literals (the sanity table); both are compared VALUE-exactly through
 * [Long.parseUnsignedLong] — a pure JDK radix-16 parse of the full unsigned range — so the
 * comparison depends on no implementation-owned formatting, only on the digest BITS.
 *
 * EXPLICIT LIMIT: the pinned upstream reference set contains NO vector at or beyond 64 KiB —
 * the codec's internal read-buffer boundary. The multi-update path above one full buffer is
 * therefore NOT covered by any genuine known answer; it is exercised only for
 * chunk-independence at the covered lengths. This is a declared fixture limit, not a gap
 * filled with computed expectations.
 */
class ServerContentHashKatTest {

    private fun hexToBytes(hex: String): ByteArray = ByteArray(hex.length / 2) { i ->
        ((Character.digit(hex[i * 2], 16) shl 4) or Character.digit(hex[i * 2 + 1], 16)).toByte()
    }

    /** THE EXACT UPSTREAM GENERATOR (XXH3_64Test.kt, verbatim — "its values must not be
     * changed"): a PRIME32-seeded multiplicative pseudorandom fill. */
    private fun upstreamBuffer(size: Int): ByteArray {
        val buffer = ByteArray(size)
        var byteGen: Long = PRIME32
        for (i in 0 until size) {
            buffer[i] = (byteGen ushr 56).toByte()
            byteGen *= PRIME64
        }
        return buffer
    }

    /** The codec's token for [input], checked for its canonical shape. */
    private fun tokenOf(input: InputStream): String {
        val token = ServerContentHash.hashStreaming(input)
        assertTrue(token.startsWith("xxh3:") && token.length == 21, "token shape: $token")
        return token
    }

    private fun tokenBits(token: String): Long =
        java.lang.Long.parseUnsignedLong(token.removePrefix("xxh3:"), 16)

    /** THE DYNATRACE REFERENCE TABLE, default-seed digests only: every one of the 2011
     * pinned entries must reproduce its transcribed `hash0` through the streaming codec. */
    @Test
    fun `every dynatrace reference entry reproduces its default-seed digest`() {
        xxh3ReferenceData.forEach { entry ->
            val token = tokenOf(ByteArrayInputStream(hexToBytes(entry.input)))
            assertEquals(
                entry.hash0,
                tokenBits(token),
                "input(${entry.input.length} hex chars): ${entry.input.take(32)}…",
            )
        }
    }

    /** THE OFFICIAL SANITY TABLE (seed-0 rows only): the exact upstream input lengths and
     * expected digests, over the exact upstream buffer generator — the empty input and the
     * 1-3/4-8/9-16/17-32/33-64/65-96/129-240 accumulator boundaries plus the block/stripe
     * shapes (403/512/2048/2240/2367). */
    @Test
    fun `official sanity table seed-zero rows reproduce the expected digests`() {
        listOf(
            0L to "2D06800538D394C2", // empty string
            1L to "C44BDFF4074EECDB", //  1 -  3
            6L to "27B56A84CD2D7325", //  4 -  8
            12L to "A713DAF0DFBB77E7", //  9 - 16
            24L to "A3FE70BF9D3510EB", // 17 - 32
            48L to "397DA259ECBA1F11", // 33 - 64
            80L to "BCDEFBBB2C47C90A", // 65 - 96
            195L to "CD94217EE362EC3A", // 129-240
            403L to "CDEB804D65C6DEA4", // one block, last stripe is overlapping
            512L to "617E49599013CB6B", // one block, finishing at stripe boundary
            2048L to "DD59E2C3A5F038E0", // 2 blocks, finishing at block boundary
            2240L to "6E73A90539CF2948", // 3 blocks, finishing at stripe boundary
            2367L to "CB37AEB9E5D361ED", // 3 blocks, last stripe is overlapping
        ).forEach { (len, expectedHex) ->
            val token = tokenOf(ByteArrayInputStream(upstreamBuffer(len.toInt())))
            assertEquals(
                java.lang.Long.parseUnsignedLong(expectedHex, 16),
                tokenBits(token),
                "sanity length $len",
            )
        }
    }

    /** STREAMING-BOUNDARY INDEPENDENCE: the SAME known expected digest must hold when the
     * source delivers its bytes in degenerate per-read chunk sizes (1, 7, 64, 1024, 65535
     * and exactly the codec's 64 KiB buffer size) — the codec must be insensitive to how
     * the stream parcels its bytes. */
    @Test
    fun `varying input chunk sizes produce the same known expected digest`() {
        val chunkSizes = listOf(1, 7, 64, 1024, 65535, 65536)
        listOf(
            0L to "2D06800538D394C2",
            1L to "C44BDFF4074EECDB",
            6L to "27B56A84CD2D7325",
            12L to "A713DAF0DFBB77E7",
            24L to "A3FE70BF9D3510EB",
            48L to "397DA259ECBA1F11",
            80L to "BCDEFBBB2C47C90A",
            195L to "CD94217EE362EC3A",
            403L to "CDEB804D65C6DEA4",
            512L to "617E49599013CB6B",
            2048L to "DD59E2C3A5F038E0",
            2240L to "6E73A90539CF2948",
            2367L to "CB37AEB9E5D361ED",
        ).forEach { (len, expectedHex) ->
            chunkSizes.forEach { chunk ->
                val token = tokenOf(
                    ChunkLimitedInputStream(
                        ByteArrayInputStream(upstreamBuffer(len.toInt())),
                        chunk,
                    )
                )
                assertEquals(
                    java.lang.Long.parseUnsignedLong(expectedHex, 16),
                    tokenBits(token),
                    "sanity length $len chunked at $chunk",
                )
            }
        }
    }

    /** The same chunk-independence sampled across the Dynatrace table (every 97th entry:
     * lengths 0..400 at 1-byte and 7-byte delivery). */
    @Test
    fun `dynatrace entries sampled at degenerate chunk sizes stay value-exact`() {
        xxh3ReferenceData.filterIndexed { index, _ -> index % 97 == 0 }.forEach { entry ->
            val expectedBits = entry.hash0
            listOf(1, 7).forEach { chunk ->
                val token = tokenOf(
                    ChunkLimitedInputStream(
                        ByteArrayInputStream(hexToBytes(entry.input)),
                        chunk,
                    )
                )
                assertEquals(expectedBits, tokenBits(token), "entry chunked at $chunk")
            }
        }
    }

    companion object {
        private val PRIME64 = 11400714785074694797u.toLong()
        private val PRIME32 = 2654435761u.toLong() and 0xffffffff
    }

    /** A source that never delivers more than [maxChunk] bytes per read — the codec's
     * internal 64 KiB buffer then advances in [maxChunk]-sized steps (or less). */
    private class ChunkLimitedInputStream(
        private val source: ByteArrayInputStream,
        private val maxChunk: Int,
    ) : InputStream() {
        override fun read(): Int = source.read()
        override fun read(b: ByteArray, off: Int, len: Int): Int =
            source.read(b, off, minOf(len, maxChunk))
    }
}
