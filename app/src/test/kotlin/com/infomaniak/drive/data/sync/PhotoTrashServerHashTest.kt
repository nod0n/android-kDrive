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

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.FilterInputStream
import java.io.InputStream

/**
 * GENUINE XXH3-64 known-answer and format tests for [PhotoTrashServerHash] — every
 * expected digest is TRANSCRIBED from pinned upstream reference sources, never computed
 * by the implementation under test.
 *
 * VECTOR PROVENANCE (the T-0079-reviewed corpus, pinned at appmattus/crypto commit
 * d53fa42addbb188117c6006b475fcb7afccdba7d):
 *  - The official xxHash sanity table (seed-0 rows) from the upstream XXH3_64Test.kt
 *    (Apache-2.0), including the EXACT upstream pseudorandom buffer generator —
 *    transcribed verbatim below.
 *  - Dynatrace reference-table entries (Copyright 2022 Dynatrace LLC, Apache-2.0;
 *    Appmattus Kotlin translation), default-seed `hash0` fields only.
 *
 * COMPARISON SHAPE: the codec emits "xxh3:" + sixteen LOWERCASE hex digits (the unsigned
 * 64-bit value); the transcribed expectations are uppercase hex literals or Longs —
 * compared VALUE-exactly through [java.lang.Long.parseUnsignedLong], a pure JDK radix-16
 * parse of the full unsigned range, so the comparison depends on no implementation-owned
 * formatting, only on the digest bits.
 */
class PhotoTrashServerHashTest {

    private fun hexToBytes(hex: String): ByteArray = ByteArray(hex.length / 2) { i ->
        ((Character.digit(hex[i * 2], 16) shl 4) or Character.digit(hex[i * 2 + 1], 16)).toByte()
    }

    /** THE EXACT UPSTREAM GENERATOR (XXH3_64Test.kt, verbatim): a PRIME32-seeded
     *  multiplicative pseudorandom fill. */
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
        val token = PhotoTrashServerHash.hashStreaming(input)
        assertTrue(token.startsWith("xxh3:") && token.length == 21, "token shape: $token")
        return token
    }

    private fun tokenBits(token: String): Long =
        java.lang.Long.parseUnsignedLong(token.removePrefix("xxh3:"), 16)

    @Test
    @DisplayName("official sanity table seed-zero rows reproduce the transcribed digests")
    fun officialSanityTableSeedZeroRows() {
        SANITY_VECTORS.forEach { (len, expectedHex) ->
            val token = tokenOf(ByteArrayInputStream(upstreamBuffer(len)))
            assertEquals(
                java.lang.Long.parseUnsignedLong(expectedHex, 16),
                tokenBits(token),
                "sanity length $len",
            )
        }
    }

    @Test
    @DisplayName("varying input chunk sizes produce the same known expected digest")
    fun varyingInputChunkSizesProduceTheSameKnownDigest() {
        val chunkSizes = listOf(1, 7, 64, 1024, 65535, 65536)
        SANITY_VECTORS.forEach { (len, expectedHex) ->
            chunkSizes.forEach { chunk ->
                val token = tokenOf(
                    ChunkLimitedInputStream(ByteArrayInputStream(upstreamBuffer(len)), chunk),
                )
                assertEquals(
                    java.lang.Long.parseUnsignedLong(expectedHex, 16),
                    tokenBits(token),
                    "sanity length $len, chunk $chunk",
                )
            }
        }
    }

    @Test
    @DisplayName("dynatrace reference entries reproduce their transcribed default-seed digests")
    fun dynatraceReferenceEntries() {
        listOf(
            "2d06800538d394c2" to "",
            "c00f9d4f580c0c3a" to "6c", // transcribed: hash0 = -0x3ff062b0a7f3f3c6L
            "d2d6d7bdb44972a4" to "2e30", // transcribed: hash0 = -0x2d2928424bb68d5cL
            "330ff8a0b67bc873" to "41fa",
        ).forEach { (expectedUnsignedHex, input) ->
            val token = tokenOf(ByteArrayInputStream(hexToBytes(input)))
            assertEquals(
                java.lang.Long.parseUnsignedLong(expectedUnsignedHex, 16),
                tokenBits(token),
                "dynatrace input $input",
            )
        }
    }

    @Test
    @DisplayName("the parse ladder admits only the strict canonical form and preserves everything else")
    fun parseLadder() {
        assertTrue(PhotoTrashServerHash.parse("xxh3:00ec7bec63e548df") is PhotoTrashServerHash.Parsed.Supported)
        assertTrue(PhotoTrashServerHash.parse("xxh3:2d06800538d394c2") is PhotoTrashServerHash.Parsed.Supported)
        // Absent: the server sent nothing.
        assertTrue(PhotoTrashServerHash.parse(null) is PhotoTrashServerHash.Parsed.Absent)
        assertTrue(PhotoTrashServerHash.parse("") is PhotoTrashServerHash.Parsed.Absent)
        // Unsupported — including a bare SHA-256 hex receipt (the app's own algorithm is
        // NOT a server identity; guessing an algorithm would manufacture byte identity):
        listOf(
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", // sha-256 hex
            "d41d8cd98f00b204e9800998ecf8427e", // md5-shaped hex
            "xxh3:00EC7BEC63E548DF", // uppercase hex
            "xxh3:00ec7bec63e548d", // fifteen digits
            "xxh3:00ec7bec63e548dff", // seventeen digits
            "xxh3: 00ec7bec63e548df", // whitespace
            "XXH3:00ec7bec63e548df", // uppercase prefix
            "00ec7bec63e548df", // bare 16-hex without the prefix
        ).forEach { raw ->
            val parsed = PhotoTrashServerHash.parse(raw)
            assertTrue(parsed is PhotoTrashServerHash.Parsed.Unsupported, "must be unsupported: $raw")
            assertEquals(raw, (parsed as PhotoTrashServerHash.Parsed.Unsupported).raw, "the raw value is preserved verbatim")
        }
    }

    @Test
    @DisplayName("canonical renders the unsigned 64-bit digest in sixteen lowercase hex digits")
    fun canonicalRendering() {
        assertEquals("xxh3:2d06800538d394c2", PhotoTrashServerHash.canonical(0x2d06800538d394c2UL))
        // A value with the high bit set: signed Long rendering would produce a minus sign
        // or 18+ digits; the unsigned ULong rendering is the canonical form.
        assertEquals("xxh3:8000000000000000", PhotoTrashServerHash.canonical(0x8000000000000000UL))
        assertFalse(PhotoTrashServerHash.canonical(ULong.MAX_VALUE).contains("-"))
        assertEquals(21, PhotoTrashServerHash.canonical(0UL).length)
        assertEquals("xxh3:0000000000000000", PhotoTrashServerHash.canonical(0UL))
    }

    private companion object {
        private val PRIME64 = 11400714785074694797u.toLong()
        private val PRIME32 = 2654435761u.toLong() and 0xffffffff

        /** THE OFFICIAL SANITY TABLE (seed-0 rows only), transcribed from the upstream
         *  XXH3_64Test.kt — the empty input, the accumulator boundaries and the
         *  block/stripe shapes. */
        val SANITY_VECTORS: List<Pair<Int, String>> = listOf(
            0 to "2D06800538D394C2", // empty string
            1 to "C44BDFF4074EECDB", //  1 -  3
            6 to "27B56A84CD2D7325", //  4 -  8
            12 to "A713DAF0DFBB77E7", //  9 - 16
            24 to "A3FE70BF9D3510EB", // 17 - 32
            48 to "397DA259ECBA1F11", // 33 - 64
            80 to "BCDEFBBB2C47C90A", // 65 - 96
            195 to "CD94217EE362EC3A", // 129-240
            403 to "CDEB804D65C6DEA4", // one block, last stripe is overlapping
            512 to "617E49599013CB6B", // one block, finishing at stripe boundary
            2048 to "DD59E2C3A5F038E0", // 2 blocks, finishing at block boundary
            2240 to "6E73A90539CF2948", // 3 blocks, finishing at stripe boundary
            2367 to "CB37AEB9E5D361ED", // 3 blocks, last stripe is overlapping
        )
    }

    /** Delivers the source in fixed-size chunks, to prove the codec is insensitive to how
     *  the stream parcels its bytes. */
    private class ChunkLimitedInputStream(source: InputStream, private val chunkSize: Int) : FilterInputStream(source) {
        override fun read(b: ByteArray, off: Int, len: Int): Int = super.read(b, off, minOf(len, chunkSize))
    }
}
