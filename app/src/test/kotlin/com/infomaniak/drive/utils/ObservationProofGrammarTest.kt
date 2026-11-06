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

import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.ObservationFormat
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.ObservedHashBasis
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.decodeObservedHashBasis
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.decodeProvenanceProofFields
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.encodeObservedHashBasis
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.parseObservationBinding
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/**
 * JVM regressions over the REAL production observation and proof grammars (the pure static
 * parsers inside DownloadOfflineFileManager's companion, made internal by the test-enablement
 * commit) — no reflection, no test mirrors, no Android.
 *
 * SCOPE LABEL (honest): the rebind test below is a HAND-BUILT PREFIX REWRITE — it pins the
 * grammar's parse→serialize→parse contract (the verbatim-basis, id-and-location-only changes)
 * through the actual pure production parse. It is NOT integrated relocation coverage: the
 * file-side relocation flows (prepareProofRelocation/completePreparedProofMove) touch
 * Context/Realm/files and stay exercised only by the (device-gated) integration layer.
 */
class ObservationProofGrammarTest {

    // =============================================== THE OBSERVATION BINDING GRAMMARS

    @Test
    fun `current frame parses typed fields and the whole location tail`() {
        val parsed = parseObservationBinding("1|42|7|123|100|200|300|/data/offline/f")
        assertEquals(ObservationFormat.CURRENT, parsed?.format)
        assertEquals(42L, parsed?.publicationId)
        assertEquals(7, parsed?.prefix?.size)
        assertEquals("/data/offline/f", parsed?.location)
        assertTrue(parsed?.carriesPublicationId == true)
    }

    @Test
    fun `current frame keeps pipe-containing destinations intact`() {
        val parsed = parseObservationBinding("1|42|7|123|100|200|300|/a|b|c")
        assertEquals("/a|b|c", parsed?.location)
        assertEquals(7, parsed?.prefix?.size)
    }

    @Test
    fun `current frame accepts a nullable empty size`() {
        val parsed = parseObservationBinding("1|42|7||100|200|300|/f")
        assertEquals(ObservationFormat.CURRENT, parsed?.format)
        assertEquals("/f", parsed?.location)
    }

    @Test
    fun `current frame is damaged by a malformed typed field`() {
        assertNull(parseObservationBinding("1|x|7|123|100|200|300|/f"))
        assertNull(parseObservationBinding("1|42|7|123|100|200|x|/f"))
        assertNull(parseObservationBinding("1|42|7|123|100|200")) // no location component
    }

    @Test
    fun `v2 observation frame parses the basis field and the tail from index eight`() {
        val parsed = parseObservationBinding("2|42|7|123|100|200|300|a|/data/offline/f")
        assertEquals(ObservationFormat.CURRENT_V2, parsed?.format)
        assertEquals(42L, parsed?.publicationId)
        assertEquals(8, parsed?.prefix?.size)
        assertEquals("a", parsed?.prefix?.get(7))
        assertEquals("/data/offline/f", parsed?.location)
        assertTrue(parsed?.carriesPublicationId == true)
    }

    @Test
    fun `v2 observation frame keeps pipe-containing destinations intact`() {
        val parsed = parseObservationBinding("2|42|7|123|100|200|300|x2d|/a|b")
        assertEquals("/a|b", parsed?.location)
        assertEquals(8, parsed?.prefix?.size)
        assertEquals("x2d", parsed?.prefix?.get(7))
    }

    @Test
    fun `malformed v2 frames fail closed and never fall through to legacy`() {
        // A bad attempt id, a bad typed field, a bad basis, a missing location, an unknown
        // head — each is UNPARSEABLE, never reinterpreted as the legacy grammar.
        assertNull(parseObservationBinding("2|x|7|123|100|200|300|a|/f"))
        assertNull(parseObservationBinding("2|42|7|123|100|x|300|a|/f"))
        assertNull(parseObservationBinding("2|42|7|123|100|200|300|zz|/f"))
        assertNull(parseObservationBinding("2|42|7|123|100|200|300|a"))
        assertNull(parseObservationBinding("3|42|7|123|100|200|300|a|/f"))
        assertNull(parseObservationBinding("2|42|7|123|100|200|300|-|/f")) // the retired sentinel
    }

    @Test
    fun `legacy frame carries a null publication id — never its version as an id`() {
        val parsed = parseObservationBinding("1|7|123|100|200|300|/data/f")
        assertEquals(ObservationFormat.LEGACY, parsed?.format)
        assertNull(parsed?.publicationId) // the second field is a VERSION, not an id
        assertEquals("/data/f", parsed?.location)
        assertEquals(6, parsed?.prefix?.size)
        assertTrue(parsed?.carriesPublicationId == false)
    }

    @Test
    fun `a wide attempt id distinguishes the current grammar from the legacy reading`() {
        // fields[1] = 2147483648 exceeds Int range: as a CURRENT attempt id it parses (the
        // frame carries a location); the legacy grammar (which would read it as a version
        // Int) is never reached.
        val parsed = parseObservationBinding("1|2147483648|0|1|2|3|4|/f")
        assertEquals(ObservationFormat.CURRENT, parsed?.format)
        assertEquals(2147483648L, parsed?.publicationId)
    }

    // ==================================================== THE SEEN-HASH BASIS CODEC

    @Test
    fun `absent basis decodes exactly`() {
        assertEquals(ObservedHashBasis.Absent, decodeObservedHashBasis("a"))
    }

    @Test
    fun `present tokens decode to the verbatim value`() {
        assertEquals(ObservedHashBasis.Seen("-"), decodeObservedHashBasis("x2d"))
        assertEquals(ObservedHashBasis.Seen("abc"), decodeObservedHashBasis("x616263"))
        assertEquals(
            ObservedHashBasis.Seen("xxh3:deadbeefdeadbeef"),
            decodeObservedHashBasis("x7878683364656164626565666465616462656566"),
        )
        // Multi-byte UTF-8 rides losslessly.
        assertEquals(ObservedHashBasis.Seen("é★"), decodeObservedHashBasis("xc3a9e29885"))
    }

    @Test
    fun `encode is total and collides with nothing — decode inverts it exactly`() {
        listOf("-", "abc", "xxh3:deadbeefdeadbeef", "with|pipe", "with\nnewline", "é★", " ")
            .forEach { token ->
                assertEquals(ObservedHashBasis.Seen(token), decodeObservedHashBasis(encodeObservedHashBasis(token)))
            }
        assertEquals(ObservedHashBasis.Absent, decodeObservedHashBasis(encodeObservedHashBasis(null)))
        assertEquals(ObservedHashBasis.Absent, decodeObservedHashBasis(encodeObservedHashBasis("")))
        // A literal "-" encodes as "x2d" — never as the absence marker.
        assertEquals("x2d", encodeObservedHashBasis("-"))
    }

    @Test
    fun `strict basis grammar rejects signs, uppercase, odd runs, emptiness and invalid utf8`() {
        assertNull(decodeObservedHashBasis("x")) // empty hex
        assertNull(decodeObservedHashBasis("xabc")) // odd length
        assertNull(decodeObservedHashBasis("xABCDEF01")) // uppercase
        assertNull(decodeObservedHashBasis("x-1")) // a sign
        assertNull(decodeObservedHashBasis("xgg")) // non-hex digits
        assertNull(decodeObservedHashBasis("x80")) // an invalid UTF-8 byte — never a mutated token
        assertNull(decodeObservedHashBasis("")) // neither marker
        assertNull(decodeObservedHashBasis("b")) // an unknown single-char marker
    }

    // ================================================ THE SHARED PROOF-FIELD DECODER

    private fun proofV2(location: String) =
        "v2|7|100|200|300|11|22|33|sha256content|777|$location"

    private fun proofV3(location: String) =
        "v3|7|100|200|300|11|22|33|sha256content|xxh3:0123456789abcdef|777|$location"

    @Test
    fun `v2 proof decodes ten body fields with the location re-joined last`() {
        val decoded = decodeProvenanceProofFields(proofV2("/data/a|b"))
        assertEquals(2, decoded?.frameVersion)
        assertEquals("11", decoded?.userId)
        assertEquals("22", decoded?.driveId)
        assertEquals("33", decoded?.fileId)
        assertEquals("sha256content", decoded?.contentHash)
        assertNull(decoded?.serverHash) // v2 carries NO server hash
        assertEquals("777", decoded?.publicationId)
        assertEquals("/data/a|b", decoded?.body?.last())
    }

    @Test
    fun `v3 proof decodes the server hash before the location`() {
        val decoded = decodeProvenanceProofFields(proofV3("/data/f"))
        assertEquals(3, decoded?.frameVersion)
        assertEquals("xxh3:0123456789abcdef", decoded?.serverHash)
        assertEquals("sha256content", decoded?.contentHash) // the LOCAL receipt, distinct
        assertEquals("777", decoded?.publicationId)
        assertEquals("/data/f", decoded?.body?.last())
    }

    @Test
    fun `a v3 proof with a malformed server hash is undecodable`() {
        assertNull(decodeProvenanceProofFields("v3|7|100|200|300|11|22|33|sha256content|MD5:x|777|/f"))
        assertNull(decodeProvenanceProofFields("v3|7|100|200|300|11|22|33|sha256content|xxh3:0123456789ABCDEF|777|/f"))
    }

    @Test
    fun `framed proofs with the wrong field count are never reparsed as legacy`() {
        assertNull(decodeProvenanceProofFields("v2|only|three|fields|here"))
        assertNull(decodeProvenanceProofFields("v3|7|100|200|300|11|22|33|sha256content|xxh3:0123456789abcdef|777"))
    }

    @Test
    fun `an unframed text takes the documented lenient legacy arm`() {
        val decoded = decodeProvenanceProofFields("whatever|legacy|split")
        assertNull(decoded?.frameVersion)
        assertEquals(listOf("whatever", "legacy", "split"), decoded?.body)
    }

    // ========================== THE GRAMMAR'S REBIND ROUND-TRIP (labelled, not integrated)

    @Test
    fun `grammar roundtrip - a hand-built prefix rewrite changes only the location tail`() {
        listOf(
            "1|42|7|123|100|200|300",
            "2|42|7|123|100|200|300|a",
            "2|42|7||100|200|300|x787868336162",
        ).forEach { prefix ->
            val parsed = parseObservationBinding("$prefix|/old/dest") ?: error("fixture must parse")
            // The relocation's own reconstruction shape (parsed.prefix + the new location).
            val reboundText = (parsed.prefix + "/new|dest").joinToString("|")
            val rebound = parseObservationBinding(reboundText) ?: error("the rebound text must parse")
            assertEquals(parsed.format, rebound.format)
            assertEquals(parsed.publicationId, rebound.publicationId)
            assertEquals(parsed.prefix.dropLast(1), rebound.prefix.dropLast(1))
            if (parsed.format == ObservationFormat.CURRENT_V2) {
                // THE RAW BASIS IS PRESERVED VERBATIM — the review-55 G1 contract.
                assertEquals(parsed.prefix.last(), rebound.prefix.last())
            }
            assertEquals("/new|dest", rebound.location)
        }
    }
}
