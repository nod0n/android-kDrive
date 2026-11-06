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
import org.junit.jupiter.api.Assertions.assertInstanceOf
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
    fun `current frame is damaged by a malformed typed field invalid in BOTH grammars`() {
        // GENUINELY invalid in both readings: fields[1] exceeds the LEGACY version's Int
        // range (so the legacy grammar cannot re-read it as a version) AND fields[2] is
        // not an Int (so the CURRENT grammar cannot parse it either) — unparseable, null.
        assertNull(parseObservationBinding("1|2147483648|x|1|2|3|4|/f"))
        // The head-1 ambiguity is EXPLICIT and accepted (the first-run finding): a string
        // that fails the CURRENT grammar but whose remaining fields satisfy the LEGACY
        // grammar IS a legacy observation — "42" reads as the version, "x|/f" is the
        // location tail. The accepted legacy behavior is pinned here, never broken to
        // satisfy a new test.
        val fallback = parseObservationBinding("1|42|7|123|100|200|x|/f")
        assertEquals(ObservationFormat.LEGACY, fallback?.format)
        assertNull(fallback?.publicationId)
        assertEquals("x|/f", fallback?.location)
        // No location component at all: six fields satisfy neither grammar.
        assertNull(parseObservationBinding("1|42|7|123|100|200"))
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

    /** THE VALUE-EXACT SEEN ASSERTION: [ObservedHashBasis.Seen] is an ordinary class (its
     * identity is never compared), so the decoded value is asserted by SUBTYPE + TOKEN —
     * the first-run lesson (the reference-equality test error), fixed in the TEST only:
     * the production type is never changed to satisfy a test. */
    private fun assertSeen(decoded: ObservedHashBasis?, expectedToken: String) {
        assertInstanceOf(ObservedHashBasis.Seen::class.java, decoded)
        assertEquals(expectedToken, (decoded as ObservedHashBasis.Seen).token)
    }

    @Test
    fun `present tokens decode to the verbatim value`() {
        assertSeen(decodeObservedHashBasis("x2d"), "-")
        assertSeen(decodeObservedHashBasis("x616263"), "abc")
        // "xxh3:deadbeefdeadbeef" byte-exact: 78 78 68 33 3a 64 65 61 64 62 65 65 66
        // 64 65 61 64 62 65 65 66 — the first run's literal omitted the 0x3a colon byte
        // (masked by the reference-equality error); corrected from the source bytes.
        assertSeen(
            decodeObservedHashBasis("x787868333a64656164626565666465616462656566"),
            "xxh3:deadbeefdeadbeef",
        )
        // Multi-byte UTF-8 rides losslessly.
        assertSeen(decodeObservedHashBasis("xc3a9e29885"), "é★")
    }

    @Test
    fun `encode is total and collides with nothing — decode inverts it exactly`() {
        listOf("-", "abc", "xxh3:deadbeefdeadbeef", "with|pipe", "with\nnewline", "é★", " ")
            .forEach { token ->
                assertSeen(decodeObservedHashBasis(encodeObservedHashBasis(token)), token)
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
            // The basis decodes to "xxh3:ab" (78 78 68 33 3a 61 62) — the literal's
            // decoded content is irrelevant to this test (only the verbatim preservation
            // is asserted), but the transcription is kept byte-honest.
            "2|42|7||100|200|300|x787868333a6162",
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
