/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2022-2026 Infomaniak Network SA
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

import com.appmattus.crypto.internal.core.xxh3.XXH3_64bits_digest
import com.appmattus.crypto.internal.core.xxh3.XXH3_64bits_reset
import com.appmattus.crypto.internal.core.xxh3.XXH3_64bits_update
import com.appmattus.crypto.internal.core.xxh3.XXH3_createState
import com.appmattus.crypto.internal.core.xxh3.XXH3_freeState
import com.appmattus.crypto.internal.core.xxh3.XXH_errorcode
import java.io.InputStream

/**
 * THE SERVER CONTENT HASH CODEC (T-0079 offline-folders stage 3; review-53 D1/D3).
 *
 * The kDrive official API exposes the file content's hash as a nullable string whose
 * documented example is "xxh3:00ec7bec63e548df". THIS BUILD SUPPORTS EXACTLY THAT:
 * XXH3-64 with the default seed (0) and the default secret — the streaming
 * [XXH3_64bits_reset] parameters, equivalent to the one-shot `XXH3_64bits()` —
 * canonicalized as the "xxh3:" prefix plus the UNSIGNED 64-bit digest in exactly
 * sixteen lowercase ASCII hex digits. Any other value — another algorithm prefix,
 * uppercase hex, a different length, whitespace — is UNSUPPORTED and never
 * authorizes anything; there is deliberately no MD5/SHA-1/SHA-256 fallback (the
 * official documentation shows no such contract, and guessing an algorithm would
 * manufacture false byte identity).
 *
 * XXH3 is NON-CRYPTOGRAPHIC. A match certifies integrity against ACCIDENTAL
 * corruption and staleness only — it is not an adversarial integrity proof. The
 * local byte receipt stays the app's own SHA-256 (UploadFile.sha256OrNull); the two
 * identities are kept separate and never conflated.
 *
 * The implementation is the source-vendored appmattus/crypto XXH3 (upstream commit
 * d53fa42addbb188117c6006b475fcb7afccdba7d, Apache-2.0; the LICENSE and the
 * provenance notes sit next to the vendored sources). The algorithm code is
 * UNCHANGED upstream code; the only deviation is the documented JVM shim in
 * MultiplyHigh.kt. The vendored symbols are `internal` — callable from this same
 * app module only. Streaming is used so a large offline file is never read whole
 * into memory: the digest state is bounded and the read buffer is fixed at
 * [BUFFER_SIZE_BYTES].
 */
object ServerContentHash {

    /** The only supported algorithm prefix (the official example's exact shape). */
    const val SUPPORTED_PREFIX = "xxh3:"

    /** The strict FULL format: the prefix plus exactly sixteen lowercase ASCII hex digits. */
    val SUPPORTED_FORMAT = Regex("xxh3:[0-9a-f]{16}")

    /** The streaming read buffer: 64 KiB — bounded, never the whole file. */
    private const val BUFFER_SIZE_BYTES = 64 * 1024

    /**
     * THE PARSE LADDER: [Parsed.Absent] (null/empty — the server sent nothing),
     * [Parsed.Unsupported] (present but not EXACTLY the supported algorithm's
     * canonical form — the raw value is preserved verbatim for the attempt
     * observation; it is never normalized, trimmed, or guessed), [Parsed.Supported]
     * (strictly canonical).
     */
    sealed class Parsed {
        object Absent : Parsed()
        class Unsupported(val raw: String) : Parsed()
        class Supported(val canonical: String) : Parsed()
    }

    fun parse(raw: String?): Parsed = when {
        raw.isNullOrEmpty() -> Parsed.Absent
        SUPPORTED_FORMAT.matches(raw) -> Parsed.Supported(raw)
        else -> Parsed.Unsupported(raw)
    }

    /** The canonical token for a computed digest: the UNSIGNED 64-bit value in exactly
     * sixteen lowercase hex digits. [java.lang.Long.toHexString] is signed/two's-
     * complement and is deliberately NOT used; ULong rendering is the canonical form. */
    fun canonical(digest: ULong): String = SUPPORTED_PREFIX + digest.toString(16).padStart(16, '0')

    /**
     * Streams [input] through XXH3-64 (default seed 0, default secret — the exact
     * parameters behind the server's documented algorithm) and returns the canonical
     * token. The input is read in bounded chunks; [onCheck] runs immediately before
     * the first read, after EVERY chunk, and after the final chunk — the caller's
     * cancellation/completion hook; a [kotlinx.coroutines.CancellationException]
     * thrown there aborts the hash mid-stream. An [java.io.IOException] propagates
     * to the caller (an honest hashing failure, never a silent mismatch).
     *
     * The stream is NOT closed here — the caller owns its lifetime (the establishment
     * hashes its own staged file AFTER the network response has closed).
     */
    fun hashStreaming(input: InputStream, onCheck: () -> Unit = {}): String {
        val state = XXH3_createState()
        var digest = 0UL
        try {
            check(XXH3_64bits_reset(state) == XXH_errorcode.XXH_OK)
            val buffer = ByteArray(BUFFER_SIZE_BYTES)
            onCheck()
            while (true) {
                val read = input.read(buffer)
                if (read == -1) break
                if (read > 0) {
                    check(XXH3_64bits_update(state, buffer, 0, read) == XXH_errorcode.XXH_OK)
                }
                onCheck()
            }
            onCheck()
            digest = XXH3_64bits_digest(state).toULong()
        } finally {
            // A JVM no-op (the state holds only managed objects); called for fidelity
            // with the C streaming contract the vendored API mirrors.
            runCatching { XXH3_freeState(state) }
        }
        return canonical(digest)
    }
}
