/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2026 Infomaniak Network SA
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.infomaniak.drive.data.sync

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.async
import kotlinx.coroutines.cancel
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Assertions.fail
import org.junit.jupiter.api.Test
import java.io.ByteArrayInputStream
import java.io.IOException
import java.io.InputStream
import java.security.MessageDigest
import java.util.concurrent.atomic.AtomicInteger

/** Actual production acquisition/stream code. Commit callbacks substitute for native Realm
 * and authority only; real transaction/lifecycle integration is covered in androidTest. */
class PhotoTrashMissingBasisTest {
    private val a = "uploaded-A".toByteArray()
    private val b = "replaced-B".toByteArray() // same length
    private val local = PhotoTrashRules.LocalObservation(true, false, a.size.toLong(), 123L)
    private fun sha(bytes: ByteArray) = MessageDigest.getInstance("SHA-256").digest(bytes).joinToString("") { "%02x".format(it) }
    private fun hash(bytes: ByteArray) = PhotoTrashServerHash.hashStreaming(ByteArrayInputStream(bytes))
    private fun binding() = PhotoTrashMissingBasis.Binding(
        "content://fixture/one", 1, 2, 3, "fixture.png", a.size.toLong(), 123L, sha(a), null,
        5L, 7L, false, false, 0L, 0L, 0L, "ACTIVE",
    )
    private fun remote() = PhotoTrashMissingBasis.Remote(3, 2, "file", a.size.toLong(), hash(a), false)

    @Test
    fun missingUploadHashIsAcquiredFromTheOriginalReceiptAndOneSourceRead() = runBlocking {
        val witness = binding()
        val opens = AtomicInteger()
        var committed: PhotoTrashMissingBasis.Binding? = null
        assertTrue(PhotoTrashMissingBasis.acquire(witness, local, remote(), {
            opens.incrementAndGet()
            ByteArrayInputStream(a)
        }) { observed, hash ->
            assertEquals(witness, observed)
            committed = observed.copy(remoteHash = hash)
            true
        })
        assertEquals(1, opens.get())
        assertEquals(witness.copy(remoteHash = hash(a)), committed, "every other field remains unchanged")
    }

    @Test
    fun matchingCurrentLocalAndRemoteReplacementCannotReplaceTheOriginalUploadReceipt() = runBlocking {
        var writes = 0
        assertFalse(PhotoTrashMissingBasis.acquire(binding(), local, remote().copy(hash = hash(b)), {
            ByteArrayInputStream(b)
        }) { _, _ -> writes++; true })
        assertEquals(0, writes, "matching B/B is not authority for uploaded A, even at the same size/mtime")
    }

    @Test
    fun remoteReplacementCannotMatchUnchangedLocalBytes() = runBlocking {
        assertFalse(PhotoTrashMissingBasis.acquire(binding(), local, remote().copy(hash = hash(b)), {
            ByteArrayInputStream(a)
        }) { _, _ -> fail("mismatching XXH3 must not commit") })
    }

    @Test
    fun missingUnsupportedAndNoncanonicalServerHashesNeverReadOrCommit() = runBlocking {
        for (value in listOf(null, "", "sha256:${sha(a)}", "xxh3:ABCDEF0123456789", " ${hash(a)}")) {
            assertFalse(PhotoTrashMissingBasis.acquire(binding(), local, remote().copy(hash = value), {
                fail("invalid hash must not open source")
            }) { _, _ -> fail("invalid hash must not commit") })
        }
    }

    @Test
    fun wrongRemoteIdentityTypeSizeOrTrashRefusesBeforeRead() = runBlocking {
        val invalid = listOf(null, remote().copy(id = 4), remote().copy(driveId = 4), remote().copy(type = "dir"),
            remote().copy(type = "unknown"), remote().copy(size = null), remote().copy(size = 999), remote().copy(trashed = true))
        for (value in invalid) assertFalse(PhotoTrashMissingBasis.acquire(binding(), local, value, {
            fail("wrong scope must not read")
        }) { _, _ -> fail("wrong scope must not commit") })
    }

    @Test
    fun missingOrChangedLocalObservationRefusesBeforeRead() = runBlocking {
        for (value in listOf(null, local.copy(exists = false), local.copy(isTrashed = true), local.copy(size = null),
            local.copy(size = 999), local.copy(lastModified = null), local.copy(lastModified = 124))) {
            assertFalse(PhotoTrashMissingBasis.acquire(binding(), value, remote(), { fail("invalid observation") }) { _, _ -> fail("commit") })
        }
    }

    @Test
    fun noTrustedUploadReceiptAndEstablishedBasisNeverAdoptNewEvidence() = runBlocking {
        val invalid = listOf(binding().copy(localDigest = null), binding().copy(localDigest = "sample"),
            binding().copy(remoteHash = hash(b)), binding().copy(remoteHash = "unsupported"), binding().copy(remoteHash = ""),
            binding().copy(pairedAt = null), binding().copy(userId = -1), binding().copy(driveId = -1), binding().copy(remoteFileId = -1))
        for (value in invalid) assertFalse(PhotoTrashMissingBasis.acquire(value, local, remote(), { fail("unbound or established") }) { _, _ -> fail("commit") })
    }

    @Test
    fun denialConflictPendingAndTrashedBaselineAreNotResetToAcquire() = runBlocking {
        val invalid = listOf(binding().copy(state = "DENIED"), binding().copy(state = "CONFLICT_LOCAL_CHANGED"),
            binding().copy(state = "CONFLICT_REMOTE_CHANGED"), binding().copy(state = "unknown"), binding().copy(pendingSince = 9),
            binding().copy(baselineTrashed = null), binding().copy(baselineTrashed = true), binding().copy(lastObservedTrashed = true))
        for (value in invalid) assertFalse(PhotoTrashMissingBasis.acquire(value, local, remote(), { fail("refusal must stand") }) { _, _ -> fail("commit") })
    }

    @Test
    fun inconclusiveEvidenceCanBeAcquiredWithoutClearingItsState() = runBlocking {
        val witness = binding().copy(state = "INCONCLUSIVE")
        assertTrue(PhotoTrashMissingBasis.acquire(witness, local, remote(), { ByteArrayInputStream(a) }) { row, _ ->
            assertEquals("INCONCLUSIVE", row.state)
            true
        })
    }

    @Test
    fun missingOrFailingSourcePreservesAndClosesWithoutCommit() = runBlocking {
        assertFalse(PhotoTrashMissingBasis.acquire(binding(), local, remote(), { null }) { _, _ -> fail("missing input") })
        assertFalse(PhotoTrashMissingBasis.acquire(binding(), local, remote(), { throw SecurityException() }) { _, _ -> fail("revoked") })
        var closed = false
        assertFalse(PhotoTrashMissingBasis.acquire(binding(), local, remote(), {
            object : InputStream() {
                override fun read(): Int = throw IOException()
                override fun close() { closed = true }
            }
        }) { _, _ -> fail("unreadable") })
        assertTrue(closed)
    }

    @Test
    fun declaredSizeMustAlsoEqualActualBytesRead() = runBlocking {
        val empty = byteArrayOf()
        val witness = binding().copy(localSize = -1, localModified = -1, localDigest = sha(empty))
        assertFalse(PhotoTrashMissingBasis.acquire(witness, local, remote().copy(hash = hash(empty)), {
            ByteArrayInputStream(empty)
        }) { _, _ -> fail("stream length differs from observed size") })
    }

    @Test
    fun oneReadSequenceCannotValidateAAndRecordASecondSourceB() = runBlocking {
        var opens = 0
        assertTrue(PhotoTrashMissingBasis.acquire(binding(), local, remote(), {
            ByteArrayInputStream(if (opens++ == 0) a else b)
        }) { _, _ -> true })
        assertEquals(1, opens, "the original SHA and XXH3 share one stream, not two separately timed reads")
    }

    @Test
    fun cancelledReadPropagatesCancellationAndNeverCommits() = runBlocking {
        var closed = false
        val work = async {
            val reader = currentCoroutineContext()
            PhotoTrashMissingBasis.acquire(binding(), local, remote(), {
                object : ByteArrayInputStream(a) {
                    override fun read(b: ByteArray, off: Int, len: Int): Int {
                        val read = super.read(b, off, len)
                        reader.cancel(CancellationException("fixture mid-read cancellation"))
                        return read
                    }
                    override fun close() { closed = true }
                }
            }) { _, _ -> fail("cancelled read") }
        }
        assertThrows(CancellationException::class.java) { runBlocking { work.await() } }
        work.cancelAndJoin()
        assertTrue(closed)
    }

    @Test
    fun cancellationImmediatelyBeforeCommitIsNotConvertedToUnknownSuccess() = runBlocking {
        assertThrows(CancellationException::class.java) {
            runBlocking {
                PhotoTrashMissingBasis.acquire(binding(), local, remote(), { ByteArrayInputStream(a) }) { _, _ ->
                    throw CancellationException("fixture commit")
                }
            }
        }
    }

    @Test
    fun aRejectedTransactionIsNotReportedAsAcquisition() = runBlocking {
        assertFalse(PhotoTrashMissingBasis.acquire(binding(), local, remote(), { ByteArrayInputStream(a) }) { _, _ -> false })
        assertFalse(PhotoTrashMissingBasis.acquire(binding(), local, remote(), { ByteArrayInputStream(a) }) { _, _ -> throw IllegalStateException() })
    }
}
