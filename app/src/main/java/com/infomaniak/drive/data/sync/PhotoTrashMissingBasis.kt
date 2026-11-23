/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2026 Infomaniak Network SA
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.infomaniak.drive.data.sync

import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import java.io.InputStream
import java.security.DigestInputStream
import java.security.MessageDigest

/** Missing evidence acquisition only. This never decides or performs a trash operation. */
internal object PhotoTrashMissingBasis {
    /** Immutable, complete row witness: no managed Realm objects or mutable Dates survive IO. */
    data class Binding(
        val uri: String,
        val userId: Int,
        val driveId: Int,
        val remoteFileId: Int,
        val fileName: String,
        val localSize: Long,
        val localModified: Long,
        val localDigest: String?,
        val remoteHash: String?,
        val pairedAt: Long?,
        val activationEpoch: Long,
        val baselineTrashed: Boolean?,
        val lastObservedTrashed: Boolean,
        val lastLocalTrashReconciledAt: Long,
        val pendingSince: Long,
        val lastRemoteTrashAppliedAt: Long,
        val state: String,
    )

    data class Remote(val id: Int, val driveId: Int, val type: String, val size: Long?, val hash: String?, val trashed: Boolean)

    /**
     * [commit] must atomically revalidate the entire binding AND scope/activation authority.
     * It may only fill the NULL hash. Callers return from this row's pass on success:
     * acquisition must not arm consent or propagate against observations preceding the write.
     */
    suspend fun acquire(
        binding: Binding,
        local: PhotoTrashRules.LocalObservation?,
        remote: Remote?,
        openOriginal: () -> InputStream?,
        commit: (Binding, String) -> Boolean,
    ): Boolean {
        val work = currentCoroutineContext()
        work.ensureActive()
        val hash = remote?.hash?.takeIf { PhotoTrashServerHash.parse(it) is PhotoTrashServerHash.Parsed.Supported }
            ?: return false
        if (binding.remoteHash != null || binding.pairedAt == null || binding.userId <= 0 || binding.driveId <= 0 ||
            binding.remoteFileId <= 0 || !SHA256.matches(binding.localDigest.orEmpty()) ||
            binding.state !in setOf("ACTIVE", "INCONCLUSIVE") || binding.pendingSince != 0L ||
            binding.baselineTrashed != false || binding.lastObservedTrashed ||
            local?.exists != true || local.isTrashed || local.size == null || local.lastModified == null ||
            remote.id != binding.remoteFileId || remote.driveId != binding.driveId || remote.type != "file" ||
            remote.trashed || remote.size != local.size ||
            (binding.localSize >= 0 && binding.localSize != local.size) ||
            (binding.localModified >= 0 && binding.localModified != local.lastModified)
        ) return false

        return try {
            val digest = MessageDigest.getInstance("SHA-256")
            var size = 0L
            val matches = openOriginal()?.use { source ->
                val counted = object : java.io.FilterInputStream(source) {
                    override fun read(): Int = super.read().also { if (it >= 0) size++ }
                    override fun read(b: ByteArray, off: Int, len: Int): Int =
                        `in`.read(b, off, len).also { if (it > 0) size += it }
                }
                val serverDigest = PhotoTrashServerHash.hashStreaming(DigestInputStream(counted, digest)) { work.ensureActive() }
                val localDigest = digest.digest().joinToString("") { "%02x".format(it) }
                size == local.size && localDigest == binding.localDigest && serverDigest == hash
            } ?: false
            work.ensureActive()
            matches && commit(binding, hash)
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (_: Exception) {
            // Unreadable/revoked/changed evidence never manufactures a basis or retries a write.
            false
        }
    }

    private val SHA256 = Regex("[0-9a-f]{64}")
}
