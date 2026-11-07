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

import com.infomaniak.drive.utils.OfflineFolderSyncUtils.DetailsDecision
import com.infomaniak.drive.utils.OfflineFolderSyncUtils.ListingDecision
import com.infomaniak.drive.utils.OfflineFolderSyncUtils.LocalCopyState
import com.infomaniak.drive.utils.OfflineFolderSyncUtils.PublicationBasis
import com.infomaniak.drive.utils.OfflineFolderSyncUtils.PublicationDecision
import com.infomaniak.drive.utils.OfflineFolderSyncUtils.RemoteMeta
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * Decisions of the recursive offline folder maintenance. The full production chain (walk ->
 * bulk download -> seam publication -> classify -> verify -> staged replace) is asserted on
 * device; these tests pin the exact decision contract that chain consumes, at decision level:
 * a DOWNLOAD or REDOWNLOAD decision costs one body request, VERIFY_DETAILS one metadata
 * request. Metadata equality alone never proves equal content (audit O4): the details request
 * is always consulted for intact copies, and a changed server hash must be detected even when
 * size and last-modified are unchanged.
 */
class OfflineFolderSyncRulesTest {

    private val meta = RemoteMeta(lastModifiedAt = 1_700_000_000L, size = 500L)
    private val basis = PublicationBasis(modifiedAt = meta.lastModifiedAt, size = meta.size, digest = "digest-1", serverHash = "hash-1")

    //region classifyLocalCopy — local edits are classified against the PRESERVED basis only

    @Test
    fun `existing bytes without a recorded basis are unknown, never auto-replaced`() {
        // Existing unreadable/unknown bytes are not empty or missing: maintenance must not
        // replace them just because a new download could be staged.
        assertEquals(LocalCopyState.UNKNOWN, OfflineFolderSyncUtils.classifyLocalCopy(basis = null, localExists = true, localSize = 500L, localModifiedSec = meta.lastModifiedAt))
        assertEquals(LocalCopyState.UNKNOWN, OfflineFolderSyncUtils.classifyLocalCopy(basis, localExists = true, localSize = null, localModifiedSec = null))
    }

    @Test
    fun `no published basis and no bytes means the copy is missing`() {
        assertEquals(LocalCopyState.MISSING, OfflineFolderSyncUtils.classifyLocalCopy(basis = null, localExists = false, localSize = null, localModifiedSec = null))
        assertEquals(LocalCopyState.MISSING, OfflineFolderSyncUtils.classifyLocalCopy(basis, localExists = false, localSize = null, localModifiedSec = null))
    }

    @Test
    fun `a copy matching the preserved basis is intact`() {
        assertEquals(LocalCopyState.INTACT, OfflineFolderSyncUtils.classifyLocalCopy(basis, localExists = true, localSize = 500L, localModifiedSec = meta.lastModifiedAt))
    }

    @Test
    fun `an edited copy is detected against the basis even when remote metadata already moved on`() {
        // The listing has already overwritten the row metadata; the basis is what protects
        // the local edit from being misclassified as remote content.
        assertEquals(LocalCopyState.LOCALLY_EDITED, OfflineFolderSyncUtils.classifyLocalCopy(basis, localExists = true, localSize = 501L, localModifiedSec = meta.lastModifiedAt))
        assertEquals(LocalCopyState.LOCALLY_EDITED, OfflineFolderSyncUtils.classifyLocalCopy(basis, localExists = true, localSize = 500L, localModifiedSec = meta.lastModifiedAt + 1))
    }

    //endregion

    //region decideFromListing — local state only; intact copies always verify with the server

    @Test
    fun `an intact copy always goes to details-with-hash because metadata equality proves nothing`() {
        assertEquals(ListingDecision.VERIFY_DETAILS, OfflineFolderSyncUtils.decideFromListing(basis, LocalCopyState.INTACT))
    }

    @Test
    fun `a missing copy downloads, locally edited and unknown copies are left alone`() {
        assertEquals(ListingDecision.DOWNLOAD, OfflineFolderSyncUtils.decideFromListing(basis, LocalCopyState.MISSING))
        assertEquals(ListingDecision.DOWNLOAD, OfflineFolderSyncUtils.decideFromListing(basis = null, local = LocalCopyState.MISSING))
        assertEquals(ListingDecision.SKIP_LOCAL_EDIT, OfflineFolderSyncUtils.decideFromListing(basis, LocalCopyState.LOCALLY_EDITED))
        assertEquals(ListingDecision.SKIP_LOCAL_EDIT, OfflineFolderSyncUtils.decideFromListing(basis = null, local = LocalCopyState.UNKNOWN))
    }

    //endregion

    //region decideFromDetails — the server hash is the exact content identity (O2/O4 contract)

    @Test
    fun `an equal server hash means metadata-only change, no body`() {
        assertEquals(
            DetailsDecision.METADATA_ONLY,
            OfflineFolderSyncUtils.decideFromDetails(meta.copy(serverHash = "hash-1"), basis),
        )
    }

    @Test
    fun `a changed server hash forces the staged redownload even under unchanged metadata`() {
        // Same size, same last-modified, different content: only the hash detects it.
        assertEquals(
            DetailsDecision.REDOWNLOAD,
            OfflineFolderSyncUtils.decideFromDetails(meta.copy(serverHash = "hash-2"), basis),
        )
    }

    @Test
    fun `a server hash without a stored counterpart cannot prove identity, one conservative staged download records it`() {
        val unpublished = PublicationBasis(modifiedAt = meta.lastModifiedAt, size = meta.size, digest = "digest-1", serverHash = null)
        assertEquals(
            DetailsDecision.REDOWNLOAD,
            OfflineFolderSyncUtils.decideFromDetails(meta.copy(serverHash = "hash-1"), unpublished),
        )
    }

    @Test
    fun `without hash support, unchanged metadata keeps the copy and changed metadata redownloads`() {
        // No hash available at all: repeated unchanged BODY downloads are forbidden, so
        // metadata equality stands (same-size/time edits are then undetectable: documented
        // limitation, honestly reported — not claimed as detected).
        val hashless = basis.copy(serverHash = null)
        assertEquals(
            DetailsDecision.METADATA_ONLY,
            OfflineFolderSyncUtils.decideFromDetails(meta.copy(serverHash = null), hashless),
        )
        assertEquals(
            DetailsDecision.REDOWNLOAD,
            OfflineFolderSyncUtils.decideFromDetails(meta.copy(lastModifiedAt = meta.lastModifiedAt + 1, serverHash = null), hashless),
        )
    }

    //endregion

    //region decidePublication — the pre-publication guards of a verified staged download
    //
    // Missing evidence always preserves: an unreadable digest, an unreadable stat, an unknown
    // copy or a same-size/time edit can never authorize destruction. Only a copy PROVEN intact
    // against the accepted publication (metadata AND digest) may be adopted or replaced.

    @Test
    fun `an unreadable existing digest preserves the copy - replacement never gambles`() {
        // Regression: a read/hash failure used to fall through the guard and proceed to delete
        // or replace bytes nobody could read.
        assertEquals(
            PublicationDecision.PRESERVE_EXISTING,
            OfflineFolderSyncUtils.decidePublication(
                basis = basis,
                localExists = true,
                localSize = basis.size,
                localModifiedSec = basis.modifiedAt,
                existingDigest = null,
                downloadDigest = "digest-2",
            ),
        )
    }

    @Test
    fun `a same-size and mtime local edit is preserved even when the staged body is unchanged`() {
        // Regression: the digest-identical early return used to re-stamp and bless an edited
        // copy as clean before the guards ran (edit hidden, accepted identity faked).
        assertEquals(
            PublicationDecision.PRESERVE_EXISTING,
            OfflineFolderSyncUtils.decidePublication(
                basis = basis,
                localExists = true,
                localSize = basis.size,
                localModifiedSec = basis.modifiedAt,
                existingDigest = "digest-edited",
                downloadDigest = "digest-1",
            ),
        )
    }

    @Test
    fun `an edited copy is preserved against a changed staged body`() {
        assertEquals(
            PublicationDecision.PRESERVE_EXISTING,
            OfflineFolderSyncUtils.decidePublication(
                basis = basis,
                localExists = true,
                localSize = basis.size,
                localModifiedSec = basis.modifiedAt,
                existingDigest = "digest-edited",
                downloadDigest = "digest-2",
            ),
        )
    }

    @Test
    fun `a verified clean copy adopts metadata when the body equals the publication`() {
        assertEquals(
            PublicationDecision.ADOPT_METADATA_ONLY,
            OfflineFolderSyncUtils.decidePublication(
                basis = basis,
                localExists = true,
                localSize = basis.size,
                localModifiedSec = basis.modifiedAt,
                existingDigest = "digest-1",
                downloadDigest = "digest-1",
            ),
        )
    }

    @Test
    fun `a verified clean copy is replaced when the server content changed`() {
        assertEquals(
            PublicationDecision.REPLACE_BYTES,
            OfflineFolderSyncUtils.decidePublication(
                basis = basis,
                localExists = true,
                localSize = basis.size,
                localModifiedSec = basis.modifiedAt,
                existingDigest = "digest-1",
                downloadDigest = "digest-2",
            ),
        )
    }

    @Test
    fun `missing local bytes are always replaced, even when the body equals the old publication`() {
        // Regression: the digest-identical early return used to setLastModified a non-existent
        // file, fail, and dead-lock the copy in a permanent MISSING retry loop.
        assertEquals(
            PublicationDecision.REPLACE_BYTES,
            OfflineFolderSyncUtils.decidePublication(
                basis = basis,
                localExists = false,
                localSize = null,
                localModifiedSec = null,
                existingDigest = null,
                downloadDigest = "digest-1",
            ),
        )
    }

    @Test
    fun `an unknown copy without a basis and unreadable stats are never replaced`() {
        assertEquals(
            PublicationDecision.PRESERVE_EXISTING,
            OfflineFolderSyncUtils.decidePublication(
                basis = null,
                localExists = true,
                localSize = 500L,
                localModifiedSec = meta.lastModifiedAt,
                existingDigest = "digest-x",
                downloadDigest = "digest-1",
            ),
        )
        assertEquals(
            PublicationDecision.PRESERVE_EXISTING,
            OfflineFolderSyncUtils.decidePublication(
                basis = basis,
                localExists = true,
                localSize = null,
                localModifiedSec = null,
                existingDigest = "digest-1",
                downloadDigest = "digest-1",
            ),
        )
        assertEquals(
            PublicationDecision.PRESERVE_EXISTING,
            OfflineFolderSyncUtils.decidePublication(
                basis = basis.copy(digest = null),
                localExists = true,
                localSize = basis.size,
                localModifiedSec = basis.modifiedAt,
                existingDigest = "digest-1",
                downloadDigest = "digest-1",
            ),
        )
    }

    //endregion

    //region Pass sequences (audit O4 regression shape, decision level)

    /**
     * One maintenance pass in the shape the production caller consumes: classify against the
     * preserved basis, stage 1 on the local state, stage 2 on the details result. A
     * DOWNLOAD/REDOWNLOAD decision performs one body request and the publication seam records
     * the basis from the accepted bytes; VERIFY_DETAILS performs one metadata request.
     */
    private class FakeFile {
        var serverMeta: RemoteMeta = RemoteMeta(lastModifiedAt = 1_700_000_000L, size = 500L, serverHash = "hash-1")
        var localBytesExist = false
        var localEdited = false
        var basis: PublicationBasis? = null
        var bodyRequests = 0
        var detailsRequests = 0

        fun runPass() {
            val local = when {
                !localBytesExist -> LocalCopyState.MISSING
                localEdited -> LocalCopyState.LOCALLY_EDITED
                else -> OfflineFolderSyncUtils.classifyLocalCopy(
                    basis = basis,
                    localExists = true,
                    localSize = basis?.size,
                    localModifiedSec = basis?.modifiedAt,
                )
            }
            when (OfflineFolderSyncUtils.decideFromListing(basis, local)) {
                ListingDecision.SKIP_LOCAL_EDIT -> Unit
                ListingDecision.DOWNLOAD -> publish()
                ListingDecision.VERIFY_DETAILS -> {
                    detailsRequests++
                    when (OfflineFolderSyncUtils.decideFromDetails(serverMeta, basis)) {
                        DetailsDecision.METADATA_ONLY -> {
                            // updatePublicationBasis: adopt the verified remote metadata, bytes
                            // kept; an unchanged file is a no-op (no per-pass row write).
                            basis = PublicationBasis(serverMeta.lastModifiedAt, serverMeta.size, basis?.digest, serverMeta.serverHash)
                        }
                        DetailsDecision.REDOWNLOAD -> publish()
                    }
                }
            }
        }

        /** The publication seam: basis recorded from accepted bytes + the details identity. */
        private fun publish() {
            bodyRequests++
            localBytesExist = true
            localEdited = false
            basis = PublicationBasis(serverMeta.lastModifiedAt, serverMeta.size, "digest-${bodyRequests}", serverMeta.serverHash)
        }
    }

    @Test
    fun `first publication, unchanged pass and changed-hash pass fetch a body only when required`() {
        val file = FakeFile()

        // Pass 1 (first): nothing published, no bytes -> exactly one body.
        file.runPass()
        assertEquals(1, file.bodyRequests)
        assertEquals(0, file.detailsRequests)
        assertEquals("hash-1", file.basis?.serverHash)

        // Pass 2 (unchanged): intact copy -> details consulted (metadata request), hash equal,
        // no body.
        file.runPass()
        assertEquals(1, file.bodyRequests)
        assertEquals(1, file.detailsRequests)

        // Pass 3 (changed hash, UNCHANGED size and last-modified, no version evidence): the
        // details response carries a different hash -> exactly one more body, basis updated.
        file.serverMeta = file.serverMeta.copy(serverHash = "hash-2")
        file.runPass()
        assertEquals(2, file.bodyRequests)
        assertEquals(2, file.detailsRequests)
        assertEquals("hash-2", file.basis?.serverHash)

        // Pass 4 (unchanged again): stable at metadata-only.
        file.runPass()
        assertEquals(2, file.bodyRequests)
        assertEquals(3, file.detailsRequests)
    }

    @Test
    fun `a locally edited copy is preserved through any number of passes`() {
        val file = FakeFile()
        file.runPass() // publish
        val publishedBasis = file.basis
        file.localEdited = true

        file.serverMeta = file.serverMeta.copy(serverHash = "hash-2", lastModifiedAt = file.serverMeta.lastModifiedAt + 5)
        repeat(3) { file.runPass() }

        assertEquals(publishedBasis, file.basis) // no pass touched the publication record
        assertEquals(1, file.bodyRequests)
        assertEquals(0, file.detailsRequests)
        assertEquals(true, file.localEdited)
    }

    @Test
    fun `without hash support an unchanged file costs metadata requests but never repeated bodies`() {
        val file = FakeFile()
        file.serverMeta = file.serverMeta.copy(serverHash = null)
        file.runPass() // first publication: one body
        assertEquals(1, file.bodyRequests)

        repeat(3) { file.runPass() }

        assertEquals(1, file.bodyRequests) // no repeated body for unchanged metadata
        assertEquals(3, file.detailsRequests) // every pass verifies with a metadata request
    }

    @Test
    fun `a stored hash missing while the server has one converges after one staged body`() {
        val file = FakeFile()
        file.runPass()
        // Simulate a publication whose hash request failed: basis without server identity.
        file.basis = file.basis?.copy(serverHash = null)

        file.runPass() // cannot prove identity -> one staged body records the hash
        assertEquals(2, file.bodyRequests)
        assertEquals("hash-1", file.basis?.serverHash)

        file.runPass() // then stable
        assertEquals(2, file.bodyRequests)
    }

    //endregion
}
