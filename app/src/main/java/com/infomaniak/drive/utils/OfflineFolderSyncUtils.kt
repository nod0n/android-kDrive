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

import android.content.Context
import androidx.annotation.VisibleForTesting
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.ApiRoutes
import com.infomaniak.drive.data.cache.DriveInfosController
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.File.SortType
import com.infomaniak.drive.data.models.OfflineFolderRoot
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.MediaUtils.deleteInMediaScan
import com.infomaniak.drive.utils.MediaUtils.isMedia
import io.realm.Realm
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.invoke
import kotlinx.coroutines.withContext
import java.io.File as JavaFile
import java.security.MessageDigest

/**
 * Recursive persistent offline folders (issue #1902), KISS implementation.
 *
 * Contract (see SCOPE-PLAN §2):
 *  - root coverage ([isRootCoveredCopy]) and offline intent ([hasLiveOfflineIntent]) are
 *    derived LIVE from the roots table and the row's flags — never from a publication basis:
 *    guards act on them across initial, pending and stale work, direct pins keep baseline
 *    semantics, and a late transfer can never resurrect withdrawn offline status;
 *  - a publication basis (remote lastModifiedAt/size + sha-256 digest of the accepted bytes +
 *    server with=hash identity) is recorded ONLY from successful accepted bytes, by the
 *    download seam ([recordAcceptedPublication]); local-edit classification uses ONLY that
 *    basis, never metadata overwritten by a later listing;
 *  - metadata equality never proves equal content: every intact copy is verified with one
 *    cheap details request per maintenance pass, and the server hash — when the server
 *    provides one — decides whether the body is re-fetched. Unchanged BODIES are never
 *    repeatedly downloaded; without hash support, unchanged metadata keeps the copy (same
 *    size/time edits are then undetectable: documented limitation);
 *  - replacements download to a staging file and atomically replace the previous copy only
 *    after a verified transfer — failure, cancellation or a locally-edited or unknown copy
 *    always preserves the previous bytes;
 *  - a covered copy's INITIAL download publishes through that same staged path (never a
 *    direct write to the final path): a cancellation discards owned staging, and a
 *    withdrawal mid-transfer skips the atomic rename instead of deleting bytes at the
 *    final path after the fact;
 *  - a rename relocation never overwrites an occupied target (rename(2) replaces
 *    silently): an occupied target refuses the move and both copies stay preserved;
 *  - a failed or incomplete traversal never proves absence: coverage is released only after a
 *    complete walk, remote deletion inside a root stops coverage without deleting bytes, and
 *    only an explicit user unmark deletes copies, through the checked path (delete first,
 *    clear state only on confirmed success).
 */
object OfflineFolderSyncUtils {

    /**
     * The publication critical section shim: dispatches to IO and runs [block] inside the
     * process-local commit authority ([OfflineCommitAuthority]) that orders the final
     * live-intent/current-content validation plus publication against EVERY coordinated
     * predicate mutation — the intent-withdrawal mutations (explicit unmark, direct unpin,
     * remote-delete release) AND the tree/pin/intent writers (cached parent-chain updates,
     * row refreshes and deletions, pin changes, registry changes) that previously bypassed
     * the coordination. Without the exclusion, a writer committing between the section's
     * last fresh read and the atomic rename could still invalidate the decision the rename
     * was about to execute.
     *
     * The block is a PLAIN (non-suspending) function: the authority is thread-owned and no
     * hold may span a suspension. The NETWORK TRANSFER and every whole-file digest stay
     * outside the critical section — the hold is local-IO sized (metadata, stat, one
     * rename/one checked deletion, one Realm transaction). All covered callers participate:
     * [stagedRedownload], [updatePublicationBasis], [recordAcceptedPublication],
     * [unmarkFolderOffline], the maintenance pass's union discharge of [syncOfflineFolderRoots]
     * and the cache-move publication seam in DownloadOfflineFileManager, plus the unmark
     * entry points in MainViewModel. A pending-marker clear that keeps [File.isOffline]
     * (bulk download completion) participates anyway (it is a coordinated writer) although
     * it does not withdraw persistent intent.
     *
     * REAL LIMITS (documented, not claimed away): this orders MUTATIONS inside one process
     * — the filesystem and Realm are NOT one atomic resource, and a crash mid-section can
     * still leave renamed bytes whose basis write never happened (such bytes classify as
     * UNKNOWN and are preserved by every later decision) or vice versa. Writers OUTSIDE the
     * process (other apps editing the offline bytes, adb, other processes) do not
     * participate — exactly as they never participated in any in-process lock; a
     * non-participating byte edit between the in-hold evidence reads and the effect remains
     * a disclosed platform-level window, and power loss is outside any app-side lock.
     */
    internal suspend fun <T> withPublicationLock(block: () -> T): T = withContext(Dispatchers.IO) {
        OfflineCommitAuthority.withCommitAuthority(block)
    }

    /**
     * Test seam: invoked at the narrow publication point of [stagedRedownload] — inside the
     * commit authority's hold, BEFORE the final live-intent read, so a regression can launch
     * positive-contention probes (a coordinated writer that must block until the section
     * completes) exactly inside the publication window. The block must not suspend. Production
     * never sets it.
     */
    @VisibleForTesting
    internal var prePublicationGate: (() -> Unit)? = null

    /**
     * Test seam: invoked inside the publication critical section AFTER the content decision
     * and BEFORE the final pre-rename authority re-check of [stagedRedownload] — so a
     * regression can perform a NON-participating cached tree update exactly inside the final
     * publication window (after the in-lock read), where production's fresh re-read must
     * observe it BEFORE the filesystem effect. Production never sets it.
     */
    @VisibleForTesting
    internal var preRenameGate: (() -> Unit)? = null

    private const val TAG = "OfflineFolderSync"

    //region Pure decision model (shared with tests)

    data class RemoteMeta(val lastModifiedAt: Long, val size: Long?, val serverHash: String? = null)

    data class PublicationBasis(val modifiedAt: Long, val size: Long?, val digest: String? = null, val serverHash: String? = null)

    enum class LocalCopyState { MISSING, INTACT, LOCALLY_EDITED, UNKNOWN }

    enum class ListingDecision { DOWNLOAD, VERIFY_DETAILS, SKIP_LOCAL_EDIT }

    enum class DetailsDecision { METADATA_ONLY, REDOWNLOAD }

    /**
     * Classifies the local copy against the preserved publication basis only. Bytes whose
     * state cannot be read (or that exist without a recorded basis) are UNKNOWN: maintenance
     * never auto-replaces unknown bytes just because a new download could be staged — only
     * provably missing bytes (MISSING) lead to a download.
     */
    fun classifyLocalCopy(basis: PublicationBasis?, localExists: Boolean, localSize: Long?, localModifiedSec: Long?): LocalCopyState = when {
        basis == null -> if (localExists) LocalCopyState.UNKNOWN else LocalCopyState.MISSING
        !localExists -> LocalCopyState.MISSING
        localSize == null || localModifiedSec == null -> LocalCopyState.UNKNOWN
        localModifiedSec != basis.modifiedAt || localSize != basis.size -> LocalCopyState.LOCALLY_EDITED
        else -> LocalCopyState.INTACT
    }

    /**
     * Stage 1 decision. Metadata equality — even against the preserved basis — cannot itself
     * prove equal content, so every intact copy is verified against the server with one cheap
     * details request; the details response (hash when available, metadata otherwise) then
     * decides. What is forbidden is repeated unchanged BODY downloads, not metadata requests.
     */
    fun decideFromListing(basis: PublicationBasis?, local: LocalCopyState): ListingDecision = when (local) {
        LocalCopyState.LOCALLY_EDITED, LocalCopyState.UNKNOWN -> ListingDecision.SKIP_LOCAL_EDIT
        LocalCopyState.MISSING -> ListingDecision.DOWNLOAD
        LocalCopyState.INTACT -> ListingDecision.VERIFY_DETAILS
    }

    /**
     * Stage 2 decision from details carrying the official with=hash identity: a hash equality
     * proves identical content (metadata-only change); a difference proves a change. Without a
     * stored hash but with a server hash available, the content cannot be proven identical, so
     * one conservative staged download (digest-checked, so an identical body is adopted without
     * replacement) records the identity. Without any hash support, metadata equality is the
     * only evidence: it cannot prove content identity (documented limitation), but repeated
     * unchanged body downloads are forbidden, so the record is left in place.
     */
    fun decideFromDetails(meta: RemoteMeta, basis: PublicationBasis?): DetailsDecision = when {
        meta.serverHash != null && meta.serverHash.isNotBlank() -> when {
            basis?.serverHash == meta.serverHash -> DetailsDecision.METADATA_ONLY
            else -> DetailsDecision.REDOWNLOAD
        }
        else -> if (metaMatchesBasis(meta, basis)) DetailsDecision.METADATA_ONLY else DetailsDecision.REDOWNLOAD
    }

    private fun metaMatchesBasis(meta: RemoteMeta, basis: PublicationBasis?): Boolean {
        if (basis == null) return false
        if (meta.lastModifiedAt != basis.modifiedAt) return false
        if (meta.size != null && basis.size != null && meta.size != basis.size) return false
        return true
    }

    /**
     * The pre-publication guards of a verified staged download, as a pure decision. Given the
     * preserved basis and the CURRENT observable state of the existing local copy, what may be
     * done with the staged bytes? Missing evidence always preserves: an unreadable or absent
     * digest, an unknown or edited copy, or an unreadable stat can never authorize destruction.
     * Only a copy PROVEN intact against the accepted publication (metadata AND digest) may be
     * adopted (metadata-only) or replaced (server content changed).
     */
    fun decidePublication(
        basis: PublicationBasis?,
        localExists: Boolean,
        localSize: Long?,
        localModifiedSec: Long?,
        existingDigest: String?,
        downloadDigest: String,
    ): PublicationDecision {
        if (!localExists) return PublicationDecision.REPLACE_BYTES // nothing local to destroy
        val localState = classifyLocalCopy(basis, localExists, localSize, localModifiedSec)
        if (localState != LocalCopyState.INTACT) return PublicationDecision.PRESERVE_EXISTING
        val preservedDigest = basis?.digest
        // Without a readable existing digest or a recorded accepted identity, the local bytes
        // cannot be proven to be the accepted publication: replacement would be a gamble.
        if (existingDigest == null || preservedDigest == null) return PublicationDecision.PRESERVE_EXISTING
        if (existingDigest != preservedDigest) return PublicationDecision.PRESERVE_EXISTING
        return if (downloadDigest == preservedDigest) {
            PublicationDecision.ADOPT_METADATA_ONLY
        } else {
            PublicationDecision.REPLACE_BYTES
        }
    }

    enum class PublicationDecision { PRESERVE_EXISTING, ADOPT_METADATA_ONLY, REPLACE_BYTES }

    /**
     * Pre-hold byte evidence for a local copy: the stat values the digest was computed from
     * plus the digest itself. A hold re-stats the copy and keeps the evidence authoritative
     * only while the stat is unchanged (coordinated writers are excluded for the whole hold;
     * a stat change means a non-participating writer touched the bytes — the evidence is
     * stale and the copy is preserved, never mutated on stale evidence).
     */
    private data class VerifiedLocalCopy(val length: Long, val modifiedSec: Long, val digest: String?)

    //endregion

    /**
     * True when this copy's lifecycle is owned by an offline folder root: the file sits inside
     * the subtree of a live [OfflineFolderRoot] of [userDrive] (a parent-chain walk over the
     * cache finds one of the roots' folders). This is INDEPENDENT of any publication basis,
     * pending marker or download history — a file is covered from the moment it lies inside a
     * marked root, whether its copy is initial, pending or stale — and independent of a direct
     * pin: a pin controls RETENTION when a root is unmarked ([unmarkFolderOffline] keeps pinned
     * copies), it does not license bypassing the safeguards of a root that currently covers the
     * file. Outside every root the chain misses and baseline direct-pin semantics apply
     * unchanged. The baseline offline sync must not apply its delete/upload/download effects to
     * covered copies first: the root maintenance pass handles remote deletions (keeping bytes),
     * refreshes changes through the staged path and preserves local edits.
     */
    fun isRootCoveredCopy(file: File, realm: Realm, userDrive: UserDrive): Boolean {
        if (file.isFolder()) return false
        val rootFolderIds = realm.where(OfflineFolderRoot::class.java)
            .equalTo(OfflineFolderRoot::driveId.name, userDrive.driveId)
            .findAll()
            .map { it.folderId }
            .toSet()
        if (rootFolderIds.isEmpty()) return false
        var parentId: Int? = file.parentId
        val visited = mutableSetOf<Int>()
        while (parentId != null && visited.add(parentId)) {
            if (parentId in rootFolderIds) return true
            // DRIVE-SCOPED walk: file ids are per-drive sequences, so the same id exists on
            // several drives of one realm — the chain must never route through another
            // drive's rows.
            val parent = realm.where(File::class.java)
                .equalTo(File::id.name, parentId)
                .equalTo(File::driveId.name, file.driveId)
                .findFirst() ?: return false
            parentId = parent.parentId
        }
        return false
    }

    /**
     * The root contract's PERSISTENT claim on this copy: live subtree coverage, or recorded
     * root ownership ([File.offlineOwnedByRoot]) that survives a cached tree/listing update
     * moving the row (or an ancestor) out of the subtree while root work was queued or
     * published. Live coverage alone cannot carry that interval: both of its reads would
     * walk the already-updated parent chain and find nothing. A direct pin SUPPRESSES the
     * recorded claim — explicit direct retention keeps the baseline (uncovered) semantics,
     * and overlapping roots keep the claim until their own withdrawals release it.
     * Ownership provenance is lifecycle evidence, not intent: [hasLiveOfflineIntent] still
     * governs whether any transfer may run at all, and the baseline offline sync must keep
     * skipping root-owned copies so it cannot undo preservation before root maintenance.
     */
    fun isRootOwnedCopy(file: File, realm: Realm, userDrive: UserDrive): Boolean {
        if (file.isFolder()) return false
        return isRootCoveredCopy(file, realm, userDrive) || (file.offlineOwnedByRoot && !file.isDirectlyPinned)
    }

    /**
     * Live offline ownership at a publication point: a pending download, a persistent offline
     * copy, a direct pin, or coverage by a live root. An explicit unmark or a remote-delete
     * release clears every signal — a transfer that started earlier must not resurrect offline
     * status that the user (or the folder contract) has withdrawn in the meantime.
     */
    fun hasLiveOfflineIntent(file: File, realm: Realm, userDrive: UserDrive): Boolean =
        file.isMarkedAsOffline || file.isOffline || file.isDirectlyPinned || isRootCoveredCopy(file, realm, userDrive)

    /**
     * Remote paths of every non-folder file inside a live root's subtree (cache BFS). Used by
     * the legacy offline cleanup to recognize entries that hold a covered copy.
     */
    fun coveredSubtreePaths(userDrive: UserDrive): Set<String> {
        val rootFolderIds = FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(OfflineFolderRoot::class.java)
                .equalTo(OfflineFolderRoot::driveId.name, userDrive.driveId)
                .findAll()
                .map { it.folderId }
        }
        if (rootFolderIds.isEmpty()) return emptySet()
        return FileController.getRealmInstance(userDrive).use { realm ->
            val paths = mutableSetOf<String>()
            rootFolderIds.forEach { rootFolderId ->
                val queue = ArrayDeque(listOf(rootFolderId))
                val visited = mutableSetOf<Int>()
                while (queue.isNotEmpty()) {
                    val current = queue.removeFirst()
                    if (!visited.add(current)) continue
                    realm.where(File::class.java).equalTo(File::parentId.name, current).findAll().forEach { child ->
                        if (!child.isFolder()) child.path.takeIf { it.isNotBlank() }?.let(paths::add)
                        if (child.isFolder()) queue.add(child.id)
                    }
                }
            }
            paths
        }
    }

    enum class WalkOutcome { COMPLETE, INCOMPLETE }

    /** Marks a folder (and its whole subtree) as persistently available offline. */
    suspend fun markFolderOffline(context: Context, folder: File, userDrive: UserDrive): Boolean {
        val root = OfflineFolderRoot(folderId = folder.id, driveId = userDrive.driveId, userId = userDrive.userId).apply { initUid() }
        // The registry mutation is a coordinated writer: it joins the commit authority so a
        // concurrent unmark of the same root (or a publication's registry reads) serializes
        // against it.
        OfflineCommitAuthority.withCommitAuthority {
            FileController.getRealmInstance(userDrive).use { realm ->
                realm.executeTransaction { mutableRealm -> mutableRealm.insertOrUpdate(root) }
            }
        }

        val foldersWithMarkedFiles = mutableListOf<Int>()
        val outcome = walkSubtree(
            context = context,
            userDrive = userDrive,
            rootFolderFile = folder,
            onFolderWithMarkedFiles = foldersWithMarkedFiles::add,
        )
        if (outcome == WalkOutcome.INCOMPLETE) {
            // The root row stays: the next sync pass re-walks and completes the marking. A
            // partial walk never enables cleanup.
            SentryLog.w(TAG, "Initial offline walk incomplete for root ${root.uid}")
        }
        // The existing bulk worker chain picks up every marked direct child per folder.
        foldersWithMarkedFiles.forEach { folderId ->
            Utils.enqueueBulkDownloadWorker(context, folderId, userDrive)
        }
        return outcome == WalkOutcome.COMPLETE
    }

    /**
     * Removes a root and stops maintaining its subtree. Copies are deleted only through the
     * checked path; files directly pinned or covered by another root keep their copies and
     * marks; copies classified as locally edited or unknown against the accepted basis are
     * PRESERVED (their bytes are the user's, never deleted by an unmark). On a REFUSED
     * deletion the root row is kept so the next pass retries; the return value reports that
     * retriable state.
     */
    suspend fun unmarkFolderOffline(context: Context, folder: File, userDrive: UserDrive): Boolean {
        val rootUid = OfflineFolderRoot.buildUid(folder.id, userDrive.driveId, userDrive.userId)
        val subtreeIds = collectSubtreeIdsFromCache(userDrive, folder.id)
        val otherRootSubtreeIds = FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(OfflineFolderRoot::class.java)
                .notEqualTo(OfflineFolderRoot::uid.name, rootUid)
                .findAll()
                .flatMap { root -> collectSubtreeIdsFromCache(userDrive, root.folderId) }
                .toSet()
        }

        var allDeletionsConfirmed = true
        subtreeIds.forEach { fileId ->
            currentCoroutineContext().ensureActive()
            // The whole per-file withdrawal (classification evidence OUTSIDE the authority,
            // then the fresh in-hold re-reads, the checked deletion and the state clear
            // INSIDE it) is serialized against every publication's final check-to-effect
            // interval AND every coordinated predicate writer, so neither can interleave
            // into the other's defined order.
            val refusal = withdrawSingleCopyInHold(
                context = context,
                userDrive = userDrive,
                fileId = fileId,
                otherRootSubtreeIds = otherRootSubtreeIds,
                excludedRootFolderId = null,
                atRecordedPath = false,
            )
            if (refusal) allDeletionsConfirmed = false
        }

        // BEYOND-SUBTREE OWNERSHIP RELEASE: rows the root contract owned whose cached row
        // has since MOVED OUT of the subtree — or that the per-file loop skipped (direct
        // pins, another root's copies) — are invisible to the subtree collection above;
        // releasing only that walk would leave their recorded claim behind forever (an
        // immortal stale ownership bit). The sweep releases every remaining claimed row:
        // another live root's coverage keeps the claim (overlapping roots), a direct pin
        // loses only the root's claim, and every other claimed row runs the SAME
        // preserve/delete decision as the in-subtree withdrawal — at the bytes' RECORDED
        // publication path first, because a moved-out row's derived path no longer
        // describes where they live.
        val beyondSubtreeConfirmed = releaseOwnershipBeyondSubtree(context, userDrive, folder.id)

        // FINAL ATOMIC CLOSER: the root-row deletion shares ONE authority hold with a fresh
        // re-run of the release sweep, so the coordinated marking transactions of a walk
        // (which join this same authority and re-check the registry inside their own
        // transaction) can neither resurrect claimed work between the sweep and the
        // deletion (they would see the registry row still present) nor after it (they see
        // it gone and skip). Byte-less claimed rows — the marking race's residue, pending
        // work that never published — are cleared here (nothing to delete, no digest
        // needed); rows that still hold bytes keep their claim and force a retriable
        // unmark, so a destructive decision is never made inside this bounded hold without
        // fresh digest evidence.
        val closerConfirmed = withContext(Dispatchers.IO) {
            val job = currentCoroutineContext()[Job]
            OfflineCommitAuthority.withCommitAuthority {
                var rowsWithBytesRemain = false
                val claimedIds = FileController.getRealmInstance(userDrive).use { realm ->
                    realm.where(File::class.java)
                        .equalTo(File::offlineOwnedByRoot.name, true)
                        .equalTo(File::driveId.name, userDrive.driveId)
                        .findAll()
                        .map { it.id }
                }
                for (fileId in claimedIds) {
                    if (job?.isActive == false) return@withCommitAuthority false // incomplete closer: retriable
                    FileController.getRealmInstance(userDrive).use { realm ->
                        val file = FileController.getFileProxyById(fileId, customRealm = realm)
                            ?: return@use // the row is gone: nothing left to release
                        if (file.isFolder()) return@use // walkSubtree never claims folders
                        if (isCoveredByRoot(file, realm, userDrive, excludedRootFolderId = folder.id)) {
                            return@use // another live owner keeps the claim until its own withdrawal
                        }
                        if (file.isDirectlyPinned) {
                            realm.executeTransaction { mutableRealm ->
                                FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.offlineOwnedByRoot = false
                            }
                            return@use
                        }
                        val recordedPath = file.offlinePublishedPath
                        val offlineFile: JavaFile? = if (recordedPath.isNullOrBlank()) {
                            file.getOfflineFile(context, userDrive.userId)
                        } else {
                            JavaFile(recordedPath)
                        }
                        if (offlineFile?.exists() == true) {
                            // Bytes still stand: never a destructive decision without the
                            // full evidence pass — keep the claim and retry on the next
                            // unmark pass.
                            rowsWithBytesRemain = true
                            return@use
                        }
                        realm.executeTransaction { mutableRealm ->
                            FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.let { proxy ->
                                proxy.isMarkedAsOffline = false
                                proxy.isOffline = false
                                if (!proxy.isDirectlyPinned) proxy.isDirectlyPinned = false
                                proxy.offlineOwnedByRoot = false
                                proxy.offlineBasisModifiedAt = 0L
                                proxy.offlineBasisSize = null
                                proxy.offlineContentDigest = null
                                proxy.serverContentHash = null
                            }
                        }
                    }
                }
                if (rowsWithBytesRemain) return@withCommitAuthority false
                FileController.getRealmInstance(userDrive).use { realm ->
                    realm.executeTransaction { mutableRealm ->
                        mutableRealm.where(OfflineFolderRoot::class.java)
                            .equalTo(OfflineFolderRoot::uid.name, rootUid)
                            .findAll()
                            .deleteAllFromRealm()
                    }
                }
                true
            }
        }

        // A refused deletion (or an incomplete closer) keeps the root row: the next unmark
        // pass retries it, and the protected files stay covered meanwhile. The unmark is
        // only complete when EVERY release confirmed.
        return allDeletionsConfirmed && beyondSubtreeConfirmed && closerConfirmed
    }

    /**
     * Pre-hold withdrawal evidence for one copy: the stat values and the digest verdict a
     * destructive decision would rest on, computed OUTSIDE the commit authority (a
     * whole-file digest never runs inside a hold). The hold re-stats before acting: an
     * unchanged stat keeps the evidence authoritative (every coordinated writer is excluded
     * for the whole hold); a changed stat means a non-participating writer touched the
     * bytes — the evidence is stale and the copy is PRESERVED, never deleted on stale
     * evidence.
     */
    private data class WithdrawalEvidence(
        val offlineFile: JavaFile?,
        val localExists: Boolean,
        val length: Long?,
        val modifiedSec: Long?,
        val preservesBytes: Boolean,
    )

    private fun readWithdrawalEvidence(
        context: Context,
        file: File,
        userDrive: UserDrive,
        atRecordedPath: Boolean,
    ): WithdrawalEvidence {
        val recordedPath = file.offlinePublishedPath
        val offlineFile: JavaFile? = when {
            atRecordedPath && !recordedPath.isNullOrBlank() -> JavaFile(recordedPath)
            else -> file.getOfflineFile(context, userDrive.userId)
        }
        val localExists = offlineFile?.exists() == true
        if (!localExists || offlineFile == null) {
            return WithdrawalEvidence(offlineFile, localExists = false, length = null, modifiedSec = null, preservesBytes = false)
        }
        val length = offlineFile.length()
        val modifiedSec = offlineFile.lastModified().div(1_000L)
        val basis = if (file.offlineBasisModifiedAt > 0L) PublicationBasis(
            modifiedAt = file.offlineBasisModifiedAt,
            size = file.offlineBasisSize,
            digest = file.offlineContentDigest,
            serverHash = file.serverContentHash,
        ) else null
        val localState = classifyLocalCopy(
            basis = basis,
            localExists = true,
            localSize = length,
            localModifiedSec = modifiedSec,
        )
        // A same-size/same-mtime edit still diverges from the accepted digest: verify the
        // accepted identity before any destructive step. An unreadable digest (null) or a
        // digest-less basis is preservation, not authority.
        val digestVerifies = basis?.digest != null && computeDigest(offlineFile) == basis.digest
        val preservesBytes = localState == LocalCopyState.LOCALLY_EDITED ||
            localState == LocalCopyState.UNKNOWN ||
            !digestVerifies
        return WithdrawalEvidence(offlineFile, localExists = true, length = length, modifiedSec = modifiedSec, preservesBytes = preservesBytes)
    }

    /**
     * ONE copy's coordinated withdrawal: classification evidence is read OUTSIDE the
     * authority ([readWithdrawalEvidence]), then the hold re-reads the row fresh, re-checks
     * every keep-copy contract, re-stats the evidence, and performs the checked deletion
     * and the state clear. Returns TRUE only for a REFUSAL (a deletion the filesystem
     * refused — the claim stays and the caller reports a retriable unmark); skips,
     * preserved releases and confirmed deletions return false.
     *
     * @param otherRootSubtreeIds the in-subtree loop's pre-computed other-roots snapshot
     *   (overlapping-roots check); null selects the sweep's live registry check against
     *   [excludedRootFolderId] instead
     * @param excludedRootFolderId the root whose own coverage must not keep the claim (the
     *   sweep runs while its root row is still registered); null for the in-subtree loop
     * @param atRecordedPath look the bytes up at their RECORDED publication path first (a
     *   moved-out row's derived path no longer describes where they live)
     */
    private suspend fun withdrawSingleCopyInHold(
        context: Context,
        userDrive: UserDrive,
        fileId: Int,
        otherRootSubtreeIds: Set<Int>?,
        excludedRootFolderId: Int?,
        atRecordedPath: Boolean,
    ): Boolean {
        // Evidence pass OUTSIDE the hold.
        val evidence = FileController.getRealmInstance(userDrive).use { realm ->
            FileController.getFileProxyById(fileId, customRealm = realm)?.let { row ->
                if (row.isFolder()) null else readWithdrawalEvidence(context, row, userDrive, atRecordedPath)
            }
        } ?: return false // vanished row or folder: nothing to release

        return withContext(Dispatchers.IO) {
            OfflineCommitAuthority.withCommitAuthority {
                FileController.getRealmInstance(userDrive).use { realm ->
                    val file = FileController.getFileProxyById(fileId, customRealm = realm)
                        ?: return@withCommitAuthority false // the row is gone: nothing left to release
                    if (file.isFolder()) return@withCommitAuthority false // walkSubtree never claims folders
                    val coveredByOtherRoot = when {
                        otherRootSubtreeIds != null -> fileId in otherRootSubtreeIds
                        excludedRootFolderId != null -> isCoveredByRoot(file, realm, userDrive, excludedRootFolderId)
                        else -> false
                    }
                    if (file.isDirectlyPinned || coveredByOtherRoot) {
                        if (excludedRootFolderId != null && file.isDirectlyPinned && !coveredByOtherRoot) {
                            // Explicit direct retention keeps its own contract (bytes, pin
                            // and offline state untouched): only the root's claim is
                            // released (the sweep's contract).
                            realm.executeTransaction { mutableRealm ->
                                FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.offlineOwnedByRoot = false
                            }
                        }
                        return@withCommitAuthority false
                    }

                    // The evidence re-stat: an unchanged stat keeps the pre-hold evidence
                    // authoritative (every coordinated writer is excluded for the whole
                    // hold); a changed stat means a non-participating writer touched the
                    // bytes — PRESERVED, never deleted on stale evidence.
                    val statUnchanged = evidence.offlineFile != null && evidence.offlineFile.exists() &&
                        evidence.offlineFile.length() == evidence.length &&
                        evidence.offlineFile.lastModified().div(1_000L) == evidence.modifiedSec
                    val preservesBytes = when {
                        evidence.localExists -> !statUnchanged || evidence.preservesBytes
                        // Bytes appeared under the coordination (an external writer): unproven — preserved.
                        else -> evidence.offlineFile?.exists() == true
                    }
                    if (preservesBytes) {
                        if (evidence.localExists && statUnchanged) {
                            SentryLog.w(TAG, "Unmark keeps edited/unknown/unverifiable bytes for file ${file.id}")
                        } else {
                            SentryLog.w(TAG, "Unmark evidence went stale under the coordination for file ${file.id}; bytes preserved")
                        }
                        realm.executeTransaction { mutableRealm ->
                            FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.let { proxy ->
                                proxy.isMarkedAsOffline = false
                                proxy.isOffline = false
                                proxy.offlineOwnedByRoot = false
                                proxy.offlineBasisModifiedAt = 0L
                                proxy.offlineBasisSize = null
                                proxy.offlineContentDigest = null
                                proxy.serverContentHash = null
                            }
                        }
                        return@withCommitAuthority false
                    }

                    // Checked deletion: clear the offline state only once the bytes are
                    // really gone. The pin writer is excluded for the whole hold (it joins
                    // this authority), so the pre-hold pin reads above are final; the
                    // in-transaction flag re-check below stays as defense-in-depth only.
                    val copyGone = if (evidence.localExists && evidence.offlineFile != null) {
                        evidence.offlineFile.delete()
                    } else {
                        true
                    }
                    if (!copyGone) {
                        SentryLog.w(TAG, "Offline copy deletion refused, keeping pin for file ${file.id}")
                        return@withCommitAuthority true
                    }
                    if (file.isMedia()) file.deleteInMediaScan(context, userDrive)
                    realm.executeTransaction { mutableRealm ->
                        FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.let { proxy ->
                            proxy.isMarkedAsOffline = false
                            proxy.isOffline = false
                            // R4 defense-in-depth: the pin writer is excluded for the whole
                            // hold, so a pin cannot land here — the flag re-read only guards
                            // against a mutation that bypassed the coordination.
                            if (!proxy.isDirectlyPinned) proxy.isDirectlyPinned = false
                            proxy.offlineOwnedByRoot = false
                            proxy.offlineBasisModifiedAt = 0L
                            proxy.offlineBasisSize = null
                            proxy.offlineContentDigest = null
                            proxy.serverContentHash = null
                        }
                    }
                    false
                }
            }
        }
    }

    /**
     * Releases the root contract's recorded ownership for every claimed row the in-subtree
     * withdrawal could not reach: rows that MOVED OUT of the subtree before the unmark (the
     * subtree collection and both coverage walks read the already-updated cache), and rows
     * the per-file loop SKIPPED (directly pinned copies, copies of another root's subtree) —
     * whose recorded claim would otherwise outlive the root (an immortal stale ownership
     * bit). Per claimed row, inside the publication critical section like every withdrawal:
     *  - live coverage by ANOTHER root keeps the claim (overlapping roots release only at
     *    their own withdrawal);
     *  - a direct pin loses ONLY the root's claim — the pin, its bytes and its own contract
     *    stand untouched;
     *  - every other claimed row runs the same preserve/delete decision as the in-subtree
     *    withdrawal, with the bytes looked up at their RECORDED publication path first (a
     *    moved-out row's derived path no longer describes where they live): edited, unknown
     *    or unverifiable bytes are preserved (their state is released, the bytes stay the
     *    user's); a provably intact copy is released through the checked deletion at the
     *    recorded path.
     * Returns true when every release confirmed (no deletion was refused); a refusal keeps
     * the claim so the next unmark pass retries it.
     */
    private suspend fun releaseOwnershipBeyondSubtree(
        context: Context,
        userDrive: UserDrive,
        withdrawnRootFolderId: Int,
    ): Boolean {
        val claimedIds = FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(File::class.java)
                .equalTo(File::offlineOwnedByRoot.name, true)
                .equalTo(File::driveId.name, userDrive.driveId)
                .findAll()
                .map { it.id }
        }
        var allConfirmed = true
        claimedIds.forEach { fileId ->
            currentCoroutineContext().ensureActive()
            // Each claimed row runs the SAME coordinated withdrawal as the in-subtree loop,
            // with the sweep's own contracts: the bytes are looked up at their RECORDED
            // publication path first, another live root's coverage keeps the claim, and a
            // direct pin loses only the root's claim ([withdrawSingleCopyInHold]'s
            // excluded-root mode). A refusal keeps the claim for the next unmark pass.
            val refusal = withdrawSingleCopyInHold(
                context = context,
                userDrive = userDrive,
                fileId = fileId,
                otherRootSubtreeIds = null,
                excludedRootFolderId = withdrawnRootFolderId,
                atRecordedPath = true,
            )
            if (refusal) allConfirmed = false
        }
        return allConfirmed
    }

    /**
     * [isRootCoveredCopy]'s parent-chain walk against a root set that EXCLUDES one folder —
     * the beyond-subtree release judges coverage by the OTHER roots only (the root being
     * withdrawn still sits in the registry while its own rows are released).
     */
    private fun isCoveredByRoot(file: File, realm: Realm, userDrive: UserDrive, excludedRootFolderId: Int): Boolean {
        val rootFolderIds = realm.where(OfflineFolderRoot::class.java)
            .equalTo(OfflineFolderRoot::driveId.name, userDrive.driveId)
            .findAll()
            .map { it.folderId }
            .toSet() - excludedRootFolderId
        if (rootFolderIds.isEmpty()) return false
        var parentId: Int? = file.parentId
        val visited = mutableSetOf<Int>()
        while (parentId != null && visited.add(parentId)) {
            if (parentId in rootFolderIds) return true
            // DRIVE-SCOPED walk, same discipline as [isRootCoveredCopy].
            val parent = realm.where(File::class.java)
                .equalTo(File::id.name, parentId)
                .equalTo(File::driveId.name, file.driveId)
                .findFirst() ?: return false
            parentId = parent.parentId
        }
        return false
    }

    /**
     * Maintenance pass over every root of the current user's drives: re-walks each root,
     * refreshes changed files and — only when EVERY registered root's walk COMPLETED and the
     * registry is unchanged since the pass started — releases coverage of remotely deleted
     * files without deleting bytes, in ONE coordinated pass-wide discharge. Hooked into the
     * existing offline sync cadence.
     */
    suspend fun syncOfflineFolderRoots(context: Context) {
        DriveInfosController.getDrives(AccountUtils.currentUserId).forEach { drive ->
            currentCoroutineContext().ensureActive()
            val userDrive = UserDrive(driveId = drive.id)
            val roots = FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(OfflineFolderRoot::class.java).findAll().map { realm.copyFromRealm(it, 0) }
            }
            if (roots.isEmpty()) return@forEach

            // Walk EVERY root first. Each walk records the ids it PROVED remotely present;
            // an incomplete walk proves nothing and disables the whole pass's release phase
            // (completing root A proves nothing about root B — a discharge judged from one
            // root's completion could clear another root's live ownership).
            var allWalksComplete = true
            val completedWalks = mutableListOf<Triple<OfflineFolderRoot, Set<Int>, Set<Int>>>()
            roots.forEach root@{ root ->
                currentCoroutineContext().ensureActive()
                val previousSubtree = collectSubtreeIdsFromCache(userDrive, root.folderId)
                val rootFolder = FileController.getRealmInstance(userDrive).use { realm ->
                    FileController.getFileProxyById(root.folderId, customRealm = realm)?.let { realm.copyFromRealm(it, 0) }
                } ?: run {
                    allWalksComplete = false // the root's folder row is gone: its walk is missing
                    return@root
                }
                val listedFileIds = mutableSetOf<Int>()
                val outcome = walkSubtree(
                    context = context,
                    userDrive = userDrive,
                    rootFolderFile = rootFolder,
                    onFileVisited = listedFileIds::add,
                )
                if (outcome == WalkOutcome.INCOMPLETE) {
                    allWalksComplete = false // incomplete traversal proves nothing
                    return@root
                }
                completedWalks.add(Triple(root, previousSubtree, listedFileIds.toSet()))

                listedFileIds.forEach { fileId ->
                    currentCoroutineContext().ensureActive()
                    refreshOfflineFile(context, userDrive, fileId)
                }
            }
            if (!allWalksComplete || completedWalks.isEmpty()) return@forEach

            // PASS-WIDE COORDINATED DISCHARGE (RESUME-b528-2212 item 2): one authority hold,
            // gated on (a) every registered root's walk having COMPLETED this pass and (b)
            // the registry re-read in the hold still matching the pass-start snapshot — a
            // root registered mid-pass has no walk yet (a missing walk disables the
            // discharge), and a root withdrawn mid-pass released its own rows through the
            // unmark path. The evidence is the UNION of the completed walks' listed ids —
            // remote presence inside a root — never the cached chain: a chain gap must never
            // read as a move-out, and a row under any registered root with a complete walk
            // is listed by that walk regardless of its cached links.
            val job = currentCoroutineContext()[Job]
            withContext(Dispatchers.IO) {
                OfflineCommitAuthority.withCommitAuthority {
                    val registryNow = FileController.getRealmInstance(userDrive).use { realm ->
                        realm.where(OfflineFolderRoot::class.java)
                            .equalTo(OfflineFolderRoot::driveId.name, userDrive.driveId)
                            .findAll()
                            .map { it.folderId }
                            .toSet()
                    }
                    if (registryNow != roots.map { it.folderId }.toSet()) {
                        SentryLog.i(TAG, "Offline root registry changed during the maintenance pass; releases skipped")
                        return@withCommitAuthority
                    }
                    val listedUnion = completedWalks.flatMapTo(mutableSetOf<Int>()) { it.third }

                    // 1. Remote-delete release: rows whose cached chain placed them inside a
                    //    root's subtree at pass start, that still carry offline intent, and
                    //    that NO completed walk lists anymore — remotely absent from every
                    //    root. Directly pinned rows keep their own contract. Bytes PRESERVED
                    //    (the remote-move contract), every offline field cleared including
                    //    the transient pending marker; the release is an intent withdrawal
                    //    under the same coordination as every other one.
                    val intentRowIds = FileController.getRealmInstance(userDrive).use { realm ->
                        realm.where(File::class.java)
                            .beginGroup()
                            .equalTo(File::isOffline.name, true)
                            .or()
                            .equalTo(File::isMarkedAsOffline.name, true)
                            .endGroup()
                            .findAll()
                            .filter { !it.isFolder() && !it.isDirectlyPinned }
                            .map { it.id }
                            .toSet()
                    }
                    val remoteDeletedIds = completedWalks.flatMapTo(mutableSetOf<Int>()) { (_, previousSubtree, _) ->
                        previousSubtree intersect intentRowIds
                    } - listedUnion
                    for (fileId in remoteDeletedIds) {
                        if (job?.isActive == false) return@withCommitAuthority
                        FileController.getRealmInstance(userDrive).use { realm ->
                            realm.executeTransaction { mutableRealm ->
                                FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.let { proxy ->
                                    proxy.isMarkedAsOffline = false
                                    proxy.isOffline = false
                                    proxy.offlineOwnedByRoot = false
                                    proxy.offlineBasisModifiedAt = 0L
                                    proxy.offlineBasisSize = null
                                    proxy.offlineContentDigest = null
                                    proxy.serverContentHash = null
                                }
                            }
                        }
                    }

                    // 2. Beyond-coverage ownership discharge: every row still carrying the
                    //    recorded claim that NO completed walk lists — no live root proves
                    //    it anymore — is released the same way (bytes PRESERVED, state
                    //    cleared). Directly pinned rows keep their own contract (the pin
                    //    suppresses the claim; the next pass after an unpin discharges
                    //    them); the pin state is re-read in the hold, fresh.
                    val claimedIds = FileController.getRealmInstance(userDrive).use { realm ->
                        realm.where(File::class.java)
                            .equalTo(File::offlineOwnedByRoot.name, true)
                            .equalTo(File::driveId.name, userDrive.driveId)
                            .findAll()
                            .map { it.id }
                            .toSet()
                    } - listedUnion
                    for (fileId in claimedIds) {
                        if (job?.isActive == false) return@withCommitAuthority
                        FileController.getRealmInstance(userDrive).use { realm ->
                            val file = FileController.getFileProxyById(fileId, customRealm = realm) ?: return@use
                            if (file.isFolder() || file.isDirectlyPinned) return@use
                            realm.executeTransaction { mutableRealm ->
                                FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.let { proxy ->
                                    proxy.isMarkedAsOffline = false
                                    proxy.isOffline = false
                                    proxy.offlineOwnedByRoot = false
                                    proxy.offlineBasisModifiedAt = 0L
                                    proxy.offlineBasisSize = null
                                    proxy.offlineContentDigest = null
                                    proxy.serverContentHash = null
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    //region Walk

    /**
     * Cursor-paginated recursive walk of [rootFolderFile]'s subtree. Each page is stored
     * through the standard [FileController.saveRemoteFiles] seam (which preserves local-only
     * fields). The walk only marks PENDING downloads: [File.isMarkedAsOffline] is the baseline
     * transient marker the bulk worker selects on and clears afterwards — the persistent
     * publication basis is recorded from ACCEPTED bytes by the download seam
     * ([recordAcceptedPublication]), never from a listing or an enqueued download. Each
     * marking also records ROOT OWNERSHIP ([File.offlineOwnedByRoot]): for newly marked
     * work the queued state is the root contract's, and EVERY other visited copy (already
     * offline, directly pinned, or previously owned) records the adoption too — the root
     * contract owns the maintenance of every copy in its subtree, and that provenance must
     * survive a cached tree update moving the row (or an ancestor) out of the subtree
     * before the worker runs ([isRootOwnedCopy]); a direct pin suppresses the recorded
     * claim while it stands, and the unmark's beyond-subtree sweep releases it when the
     * last owner withdraws.
     * Root COVERAGE itself stays derived live from the roots table ([isRootCoveredCopy]).
     * Files that already keep an offline copy ([File.isOffline]) or a
     * direct pin ([File.isDirectlyPinned]) are not re-marked: their bytes are preserved and
     * maintained by their existing contract. Every visited non-folder file is reported
     * through [onFileVisited] (coverage bookkeeping includes pre-existing pins).
     */
    private suspend fun walkSubtree(
        context: Context,
        userDrive: UserDrive,
        rootFolderFile: File,
        onFileVisited: ((Int) -> Unit)? = null,
        onFolderWithMarkedFiles: ((Int) -> Unit)? = null,
    ): WalkOutcome {
        val okHttpClient = AccountUtils.getHttpClient(userDrive.userId, null)
        val rootUid = OfflineFolderRoot.buildUid(rootFolderFile.id, userDrive.driveId, userDrive.userId)
        val queue = ArrayDeque<Pair<Int, File?>>()
        queue.add(rootFolderFile.id to rootFolderFile)
        val visitedFolders = mutableSetOf<Int>()

        while (queue.isNotEmpty()) {
            currentCoroutineContext().ensureActive()
            val (folderId, folderFile) = queue.removeFirst()
            if (!visitedFolders.add(folderId)) continue

            var cursor: String? = null
            var isFirstPage = true
            var folderHasMarkedFiles = false
            do {
                currentCoroutineContext().ensureActive()
                val response = Dispatchers.IO {
                    ApiRepository.getFolderFiles(okHttpClient, userDrive.driveId, folderId, cursor, SortType.NAME_AZ)
                }
                if (!response.isSuccess()) return WalkOutcome.INCOMPLETE
                val page = response.data ?: emptyList()

                FileController.getRealmInstance(userDrive).use { realm ->
                    val localFolderProxy = FileController.getFileProxyById(folderId, customRealm = realm)
                    FileController.saveRemoteFiles(
                        realm = realm,
                        localFolderProxy = localFolderProxy,
                        remoteFolder = folderFile,
                        apiResponse = response,
                        isFirstPage = isFirstPage,
                        isCompleteFolder = false,
                    )
                    // The marking/acquisition transaction is a coordinated writer: it joins
                    // the commit authority AND re-checks the registry INSIDE its own
                    // transaction — a root withdrawn mid-walk (an unmark's coordinated
                    // deletion serialized against this hold) marks and claims nothing, so a
                    // dying root can never resurrect offline work or ownership claims.
                    OfflineCommitAuthority.withCommitAuthority {
                        realm.executeTransaction { mutableRealm ->
                            if (mutableRealm.where(OfflineFolderRoot::class.java)
                                    .equalTo(OfflineFolderRoot::uid.name, rootUid)
                                    .count() == 0L
                            ) {
                                return@executeTransaction // the root was withdrawn mid-walk
                            }
                            page.forEach { child ->
                                val proxy = mutableRealm.where(File::class.java)
                                    .equalTo(File::uid.name, child.uid)
                                    .findFirst() ?: return@forEach
                                if (proxy.isTrashed() || proxy.isFolder()) return@forEach
                                if (!proxy.isMarkedAsOffline && !proxy.isOffline && !proxy.isDirectlyPinned) {
                                    proxy.isMarkedAsOffline = true
                                    folderHasMarkedFiles = true
                                }
                                // ROOT OWNERSHIP is recorded for EVERY visited copy of the root's
                                // subtree — not only brand-new pending work: an already-offline or
                                // directly-pinned copy the root adopts keeps its own contract, but
                                // the root contract also owns its maintenance, and a later cached
                                // move-out must not strip that protection with both coverage walks
                                // dead (isRootOwnedCopy). The direct pin suppresses the recorded
                                // claim while it stands (baseline semantics preserved), and the
                                // unmark's beyond-subtree sweep releases it when the last owner
                                // withdraws.
                                proxy.offlineOwnedByRoot = true
                                onFileVisited?.invoke(proxy.id)
                            }
                        }
                    }
                }

                page.filter { it.isFolder() && !it.isTrashed() }.forEach { child -> queue.add(child.id to child) }
                cursor = response.cursor.takeIf { response.hasMore }
                isFirstPage = false
            } while (cursor != null)

            if (folderHasMarkedFiles) onFolderWithMarkedFiles?.invoke(folderId)
        }

        return WalkOutcome.COMPLETE
    }

    /** Cache-only BFS of the subtree ids (folders and files) below [folderId]. */
    private fun collectSubtreeIdsFromCache(userDrive: UserDrive, folderId: Int): Set<Int> {
        val ids = mutableSetOf<Int>()
        FileController.getRealmInstance(userDrive).use { realm ->
            val queue = ArrayDeque(listOf(folderId))
            val visited = mutableSetOf<Int>()
            while (queue.isNotEmpty()) {
                val current = queue.removeFirst()
                if (!visited.add(current)) continue
                realm.where(File::class.java).equalTo(File::parentId.name, current).findAll().forEach { child ->
                    if (ids.add(child.id)) queue.add(child.id)
                }
            }
        }
        return ids
    }

    //endregion

    //region Per-file refresh (maintenance)

    /**
     * Stage 1 + stage 2 for one file of a complete walk. Uses the preserved basis, keeps
     * locally-edited and unknown copies untouched, and replaces changed copies through staged
     * downloads. A copy missing at the CURRENT path is first looked up at its recorded
     * publication path ([relocatePublishedCopy]): a server-side rename/move must relocate the
     * accepted bytes, never re-download over them.
     */
    suspend fun refreshOfflineFile(context: Context, userDrive: UserDrive, fileId: Int) {
        val snapshot = FileController.getRealmInstance(userDrive).use { realm ->
            FileController.getFileProxyById(fileId, customRealm = realm)?.let { realm.copyFromRealm(it, 0) }
        } ?: return
        if (snapshot.isTrashed() || snapshot.isFolder()) return

        // A recorded basis always carries a real remote timestamp; 0 means never published.
        val basis = if (snapshot.offlineBasisModifiedAt > 0L) PublicationBasis(
            modifiedAt = snapshot.offlineBasisModifiedAt,
            size = snapshot.offlineBasisSize,
            digest = snapshot.offlineContentDigest,
            serverHash = snapshot.serverContentHash,
        ) else null
        var offlineFile = snapshot.getOfflineFile(context, userDrive.userId)
        var localExists = offlineFile?.exists() == true
        var localState = classifyLocalCopy(
            basis = basis,
            localExists = localExists,
            localSize = offlineFile?.takeIf { localExists }?.length(),
            localModifiedSec = offlineFile?.takeIf { localExists }?.lastModified()?.div(1_000L),
        )

        if (localState == LocalCopyState.MISSING) {
            relocatePublishedCopy(userDrive, snapshot, offlineFile)?.let { relocated ->
                offlineFile = relocated
                localExists = true
                localState = classifyLocalCopy(
                    basis = basis,
                    localExists = true,
                    localSize = relocated.length(),
                    localModifiedSec = relocated.lastModified().div(1_000L),
                )
            }
        }

        when (decideFromListing(basis, localState)) {
            ListingDecision.SKIP_LOCAL_EDIT -> return
            ListingDecision.DOWNLOAD -> {
                // The body is fetched without a response that establishes its hash identity:
                // the snapshot's hash (a preserved listing value) is NOT evidence for this
                // body, so the publication records an unknown identity instead.
                stagedRedownload(context, userDrive, snapshot, establishedServerHash = null)
                return
            }
            ListingDecision.VERIFY_DETAILS -> Unit
        }

        val okHttpClient = AccountUtils.getHttpClient(userDrive.userId, null)
        val details = Dispatchers.IO {
            ApiRepository.getFileDetailsWithHash(snapshot, okHttpClient)
        }
        val remoteFile = details.data
        if (!details.isSuccess() || remoteFile == null) return // inconclusive: nothing is proven

        val detailsMeta = RemoteMeta(
            lastModifiedAt = remoteFile.lastModifiedAt,
            size = remoteFile.size,
            serverHash = remoteFile.serverContentHash,
        )
        when (decideFromDetails(detailsMeta, basis)) {
            DetailsDecision.METADATA_ONLY -> updatePublicationBasis(userDrive, fileId, remoteFile, offlineFile)
            // The details response established this body's with=hash identity.
            DetailsDecision.REDOWNLOAD -> stagedRedownload(context, userDrive, remoteFile, establishedServerHash = remoteFile.serverContentHash)
        }
    }

    /**
     * A rename/move on the server changes the row path the offline location derives from: a
     * copy published earlier now sits at a stale path while the current location reads
     * MISSING. If the accepted bytes still live at the recorded publication path and no other
     * row claims it, they are RELOCATED with one rename — never re-downloaded over — so a
     * clean copy stays intact and a locally edited copy stays preserved (and classified) at
     * the renamed location. An occupied target refuses the move: rename(2) replaces
     * silently, so bytes this row cannot claim are never overwritten, and any refusal keeps
     * the old bytes where they are, preserved. The existence check immediately before the
     * rename narrows the exposure; POSIX offers no portable no-replace rename, so a
     * byte-appearance inside that last window stays an unclosable platform race (disclosed),
     * like every Java relocation.
     */
    private suspend fun relocatePublishedCopy(userDrive: UserDrive, snapshot: File, offlineFile: JavaFile?): JavaFile? = withContext(Dispatchers.IO) {
        val target = offlineFile ?: return@withContext null
        val recordedPath = snapshot.offlinePublishedPath
        if (recordedPath.isNullOrBlank() || recordedPath == target.path) return@withContext null
        val previous = JavaFile(recordedPath)
        if (!previous.exists()) return@withContext null
        val claimedByAnotherRow = FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(File::class.java)
                .equalTo(File::offlinePublishedPath.name, recordedPath)
                .notEqualTo(File::id.name, snapshot.id)
                .count() > 0
        }
        if (claimedByAnotherRow) return@withContext null // another file legitimately lives there now
        if (target.exists()) {
            // rename(2) would silently REPLACE these bytes. They are provably not this row's
            // accepted copy (the current path classified as MISSING, which is what led here),
            // so they belong to nobody this code may destroy: the old copy stays at its
            // recorded path (recoverable) and the foreign bytes stay put. The staged
            // replacement path preserves them too: unproven bytes never authorize destruction.
            SentryLog.w(TAG, "Relocation target already holds bytes for file ${snapshot.id}; both copies preserved")
            return@withContext null
        }
        val parent = target.parentFile ?: return@withContext null
        if (!parent.exists() && !parent.mkdirs()) return@withContext null
        if (!previous.renameTo(target)) {
            SentryLog.w(TAG, "Relocation refused for file ${snapshot.id}; previous copy kept at its recorded path")
            return@withContext null
        }
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { mutableRealm ->
                FileController.getFileProxyById(snapshot.id, customRealm = mutableRealm)?.let { proxy ->
                    proxy.offlinePublishedPath = target.path
                }
            }
        }
        SentryLog.d(TAG, "Relocated the offline copy of ${snapshot.id} to its renamed location")
        target
    }

    /**
     * Metadata-only change: adopt new remote metadata while keeping the verified bytes.
     * Returns true when the basis is now consistent with the remote metadata (adopted, or
     * already equal), false when the adoption DECLINED (row vanished or offline intent
     * withdrawn) — the caller must report that accurately, never as a publication.
     *
     * The local byte evidence (stat + digest) is computed OUTSIDE the commit authority — a
     * whole-file digest never runs inside a hold. The final live-intent re-read, the stat
     * confirmation, the local timestamp adoption and the basis record run INSIDE the hold:
     * the timestamp can no longer be mutated before a decline that the coordination could
     * have caught (a covered withdrawal — or any coordinated writer — cannot commit between
     * the in-hold intent read and the record).
     */
    private suspend fun updatePublicationBasis(userDrive: UserDrive, fileId: Int, remoteFile: File, offlineFile: JavaFile?): Boolean = withContext(Dispatchers.IO) {
        val verifiedBytes = offlineFile?.takeIf { it.exists() }?.let { copy ->
            VerifiedLocalCopy(length = copy.length(), modifiedSec = copy.lastModified().div(1_000L), digest = computeDigest(copy))
        }
        OfflineCommitAuthority.withCommitAuthority {
            updatePublicationBasisLocked(userDrive, fileId, remoteFile, offlineFile, verifiedBytes)
        }
    }

    private fun updatePublicationBasisLocked(
        userDrive: UserDrive,
        fileId: Int,
        remoteFile: File,
        offlineFile: JavaFile?,
        verifiedBytes: VerifiedLocalCopy?,
    ): Boolean {
        FileController.getRealmInstance(userDrive).use { realm ->
            val proxy = FileController.getFileProxyById(fileId, customRealm = realm) ?: return false
            if (!hasLiveOfflineIntent(proxy, realm, userDrive)) {
                SentryLog.w(TAG, "Offline intent withdrawn before metadata adoption for file $fileId; adoption skipped")
                return false
            }
            val metadataUnchanged = proxy.offlineBasisModifiedAt == remoteFile.lastModifiedAt &&
                (remoteFile.size == null || proxy.offlineBasisSize == null || proxy.offlineBasisSize == remoteFile.size)
            if (metadataUnchanged) {
                // The basis fields are already consistent — a no-op for them, not a
                // withdrawal. The server IDENTITY may still be missing though: a missing-copy
                // restore records an unknown identity by design, and the conservative
                // digest-verified download that follows exists to adopt exactly this hash.
                // Recording it here (only an established, non-blank hash, only when the row
                // does not already carry it) is what makes the adoption converge — otherwise
                // every later pass repeats the same unchanged body download, which the
                // maintenance policy forbids. This branch is only ever entered with byte
                // evidence for the identity: the details route requires the basis hash to
                // already equal the remote hash (so no write is needed), and the staged route
                // has just proven download-digest == preserved-digest inside this same lock —
                // an established identity is never overwritten without that evidence.
                val establishedHash = remoteFile.serverContentHash
                var recorded = true
                if (!establishedHash.isNullOrBlank() && proxy.serverContentHash != establishedHash) {
                    recorded = false
                    realm.executeTransaction { mutableRealm ->
                        FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.let { row ->
                            if (hasLiveOfflineIntent(row, mutableRealm, userDrive)) {
                                row.serverContentHash = establishedHash
                                recorded = true
                            }
                        }
                    }
                }
                // A decline inside the transaction (row vanished or intent withdrawn by a
                // mutation outside the coordination) is a DECLINE, reported as one — never
                // as a successful adoption (same discipline as the basis record below).
                return recorded
            }

            if (offlineFile?.exists() == true) {
                // The byte evidence was computed OUTSIDE the hold; the hold re-stats. An
                // unchanged stat keeps it authoritative (every coordinated writer is
                // excluded); a changed or missing stat means stale evidence — the preserved
                // bytes keep their recorded basis (an adoption skip, not a withdrawal).
                val statCurrent = verifiedBytes != null &&
                    verifiedBytes.length == offlineFile.length() &&
                    verifiedBytes.modifiedSec == offlineFile.lastModified().div(1_000L)
                if (!statCurrent) {
                    SentryLog.w(TAG, "Local bytes changed under the coordination for file $fileId; metadata-only adoption skipped")
                    return true // preserved bytes are consistent with THEIR basis; not a withdrawal
                }
                if (proxy.offlineContentDigest != null) {
                    val evidenceDigest = verifiedBytes?.digest
                    if (evidenceDigest == null || evidenceDigest != proxy.offlineContentDigest) {
                        SentryLog.w(TAG, "Local bytes diverge from the accepted digest for file $fileId; metadata-only adoption skipped")
                        return true // preserved bytes are consistent with THEIR basis; not a withdrawal
                    }
                }
                if (!offlineFile.setLastModified(remoteFile.getLastModifiedInMilliSecond())) {
                    SentryLog.w(TAG, "Local timestamp refused for file $fileId; metadata-only adoption skipped")
                    return true // the preserved basis still describes the actual bytes
                }
            }
            var recorded = false
            realm.executeTransaction { mutableRealm ->
                FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.let { row ->
                    // Defense-in-depth inside the transaction (the lock excludes every
                    // covered withdrawal for this whole section).
                    if (!hasLiveOfflineIntent(row, mutableRealm, userDrive)) {
                        SentryLog.w(TAG, "Offline intent withdrawn before the metadata adoption for file $fileId; nothing recorded")
                        return@executeTransaction
                    }
                    row.lastModifiedAt = remoteFile.lastModifiedAt
                    row.size = remoteFile.size
                    // The accepted identity advances ONLY here, on a digest-verified adoption
                    // (the previously recorded identity still describes these bytes), and is
                    // never erased by a hash-less response.
                    remoteFile.serverContentHash?.let { row.serverContentHash = it }
                    row.offlineBasisModifiedAt = remoteFile.lastModifiedAt
                    row.offlineBasisSize = remoteFile.size
                    offlineFile?.let { row.offlinePublishedPath = it.path }
                    recorded = true
                }
            }
            // Same discipline as the metadata-unchanged branch above: a decline inside the
            // transaction (row vanished or intent withdrawn by a mutation outside the
            // coordination) is reported as a DECLINE, never as a successful adoption.
            return recorded
        }
    }

    /** Outcome of a staged publication attempt (shared by maintenance and the download seam). */
    enum class StagedPublication {
        /** The staged body was published with one atomic rename and the basis recorded. */
        PUBLISHED,
        /** The downloaded body equals the preserved publication: metadata-only adoption. */
        ADOPTED_METADATA_ONLY,
        /** No live offline intent (or row) at the publication point: nothing was mutated. */
        INTENT_WITHDRAWN,
        /** The existing copy is edited, unknown or unverifiable and was preserved untouched. */
        PRESERVED_EXISTING,
        /** The transfer or the publication step failed; previous bytes preserved. */
        FAILED,
    }

    /**
     * Downloads the current remote body to a staging file and PUBLISHES it with one atomic
     * rename over the previous copy (this storage's rename(2) replaces atomically: a crash or
     * a failed rename always leaves either the old or the new complete file — the previous
     * bytes are never deleted separately). ALL guards run before any mutation, on IO: live
     * offline intent re-read from the row (a stale transfer must not resurrect offline status
     * withdrawn by an unmark or a release), cancellation, the current copy's state decided by
     * [decidePublication] (an edited, unknown or unverifiable copy always preserves — an
     * unreadable digest can never authorize destruction), and a vanished row. The staging
     * file carries the remote timestamp BEFORE the rename: a refused timestamp aborts the
     * publication, so the basis can never describe an mtime the bytes do not carry (that
     * would freeze a clean publication as an "edit"). A body whose digest equals the
     * preserved publication adopts metadata only. [establishedServerHash] is the with=hash
     * identity the driving response established for THIS body, or null when nothing
     * established it (a listing snapshot) — the record then clears rather than binding an
     * old identity to new content. Every refusal preserves the previous bytes and state.
     * Returns the [StagedPublication] outcome; every non-PUBLISHED outcome leaves the
     * previous bytes and state untouched. The response is closed on every return path.
     */
    suspend fun stagedRedownload(
        context: Context,
        userDrive: UserDrive,
        file: File,
        establishedServerHash: String?,
    ): StagedPublication = withContext(Dispatchers.IO) {
        val okHttpClient = AccountUtils.getHttpClient(userDrive.userId, null)
        val offlineFile = file.getOfflineFile(context, userDrive.userId) ?: return@withContext StagedPublication.FAILED
        val parentDir = offlineFile.parentFile ?: return@withContext StagedPublication.FAILED
        if (!parentDir.exists()) parentDir.mkdirs() // newly discovered nested files
        if (!parentDir.isDirectory) return@withContext StagedPublication.FAILED
        val staging = JavaFile.createTempFile("offline_staging_", null, parentDir)

        try {
            val response = DownloadOfflineFileManager.downloadFileResponseAsync(ApiRoutes.downloadFile(file), okHttpClient)
            val saved = response.use { resp ->
                if (!resp.isSuccessful) return@withContext StagedPublication.FAILED
                DownloadOfflineFileManager.saveRemoteData(TAG, resp, staging)
            }
            if (!saved) return@withContext StagedPublication.FAILED
            // Exact-byte guard against the server's own length when available
            if (file.size != null && staging.length() != file.size) return@withContext StagedPublication.FAILED
            val digest = computeDigest(staging) ?: return@withContext StagedPublication.FAILED

            val rowRead = FileController.getRealmInstance(userDrive).use { realm ->
                FileController.getFileProxyById(file.id, customRealm = realm)
                    ?.let { realm.copyFromRealm(it, 0) }
                    ?.let { it to hasLiveOfflineIntent(it, realm, userDrive) }
            } ?: run {
                SentryLog.w(TAG, "Row vanished during staged download for file ${file.id}; replacement aborted")
                // No row means no authority to publish anything — same as a withdrawn intent:
                // stale work must not retry-loop, it completes without mutating.
                return@withContext StagedPublication.INTENT_WITHDRAWN
            }
            val (_, authorized) = rowRead
            if (!authorized) {
                SentryLog.w(TAG, "Offline intent withdrawn during staged download for file ${file.id}; publication skipped")
                return@withContext StagedPublication.INTENT_WITHDRAWN
            }

            currentCoroutineContext().ensureActive()

            // CONTENT EVIDENCE PREVIEW — computed OUTSIDE the hold: a whole-file digest never
            // runs inside the commit authority (a hold must stay local-IO sized, and UI-thread
            // writers wait on it). The hold re-stats the copy below and keeps this digest as
            // evidence only when the stat is unchanged.
            val previewExists = offlineFile.exists()
            val previewLength = if (previewExists) offlineFile.length() else null
            val previewModifiedSec = if (previewExists) offlineFile.lastModified().div(1_000L) else null
            val previewDigest = if (previewExists) computeDigest(offlineFile) else null

            // PUBLICATION CRITICAL SECTION: from the FINAL live-intent and content reads
            // through the rename and the basis record, coordinated under the commit authority
            // with EVERY predicate mutation — the covered withdrawals AND the previously
            // non-participating tree/pin/intent writers (a cached tree move can no longer
            // commit between the in-hold checks and the rename; it either commits before the
            // hold — the fresh reads decline before the effect — or after the record, where
            // it observes the completed publication). The network transfer and the digests
            // stayed OUTSIDE; the hold is local-IO sized. ALL DECISIVE EVIDENCE is (re-)
            // read INSIDE the coordination: the pre-hold row/digest reads above are only a
            // preview — an edit or withdrawal landing before the section is judged from
            // fresh evidence here, never from stale content.
            val publicationOutcome = withPublicationLock {
                prePublicationGate?.invoke()
                val inLockRead = FileController.getRealmInstance(userDrive).use { realm ->
                    FileController.getFileProxyById(file.id, customRealm = realm)?.let { row ->
                        val detached = realm.copyFromRealm(row, 0)
                        Triple(
                            detached,
                            hasLiveOfflineIntent(detached, realm, userDrive),
                            isRootCoveredCopy(detached, realm, userDrive),
                        )
                    }
                } ?: run {
                    SentryLog.w(TAG, "Row vanished at publication for file ${file.id}; staging discarded")
                    return@withPublicationLock StagedPublication.INTENT_WITHDRAWN
                }
                val (authoritativeRow, intentStillLiveAtPublication, coveredAtPublication) = inLockRead
                if (!intentStillLiveAtPublication) {
                    SentryLog.w(TAG, "Offline intent withdrawn at publication for file ${file.id}; staging discarded")
                    // Only the OWNED staging artifact is removed (in the finally below); the
                    // copy already at the offline path is not provably this staging's and is
                    // never destroyed here.
                    return@withPublicationLock StagedPublication.INTENT_WITHDRAWN
                }
                // Stale root ownership at the publication point: the copy the root contract
                // queued or published is now outside every root and not directly pinned — the
                // contract's reach ended (a real move during the transfer, or a cached tree
                // update that moved the row out first). A fresh publication here would write
                // isOffline at the moved-out location and fabricate retention the user never
                // chose (a pending root file moving out mid-flight would republish itself as
                // an accidental direct pin). The staging is discarded; any previously
                // published bytes and state stand untouched.
                if (authoritativeRow.offlineOwnedByRoot && !authoritativeRow.isDirectlyPinned && !coveredAtPublication) {
                    SentryLog.w(TAG, "Root ownership stale at publication for file ${file.id}; staging discarded")
                    return@withPublicationLock StagedPublication.INTENT_WITHDRAWN
                }

                // Content evidence under the coordination: the whole-file digest was computed
                // OUTSIDE the hold (the preview above); the hold re-stats the copy and keeps
                // the preview digest only when the stat is unchanged. Every coordinated
                // writer is excluded for the whole hold, so an app-side byte change between
                // the preview and here is impossible by construction; a stat change means the
                // evidence is stale (an external, non-participating writer) and the copy is
                // treated as unproven — PRESERVED, never replaced on stale evidence.
                val currentExists = offlineFile.exists()
                val currentLength = if (currentExists) offlineFile.length() else null
                val currentModifiedSec = if (currentExists) offlineFile.lastModified().div(1_000L) else null
                val digestEvidenceCurrent = currentExists && previewExists &&
                    currentLength == previewLength && currentModifiedSec == previewModifiedSec
                val existingDigest = if (digestEvidenceCurrent) previewDigest else null
                val basis = PublicationBasis(
                    modifiedAt = authoritativeRow.offlineBasisModifiedAt,
                    size = authoritativeRow.offlineBasisSize,
                    digest = authoritativeRow.offlineContentDigest,
                    serverHash = authoritativeRow.serverContentHash,
                )
                when (decidePublication(
                    basis = basis,
                    localExists = currentExists,
                    localSize = currentLength,
                    localModifiedSec = currentModifiedSec,
                    existingDigest = existingDigest,
                    downloadDigest = digest,
                )) {
                    PublicationDecision.PRESERVE_EXISTING -> {
                        SentryLog.w(TAG, "Existing copy edited, unknown or unverifiable for file ${file.id}; replacement aborted")
                        return@withPublicationLock StagedPublication.PRESERVED_EXISTING
                    }
                    PublicationDecision.ADOPT_METADATA_ONLY -> {
                        // Body identical to the preserved publication: metadata-only
                        // adoption on the verified clean copy (timestamp and basis move
                        // together; the bytes are never touched). The preview byte evidence
                        // is current (an ADOPT decision requires it); pass it so the record
                        // re-stats instead of re-digesting inside the hold. A declined
                        // adoption is a withdrawn intent: report it accurately.
                        val verified = VerifiedLocalCopy(
                            length = currentLength ?: return@withPublicationLock StagedPublication.PRESERVED_EXISTING,
                            modifiedSec = currentModifiedSec ?: return@withPublicationLock StagedPublication.PRESERVED_EXISTING,
                            digest = previewDigest,
                        )
                        val adopted = updatePublicationBasisLocked(userDrive, file.id, file, offlineFile, verified)
                        return@withPublicationLock if (adopted) {
                            StagedPublication.ADOPTED_METADATA_ONLY
                        } else {
                            StagedPublication.INTENT_WITHDRAWN
                        }
                    }
                    PublicationDecision.REPLACE_BYTES -> Unit
                }

                // FINAL PRE-RENAME AUTHORITY RE-CHECK (FHNW-caf1 R3): the in-lock span
                // includes the full digest of the existing copy — seconds for a large file —
                // and tree updates do not participate in this lock. A move-out landing in
                // that span must be observed at the LAST point before the filesystem effect:
                // intent AND stale-ownership are re-derived on a FRESH read, so a refusal
                // here preserves the previous bytes instead of replacing them and declining
                // only in the record.
                preRenameGate?.invoke()
                val freshState = FileController.getRealmInstance(userDrive).use { realm ->
                    FileController.getFileProxyById(file.id, customRealm = realm)?.let { row ->
                        val detached = realm.copyFromRealm(row, 0)
                        Triple(
                            detached,
                            hasLiveOfflineIntent(detached, realm, userDrive),
                            isRootCoveredCopy(detached, realm, userDrive),
                        )
                    }
                }
                when {
                    freshState == null -> {
                        SentryLog.w(TAG, "Row vanished before the rename for file ${file.id}; staging discarded")
                        return@withPublicationLock StagedPublication.INTENT_WITHDRAWN
                    }
                    !freshState.second ||
                        (freshState.first.offlineOwnedByRoot && !freshState.first.isDirectlyPinned && !freshState.third) -> {
                        SentryLog.w(TAG, "Offline intent or root ownership stale before the rename for file ${file.id}; staging discarded, previous copy preserved")
                        return@withPublicationLock StagedPublication.INTENT_WITHDRAWN
                    }
                }

                // The publication timestamp is set on the STAGING file, before the atomic
                // step: a refusal aborts here, so the previous good bytes are never
                // sacrificed for a basis that would misdescribe the new ones.
                if (!staging.setLastModified(file.getLastModifiedInMilliSecond())) {
                    SentryLog.w(TAG, "Staging timestamp refused for file ${file.id}; publication aborted, previous copy preserved")
                    return@withPublicationLock StagedPublication.FAILED
                }

                // One atomic publication step: rename(2) over the existing file (rename
                // keeps the staging mtime). Failure keeps the old bytes (staging is removed
                // in finally).
                if (!staging.renameTo(offlineFile)) {
                    SentryLog.w(TAG, "Atomic replace refused for file ${file.id}; previous copy preserved")
                    return@withPublicationLock StagedPublication.FAILED
                }

                var recorded = false
                FileController.getRealmInstance(userDrive).use { realm ->
                    realm.executeTransaction { mutableRealm ->
                        FileController.getFileProxyById(file.id, customRealm = mutableRealm)?.let { proxy ->
                            // Defense-in-depth: the lock already excludes every covered
                            // withdrawal for this whole section — a decline here would mean
                            // a mutation that did not participate in the coordination.
                            if (!hasLiveOfflineIntent(proxy, mutableRealm, userDrive)) {
                                SentryLog.w(TAG, "Offline intent withdrawn before the basis record for file ${file.id}; nothing recorded")
                                return@executeTransaction
                            }
                            // THE FINAL PUBLICATION WINDOW: a cached tree refresh does not
                            // participate in the coordination and can move the row (or an
                            // ancestor) out between the in-lock read and THIS record. The same
                            // stale-ownership condition as the in-lock refusal, re-judged on
                            // the LIVE row: the root contract's reach ended — no fresh
                            // publication may record a basis or offline status for a moved-out
                            // owned copy (a pending one would fabricate retention). Nothing is
                            // recorded; the outcome is the INTENT_WITHDRAWN path — the bytes
                            // now at the final path are an UNACCEPTED copy (classified UNKNOWN
                            // against the untouched basis, so every later decision preserves
                            // them), and the stale claim stands for the unmark's beyond-subtree
                            // sweep to release.
                            if (proxy.offlineOwnedByRoot && !proxy.isDirectlyPinned && !isRootCoveredCopy(proxy, mutableRealm, userDrive)) {
                                SentryLog.w(TAG, "Root ownership stale at the basis record for file ${file.id}; nothing recorded")
                                return@executeTransaction
                            }
                            proxy.lastModifiedAt = file.lastModifiedAt
                            proxy.size = file.size
                            // Only an identity established for THIS accepted body is recorded;
                            // an unestablished one clears the record (unknown identity).
                            proxy.serverContentHash = establishedServerHash
                            proxy.offlineBasisModifiedAt = file.lastModifiedAt
                            proxy.offlineBasisSize = file.size
                            proxy.offlineContentDigest = digest
                            proxy.offlinePublishedPath = offlineFile.path
                            // A publication under live root coverage records the root contract
                            // as the owner: the provenance survives later cached tree updates
                            // that move the row out of the subtree ([isRootOwnedCopy]).
                            if (isRootCoveredCopy(proxy, mutableRealm, userDrive)) proxy.offlineOwnedByRoot = true
                            proxy.isOffline = true
                            recorded = true
                        }
                    }
                }
                if (!recorded) {
                    // The state write declined: the bytes now at the final path are an
                    // UNACCEPTED copy. Report the withdrawal accurately and never scan an
                    // unaccepted copy into MediaStore as if it were the owned publication.
                    return@withPublicationLock StagedPublication.INTENT_WITHDRAWN
                }
                if (file.isMedia()) MediaUtils.scanFile(context, offlineFile)
                StagedPublication.PUBLISHED
            }
            return@withContext publicationOutcome
        } finally {
            if (staging.exists()) staging.delete()
        }
    }

    /**
     * Digest evidence for a publication computed OUTSIDE the commit authority (the
     * cache-move seam digests its staged bytes before the hold that publishes them).
     * Package-facing accessor for the private [computeDigest].
     */
    internal fun computeDigestForEvidence(file: JavaFile): String? = computeDigest(file)

    private fun computeDigest(file: JavaFile): String? = runCatching {
        val digest = MessageDigest.getInstance("SHA-256")
        file.inputStream().use { input ->
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            while (true) {
                val read = input.read(buffer)
                if (read < 0) break
                digest.update(buffer, 0, read)
            }
        }
        digest.digest().joinToString("") { "%02x".format(it) }
    }.getOrNull()

    /** Outcome of a publication through the seam. */
    enum class PublicationOutcome {
        /** Basis + offline status recorded. */
        PUBLISHED,
        /** The offline intent was withdrawn during the transfer: nothing was recorded, and the caller must clean up the written bytes. */
        INTENT_WITHDRAWN,
        /** The bytes could not be digested: nothing recorded; the copy stays unrecorded (UNKNOWN) and PRESERVED. */
        NOT_RECORDED,
    }

    /**
     * Narrow publication seam, called from the accepted offline-write path (a copy was
     * successfully written or moved from an intact cache): records the preserved basis from
     * the ACCEPTED bytes — the remote metadata of the details response that drove the download
     * plus the sha-256 digest of the bytes on disk — and sets the persistent offline status.
     * NEVER called from a listing or a merely enqueued download. Live offline intent is
     * re-checked FIRST: a transfer that started while the file was wanted must not resurrect
     * offline status withdrawn since (explicit unmark, remote-delete release). The server hash
     * is recorded only when the driving response actually established it for THIS body (a
     * captured with=hash); a null capture CLEARS the record — an old hash must never describe
     * new bytes. Digest failure records nothing: an unrecorded basis classifies the copy as
     * UNKNOWN, which maintenance leaves untouched.
     */
    suspend fun recordAcceptedPublication(
        userDrive: UserDrive,
        remoteFile: File,
        offlineFile: JavaFile,
        acceptedServerHash: String?,
    ): PublicationOutcome = withContext(Dispatchers.IO) {
        // The FINAL content evidence (the digest being recorded) is computed OUTSIDE the
        // commit authority — a whole-file digest never runs inside a hold. The stat
        // re-verification, the live-intent validation and the basis record ALL run INSIDE
        // the hold: evidence whose stat changed under the coordination is stale (a
        // non-participating writer) and records nothing; every coordinated writer is
        // excluded for the whole hold.
        val acceptedLength = offlineFile.length()
        val acceptedDigest = computeDigest(offlineFile)
        OfflineCommitAuthority.withCommitAuthority {
            recordAcceptedPublicationInHold(userDrive, remoteFile, offlineFile, acceptedServerHash, acceptedDigest, acceptedLength)
        }
    }

    /**
     * The accepted-publication record INSIDE an already-held commit authority (the
     * cache-move publication seam publishes and records in one hold; the digest evidence
     * [acceptedDigest]/[acceptedLength] was computed outside — for a staged publication the
     * evidence describes the staged bytes, which the rename makes identical). Non-suspending
     * by contract; MUST be called under [OfflineCommitAuthority.withCommitAuthority].
     */
    internal fun recordAcceptedPublicationInHold(
        userDrive: UserDrive,
        remoteFile: File,
        offlineFile: JavaFile,
        acceptedServerHash: String?,
        acceptedDigest: String?,
        acceptedLength: Long,
    ): PublicationOutcome {
        if (acceptedDigest == null || offlineFile.length() != acceptedLength) {
            SentryLog.w(TAG, "Accepted offline bytes could not be digested (or changed under the coordination) for file ${remoteFile.id}; basis not recorded")
            return PublicationOutcome.NOT_RECORDED
        }
        FileController.getRealmInstance(userDrive).use { realm ->
            val proxy = FileController.getFileProxyById(remoteFile.id, customRealm = realm)
                ?: return PublicationOutcome.INTENT_WITHDRAWN
            if (!hasLiveOfflineIntent(proxy, realm, userDrive)) {
                SentryLog.w(TAG, "Offline intent withdrawn during download for file ${remoteFile.id}; publication skipped")
                return PublicationOutcome.INTENT_WITHDRAWN
            }
            var recorded = false
            realm.executeTransaction { mutableRealm ->
                FileController.getFileProxyById(remoteFile.id, customRealm = mutableRealm)?.let { row ->
                    // Defense-in-depth inside the transaction (the hold excludes every
                    // coordinated writer for this whole section).
                    if (!hasLiveOfflineIntent(row, mutableRealm, userDrive)) {
                        SentryLog.w(TAG, "Offline intent withdrawn before the accepted basis record for file ${remoteFile.id}; nothing recorded")
                        return@executeTransaction
                    }
                    // The same final-publication-window defense as the staged record: the
                    // root contract's reach has ended for a moved-out owned copy, and no
                    // accepted publication may record a basis or offline status for one.
                    // The claim stands for the unmark's beyond-subtree sweep.
                    if (row.offlineOwnedByRoot && !row.isDirectlyPinned && !isRootCoveredCopy(row, mutableRealm, userDrive)) {
                        SentryLog.w(TAG, "Root ownership stale at the accepted basis record for file ${remoteFile.id}; nothing recorded")
                        return@executeTransaction
                    }
                    row.lastModifiedAt = remoteFile.lastModifiedAt
                    row.size = remoteFile.size
                    // Only an identity established for THIS accepted body is recorded; a
                    // hash-less capture clears the record (unknown identity).
                    row.serverContentHash = acceptedServerHash
                    row.offlineBasisModifiedAt = remoteFile.lastModifiedAt
                    row.offlineBasisSize = remoteFile.size
                    row.offlineContentDigest = acceptedDigest
                    row.offlinePublishedPath = offlineFile.path
                    // A publication under live root coverage records the root contract as
                    // the owner (see the staged record above).
                    if (isRootCoveredCopy(row, mutableRealm, userDrive)) row.offlineOwnedByRoot = true
                    row.isOffline = true
                    recorded = true
                }
            }
            return if (recorded) PublicationOutcome.PUBLISHED else PublicationOutcome.INTENT_WITHDRAWN
        }
    }

    //endregion
}
