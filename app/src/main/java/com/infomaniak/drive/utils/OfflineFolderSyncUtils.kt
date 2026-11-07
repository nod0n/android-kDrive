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
import com.infomaniak.drive.data.api.ErrorCode
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
     * A certified-fresh decision read (RESUME-1189-2245 item 3 / FHNW-1189 B): opening an
     * instance does not BY ITSELF prove a latest read view — when a legitimate second
     * reference is already open on this thread, the Realm cache returns that retained
     * instance and its (older) MVCC view, and no per-call .use balance proves otherwise.
     * [Realm.refresh] advances the read view to the latest committed version (a cheap no-op
     * when already current; write transactions advance their own view at begin). Every
     * decision read that authorizes a filesystem effect goes through this. A failed
     * certification THROWS: every caller's path treats that as a refusal/preserve — bytes
     * are never mutated on an uncertified read.
     */
    internal fun certifiedFreshRealm(userDrive: UserDrive?): Realm {
        val realm = FileController.getRealmInstance(userDrive)
        try {
            check(!realm.isInTransaction) { "a certified-fresh read cannot start inside a transaction" }
            realm.refresh()
        } catch (t: Throwable) {
            // The caller's .use was never entered (this reference was not handed over), so
            // the failed acquisition closes its own instance (RESUME-962c item 3): the
            // failure evidence (the thrown check/refresh error) is preserved for the caller,
            // which treats it as a refusal/preserve.
            runCatching { realm.close() }
            throw t
        }
        return realm
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

    /**
     * Test seam (FHNW-1189 E1): invoked inside a withdrawal's hold AFTER the fresh
     * keep-contract reads and BEFORE the deletion decision, so a regression can launch the
     * real pin writer here and prove it QUEUES at the real authority acquisition (it cannot
     * commit inside the hold). Production never sets it.
     */
    @VisibleForTesting
    internal var preWithdrawalDecisionGate: (() -> Unit)? = null

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
        val rootFolderIds = certifiedFreshRealm(userDrive).use { realm ->
            realm.where(OfflineFolderRoot::class.java)
                .equalTo(OfflineFolderRoot::driveId.name, userDrive.driveId)
                .findAll()
                .map { it.folderId }
        }
        if (rootFolderIds.isEmpty()) return emptySet()
        return certifiedFreshRealm(userDrive).use { realm ->
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
            certifiedFreshRealm(userDrive).use { realm ->
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
        val otherRootSubtreeIds = certifiedFreshRealm(userDrive).use { realm ->
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
                val claimedIds = certifiedFreshRealm(userDrive).use { realm ->
                    realm.where(File::class.java)
                        .equalTo(File::offlineOwnedByRoot.name, true)
                        .equalTo(File::driveId.name, userDrive.driveId)
                        .findAll()
                        .map { it.id }
                }
                // BATCHED CLOSER (FHNW-1189 A7): the classification reads run first and the
                // byte-less clears — the marking race's residue and the pin-only releases —
                // commit in ONE transaction under this hold instead of one transaction per
                // row. A synchronous UI-thread writer waits on the fair lock for the hold's
                // whole span, so the span is kept to one bounded transaction plus the final
                // registry deletion.
                val claimOnlyIds = mutableListOf<Int>() // a direct pin loses ONLY the root's claim
                val bytelessToClear = mutableListOf<Int>() // nothing to delete: the full state clear
                certifiedFreshRealm(userDrive).use { realm ->
                    for (fileId in claimedIds) {
                        if (job?.isActive == false) return@withCommitAuthority false // incomplete closer: retriable
                        val file = FileController.getFileProxyById(fileId, customRealm = realm)
                            ?: continue // the row is gone: nothing left to release
                        if (file.isFolder()) continue // walkSubtree never claims folders
                        if (isCoveredByRoot(file, realm, userDrive, excludedRootFolderId = folder.id)) {
                            continue // another live owner keeps the claim until its own withdrawal
                        }
                        if (file.isDirectlyPinned) {
                            claimOnlyIds.add(fileId)
                            continue
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
                            continue
                        }
                        bytelessToClear.add(fileId)
                    }
                    if (claimOnlyIds.isNotEmpty() || bytelessToClear.isNotEmpty()) {
                        realm.executeTransaction { mutableRealm ->
                            claimOnlyIds.forEach { claimOnlyId ->
                                FileController.getFileProxyById(claimOnlyId, customRealm = mutableRealm)
                                    ?.offlineOwnedByRoot = false
                            }
                            bytelessToClear.forEach { clearedId -> clearClaimedRowFields(mutableRealm, clearedId) }
                        }
                    }
                }
                if (rowsWithBytesRemain) return@withCommitAuthority false
                certifiedFreshRealm(userDrive).use { realm ->
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

    /** The claimed-row state clear shared by every withdrawal/release path. */
    private fun clearClaimedRowFields(realm: Realm, fileId: Int) {
        FileController.getFileProxyById(fileId, customRealm = realm)?.let { proxy ->
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

    /**
     * The byte-evidence SNAPSHOT for one copy: the stat values and the classification a
     * destructive decision would rest on, captured together with the MUTATION GENERATION
     * under one short hold (RESUME-1189-2245 item 4). The digest verdict runs OUTSIDE the
     * hold (a whole-file digest never runs inside one) and the deletion hold re-reads the
     * generation: a participating mutation between the snapshot and the acquisition
     * invalidates the evidence (preserve/retry) instead of authorizing a stale destructive
     * step; a changed stat means a non-participating writer touched the bytes — the
     * evidence is stale and the copy is PRESERVED, never deleted on stale evidence. The
     * residual external SAME-STAT edit class (same length, same second-rounded mtime,
     * spanning the unbounded lock wait) is disclosed in [OfflineCommitAuthority]'s contract.
     */
    private data class WithdrawalSnapshot(
        val offlineFile: JavaFile?,
        val localExists: Boolean,
        val length: Long?,
        val modifiedSec: Long?,
        val basis: PublicationBasis?,
        val localState: LocalCopyState,
    )

    private fun readWithdrawalSnapshot(
        context: Context,
        file: File,
        userDrive: UserDrive,
        atRecordedPath: Boolean,
    ): WithdrawalSnapshot {
        val recordedPath = file.offlinePublishedPath
        val offlineFile: JavaFile? = when {
            atRecordedPath && !recordedPath.isNullOrBlank() -> JavaFile(recordedPath)
            else -> file.getOfflineFile(context, userDrive.userId)
        }
        val localExists = offlineFile?.exists() == true
        if (!localExists || offlineFile == null) {
            return WithdrawalSnapshot(offlineFile, localExists = false, length = null, modifiedSec = null, basis = null, localState = LocalCopyState.MISSING)
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
        return WithdrawalSnapshot(offlineFile, localExists = true, length = length, modifiedSec = modifiedSec, basis = basis, localState = localState)
    }

    /**
     * ONE copy's coordinated withdrawal: the byte-evidence snapshot (stat + classification +
     * MUTATION GENERATION) is captured under one short hold, the digest verdict runs OUTSIDE
     * the hold, then the deletion hold re-reads the row fresh, re-checks every keep-copy
     * contract, re-reads the generation (a participating mutation between the snapshot and
     * the acquisition invalidates the evidence — the attempt retries once, a second
     * invalidation PRESERVES), re-stats the evidence, and performs the checked deletion and
     * the state clear. Returns TRUE only for a REFUSAL (a deletion the filesystem refused —
     * the claim stays and the caller reports a retriable unmark); skips, preserved releases
     * and confirmed deletions return false.
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
        var attempt = 0
        while (true) {
            // Snapshot hold (OBSERVATION — an evidence capture never advances the
            // generation, RESUME-962c item 1): the row fields, the byte stat and the
            // generation, together.
            val snapshot = OfflineCommitAuthority.withCommitAuthorityForEvidence {
                certifiedFreshRealm(userDrive).use { realm ->
                    FileController.getFileProxyById(fileId, customRealm = realm)?.let { row ->
                        if (row.isFolder()) null
                        else readWithdrawalSnapshot(context, row, userDrive, atRecordedPath) to
                            OfflineCommitAuthority.currentGenerationUnderAuthority()
                    }
                }
            } ?: return false // vanished row or folder: nothing to release
            val (evidence, snapshotGeneration) = snapshot

            // The digest verdict, OUTSIDE the hold (unbounded IO): a same-size/same-mtime
            // edit still diverges from the accepted digest, so the accepted identity is
            // verified before any destructive step. An unreadable digest (null) or a
            // digest-less basis is preservation, not authority.
            val digestVerifies = evidence.basis?.digest != null && evidence.offlineFile != null &&
                computeDigest(evidence.offlineFile) == evidence.basis.digest
            val snapshotPreservesBytes = evidence.localState == LocalCopyState.LOCALLY_EDITED ||
                evidence.localState == LocalCopyState.UNKNOWN ||
                !digestVerifies

            var evidenceInvalidated = false
            val refusal = withContext(Dispatchers.IO) {
                OfflineCommitAuthority.withCommitAuthority {
                    certifiedFreshRealm(userDrive).use { realm ->
                        // EVIDENCE CERTIFICATION first: a participating mutation committed
                        // between the snapshot and this acquisition — the snapshot cannot
                        // authorize a destructive step.
                        if (OfflineCommitAuthority.currentGenerationUnderAuthority() != snapshotGeneration) {
                            evidenceInvalidated = true
                            return@withCommitAuthority false
                        }
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
                            evidence.localExists -> !statUnchanged || snapshotPreservesBytes
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
                                clearClaimedRowFields(mutableRealm, fileId)
                            }
                            return@withCommitAuthority false
                        }

                        // Test seam (FHNW-1189 E1): the real pin writer can be launched and
                        // proven queued exactly here — it cannot commit inside the hold.
                        preWithdrawalDecisionGate?.invoke()

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
                            clearClaimedRowFields(mutableRealm, fileId)
                        }
                        false
                    }
                }
            }
            if (evidenceInvalidated && attempt == 0) {
                attempt = 1
                continue // one retry with a fresh snapshot; a second invalidation preserves
            }
            return refusal
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
        val claimedIds = certifiedFreshRealm(userDrive).use { realm ->
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
        return isCoveredByAnyRootOf(file, realm, userDrive, rootFolderIds)
    }

    /**
     * The parent-chain walk against an EXPLICIT root-folder-id set — the maintenance pass's
     * release phases use it to keep every row a root WITHOUT a complete walk this pass might
     * still own (an unproven root's remote status is no evidence for anybody else's release).
     */
    private fun isCoveredByAnyRootOf(file: File, realm: Realm, userDrive: UserDrive, rootFolderIds: Set<Int>): Boolean {
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
     * refreshes changed files and — with per-root complete-walk evidence and an unchanged
     * registry — releases coverage of remotely deleted files without deleting bytes, in ONE
     * coordinated pass-wide release. Hooked into the existing offline sync cadence.
     */
    suspend fun syncOfflineFolderRoots(context: Context) {
        DriveInfosController.getDrives(AccountUtils.currentUserId).forEach { drive ->
            currentCoroutineContext().ensureActive()
            val userDrive = UserDrive(driveId = drive.id)
            val roots = certifiedFreshRealm(userDrive).use { realm ->
                realm.where(OfflineFolderRoot::class.java).findAll().map { realm.copyFromRealm(it, 0) }
            }
            // RESUME-962c item 2b: NO early return for an empty registry. A drive whose
            // last root was withdrawn (the missing-root resolution's definitive answer,
            // completed unmarks' residues) still owns CLAIMED rows, and the pass's union
            // discharge below is the machinery that releases them (bytes preserved). With
            // zero roots the discharge is vacuously all-complete and the empty registry
            // matches its pass-start snapshot, so the orphaned claims converge on the
            // SUBSEQUENT pass instead of being immortal (the sole-root definitive-gone
            // schedule's promised "later pass" release is THIS path).


            // PASS-START CLAIMED SNAPSHOT (FHNW-1189 A6): the rows carrying the recorded
            // claim when the pass started. The beyond-coverage discharge releases only rows
            // that were claimed AT PASS START — a claim created MID-PASS (an attached old
            // walk marking under a same-id remove+re-register, or any walk's own marking)
            // is judged by the pass that walks it, never by stale union evidence from
            // another registration's walk.
            val previousClaimedIds = certifiedFreshRealm(userDrive).use { realm ->
                realm.where(File::class.java)
                    .equalTo(File::offlineOwnedByRoot.name, true)
                    .equalTo(File::driveId.name, userDrive.driveId)
                    .findAll()
                    .map { it.id }
                    .toSet()
            }

            // Walk EVERY registered root first. Each walk records the ids it PROVED remotely
            // present; an incomplete walk proves nothing. A root whose CACHED folder row is
            // missing gets an AUTHORITATIVE remote resolution instead of a walk (FHNW-1189
            // A3): a missing cache row is NOT evidence of a remote deletion, so the
            // registration survives unless the remote answer is definitive. Every root
            // WITHOUT a complete walk this pass (failed listing, unresolved missing row)
            // withholds the whole release hold below (RESUME-962c item 5 / FHNW-962c P2-5):
            // its remote status is unproven, and a row omitted from a completed root's
            // listing may have MOVED into it — negative cached ancestry is not negative
            // coverage, so no claim or intent is released on partial-pass evidence.
            var allWalksComplete = true
            val completedWalks = mutableListOf<Triple<OfflineFolderRoot, Set<Int>, Set<Int>>>()
            roots.forEach root@{ root ->
                currentCoroutineContext().ensureActive()
                val previousSubtree = collectSubtreeIdsFromCache(userDrive, root.folderId)
                val rootFolder = certifiedFreshRealm(userDrive).use { realm ->
                    FileController.getFileProxyById(root.folderId, customRealm = realm)?.let { realm.copyFromRealm(it, 0) }
                } ?: run {
                    when (resolveMissingRootFolder(userDrive, root)) {
                        MissingRootResolution.RECOVERED -> Unit // the row is back: the next pass walks it
                        MissingRootResolution.DEFINITIVELY_GONE -> Unit // the registration was withdrawn; its claims ride the SUBSEQUENT pass's discharge (bytes preserved)
                        MissingRootResolution.UNRESOLVED -> Unit // inconclusive answer: the registration stands, retry next pass
                    }
                    allWalksComplete = false // no walk evidence exists for this root
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

            // PASS-WIDE COORDINATED RELEASE (RESUME-b528-2212 item 2, extended by
            // FHNW-1189 A3/A6/A7, RESTRUCTURED by RESUME-962c item 5 / FHNW-962c P2-5,
            // registration LIFETIME by RESUME-33dd item 2): one authority hold, gated on
            // the registry re-read still matching the pass-start snapshot AS REGISTRATION
            // LIFETIMES (a root registered or withdrawn mid-pass invalidates the snapshot,
            // and a same-id remove+re-register is a DIFFERENT lifetime) AND on EVERY
            // registered root's walk having completed this pass. The former
            // per-completed-root partial-pass release is GONE — FHNW-962c P2-5 proved its
            // schedule: X sat in A's cached previousSubtree, moved to B remotely during the
            // pass, B's walk was incomplete, and X's cached chain still reached A — so the
            // unproven-root keep-check (a cached-chain walk) could not see B and released
            // X's claim and intent although an unproven root may own it. Negative cached
            // ancestry is not negative coverage. With the all-complete gate, a row omitted
            // from every completed listing is remotely absent from every PROVEN root, and a
            // row that moved into an unproven root's subtree is re-listed there (kept) or
            // the whole release defers to the next pass. TWO phases with DIFFERENT target
            // sets, the SAME evidence bar:
            //  1. Remote-delete release — rows in a completed root's previous cached
            //     subtree, still carrying intent, that no completed walk lists.
            //  2. Beyond-coverage discharge — rows claimed at PASS START that no completed
            //     walk lists (also the empty-registry pass's orphaned-claim convergence,
            //     RESUME-962c item 2b).
            val job = currentCoroutineContext()[Job]
            withContext(Dispatchers.IO) {
                OfflineCommitAuthority.withCommitAuthority {
                    // THE REGISTRATION-LIFETIME MAP (RESUME-33dd item 2): the folder-id set
                    // alone cannot distinguish a registration from its remove+re-register
                    // successor — an unmark+re-mark of the SAME folder while the pass is
                    // parked (the parked refresh spans are real suspension windows) rebuilds
                    // the identical uid set but a NEW contract lifetime, because markedAt is
                    // stamped at every mark. Judging this pass's union evidence against the
                    // successor's claims would release a claim the successor's own marking
                    // walk just re-acquired (the same-row ABA): the gate therefore compares
                    // the (uid, markedAt) lifetime map against the pass-start snapshot. A
                    // mid-pass re-registration defers the WHOLE release; the NEXT pass
                    // captures the successor at ITS start and its own walk judges the row
                    // from current evidence. markedAt has millisecond precision — a
                    // same-millisecond re-register is indistinguishable and merely leaves
                    // the disclosed bounded-churn residual (never a byte decision).
                    val registryLifetimes = certifiedFreshRealm(userDrive).use { realm ->
                        realm.where(OfflineFolderRoot::class.java)
                            .equalTo(OfflineFolderRoot::driveId.name, userDrive.driveId)
                            .findAll()
                            .associate { it.uid to it.markedAt.time }
                    }
                    if (registryLifetimes != roots.associate { it.uid to it.markedAt.time }) {
                        SentryLog.i(TAG, "Offline root registry changed during the maintenance pass; releases skipped")
                        return@withCommitAuthority
                    }
                    if (!allWalksComplete) {
                        SentryLog.i(TAG, "An offline root's walk is incomplete; the release pass defers — no claim or intent is judged on partial-pass evidence")
                        return@withCommitAuthority
                    }
                    val listedUnion = completedWalks.flatMapTo(mutableSetOf<Int>()) { it.third }

                    // 1. Remote-delete release: rows whose cached chain placed them inside a
                    //    completed root's subtree at pass start, that still carry offline
                    //    intent, and that NO completed walk lists anymore — remotely absent
                    //    from every proven root. Directly pinned rows keep their own
                    //    contract. Bytes PRESERVED (the remote-move contract); the release
                    //    is an intent withdrawal under the same coordination as every
                    //    other one.
                    val intentRowIds = certifiedFreshRealm(userDrive).use { realm ->
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
                    if (remoteDeletedIds.isNotEmpty()) {
                        // FHNW-1189 A7: ONE transaction for the whole release phase — a
                        // synchronous UI-thread writer waits on the fair lock for the
                        // hold's span, so the span stays one bounded transaction.
                        certifiedFreshRealm(userDrive).use { realm ->
                            realm.executeTransaction { mutableRealm ->
                                for (fileId in remoteDeletedIds) {
                                    if (job?.isActive == false) return@executeTransaction
                                    val proxy = FileController.getFileProxyById(fileId, customRealm = mutableRealm)
                                        ?: continue
                                    clearClaimedRowFields(mutableRealm, fileId)
                                }
                            }
                        }
                    }

                    // 2. Beyond-coverage ownership discharge: rows claimed at PASS START
                    //    that NO completed walk lists — no live root proves them anymore —
                    //    released the same way (bytes PRESERVED, state cleared). The
                    //    all-complete gate above covers this phase too (with zero registered
                    //    roots it is the orphaned-claim convergence of RESUME-962c item 2b).
                    //    Directly pinned rows keep their own contract (the pin suppresses
                    //    the claim; the next pass after an unpin discharges them); the pin
                    //    state is re-read fresh inside the transaction.
                    val claimedNow = certifiedFreshRealm(userDrive).use { realm ->
                        realm.where(File::class.java)
                            .equalTo(File::offlineOwnedByRoot.name, true)
                            .equalTo(File::driveId.name, userDrive.driveId)
                            .findAll()
                            .map { it.id }
                            .toSet()
                    }
                    val dischargeTargets = (claimedNow intersect previousClaimedIds) - listedUnion
                    if (dischargeTargets.isNotEmpty()) {
                        certifiedFreshRealm(userDrive).use { realm ->
                            realm.executeTransaction { mutableRealm ->
                                for (fileId in dischargeTargets) {
                                    if (job?.isActive == false) return@executeTransaction
                                    val proxy = FileController.getFileProxyById(fileId, customRealm = mutableRealm)
                                        ?: continue
                                    if (proxy.isFolder() || proxy.isDirectlyPinned) continue
                                    clearClaimedRowFields(mutableRealm, fileId)
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    private enum class MissingRootResolution { RECOVERED, DEFINITIVELY_GONE, UNRESOLVED }

    /**
     * FHNW-1189 A3: the authoritative remote status of a root whose CACHED folder row is
     * gone. A missing cache row is NEVER evidence of a remote deletion (cache loss, a
     * broken ancestor link and a partial listing refresh look identical), so the
     * registration is withdrawn only on a DEFINITIVE remote answer:
     *  - a live folder -> the cached row is recovered (a coordinated listing write) and
     *    the next pass walks the root again;
     *  - a trashed folder or the API's object_not_found -> definitive: the registration
     *    is withdrawn (a coordinated registry deletion; bytes are NEVER deleted here —
     *    the claims release through the maintenance discharge on the SUBSEQUENT pass,
     *    which no longer exits at an empty registry (RESUME-962c item 2b). The release
     *    deliberately does NOT run inside this resolution: remaining roots' walk status
     *    is unproven at this point, and a claim whose cached chain reaches the ended
     *    root may have moved into an unproven one (the P2-5 discipline applies to this
     *    contract end too);
     *  - anything else (network, permission, another code) -> unresolved: the
     *    registration and the row stand and the next pass retries.
     */
    private suspend fun resolveMissingRootFolder(userDrive: UserDrive, root: OfflineFolderRoot): MissingRootResolution {
        val okHttpClient = AccountUtils.getHttpClient(userDrive.userId, null)
        val details = Dispatchers.IO {
            ApiRepository.getFileDetails(File(id = root.folderId, driveId = userDrive.driveId), okHttpClient)
        }
        val remoteFolder = details.data
        return when {
            details.isSuccess() && remoteFolder != null && !remoteFolder.isTrashed() -> {
                // RESUME-33dd item 1: the recovery writes only when the response identifies
                // the REQUESTED folder on the REQUESTED drive — a live answer for any other
                // row is inconclusive for this registration (it stands; the next pass
                // retries), never a recovery and never a withdrawal.
                if (remoteFolder.id != root.folderId || remoteFolder.driveId != userDrive.driveId) {
                    SentryLog.w(
                        TAG,
                        "Offline root ${root.uid}: the remote details answer does not identify the requested folder; the registration stands",
                    )
                    MissingRootResolution.UNRESOLVED
                } else {
                    // RESUME-962c item 2: the cached row is ABSENT here — the very premise of
                    // this branch — so the refresh-only [FileController.updateExistingFile] was
                    // a silent no-op that still reported recovery. The coordinated INSERT
                    // lever actually creates the row (the remote details carry the folder's
                    // identity; the registry's intent stands untouched). RESUME-33dd item 1:
                    // the lever rechecks absence inside its write transaction — a row a
                    // concurrent listing writer recreated while this request was in flight is
                    // PRESERVED (it is the newer state), not overwritten by this detached
                    // snapshot.
                    val inserted = certifiedFreshRealm(userDrive).use { realm ->
                        FileController.upsertMissingRemoteFile(newFile = remoteFolder, realm = realm)
                    }
                    if (inserted) {
                        SentryLog.i(TAG, "Offline root ${root.uid}: the cached folder row was recovered from the remote details")
                    } else {
                        SentryLog.i(
                            TAG,
                            "Offline root ${root.uid}: the cached folder row already reappeared (a concurrent writer recreated it); the recovery preserved it",
                        )
                    }
                    MissingRootResolution.RECOVERED
                }
            }
            remoteFolder?.isTrashed() == true || details.error?.code == ErrorCode.OBJECT_NOT_FOUND -> {
                OfflineCommitAuthority.withCommitAuthority {
                    certifiedFreshRealm(userDrive).use { realm ->
                        realm.executeTransaction { mutableRealm ->
                            mutableRealm.where(OfflineFolderRoot::class.java)
                                .equalTo(OfflineFolderRoot::uid.name, root.uid)
                                .findAll()
                                .deleteAllFromRealm()
                        }
                    }
                }
                SentryLog.i(
                    TAG,
                    "Offline root ${root.uid}: the remote folder is definitively " +
                        "${if (remoteFolder?.isTrashed() == true) "trashed" else "gone"}; registration withdrawn, bytes preserved",
                )
                MissingRootResolution.DEFINITIVELY_GONE
            }
            else -> {
                SentryLog.w(
                    TAG,
                    "Offline root ${root.uid}: the remote folder status is inconclusive " +
                        "(${details.error?.code ?: details.error?.exception?.javaClass?.simpleName}); the registration stands",
                )
                MissingRootResolution.UNRESOLVED
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

                certifiedFreshRealm(userDrive).use { realm ->
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
        certifiedFreshRealm(userDrive).use { realm ->
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
        val snapshot = certifiedFreshRealm(userDrive).use { realm ->
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
            relocatePublishedCopy(context, userDrive, snapshot, offlineFile)?.let { relocated ->
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
    private suspend fun relocatePublishedCopy(
        context: Context,
        userDrive: UserDrive,
        snapshot: File,
        offlineFile: JavaFile?,
    ): JavaFile? = withContext(Dispatchers.IO) {
        val target = offlineFile ?: return@withContext null
        val recordedPath = snapshot.offlinePublishedPath
        if (recordedPath.isNullOrBlank() || recordedPath == target.path) return@withContext null
        val previous = JavaFile(recordedPath)
        if (!previous.exists()) return@withContext null
        // The relocation is a publication-adjacent filesystem effect (the accepted bytes
        // move and the recorded path is repointed): its checks, the rename and the record
        // run INSIDE the commit authority — and (FHNW-1189 A4) the row's complete authority
        // is RE-DERIVED from a fresh certified read, never from the pre-hold snapshot: a
        // withdrawal that preserved the bytes, a second tree move, or an intent change may
        // all have completed before this hold acquired, and executing against the stale
        // snapshot would move bytes whose contract already ended. The rename is
        // instantaneous (no byte copy), so the hold stays local-IO sized.
        OfflineCommitAuthority.withCommitAuthority {
            certifiedFreshRealm(userDrive).use { realm ->
                val freshRow = FileController.getFileProxyById(snapshot.id, customRealm = realm)
                    ?: return@withCommitAuthority null // the row is gone: nothing to relocate
                if (!hasLiveOfflineIntent(freshRow, realm, userDrive)) {
                    SentryLog.w(TAG, "Relocation declined for file ${snapshot.id}: the offline intent no longer stands; bytes stay where they are")
                    return@withCommitAuthority null
                }
                if (freshRow.offlineOwnedByRoot && !freshRow.isDirectlyPinned && !isRootCoveredCopy(freshRow, realm, userDrive)) {
                    SentryLog.w(TAG, "Relocation declined for file ${snapshot.id}: root ownership stale; bytes stay where they are")
                    return@withCommitAuthority null
                }
                val freshRecordedPath = freshRow.offlinePublishedPath
                if (freshRecordedPath.isNullOrBlank() || freshRecordedPath != recordedPath) {
                    SentryLog.w(TAG, "Relocation declined for file ${snapshot.id}: the recorded path moved on; bytes stay where they are")
                    return@withCommitAuthority null
                }
                val freshTarget = freshRow.getOfflineFile(context, userDrive.userId)
                if (freshTarget == null || freshTarget.path != target.path) {
                    SentryLog.w(TAG, "Relocation declined for file ${snapshot.id}: the derived target moved on; bytes stay where they are")
                    return@withCommitAuthority null
                }
                val claimedByAnotherRow = realm.where(File::class.java)
                    .equalTo(File::offlinePublishedPath.name, recordedPath)
                    .notEqualTo(File::id.name, snapshot.id)
                    .count() > 0
                if (claimedByAnotherRow) return@withCommitAuthority null // another file legitimately lives there now
                if (target.exists()) {
                    // rename(2) would silently REPLACE these bytes. They are provably not this row's
                    // accepted copy (the current path classified as MISSING, which is what led here),
                    // so they belong to nobody this code may destroy: the old copy stays at its
                    // recorded path (recoverable) and the foreign bytes stay put. The staged
                    // replacement path preserves them too: unproven bytes never authorize destruction.
                    SentryLog.w(TAG, "Relocation target already holds bytes for file ${snapshot.id}; both copies preserved")
                    return@withCommitAuthority null
                }
                val parent = target.parentFile ?: return@withCommitAuthority null
                if (!parent.exists() && !parent.mkdirs()) return@withCommitAuthority null
                if (!previous.renameTo(target)) {
                    SentryLog.w(TAG, "Relocation refused for file ${snapshot.id}; previous copy kept at its recorded path")
                    return@withCommitAuthority null
                }
                realm.executeTransaction { mutableRealm ->
                    FileController.getFileProxyById(snapshot.id, customRealm = mutableRealm)?.let { proxy ->
                        proxy.offlinePublishedPath = target.path
                    }
                }
                SentryLog.d(TAG, "Relocated the offline copy of ${snapshot.id} to its renamed location")
                target
            }
        }
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
        // Byte-evidence SNAPSHOT (stat + MUTATION GENERATION) under one short hold; the
        // whole-file digest runs OUTSIDE the hold (it never runs inside one). At the record
        // hold, a generation change means a participating mutation committed between the
        // snapshot and the acquisition: the evidence is uncertified and the adoption is
        // SKIPPED (the preserved bytes stay consistent with THEIR basis) — never recorded
        // from stale evidence.
        val (byteStat, snapshotGeneration) = OfflineCommitAuthority.withCommitAuthorityForEvidence {
            offlineFile?.takeIf { it.exists() }?.let { copy ->
                copy.length() to copy.lastModified().div(1_000L)
            } to OfflineCommitAuthority.currentGenerationUnderAuthority()
        }
        val digest = if (byteStat != null) computeDigest(offlineFile!!) else null
        val verifiedBytes = if (byteStat != null && digest != null) {
            VerifiedLocalCopy(length = byteStat.first, modifiedSec = byteStat.second, digest = digest)
        } else {
            null
        }
        OfflineCommitAuthority.withCommitAuthority {
            if (OfflineCommitAuthority.currentGenerationUnderAuthority() != snapshotGeneration) {
                SentryLog.w(TAG, "App state changed between the byte snapshot and the adoption for file $fileId; metadata-only adoption skipped")
                return@withCommitAuthority true // preserved bytes are consistent with THEIR basis; not a withdrawal
            }
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
        // CERTIFIED decision read (RESUME-962c item 3): this read authorizes the timestamp
        // adoption EFFECT below — a retained stale view here would setLastModified and write
        // basis fields for an intent that was already withdrawn.
        certifiedFreshRealm(userDrive).use { realm ->
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

            val rowRead = certifiedFreshRealm(userDrive).use { realm ->
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

            // CONTENT EVIDENCE — attempt loop (RESUME-1189-2245 item 4 / FHNW-1189 A5+C):
            // the stat values and the MUTATION GENERATION are captured together under ONE
            // short hold; the whole-file digest runs OUTSIDE the hold (a hold never
            // digests, and UI-thread writers wait on it). At the publication hold, a
            // generation change means a participating mutation committed between the
            // snapshot and the acquisition — equal length and second-rounded mtime certify
            // nothing by themselves — so the evidence is INVALIDATED: the attempt retries
            // once with a fresh snapshot; a second invalidation PRESERVES (uncertifiable
            // evidence never authorizes destruction).
            var attempt = 0
            while (true) {
                // OBSERVATION acquisition (RESUME-962c item 1): the evidence capture never
                // advances the generation — only a real mutation between the capture and
                // the publication hold can.
                val (preview, snapshotGeneration) = OfflineCommitAuthority.withCommitAuthorityForEvidence {
                    val previewExists = offlineFile.exists()
                    Triple(
                        previewExists,
                        if (previewExists) offlineFile.length() else null,
                        if (previewExists) offlineFile.lastModified().div(1_000L) else null,
                    ) to OfflineCommitAuthority.currentGenerationUnderAuthority()
                }
                val (previewExists, previewLength, previewModifiedSec) = preview
                val previewDigest = if (previewExists) computeDigest(offlineFile) else null
                var evidenceInvalidated = false

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
                // EVIDENCE CERTIFICATION (generation): a participating mutation committed
                // between the pre-hold snapshot and this acquisition — the preview evidence
                // cannot authorize anything (see the attempt loop above).
                if (OfflineCommitAuthority.currentGenerationUnderAuthority() != snapshotGeneration) {
                    evidenceInvalidated = true
                    SentryLog.w(TAG, "App state changed between the content snapshot and the publication for file ${file.id}; evidence invalidated")
                    return@withPublicationLock StagedPublication.PRESERVED_EXISTING
                }
                val inLockRead = certifiedFreshRealm(userDrive).use { realm ->
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
                val freshState = certifiedFreshRealm(userDrive).use { realm ->
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
                certifiedFreshRealm(userDrive).use { realm ->
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
                if (evidenceInvalidated && attempt == 0) {
                    attempt = 1
                    continue // one retry with a fresh snapshot; a second invalidation preserves
                }
                return@withContext publicationOutcome
            }
            // Unreachable: the attempt loop only continues (one retry) or returns the
            // outcome; the terminator gives the block the publication's type.
            error("unreachable: the staged publication attempt loop never completes normally")
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
        // Byte-evidence SNAPSHOT (stat + MUTATION GENERATION) under one short hold; the
        // whole-file digest (the identity being recorded) runs OUTSIDE the hold. At the
        // record hold, a generation change or a stat change means the bytes the digest
        // describes are no longer the bytes at the offline path (a participating mutation —
        // e.g. another publication — replaced them): nothing is recorded, and the OTHER
        // publication's basis classifies the bytes. The residual external SAME-STAT edit
        // class (same length, same second-rounded mtime, spanning the unbounded lock wait)
        // is disclosed in [OfflineCommitAuthority]'s contract.
        val (acceptedStat, snapshotGeneration) = OfflineCommitAuthority.withCommitAuthorityForEvidence {
            (offlineFile.length() to offlineFile.lastModified().div(1_000L)) to
                OfflineCommitAuthority.currentGenerationUnderAuthority()
        }
        val acceptedDigest = computeDigest(offlineFile)
        OfflineCommitAuthority.withCommitAuthority {
            if (OfflineCommitAuthority.currentGenerationUnderAuthority() != snapshotGeneration ||
                offlineFile.length() != acceptedStat.first ||
                offlineFile.lastModified().div(1_000L) != acceptedStat.second
            ) {
                SentryLog.w(TAG, "Accepted offline bytes changed under the coordination for file ${remoteFile.id}; basis not recorded")
                return@withCommitAuthority PublicationOutcome.NOT_RECORDED
            }
            recordAcceptedPublicationInHold(userDrive, remoteFile, offlineFile, acceptedServerHash, acceptedDigest, acceptedStat.first)
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
        // CERTIFIED decision read (RESUME-962c item 3): this is the record's pre-effect
        // authority read — a retained stale view must not let a withdrawn intent record.
        certifiedFreshRealm(userDrive).use { realm ->
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
