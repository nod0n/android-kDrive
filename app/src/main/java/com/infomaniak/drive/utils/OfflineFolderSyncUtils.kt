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
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.invoke
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
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
     * The publication critical section: ONE process-local lock ordering every final
     * live-intent/current-content validation plus publication against every
     * intent-withdrawal mutation (explicit unmark, direct unpin, remote-delete release).
     * Without it, an unmark completing between the last live-intent read and the atomic
     * rename could still be followed by bytes being re-published at the final path.
     *
     * The NETWORK TRANSFER stays outside the critical section — only the narrow
     * check-to-publish interval (final intent read, timestamp, rename, basis record) and
     * the equally narrow withdrawal mutations are serialized; the sections are local-IO
     * sized (metadata, digest, one rename, one Realm transaction). All covered callers
     * participate: [stagedRedownload], [updatePublicationBasis], [recordAcceptedPublication],
     * [unmarkFolderOffline] and the remote-delete release of [syncOfflineFolderRoots], plus
     * the unmark entry points in MainViewModel. A pending-marker clear that keeps
     * [File.isOffline] (bulk download completion) does NOT withdraw persistent intent and
     * needs no participation.
     *
     * REAL LIMITS (documented, not claimed away): this orders MUTATIONS inside one process
     * — the filesystem and Realm are NOT one atomic resource, and a crash mid-section can
     * still leave renamed bytes whose basis write never happened (such bytes classify as
     * UNKNOWN and are preserved by every later decision) or vice versa. EXTERNAL writers
     * that do not participate (other processes, adb, unsynchronized code paths that bypass
     * the covered entry points) are outside any in-process mutex's reach: a non-covered
     * byte edit between the in-section evidence reads and the rename remains a disclosed
     * platform-level window, and power loss is outside any app-side lock.
     */
    private val publicationLock = Mutex()

    /** Runs [block] inside the publication critical section (see class-level kDoc). */
    internal suspend fun <T> withPublicationLock(block: suspend () -> T): T = publicationLock.withLock { block() }

    /**
     * Test seam: invoked at the narrow publication point of [stagedRedownload] — inside the
     * publication critical section, BEFORE the final live-intent read, so a regression can
     * withdraw the offline intent (through direct row writes or a concurrent unmark, which
     * suspends on the same lock) exactly inside the publication window. Production never
     * sets it.
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
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { mutableRealm -> mutableRealm.insertOrUpdate(root) }
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
            // The whole per-file withdrawal (classification, digest evidence, checked
            // deletion, state clear) runs inside the publication critical section: it is
            // serialized against every publication's final check-to-publish interval, so
            // neither can interleave into the other's defined order.
            val keptAgainstRefusal = withPublicationLock {
                FileController.getRealmInstance(userDrive).use { realm ->
                val file = FileController.getFileProxyById(fileId, customRealm = realm) ?: return@use false
                val keepCopy = file.isFolder() || file.isDirectlyPinned || file.id in otherRootSubtreeIds
                if (keepCopy) return@use false

                val offlineFile = file.getOfflineFile(context, userDrive.userId)
                val localExists = offlineFile?.exists() == true

                // Classify against the accepted basis BEFORE any destructive step: changed and
                // unknown bytes belong to the user — release coverage, keep the bytes. Missing
                // evidence preserves too: an unreadable digest, or an accepted identity that was
                // never recorded, can never authorize deletion.
                if (localExists) {
                    val basis = if (file.offlineBasisModifiedAt > 0L) PublicationBasis(
                        modifiedAt = file.offlineBasisModifiedAt,
                        size = file.offlineBasisSize,
                        digest = file.offlineContentDigest,
                        serverHash = file.serverContentHash,
                    ) else null
                    val localState = classifyLocalCopy(
                        basis = basis,
                        localExists = localExists,
                        localSize = offlineFile?.takeIf { localExists }?.length(),
                        localModifiedSec = offlineFile?.takeIf { localExists }?.lastModified()?.div(1_000L),
                    )
                    // A same-size/same-mtime edit still diverges from the accepted digest:
                    // verify the accepted identity before the destructive step. An unreadable
                    // digest (null) or a digest-less basis is preservation, not authority.
                    val digestVerifies = basis?.digest != null &&
                        withContext(Dispatchers.IO) { computeDigest(offlineFile) } == basis.digest
                    val preservesBytes = localState == LocalCopyState.LOCALLY_EDITED ||
                        localState == LocalCopyState.UNKNOWN ||
                        !digestVerifies
                    if (preservesBytes) {
                        SentryLog.w(TAG, "Unmark keeps edited/unknown/unverifiable bytes for file ${file.id}")
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
                        return@use false
                    }
                }

                // Checked deletion: clear the offline state only once the bytes are really gone.
                // R4 (FHNW-caf1): the pin decision was read at the section's start; a user pin
                // landing inside this span (setDirectlyPinned does not participate in this
                // coordination) must not have its copy deleted by the withdrawal. The flag is
                // re-read at the LAST point before the destructive step: a newly pinned row is
                // skipped like any keepCopy row — the pin's own contract retains the copy.
                val pinnedSinceRead = FileController.getFileProxyById(fileId, customRealm = realm)
                    ?.isDirectlyPinned ?: return@use false
                if (pinnedSinceRead) return@use false
                val copyGone = if (localExists) {
                    withContext(Dispatchers.IO) { offlineFile.delete() }
                } else {
                    true
                }
                if (!copyGone) {
                    SentryLog.w(TAG, "Offline copy deletion refused, keeping pin for file ${file.id}")
                    return@use true
                }
                if (file.isMedia()) file.deleteInMediaScan(context, userDrive)
                realm.executeTransaction { mutableRealm ->
                    FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.let { proxy ->
                        proxy.isMarkedAsOffline = false
                        proxy.isOffline = false
                        // R4 defense-in-depth: a pin that landed after the pre-deletion
                        // re-check keeps its FLAG (the user's choice is not clobbered by
                        // the withdrawal); the rest of the offline state is still released.
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
            if (keptAgainstRefusal) allDeletionsConfirmed = false
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

        // A refused deletion keeps the root row: the next unmark pass retries it, and the
        // protected files stay covered meanwhile.
        if (allDeletionsConfirmed && beyondSubtreeConfirmed) FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(OfflineFolderRoot::class.java)
                    .equalTo(OfflineFolderRoot::uid.name, rootUid)
                    .findAll()
                    .deleteAllFromRealm()
            }
        }
        // A refused beyond-subtree deletion is reported to the caller too (FHNW-caf1 intake
        // WIP observation): the unmark is only complete when EVERY release confirmed.
        return allDeletionsConfirmed && beyondSubtreeConfirmed
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
            val confirmed = withPublicationLock {
                FileController.getRealmInstance(userDrive).use { realm ->
                    val file = FileController.getFileProxyById(fileId, customRealm = realm)
                        ?: return@use true // the row is gone: nothing left to release
                    if (file.isFolder()) return@use true // walkSubtree never claims folders
                    if (isCoveredByRoot(file, realm, userDrive, excludedRootFolderId = withdrawnRootFolderId)) {
                        return@use true // another live owner keeps the claim until its own withdrawal
                    }
                    if (file.isDirectlyPinned) {
                        // Explicit direct retention keeps its own contract (bytes, pin and
                        // offline state untouched): only the root's claim is released.
                        realm.executeTransaction { mutableRealm ->
                            FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.offlineOwnedByRoot = false
                        }
                        return@use true
                    }

                    // Full release decision — the same classification, digest verification,
                    // checked deletion and state clear as the in-subtree withdrawal, but the
                    // bytes are looked up at their RECORDED publication path first.
                    val recordedPath = file.offlinePublishedPath
                    val offlineFile: JavaFile? = if (recordedPath.isNullOrBlank()) {
                        file.getOfflineFile(context, userDrive.userId)
                    } else {
                        JavaFile(recordedPath)
                    }
                    val localExists = offlineFile?.exists() == true

                    // Classify against the accepted basis BEFORE any destructive step: changed
                    // and unknown bytes belong to the user — release the claim, keep the bytes.
                    if (localExists) {
                        val basis = if (file.offlineBasisModifiedAt > 0L) PublicationBasis(
                            modifiedAt = file.offlineBasisModifiedAt,
                            size = file.offlineBasisSize,
                            digest = file.offlineContentDigest,
                            serverHash = file.serverContentHash,
                        ) else null
                        val localState = classifyLocalCopy(
                            basis = basis,
                            localExists = localExists,
                            localSize = offlineFile?.takeIf { localExists }?.length(),
                            localModifiedSec = offlineFile?.takeIf { localExists }?.lastModified()?.div(1_000L),
                        )
                        val digestVerifies = basis?.digest != null && offlineFile != null &&
                            withContext(Dispatchers.IO) { computeDigest(offlineFile) } == basis.digest
                        val preservesBytes = localState == LocalCopyState.LOCALLY_EDITED ||
                            localState == LocalCopyState.UNKNOWN ||
                            !digestVerifies
                        if (preservesBytes) {
                            SentryLog.w(TAG, "Beyond-subtree unmark keeps edited/unknown/unverifiable bytes for file ${file.id}")
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
                            return@use true
                        }
                    }

                    // Checked deletion: clear the offline state only once the bytes are really gone.
                    // R4, same as the in-subtree withdrawal: the pin flag is re-read at the
                    // last point before the destructive step.
                    val pinnedSinceRead = FileController.getFileProxyById(fileId, customRealm = realm)
                        ?.isDirectlyPinned ?: return@use true
                    if (pinnedSinceRead) {
                        realm.executeTransaction { mutableRealm ->
                            FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.offlineOwnedByRoot = false
                        }
                        return@use true
                    }
                    val copyGone = if (localExists && offlineFile != null) {
                        withContext(Dispatchers.IO) { offlineFile.delete() }
                    } else {
                        true
                    }
                    if (!copyGone) {
                        SentryLog.w(TAG, "Beyond-subtree offline copy deletion refused, keeping the claim for file ${file.id}")
                        return@use false // the claim stays: the next unmark pass retries it
                    }
                    if (file.isMedia()) file.deleteInMediaScan(context, userDrive)
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
                    true
                }
            }
            if (!confirmed) allConfirmed = false
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
     * Maintenance-side ownership release (FHNW-caf1 R2) — run ONLY after a COMPLETE root
     * walk: [listedFileIds] are excluded because the walk PROVED them remotely present
     * inside this root, whatever the cached parent chain looks like (a chain gap must never
     * read as a move-out). Every row still carrying the recorded claim that no live root
     * covers anymore ([isCoveredByRoot] against the other roots; direct pins are excluded —
     * their own contract keeps its suppression until the pin ends, and the next pass after
     * an unpin discharges them) is released under the publication critical section with the
     * remote-delete release's exact postcondition: bytes PRESERVED (the remote-move
     * contract is distinct from explicit unmark's clean-copy removal), every offline field
     * cleared including the transient pending marker.
     */
    private suspend fun releaseOwnershipBeyondCoverage(
        userDrive: UserDrive,
        excludedRootFolderId: Int,
        listedFileIds: Set<Int>,
    ) {
        val claimedIds = FileController.getRealmInstance(userDrive).use { realm ->
            realm.where(File::class.java)
                .equalTo(File::offlineOwnedByRoot.name, true)
                .equalTo(File::driveId.name, userDrive.driveId)
                .notEqualTo(File::isDirectlyPinned.name, true)
                .findAll()
                .map { it.id }
                .toSet()
        } - listedFileIds
        claimedIds.forEach { fileId ->
            currentCoroutineContext().ensureActive()
            withPublicationLock {
                FileController.getRealmInstance(userDrive).use { realm ->
                    val file = FileController.getFileProxyById(fileId, customRealm = realm) ?: return@use
                    if (file.isFolder()) return@use // walkSubtree never claims folders
                    if (isCoveredByRoot(file, realm, userDrive, excludedRootFolderId)) return@use // another live owner keeps it
                    // The remote-delete release's postcondition, plus the pending marker:
                    // bytes preserved, every offline field cleared.
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

    /**
     * Maintenance pass over every root of the current user's drives: re-walks each root,
     * refreshes changed files and — only after a COMPLETE walk — releases coverage of
     * remotely deleted files without deleting bytes. Hooked into the existing offline sync
     * cadence.
     */
    suspend fun syncOfflineFolderRoots(context: Context) {
        DriveInfosController.getDrives(AccountUtils.currentUserId).forEach { drive ->
            currentCoroutineContext().ensureActive()
            val userDrive = UserDrive(driveId = drive.id)
            val roots = FileController.getRealmInstance(userDrive).use { realm ->
                realm.where(OfflineFolderRoot::class.java).findAll().map { realm.copyFromRealm(it, 0) }
            }
            roots.forEach root@{ root ->
                currentCoroutineContext().ensureActive()
                val previousSubtree = collectSubtreeIdsFromCache(userDrive, root.folderId)
                val rootFolder = FileController.getRealmInstance(userDrive).use { realm ->
                    FileController.getFileProxyById(root.folderId, customRealm = realm)?.let { realm.copyFromRealm(it, 0) }
                } ?: return@root

                val listedFileIds = mutableSetOf<Int>()
                val outcome = walkSubtree(
                    context = context,
                    userDrive = userDrive,
                    rootFolderFile = rootFolder,
                    onFileVisited = listedFileIds::add,
                )
                if (outcome == WalkOutcome.INCOMPLETE) return@root // incomplete traversal proves nothing

                // Remotely deleted within a complete walk: release coverage, keep the bytes.
                // Coverage is the persistent copy state (isOffline + recorded basis), never the
                // transient pending-download marker; directly pinned copies stay with their own
                // contract (retained until withdrawn) and are not released here. Files marked
                // pending but never yet published are released too: their download intent died
                // with the remote file, and no stale queued worker may keep trying bodies for
                // them. An INCOMPLETE traversal never reaches this code.
                val remoteDeletedIds = (FileController.getRealmInstance(userDrive).use { realm ->
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
                } intersect previousSubtree) - listedFileIds
                remoteDeletedIds.forEach { fileId ->
                    // The coverage release is an intent withdrawal: it participates in the
                    // publication critical section like every other covered withdrawal.
                    withPublicationLock {
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
                }

                // BEYOND-COVERAGE OWNERSHIP RELEASE (FHNW-caf1 R2): a cached tree update can
                // move an owned row out of every root between passes — the COMPLETE walk
                // above proved the row absent from THIS root's remote subtree (an incomplete
                // walk proves nothing and released nothing), and the rows the walk DID list
                // are excluded: they are remotely present inside a root and keep their
                // contract regardless of cache chain gaps. Releasing the recorded claim here
                // (bytes PRESERVED — the remote-move contract, distinct from explicit
                // unmark's clean-copy removal) keeps a moved-out owned row from freezing
                // forever: no walk lists it, no release found it, every baseline guard
                // skipped it.
                releaseOwnershipBeyondCoverage(userDrive, excludedRootFolderId = root.folderId, listedFileIds = listedFileIds)

                listedFileIds.forEach { fileId ->
                    currentCoroutineContext().ensureActive()
                    refreshOfflineFile(context, userDrive, fileId)
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
                    realm.executeTransaction { mutableRealm ->
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
     * The final live-intent re-read, the local timestamp adoption and the basis record run
     * INSIDE the publication critical section: the timestamp can no longer be mutated
     * before a decline that the coordination could have caught (a covered withdrawal
     * cannot complete between the in-lock intent read and the record). The digest
     * verification of the local bytes stays before the section — it is content evidence,
     * not intent evidence.
     */
    private suspend fun updatePublicationBasis(userDrive: UserDrive, fileId: Int, remoteFile: File, offlineFile: JavaFile?): Boolean = withContext(Dispatchers.IO) {
        withPublicationLock { updatePublicationBasisLocked(userDrive, fileId, remoteFile, offlineFile) }
    }

    private fun updatePublicationBasisLocked(userDrive: UserDrive, fileId: Int, remoteFile: File, offlineFile: JavaFile?): Boolean {
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
                val basisDigest = proxy.offlineContentDigest
                if (basisDigest != null) {
                    val localDigest = computeDigest(offlineFile)
                    if (localDigest == null || localDigest != basisDigest) {
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

            // PUBLICATION CRITICAL SECTION: from the FINAL live-intent and content reads
            // through the rename and the basis record, coordinated under [publicationLock]
            // with every intent-withdrawal mutation — an unmark can no longer complete
            // between the final checks and the bytes' replacement (it suspends on the same
            // lock; the defined orders are "withdrawn first -> nothing published" or
            // "published first -> the unmark removes it afterwards"). The network transfer
            // stayed OUTSIDE; the section is local-IO sized. ALL DECISIVE EVIDENCE is (re-)
            // read INSIDE the coordination: the pre-lock row/digest reads above are only a
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

                // Content evidence under the coordination: an edit that landed after the
                // preview digest read is seen HERE — stale evidence never authorizes the
                // rename over it.
                val existingDigest = if (offlineFile.exists()) computeDigest(offlineFile) else null
                val basis = PublicationBasis(
                    modifiedAt = authoritativeRow.offlineBasisModifiedAt,
                    size = authoritativeRow.offlineBasisSize,
                    digest = authoritativeRow.offlineContentDigest,
                    serverHash = authoritativeRow.serverContentHash,
                )
                when (decidePublication(
                    basis = basis,
                    localExists = offlineFile.exists(),
                    localSize = offlineFile.takeIf { it.exists() }?.length(),
                    localModifiedSec = offlineFile.takeIf { it.exists() }?.lastModified()?.div(1_000L),
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
                        // together; the bytes are never touched). A declined adoption is a
                        // withdrawn intent: report it accurately.
                        val adopted = updatePublicationBasisLocked(userDrive, file.id, file, offlineFile)
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
        // The FINAL content evidence (the digest being recorded), the live-intent
        // validation and the basis record ALL run INSIDE the publication critical section:
        // evidence read before acquiring the lock would be stale by the time it is
        // recorded.
        withPublicationLock {
            val digest = computeDigest(offlineFile)
            if (digest == null) {
                SentryLog.w(TAG, "Accepted offline bytes could not be digested for file ${remoteFile.id}; basis not recorded")
                return@withPublicationLock PublicationOutcome.NOT_RECORDED
            }
            FileController.getRealmInstance(userDrive).use { realm ->
                val proxy = FileController.getFileProxyById(remoteFile.id, customRealm = realm)
                    ?: return@withPublicationLock PublicationOutcome.INTENT_WITHDRAWN
                if (!hasLiveOfflineIntent(proxy, realm, userDrive)) {
                    SentryLog.w(TAG, "Offline intent withdrawn during download for file ${remoteFile.id}; publication skipped")
                    return@withPublicationLock PublicationOutcome.INTENT_WITHDRAWN
                }
                var recorded = false
                realm.executeTransaction { mutableRealm ->
                    FileController.getFileProxyById(remoteFile.id, customRealm = mutableRealm)?.let { row ->
                        // Defense-in-depth inside the transaction (the lock excludes every
                        // covered withdrawal for this whole section).
                        if (!hasLiveOfflineIntent(row, mutableRealm, userDrive)) {
                            SentryLog.w(TAG, "Offline intent withdrawn before the accepted basis record for file ${remoteFile.id}; nothing recorded")
                            return@executeTransaction
                        }
                        // The same final-publication-window defense as the staged record: a
                        // non-participating refresh can move the row out before THIS record —
                        // the root contract's reach has then ended, and no accepted
                        // publication may record a basis or offline status for a moved-out
                        // owned copy. The claim stands for the unmark's beyond-subtree sweep.
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
                        row.offlineContentDigest = digest
                        row.offlinePublishedPath = offlineFile.path
                        // A publication under live root coverage records the root contract as
                        // the owner (see the staged record above).
                        if (isRootCoveredCopy(row, mutableRealm, userDrive)) row.offlineOwnedByRoot = true
                        row.isOffline = true
                        recorded = true
                    }
                }
                if (recorded) PublicationOutcome.PUBLISHED else PublicationOutcome.INTENT_WITHDRAWN
            }
        }
    }

    //endregion
}
