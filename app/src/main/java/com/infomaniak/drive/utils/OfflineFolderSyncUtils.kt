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
            val parent = realm.where(File::class.java).equalTo(File::id.name, parentId).findFirst() ?: return false
            parentId = parent.parentId
        }
        return false
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
            val keptAgainstRefusal = FileController.getRealmInstance(userDrive).use { realm ->
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
                        proxy.isDirectlyPinned = false
                        proxy.offlineBasisModifiedAt = 0L
                        proxy.offlineBasisSize = null
                        proxy.offlineContentDigest = null
                        proxy.serverContentHash = null
                    }
                }
                false
            }
            if (keptAgainstRefusal) allDeletionsConfirmed = false
        }

        // A refused deletion keeps the root row: the next unmark pass retries it, and the
        // protected files stay covered meanwhile.
        if (allDeletionsConfirmed) FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(OfflineFolderRoot::class.java)
                    .equalTo(OfflineFolderRoot::uid.name, rootUid)
                    .findAll()
                    .deleteAllFromRealm()
            }
        }
        return allDeletionsConfirmed
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
                    FileController.getRealmInstance(userDrive).use { realm ->
                        realm.executeTransaction { mutableRealm ->
                            FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.let { proxy ->
                                proxy.isMarkedAsOffline = false
                                proxy.isOffline = false
                                proxy.offlineBasisModifiedAt = 0L
                                proxy.offlineBasisSize = null
                                proxy.offlineContentDigest = null
                                proxy.serverContentHash = null
                            }
                        }
                    }
                }

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
     * ([recordAcceptedPublication]), never from a listing or an enqueued download. Root
     * COVERAGE itself is never recorded here: it is derived live from the roots table
     * ([isRootCoveredCopy]). Files that already keep an offline copy ([File.isOffline]) or a
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
     * Metadata-only change: adopt new remote metadata while keeping the verified bytes. ALL
     * guards run before any mutation: live offline intent (an unmark or release during the
     * details call must not be overridden), an actual metadata difference (an unchanged file
     * is a no-op, not a per-pass write), and a digest verification of the local copy (a
     * same-size/same-mtime edit that classified as intact must not be re-stamped as clean).
     * The local file timestamp follows the adopted metadata FIRST — if it cannot be updated
     * the adoption is skipped, so the preserved basis always describes the actual local bytes.
     */
    private suspend fun updatePublicationBasis(userDrive: UserDrive, fileId: Int, remoteFile: File, offlineFile: JavaFile?) = withContext(Dispatchers.IO) {
        FileController.getRealmInstance(userDrive).use { realm ->
            val proxy = FileController.getFileProxyById(fileId, customRealm = realm) ?: return@withContext
            if (!hasLiveOfflineIntent(proxy, realm, userDrive)) {
                SentryLog.w(TAG, "Offline intent withdrawn before metadata adoption for file $fileId; adoption skipped")
                return@withContext
            }
            val metadataUnchanged = proxy.offlineBasisModifiedAt == remoteFile.lastModifiedAt &&
                (remoteFile.size == null || proxy.offlineBasisSize == null || proxy.offlineBasisSize == remoteFile.size)
            if (metadataUnchanged) return@withContext

            if (offlineFile?.exists() == true) {
                val basisDigest = proxy.offlineContentDigest
                if (basisDigest != null) {
                    val localDigest = computeDigest(offlineFile)
                    if (localDigest == null || localDigest != basisDigest) {
                        SentryLog.w(TAG, "Local bytes diverge from the accepted digest for file $fileId; metadata-only adoption skipped")
                        return@withContext
                    }
                }
                if (!offlineFile.setLastModified(remoteFile.getLastModifiedInMilliSecond())) {
                    SentryLog.w(TAG, "Local timestamp refused for file $fileId; metadata-only adoption skipped")
                    return@withContext
                }
            }
            realm.executeTransaction { mutableRealm ->
                FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.let { row ->
                    row.lastModifiedAt = remoteFile.lastModifiedAt
                    row.size = remoteFile.size
                    // The accepted identity advances ONLY here, on a digest-verified adoption
                    // (the previously recorded identity still describes these bytes), and is
                    // never erased by a hash-less response.
                    remoteFile.serverContentHash?.let { row.serverContentHash = it }
                    row.offlineBasisModifiedAt = remoteFile.lastModifiedAt
                    row.offlineBasisSize = remoteFile.size
                    offlineFile?.let { row.offlinePublishedPath = it.path }
                }
            }
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
            val (currentRow, authorized) = rowRead
            if (!authorized) {
                SentryLog.w(TAG, "Offline intent withdrawn during staged download for file ${file.id}; publication skipped")
                return@withContext StagedPublication.INTENT_WITHDRAWN
            }

            // ---- Guards immediately before any mutation (all preserve) ----
            currentCoroutineContext().ensureActive()
            val existingDigest = if (offlineFile.exists()) computeDigest(offlineFile) else null
            val basis = PublicationBasis(
                modifiedAt = currentRow.offlineBasisModifiedAt,
                size = currentRow.offlineBasisSize,
                digest = currentRow.offlineContentDigest,
                serverHash = currentRow.serverContentHash,
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
                    return@withContext StagedPublication.PRESERVED_EXISTING
                }
                PublicationDecision.ADOPT_METADATA_ONLY -> {
                    // Body identical to the preserved publication: metadata-only adoption on
                    // the verified clean copy (timestamp and basis move together in
                    // [updatePublicationBasis]; the bytes are never touched).
                    updatePublicationBasis(userDrive, file.id, file, offlineFile)
                    return@withContext StagedPublication.ADOPTED_METADATA_ONLY
                }
                PublicationDecision.REPLACE_BYTES -> Unit
            }

            // The publication timestamp is set on the STAGING file, before the atomic step:
            // a refusal aborts here, so the previous good bytes are never sacrificed for a
            // basis that would misdescribe the new ones.
            if (!staging.setLastModified(file.getLastModifiedInMilliSecond())) {
                SentryLog.w(TAG, "Staging timestamp refused for file ${file.id}; publication aborted, previous copy preserved")
                return@withContext StagedPublication.FAILED
            }

            // One atomic publication step: rename(2) over the existing file (rename keeps the
            // staging mtime). Failure keeps the old bytes (staging is removed in finally).
            if (!staging.renameTo(offlineFile)) {
                SentryLog.w(TAG, "Atomic replace refused for file ${file.id}; previous copy preserved")
                return@withContext StagedPublication.FAILED
            }

            FileController.getRealmInstance(userDrive).use { realm ->
                realm.executeTransaction { mutableRealm ->
                    FileController.getFileProxyById(file.id, customRealm = mutableRealm)?.let { proxy ->
                        proxy.lastModifiedAt = file.lastModifiedAt
                        proxy.size = file.size
                        // Only an identity established for THIS accepted body is recorded;
                        // an unestablished one clears the record (unknown identity).
                        proxy.serverContentHash = establishedServerHash
                        proxy.offlineBasisModifiedAt = file.lastModifiedAt
                        proxy.offlineBasisSize = file.size
                        proxy.offlineContentDigest = digest
                        proxy.offlinePublishedPath = offlineFile.path
                        proxy.isOffline = true
                    }
                }
            }
            if (file.isMedia()) MediaUtils.scanFile(context, offlineFile)
            StagedPublication.PUBLISHED
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
        FileController.getRealmInstance(userDrive).use { realm ->
            val proxy = FileController.getFileProxyById(remoteFile.id, customRealm = realm)
                ?: return@withContext PublicationOutcome.INTENT_WITHDRAWN
            if (!hasLiveOfflineIntent(proxy, realm, userDrive)) {
                SentryLog.w(TAG, "Offline intent withdrawn during download for file ${remoteFile.id}; publication skipped")
                return@withContext PublicationOutcome.INTENT_WITHDRAWN
            }
            val digest = computeDigest(offlineFile)
            if (digest == null) {
                SentryLog.w(TAG, "Accepted offline bytes could not be digested for file ${remoteFile.id}; basis not recorded")
                return@withContext PublicationOutcome.NOT_RECORDED
            }
            realm.executeTransaction { mutableRealm ->
                FileController.getFileProxyById(remoteFile.id, customRealm = mutableRealm)?.let { row ->
                    row.lastModifiedAt = remoteFile.lastModifiedAt
                    row.size = remoteFile.size
                    // Only an identity established for THIS accepted body is recorded; a
                    // hash-less capture clears the record (unknown identity).
                    row.serverContentHash = acceptedServerHash
                    row.offlineBasisModifiedAt = remoteFile.lastModifiedAt
                    row.offlineBasisSize = remoteFile.size
                    row.offlineContentDigest = digest
                    row.offlinePublishedPath = offlineFile.path
                    row.isOffline = true
                }
            }
            PublicationOutcome.PUBLISHED
        }
    }

    //endregion
}
