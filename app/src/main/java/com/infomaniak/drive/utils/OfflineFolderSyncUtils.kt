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

    //endregion

    /**
     * True when this copy's lifecycle is owned by an offline folder root: the persistent
     * coverage marker is the accepted-basis record. The baseline offline sync must not apply
     * its delete/upload/download effects to such copies first — the root maintenance pass
     * handles remote deletions (keeping bytes), refreshes changes through the staged path and
     * preserves local edits.
     */
    fun isRootCoveredCopy(file: File): Boolean = file.offlineBasisModifiedAt > 0L

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
                // unknown bytes belong to the user — release coverage, keep the bytes.
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
                if (localState == LocalCopyState.LOCALLY_EDITED || localState == LocalCopyState.UNKNOWN) {
                    if (localExists) {
                        SentryLog.w(TAG, "Unmark keeps locally edited/unknown bytes for file ${file.id}")
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

                // A same-size/same-mtime edit still diverges from the accepted digest: verify
                // the accepted identity before the destructive step (unpin is rare; the hash
                // cost is fine). A mismatch is preservation, not authority to delete.
                if (localExists && basis?.digest != null) {
                    val localDigest = withContext(Dispatchers.IO) { computeDigest(offlineFile) }
                    if (localDigest != null && localDigest != basis.digest) {
                        SentryLog.w(TAG, "Unmark keeps bytes diverging from the accepted digest for file ${file.id}")
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
                // transient pending-download marker.
                val remoteDeletedIds = (FileController.getRealmInstance(userDrive).use { realm ->
                    realm.where(File::class.java)
                        .equalTo(File::isOffline.name, true)
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
     * ([recordAcceptedPublication]), never from a listing or an enqueued download. Files that
     * already keep an offline copy ([File.isOffline]) are not re-marked: their bytes are
     * preserved and maintained by the existing offline cadence. The walk NEVER touches
     * [File.isDirectlyPinned]: that flag records independent user pin intent (direct-pin UI,
     * migration), not root coverage — otherwise marking an overlapping root would turn every
     * existing root copy into a permanent pin. Every visited non-folder file is reported
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
                            if (!proxy.isMarkedAsOffline && !proxy.isOffline) {
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
     * downloads.
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
        val offlineFile = snapshot.getOfflineFile(context, userDrive.userId)
        val localExists = offlineFile?.exists() == true
        val localState = classifyLocalCopy(
            basis = basis,
            localExists = localExists,
            localSize = offlineFile?.takeIf { localExists }?.length(),
            localModifiedSec = offlineFile?.takeIf { localExists }?.lastModified()?.div(1_000L),
        )

        when (decideFromListing(basis, localState)) {
            ListingDecision.SKIP_LOCAL_EDIT -> return
            ListingDecision.DOWNLOAD -> {
                stagedRedownload(context, userDrive, snapshot, assumedLocalState = localState)
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
            DetailsDecision.REDOWNLOAD -> stagedRedownload(context, userDrive, remoteFile, assumedLocalState = LocalCopyState.INTACT)
        }
    }

    /**
     * Metadata-only change: adopt new remote metadata while keeping the verified bytes. The
     * local file timestamp follows the adopted metadata FIRST — if it cannot be updated the
     * adoption is skipped, so the preserved basis always describes the actual local bytes
     * (a hash-identical timestamp change must never make a clean copy classify as edited).
     */
    private suspend fun updatePublicationBasis(userDrive: UserDrive, fileId: Int, remoteFile: File, offlineFile: JavaFile?) = withContext(Dispatchers.IO) {
        if (offlineFile?.exists() == true) {
            if (!offlineFile.setLastModified(remoteFile.getLastModifiedInMilliSecond())) {
                SentryLog.w(TAG, "Local timestamp refused for file $fileId; metadata-only adoption skipped")
                return@withContext
            }
        }
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { mutableRealm ->
                FileController.getFileProxyById(fileId, customRealm = mutableRealm)?.let { proxy ->
                    proxy.lastModifiedAt = remoteFile.lastModifiedAt
                    proxy.size = remoteFile.size
                    // The accepted identity advances ONLY here, on a hash-verified adoption,
                    // and is never erased by a hash-less response.
                    remoteFile.serverContentHash?.let { proxy.serverContentHash = it }
                    proxy.offlineBasisModifiedAt = remoteFile.lastModifiedAt
                    proxy.offlineBasisSize = remoteFile.size
                }
            }
        }
    }

    /**
     * Downloads the current remote body to a staging file and atomically replaces the offline
     * copy only after a verified transfer. A body whose digest equals the preserved digest is
     * content-identical: the basis is updated without replacing the bytes. The previous copy
     * and all preserved state survive failure, refusal and cancellation.
     */
    /**
     * Downloads the current remote body to a staging file and PUBLISHES it with one atomic
     * rename over the previous copy (this storage's rename(2) replaces atomically: a crash or
     * a failed rename always leaves either the old or the new complete file — the previous
     * bytes are never deleted separately). Guards immediately before the destructive step, all
     * on IO: cancellation, a local edit that appeared during the transfer (re-classified
     * against the same preserved basis), an existing copy whose bytes no longer match the
     * accepted digest (a same-size/time edit), and a vanished row. Every refusal preserves
     * the previous bytes and state. The response is closed on every return path.
     */
    private suspend fun stagedRedownload(
        context: Context,
        userDrive: UserDrive,
        file: File,
        assumedLocalState: LocalCopyState,
    ): Boolean = withContext(Dispatchers.IO) {
        val okHttpClient = AccountUtils.getHttpClient(userDrive.userId, null)
        val offlineFile = file.getOfflineFile(context, userDrive.userId) ?: return@withContext false
        val parentDir = offlineFile.parentFile ?: return@withContext false
        if (!parentDir.exists()) parentDir.mkdirs() // newly discovered nested files
        if (!parentDir.isDirectory) return@withContext false
        val staging = JavaFile.createTempFile("offline_staging_", null, parentDir)

        try {
            val response = DownloadOfflineFileManager.downloadFileResponseAsync(ApiRoutes.downloadFile(file), okHttpClient)
            val saved = response.use { resp ->
                if (!resp.isSuccessful) return@withContext false
                DownloadOfflineFileManager.saveRemoteData(TAG, resp, staging)
            }
            if (!saved) return@withContext false
            // Exact-byte guard against the server's own length when available
            if (file.size != null && staging.length() != file.size) return@withContext false
            val digest = computeDigest(staging) ?: return@withContext false

            val currentRow = FileController.getRealmInstance(userDrive).use { realm ->
                FileController.getFileProxyById(file.id, customRealm = realm)?.let { realm.copyFromRealm(it, 0) }
            } ?: run {
                SentryLog.w(TAG, "Row vanished during staged download for file ${file.id}; replacement aborted")
                return@withContext false
            }
            val preservedDigest = currentRow.offlineContentDigest

            if (preservedDigest != null && preservedDigest == digest) {
                // Body identical to the preserved publication: metadata-only adoption. The
                // local timestamp must follow the adopted metadata, or a clean copy would
                // classify as locally edited on the next pass; on failure the basis stays
                // consistent with the untouched bytes (a harmless metadata re-check loop).
                if (!offlineFile.setLastModified(file.getLastModifiedInMilliSecond())) return@withContext false
                updatePublicationBasis(userDrive, file.id, file, offlineFile)
                return@withContext true
            }

            // ---- Guards immediately before the destructive publication (all preserve) ----
            currentCoroutineContext().ensureActive()
            val reClassified = classifyLocalCopy(
                basis = PublicationBasis(
                    modifiedAt = currentRow.offlineBasisModifiedAt,
                    size = currentRow.offlineBasisSize,
                    digest = preservedDigest,
                    serverHash = currentRow.serverContentHash,
                ),
                localExists = offlineFile.exists(),
                localSize = offlineFile.takeIf { it.exists() }?.length(),
                localModifiedSec = offlineFile.takeIf { it.exists() }?.lastModified()?.div(1_000L),
            )
            if (reClassified == LocalCopyState.LOCALLY_EDITED || reClassified == LocalCopyState.UNKNOWN) {
                SentryLog.w(TAG, "Local copy changed during staged download for file ${file.id}; replacement aborted")
                return@withContext false
            }
            if (offlineFile.exists() && preservedDigest != null) {
                val existingDigest = computeDigest(offlineFile)
                if (existingDigest != null && existingDigest != preservedDigest) {
                    // Existing bytes diverged from the accepted publication (equal size/mtime
                    // edit): unknown identity is preservation, never authority to overwrite.
                    SentryLog.w(TAG, "Existing copy digest diverged from the accepted basis for file ${file.id}; replacement aborted")
                    return@withContext false
                }
            }

            // One atomic publication step: rename(2) over the existing file. Failure keeps
            // the old bytes (staging is removed in finally).
            if (!staging.renameTo(offlineFile)) {
                SentryLog.w(TAG, "Atomic replace refused for file ${file.id}; previous copy preserved")
                return@withContext false
            }
            offlineFile.setLastModified(file.getLastModifiedInMilliSecond())

            FileController.getRealmInstance(userDrive).use { realm ->
                realm.executeTransaction { mutableRealm ->
                    FileController.getFileProxyById(file.id, customRealm = mutableRealm)?.let { proxy ->
                        proxy.lastModifiedAt = file.lastModifiedAt
                        proxy.size = file.size
                        proxy.serverContentHash = file.serverContentHash
                        proxy.offlineBasisModifiedAt = file.lastModifiedAt
                        proxy.offlineBasisSize = file.size
                        proxy.offlineContentDigest = digest
                        proxy.isOffline = true
                    }
                }
            }
            if (file.isMedia()) MediaUtils.scanFile(context, offlineFile)
            true
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

    /**
     * Narrow publication seam, called from the accepted offline-write path (a copy was
     * successfully written, moved from an intact cache, or confirmed intact): records the
     * preserved basis from the ACCEPTED bytes — the remote metadata of the details response
     * that drove the download (including its with=hash identity) plus the sha-256 digest of
     * the bytes on disk. Never called from a listing or a merely enqueued download. Digest
     * failure records nothing: an unrecorded basis classifies the copy as UNKNOWN, which
     * maintenance leaves untouched.
     */
    suspend fun recordAcceptedPublication(
        userDrive: UserDrive,
        remoteFile: File,
        offlineFile: JavaFile,
        acceptedServerHash: String?,
    ) = withContext(Dispatchers.IO) {
        val digest = computeDigest(offlineFile)
        if (digest == null) {
            SentryLog.w(TAG, "Accepted offline bytes could not be digested for file ${remoteFile.id}; basis not recorded")
            return@withContext
        }
        FileController.getRealmInstance(userDrive).use { realm ->
            realm.executeTransaction { mutableRealm ->
                FileController.getFileProxyById(remoteFile.id, customRealm = mutableRealm)?.let { proxy ->
                    proxy.lastModifiedAt = remoteFile.lastModifiedAt
                    proxy.size = remoteFile.size
                    // Only the identity of the accepted body is recorded here — captured from
                    // the raw with=hash response that drove this transfer, never from a later
                    // listing/details refresh.
                    proxy.serverContentHash = acceptedServerHash
                    proxy.offlineBasisModifiedAt = remoteFile.lastModifiedAt
                    proxy.offlineBasisSize = remoteFile.size
                    proxy.offlineContentDigest = digest
                    proxy.isOffline = true
                }
            }
        }
    }

    //endregion
}
