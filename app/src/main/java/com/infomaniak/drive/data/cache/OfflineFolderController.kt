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
package com.infomaniak.drive.data.cache

import android.content.Context
import android.net.Uri
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.OfflineRoot
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.IOFile
import com.infomaniak.drive.utils.MediaUtils.deleteInMediaScan
import com.infomaniak.drive.utils.MediaUtils.isMedia
import com.infomaniak.drive.utils.OfflineFileCoordination
import com.infomaniak.drive.utils.OfflineFolderPolicy
import com.infomaniak.drive.utils.SyncUtils.syncImmediately
import com.infomaniak.drive.utils.Utils.ROOT_ID
import io.realm.Realm
import java.util.Date

/**
 * Realm data access for the durable offline folder roots (issue #1902).
 *
 * A root is a folder the user explicitly marked as available offline. Coverage of the descendants
 * is materialized on [File.isInOfflineFolder] (as opposed to the explicit [File.isOffline] pin),
 * so that removing a root can never strip a separately pinned file or a file still covered by
 * another overlapping root.
 */
object OfflineFolderController {

    data class RemovalResult(
        var rootExisted: Boolean = false,
        var removedOfflineFilesCount: Int = 0,
        var keptOfflineFilesCount: Int = 0,
    )

    fun addOfflineRoot(
        folder: File,
        userDrive: UserDrive = UserDrive(),
        customRealm: Realm? = null,
    ): Boolean {
        val block: (Realm) -> Boolean = { realm ->
            val existingRoot = getOfflineRoot(folder.id, customRealm = realm)
            if (existingRoot != null && !existingRoot.isPendingRemoval) {
                false
            } else if (existingRoot != null) {
                // Re-marking a root whose deferred cleanup is still running revives it.
                realm.executeTransaction { if (existingRoot.isValid) existingRoot.isPendingRemoval = false }
                updateRootFolderFlags(folder.id, realm, isInOfflineFolder = true, isMarkedAsOffline = true)
                true
            } else {
                realm.executeTransaction {
                    val root = OfflineRoot(
                        folderId = folder.id,
                        driveId = userDrive.driveId,
                        userId = userDrive.userId,
                        name = folder.name,
                        path = folder.path,
                        createdAt = Date(),
                    ).apply { initUid() }
                    it.copyToRealm(root)
                }
                // Mark the root folder itself as covered and its walk as pending until the
                // BulkDownloadWorker has walked it once.
                updateRootFolderFlags(folder.id, realm, isInOfflineFolder = true, isMarkedAsOffline = true)
                true
            }
        }
        return customRealm?.let(block) ?: FileController.getRealmInstance(userDrive).use(block)
    }

    fun removeOfflineRoot(
        folderId: Int,
        userDrive: UserDrive = UserDrive(),
        customRealm: Realm? = null,
    ): RemovalResult {
        val context = Realm.getApplicationContext() ?: return RemovalResult()
        val block: (Realm) -> RemovalResult = { realm ->
            val root = getOfflineRoot(folderId, customRealm = realm)
            if (root == null) {
                RemovalResult()
            } else {
                // If the folder is still covered by an ancestor root, everything below stays
                // covered: only the root record goes away.
                val isCoveredByAncestorRoot = isInsideActiveRoot(realm, folderId)

                if (isCoveredByAncestorRoot) {
                    realm.executeTransaction { if (root.isValid) root.deleteFromRealm() }
                    RemovalResult(rootExisted = true)
                } else {
                    // Persist the deferred-cleanup intent BEFORE any cleanup runs: the record is
                    // the durable owner and must exist while protected copies remain (issue:
                    // round 3 #8).
                    realm.executeTransaction { if (root.isValid) root.isPendingRemoval = true }

                    val result = clearCoverageRecursively(context, realm, folderId, userDrive) {
                        // Abort as soon as the record is gone (completed elsewhere) or revived.
                        realm.where(OfflineRoot::class.java)
                            .equalTo(OfflineRoot::folderId.name, folderId)
                            .findFirst()
                            ?.takeIf { it.isValid && it.isPendingRemoval } == null
                    }
                    result.rootExisted = true

                    if (hasProtectedFilesRecursively(folderId, context, userDrive, realm)) {
                        // Protected copies remain (pending uploads or unsynchronized edits): the
                        // record stays as the durable owner until the offline sync finishes it.
                        Unit
                    } else {
                        // Nothing protected remains: delete the still-pending generation (a
                        // concurrent re-mark revives the root and cancels this removal).
                        realm.executeTransaction {
                            realm.where(OfflineRoot::class.java)
                                .equalTo(OfflineRoot::folderId.name, folderId)
                                .findFirst()
                                ?.takeIf { it.isValid && it.isPendingRemoval }
                                ?.deleteFromRealm()
                        }
                    }
                    result
                }
            }
        }
        return customRealm?.let(block) ?: FileController.getRealmInstance(userDrive).use(block)
    }

    /**
     * Whether a root record exists and is active (not pending removal). Only active roots
     * establish coverage and validate downloads (issue #1902).
     *
     * The whole evaluation happens inside the Realm session: a managed root must never be read
     * after its Realm closed (issue: round 3 #1).
     */
    fun hasActiveOfflineRoot(folderId: Int, userDrive: UserDrive = UserDrive(), customRealm: Realm? = null): Boolean {
        val block: (Realm) -> Boolean = { realm ->
            realm.where(OfflineRoot::class.java)
                .equalTo(OfflineRoot::folderId.name, folderId)
                .findFirst()
                ?.takeIf { it.isValid }?.let { !it.isPendingRemoval } == true
        }
        return customRealm?.let(block) ?: FileController.getRealmInstance(userDrive).use(block)
    }

    /**
     * Whether [fileId] is itself an active offline root or lies under one. Unlike
     * [isInsideRootRecord], this counts the item's own root row, so top-level root rows are
     * never classified as orphans by the drive-wide sweep (issue: round 3 #10).
     */
    fun isCoveredByRootIncludingSelf(fileId: Int, userDrive: UserDrive = UserDrive(), customRealm: Realm? = null): Boolean {
        val block: (Realm) -> Boolean = { realm ->
            realm.where(OfflineRoot::class.java)
                .equalTo(OfflineRoot::folderId.name, fileId)
                .findFirst()
                ?.takeIf { it.isValid && !it.isPendingRemoval } != null
                    || isInsideRootRecord(realm, fileId)
        }
        return customRealm?.let(block) ?: FileController.getRealmInstance(userDrive).use(block)
    }

    fun getOfflineRoot(folderId: Int, userDrive: UserDrive = UserDrive(), customRealm: Realm? = null): OfflineRoot? {
        val block: (Realm) -> OfflineRoot? = { realm ->
            realm.where(OfflineRoot::class.java)
                .equalTo(OfflineRoot::folderId.name, folderId)
                .findFirst()
        }
        return customRealm?.let(block) ?: FileController.getRealmInstance(userDrive).use(block)
    }

    fun getOfflineRoots(userDrive: UserDrive = UserDrive(), customRealm: Realm? = null): List<OfflineRoot> {
        val block: (Realm) -> List<OfflineRoot> = { realm ->
            realm.where(OfflineRoot::class.java).findAll().map { realm.copyFromRealm(it, 0) }
        }
        return customRealm?.let(block) ?: FileController.getRealmInstance(userDrive).use(block)
    }

    fun isOfflineRoot(folderId: Int, userDrive: UserDrive = UserDrive(), customRealm: Realm? = null): Boolean {
        return getOfflineRoot(folderId, userDrive, customRealm) != null
    }

    fun updateLastSyncedAt(folderId: Int, userDrive: UserDrive = UserDrive(), customRealm: Realm? = null) {
        val block: (Realm) -> Unit = { realm ->
            getOfflineRoot(folderId, customRealm = realm)?.let { root ->
                realm.executeTransaction { if (root.isValid) root.lastSyncedAt = Date() }
            }
        }
        customRealm?.let(block) ?: FileController.getRealmInstance(userDrive).use(block)
    }

    /**
     * Whether any file or folder below [rootFolderId] (inclusive) is still waiting to be
     * downloaded by a walk. Used by the offline sync to resume interrupted walks.
     *
     * Traverses the durable parentId relationships rather than the folder children lists, which
     * a partially completed walk may have replaced.
     */
    fun hasPendingFilesRecursively(rootFolderId: Int, customRealm: Realm? = null): Boolean {
        val block: (Realm) -> Boolean = { realm ->
            val rootProxy = FileController.getFileById(realm, rootFolderId)
            var hasPending = rootProxy?.isMarkedAsOffline == true
            val visited = mutableSetOf(rootFolderId)
            val queue = ArrayDeque(listOf(rootFolderId))

            while (queue.isNotEmpty() && !hasPending) {
                val folderId = queue.removeFirst()
                val children = realm.where(File::class.java)
                    .equalTo(File::parentId.name, folderId)
                    .findAll()
                for (child in children) {
                    if (child.isMarkedAsOffline) {
                        hasPending = true
                        break
                    }
                    if (child.isFolder() && visited.add(child.id)) queue.add(child.id)
                }
            }
            hasPending
        }
        return customRealm?.let(block) ?: FileController.getRealmInstance().use(block)
    }

    /**
     * Whether [folderId] has an ancestor — other than itself — which is an ACTIVE offline root
     * (not pending removal). This is the AUTHORIZATION predicate: only active roots establish
     * coverage for publications, completions and unpin materialization (issue: round 4 #10).
     * Works for files too: it walks the durable parentId chain.
     */
    fun isInsideActiveRoot(realm: Realm, folderId: Int): Boolean {
        return walkAncestryForRoot(realm, folderId) { it.isValid && !it.isPendingRemoval }
    }

    /**
     * Whether [folderId] has an ancestor — other than itself — with ANY offline root record,
     * including records pending removal. This is the PROTECTION predicate: a pending-removal
     * record still owns its subtree's deferred cleanup, so the drive-wide sweep must not clear
     * its rows (issue: round 4 #10).
     */
    fun isInsideRootRecord(realm: Realm, folderId: Int): Boolean {
        return walkAncestryForRoot(realm, folderId) { it.isValid }
    }

    private fun walkAncestryForRoot(realm: Realm, folderId: Int, isMatchingRoot: (OfflineRoot) -> Boolean): Boolean {
        val visited = mutableSetOf(folderId)
        var currentId: Int? = folderId
        while (currentId != null && currentId != ROOT_ID) {
            val folder = FileController.getFileById(realm, currentId) ?: return false
            if (folder.id != folderId) {
                getOfflineRoot(folder.id, customRealm = realm)?.let { root ->
                    if (isMatchingRoot(root)) return true
                }
            }
            currentId = folder.parentId.takeIf { visited.add(it) }
        }
        return false
    }

    /**
     * Whether a file (or folder) is still covered by any offline root, i.e. its inherited state
     * and offline copy must be preserved even when its explicit pin is removed.
     */
    fun isFileCoveredByAnyRoot(fileId: Int, userDrive: UserDrive = UserDrive(), customRealm: Realm? = null): Boolean {
        val block: (Realm) -> Boolean = { realm -> isInsideRootRecord(realm, fileId) }
        return customRealm?.let(block) ?: FileController.getRealmInstance(userDrive).use(block)
    }

    /**
     * Whether any non-pinned descendant of [rootFolderId] still holds a protected offline copy
     * (pending upload or unsynchronized edit): while that is true, the root record must stay as
     * the durable owner of the deferred cleanup (issue #1902).
     */
    fun hasProtectedFilesRecursively(
        rootFolderId: Int,
        context: Context,
        userDrive: UserDrive,
        customRealm: Realm,
    ): Boolean {
        val visited = mutableSetOf(rootFolderId)
        val queue = ArrayDeque(listOf(rootFolderId))

        while (queue.isNotEmpty()) {
            val folderId = queue.removeFirst()
            val children = customRealm.where(File::class.java)
                .equalTo(File::parentId.name, folderId)
                .findAll()
            for (child in children.createSnapshot()) {
                if (child.isFolder()) {
                    if (visited.add(child.id)) queue.add(child.id)
                    continue
                }
                if (child.isOffline) continue
                val offlineFile = child.getOfflineFile(context, userDrive.userId) ?: continue
                if (!offlineFile.exists()) continue
                when (protectionStateOf(child, offlineFile, userDrive)) {
                    CopyProtection.PENDING_UPLOAD, CopyProtection.DIRTY -> return true
                    CopyProtection.UPLOADED, CopyProtection.NONE -> Unit
                }
            }
        }
        return false
    }

    /**
     * Whether any descendant of [folderId] (durable parentId traversal, directories excluded)
     * still holds offline state (issue: round 5 #10): an explicit pin, a pending download
     * marker, or inherited coverage. While that is true the folder row is REQUIRED ancestry —
     * deleting it would strand the descendants' metadata and break every traversal (deferred
     * cleanup, orphan sweep, offline path resolution) that walks parentId chains.
     */
    fun hasLocallyOwnedDescendant(folderId: Int, realm: Realm): Boolean {
        val visited = mutableSetOf(folderId)
        val queue = ArrayDeque(listOf(folderId))
        while (queue.isNotEmpty()) {
            val currentId = queue.removeFirst()
            val children = realm.where(File::class.java)
                .equalTo(File::parentId.name, currentId)
                .findAll()
                .createSnapshot()
            for (child in children) {
                if (child.isFolder()) {
                    if (visited.add(child.id)) queue.add(child.id)
                    continue
                }
                if (child.isOffline || child.isMarkedAsOffline || child.isInOfflineFolder) return true
            }
        }
        return false
    }

    /**
     * Revisit of roots whose removal was deferred because protected copies remained: re-run the
     * coverage clearing, then delete the record once nothing protected is left. Called by the
     * offline sync so the deferred intent always has a durable owner (issue #1902).
     */
    fun processPendingRemovalRoots(context: Context, userDrive: UserDrive, customRealm: Realm) {
        getOfflineRoots(customRealm = customRealm).filter { it.isPendingRemoval }.forEach { root ->
            // Re-validate against the live record: the root may have been revived since the
            // detached list was built (issue: round 4 #11).
            val stillPending = customRealm.where(OfflineRoot::class.java)
                .equalTo(OfflineRoot::folderId.name, root.folderId)
                .findFirst()
                ?.takeIf { it.isValid && it.isPendingRemoval } != null
            if (!stillPending) return@forEach

            clearCoverageRecursively(context, customRealm, root.folderId, userDrive) {
                customRealm.where(OfflineRoot::class.java)
                    .equalTo(OfflineRoot::folderId.name, root.folderId)
                    .findFirst()
                    ?.takeIf { it.isValid && it.isPendingRemoval } == null
            }

            // Delete only the still-pending generation, and only once nothing protected remains
            // (issue: round 5 #11): pending uploads or snapshot failures retain bytes and flags,
            // and the pending record is the durable owner that schedules their eventual cleanup.
            // A revived root keeps its record through the same live re-check.
            if (hasProtectedFilesRecursively(root.folderId, context, userDrive, customRealm)) return@forEach

            customRealm.executeTransaction {
                customRealm.where(OfflineRoot::class.java)
                    .equalTo(OfflineRoot::folderId.name, root.folderId)
                    .findFirst()
                    ?.takeIf { it.isValid && it.isPendingRemoval }
                    ?.deleteFromRealm()
            }
        }
    }

    /**
     * Resolves the row that owns the existing bytes at [target], by derived offline path, among
     * candidates sharing the incoming file's parent and name (the incoming file's own row
     * included). Returns null when no row claims that path: the bytes' ownership is unknown and
     * must be preserved, not replaced (issue: round 6 #3). The returned row is a detached
     * snapshot and stays readable after the Realm session closes.
     */
    internal fun destinationOwnerRow(
        realm: Realm,
        context: Context,
        userDrive: UserDrive,
        incoming: File,
        target: IOFile,
    ): File? {
        val candidates = realm.where(File::class.java)
            .equalTo(File::parentId.name, incoming.parentId)
            .equalTo(File::name.name, incoming.name)
            .findAll()
            .createSnapshot()
        for (candidate in candidates) {
            val candidatePath = candidate.getOfflineFile(context, userDrive.userId)?.path ?: continue
            if (candidatePath == target.path) return candidate
        }
        return null
    }

    /**
     * Reconciles an inherited or pinned copy at its OLD location with the NEW remote identity
     * after a rename/move, BEFORE the new metadata is committed (issue: round 4 #9):
     *  - a copy backing a pending upload keeps its bytes (the upload reads that exact path);
     *  - an edited copy is snapshotted into the durable upload queue under the NEW identity, and
     *    the old bytes are deleted ONLY after that durable registration succeeded — on failure
     *    the caller must keep the old location (issue: round 4 #2);
     *  - an already-uploaded revision's stale local duplicate is deleted (content is on server);
     *  - a clean copy is migrated by ATOMIC rename, never over an EXISTING destination: any
     *    existing target is ambiguously owned (its owning file and baseline revision are not
     *    establishable here) and keeps its bytes, and both sources are retained by deferring
     *    the commit — issues: round 4 #5, round 5 #3.
     *
     * Returns true when the old location is fully handled and the caller may commit the new
     * metadata; false means preservation is incomplete and committing would strand the old copy.
     *
     * [oldIdentityName]/[oldIdentityParentId]/[oldLastModifiedAt] describe the OLD row (the only
     * reliable identity for the old bytes); [newIdentity] is the fresh remote File. Directory
     * rows are not file content and must not be passed here (issue: round 4 #12).
     */
    fun reconcileMovedInheritedCopy(
        context: Context,
        oldOfflineFile: IOFile,
        oldIdentityName: String,
        oldIdentityParentId: Int,
        oldLastModifiedAt: Long,
        newIdentity: File,
        userDrive: UserDrive,
        newOfflineTarget: IOFile? = null,
    ): Boolean {
        // Copy ownership and mutation coordination (issue: round 4 #4): move reconciliation
        // competes with publication, cleanup and upload registration for the same bytes.
        synchronized(OfflineFileCoordination.lockFor(userDrive, newIdentity.id)) {
            val fileModifiedAt = Date(oldOfflineFile.lastModified())
            return when {
                // A pending upload owns the bytes at the old path until it completes (issue:
                // round 5 #9): committing the new metadata now would leave the upload reading a
                // location the walk no longer tracks. The same applies to an UNSENT record
                // retained after failure/cancellation (issue: round 6 #1). Deferring keeps the
                // old row and location authoritative; the caller retries reconciliation after
                // the upload finished.
                UploadFile.hasPendingUpload(uri = Uri.fromFile(oldOfflineFile).toString()) ||
                    UploadFile.hasRetainedUnsentUpload(uri = Uri.fromFile(oldOfflineFile).toString()) ||
                    UploadFile.hasUploadRecordFor(
                        fileName = oldIdentityName,
                        remoteFolder = oldIdentityParentId,
                        driveId = userDrive.driveId,
                        userId = userDrive.userId,
                        fileModifiedAt = fileModifiedAt,
                        pendingOnly = true,
                    ) -> false
                UploadFile.hasUploadRecordFor(
                    fileName = oldIdentityName,
                    remoteFolder = oldIdentityParentId,
                    driveId = userDrive.driveId,
                    userId = userDrive.userId,
                    fileModifiedAt = fileModifiedAt,
                    pendingOnly = false,
                ) -> {
                    runCatching { oldOfflineFile.delete() }.isSuccess
                }
                OfflineFolderPolicy.isLocallyModified(
                    offlineFileLastModifiedMillis = oldOfflineFile.lastModified(),
                    remoteLastModifiedAtSeconds = oldLastModifiedAt,
                ) -> {
                    val registered = registerDirtySnapshot(context, newIdentity, oldOfflineFile, userDrive)
                    if (registered) runCatching { oldOfflineFile.delete() }.isSuccess else false
                }
                else -> {
                    // An explicit target is used by callers whose new location cannot be
                    // derived from [newIdentity]'s not-yet-committed metadata (UI move/rename
                    // flows): it must be the exact path the row will resolve to after commit.
                    val target = newOfflineTarget ?: newIdentity.getOfflineFile(context, userDrive.userId)
                    if (target == null) {
                        false
                    } else {
                        // The destination is preserved whenever it exists (issue: round 5 #3):
                        // from here its owning file and baseline revision cannot be established
                        // — the row→path mapping is derived from metadata this caller has not
                        // committed yet — so an existing target is ambiguously owned and must
                        // never be replaced. The previous baseline compared its timestamp
                        // against the INCOMING file's revision, which declared another file's
                        // older dirty copy unprotected and overwrote its sole edit. The
                        // destination's own owner resolves the path independently (its own
                        // reconciliation, the stale cleanup, or its upload completion), and a
                        // later walk retries this move once the path is free.
                        if (target.exists()) {
                            false
                        } else {
                            runCatching {
                                target.parentFile?.mkdirs()
                                oldOfflineFile.renameTo(target)
                            }.getOrDefault(false)
                        }
                    }
                }
            }
        }
    }

    /**
     * Shared upsert boundary (issue: round 5 #5): preserves owned copies at their OLD location
     * before ANY metadata replacement — ordinary listings and activity refreshes included, not
     * only the folder walk. A file whose reconciliation is incomplete keeps its OLD metadata:
     * the OLD row is returned in its place so the caller commits a coherent state, and no
     * edited bytes are stranded at an untracked path.
     *
     * MUST run OUTSIDE a Realm write transaction: reconciliation takes the shared per-file
     * coordination and performs file I/O. Without an application context the input is returned
     * unchanged (no preservation possible — same as the pre-feature behavior).
     */
    fun reconcileOwnedCopiesBeforeCommit(
        realm: Realm,
        remoteFiles: List<File>,
        userDrive: UserDrive,
    ): List<File> {
        val context = Realm.getApplicationContext() ?: return remoteFiles
        if (remoteFiles.isEmpty()) return remoteFiles
        return remoteFiles.map { remoteFile ->
            val oldRow = FileController.getFileById(realm, remoteFile.id) ?: return@map remoteFile
            if (oldRow.isFolder()) return@map remoteFile
            val oldOffline = oldRow.getOfflineFile(context, userDrive.userId)?.takeIf { it.exists() }
                ?: return@map remoteFile
            // Location changes are detected on the RESOLVED offline paths (issue: round 5 #7).
            val newOffline = remoteFile.getOfflineFile(context, userDrive.userId)
            val locationChanged = newOffline == null || newOffline.path != oldOffline.path
            if (!locationChanged) return@map remoteFile
            if (reconcileMovedInheritedCopy(
                    context = context,
                    oldOfflineFile = oldOffline,
                    oldIdentityName = oldRow.name,
                    oldIdentityParentId = oldRow.parentId,
                    oldLastModifiedAt = oldRow.lastModifiedAt,
                    newIdentity = remoteFile,
                    userDrive = userDrive,
                )
            ) {
                remoteFile
            } else {
                // Preservation incomplete (pending upload, snapshot failed, protected
                // destination): keep the OLD row authoritative for this commit.
                oldRow
            }
        }
    }

    /** Outcome of the removal preservation hook (issue: round 4 #6). */
    enum class PreservationOutcome {
        /** Content is durably safe (uploaded, snapshotted, or nothing to preserve): bytes and row can go. */
        CONTENT_SAFE,
        /** An unsent revision is only local: bytes AND row/metadata must be retained. */
        PRESERVED,
    }

    /**
     * Preservation hook for row-removal paths such as remote trash/delete propagation (issues:
     * round 3 #6 and round 4 #6): a copy backing a pending upload is PRESERVED together with its
     * row and metadata (deleting the row would make the unsent edit undiscoverable); an edited
     * copy not yet registered is snapshotted into the durable upload queue first — and if that
     * registration fails, the row is retained too. Only a content-safe copy allows deletion.
     * Directories are not file-content: their children are preserved individually by the
     * recursive removal (issue: round 4 #12).
     */
    fun preserveInheritedCopyBeforeRemoval(file: File, userDrive: UserDrive): PreservationOutcome {
        val context = Realm.getApplicationContext() ?: return PreservationOutcome.PRESERVED
        if (file.isOffline || file.isFolder() || !file.isInOfflineFolder) return PreservationOutcome.CONTENT_SAFE
        val offlineFile = file.getOfflineFile(context, userDrive.userId) ?: return PreservationOutcome.CONTENT_SAFE
        if (!offlineFile.exists()) return PreservationOutcome.CONTENT_SAFE
        return when (protectionStateOf(file, offlineFile, userDrive)) {
            CopyProtection.PENDING_UPLOAD -> PreservationOutcome.PRESERVED
            CopyProtection.DIRTY ->
                if (registerDirtySnapshot(context, file, offlineFile, userDrive)) {
                    PreservationOutcome.CONTENT_SAFE
                } else {
                    // Snapshot registration failed: keep bytes AND identity/metadata (round 4 #6).
                    PreservationOutcome.PRESERVED
                }
            CopyProtection.UPLOADED, CopyProtection.NONE -> PreservationOutcome.CONTENT_SAFE
        }
    }

    private fun updateRootFolderFlags(folderId: Int, realm: Realm, isInOfflineFolder: Boolean, isMarkedAsOffline: Boolean) {
        FileController.updateFile(folderId, realm) { file ->
            file.isInOfflineFolder = isInOfflineFolder
            file.isMarkedAsOffline = isMarkedAsOffline
        }
    }

    /**
     * Breadth-first clearing of the inherited coverage below [rootFolderId] (inclusive).
     *
     * Callers must have checked that [rootFolderId] is not itself covered by an ancestor root.
     * Explicit pins ([File.isOffline]) are left untouched, subtrees covered by another remaining
     * root are skipped, and unsynchronized edits are snapshotted into the upload queue so that
     * removing a folder can never lose an offline edit. Traversal uses the durable parentId
     * relationships, and only files inside the app offline storage are ever deleted.
     */
    private fun clearCoverageRecursively(
        context: Context,
        realm: Realm,
        rootFolderId: Int,
        userDrive: UserDrive,
        isAborted: () -> Boolean = { false },
    ): RemovalResult {
        val result = RemovalResult()
        val visited = mutableSetOf(rootFolderId)
        val skippedSubtreeIds = mutableSetOf<Int>()
        val queue = ArrayDeque(listOf(rootFolderId))

        while (queue.isNotEmpty()) {
            val folderId = queue.removeFirst()
            if (folderId in skippedSubtreeIds) continue
            if (isAborted()) {
                // The root was revived or its cleanup completed elsewhere mid-traversal: stop
                // mutating on its behalf (issue: round 4 #11).
                SentryLog.i(TAG, "Deferred cleanup of root $rootFolderId aborted mid-traversal")
                return result
            }

            // Subtree covered by another remaining root: skip it entirely. Checked BEFORE the
            // row lookup so a missing folder row cannot bypass the protection.
            if (folderId != rootFolderId && getOfflineRoot(folderId, customRealm = realm) != null) {
                skippedSubtreeIds.add(folderId)
                continue
            }

            val folder = FileController.getFileById(realm, folderId)
            // A missing folder row no longer drops the whole subtree (issue: round 5 #10): the
            // children are enumerated through the durable parentId relationships below, so their
            // coverage cleanup and preserved-copy handling still run.
            if (folder != null && folder.isUsable()) {
                clearFileCoverage(
                    context = context,
                    realm = realm,
                    file = folder,
                    userDrive = userDrive,
                    result = result,
                    isFolder = true,
                    isAborted = isAborted,
                )
            }

            realm.where(File::class.java)
                .equalTo(File::parentId.name, folderId)
                .findAll()
                .createSnapshot()
                .forEach { child ->
                    if (child.isFolder()) {
                        if (visited.add(child.id)) queue.add(child.id)
                    } else {
                        clearFileCoverage(
                            context = context,
                            realm = realm,
                            file = child,
                            userDrive = userDrive,
                            result = result,
                            isFolder = false,
                            isAborted = isAborted,
                        )
                    }
                }
        }
        return result
    }

    /**
     * Single decision point for dropping a file's (or folder row's) inherited coverage, shared by
     * the root removal, the walk's stale cleanup and the per-file sync. Decides flags AND bytes
     * together:
     *  - explicit pins are never touched;
     *  - copies backing a pending upload (by uri or by content revision) keep their bytes and
     *    their flags, so the per-file offline sync keeps discovering and uploading the edit (the
     *    file then behaves like a pin until the user removes it individually);
     *  - a locally modified copy with no registered upload is first snapshotted into a unique,
     *    durable per-revision upload file, then coverage is cleared; the regular worker uploads
     *    the snapshot and the owned snapshot file is removed on completion;
     *  - a revision that was already uploaded is content-safe: the stale local duplicate is
     *    cleared;
     *  - plain covered copies are cleared and deleted (offline cache only).
     */
    fun clearFileCoverage(
        context: Context,
        realm: Realm,
        file: File,
        userDrive: UserDrive,
        result: RemovalResult,
        isFolder: Boolean,
        isAborted: () -> Boolean = { false },
    ) {
        if (file.isOffline) return

        if (isFolder) {
            if (!isAborted()) clearInheritedFlags(realm, file)
            return
        }

        val offlineFile = file.getOfflineFile(context, userDrive.userId) ?: return
        if (!offlineFile.exists()) {
            clearInheritedFlags(realm, file)
            return
        }

        // Copy ownership and mutation coordination (issue: round 4 #4): the protection check and
        // the flag/byte writes are one coordinated section.
        synchronized(OfflineFileCoordination.lockFor(userDrive, file.id)) {
            // Re-validate the removal intent INSIDE the coordinated section (issue: round 5 #12):
            // the owning root may have been re-enabled while this file waited for the lock, and
            // the once-per-folder abort check cannot protect the siblings queued behind it.
            if (isAborted()) return@synchronized
            when (protectionStateOf(file, offlineFile, userDrive)) {
                CopyProtection.PENDING_UPLOAD -> result.keptOfflineFilesCount++
                CopyProtection.UPLOADED -> {
                    // The revision is on the server: the local copy is a stale duplicate.
                    clearInheritedFlags(realm, file)
                    deleteOfflineCopy(context, file, offlineFile, userDrive, result)
                }
                CopyProtection.DIRTY -> {
                    val uploadRegistered = registerDirtySnapshot(context, file, offlineFile, userDrive)
                    if (!uploadRegistered) {
                        // Keep the bytes and the flags rather than losing the edit; a later attempt
                        // (next sync) retries the snapshot.
                        result.keptOfflineFilesCount++
                        return@synchronized
                    }
                    clearInheritedFlags(realm, file)
                    deleteOfflineCopy(context, file, offlineFile, userDrive, result)
                }
                CopyProtection.NONE -> {
                    clearInheritedFlags(realm, file)
                    deleteOfflineCopy(context, file, offlineFile, userDrive, result)
                }
            }
        }
    }

    internal enum class CopyProtection { NONE, PENDING_UPLOAD, UPLOADED, DIRTY }

    /**
     * Preservation state of an existing offline copy, shared with the deferred-cleanup
     * completion check (issue #1902): [PENDING_UPLOAD] and [DIRTY] copies must survive cleanup,
     * [UPLOADED] copies are content-safe duplicates, [NONE] copies are clearable.
     */
    internal fun protectionStateOf(file: File, offlineFile: IOFile, userDrive: UserDrive): CopyProtection {
        val fileModifiedAt = Date(offlineFile.lastModified())
        return when {
            // A pending upload owns the bytes; so does an UNSENT record retained after a
            // failure or cancellation (soft-deleted): it is still the only copy of the edit,
            // regardless of the row's advanced clean baseline (issue: round 6 #1).
            UploadFile.hasPendingUpload(uri = Uri.fromFile(offlineFile).toString()) ||
                UploadFile.hasRetainedUnsentUpload(uri = Uri.fromFile(offlineFile).toString()) -> CopyProtection.PENDING_UPLOAD
            UploadFile.hasUploadRecordFor(
                fileName = file.name,
                remoteFolder = file.parentId,
                driveId = userDrive.driveId,
                userId = userDrive.userId,
                fileModifiedAt = fileModifiedAt,
                pendingOnly = true,
            ) -> CopyProtection.PENDING_UPLOAD
            UploadFile.hasUploadRecordFor(
                fileName = file.name,
                remoteFolder = file.parentId,
                driveId = userDrive.driveId,
                userId = userDrive.userId,
                fileModifiedAt = fileModifiedAt,
                pendingOnly = false,
            ) -> CopyProtection.UPLOADED
            OfflineFolderPolicy.isLocallyModified(
                offlineFileLastModifiedMillis = offlineFile.lastModified(),
                remoteLastModifiedAtSeconds = file.lastModifiedAt,
            ) -> CopyProtection.DIRTY
            else -> CopyProtection.NONE
        }
    }

    internal fun registerDirtySnapshot(context: Context, file: File, offlineFile: IOFile, userDrive: UserDrive): Boolean {
        // Upload-source registration competes with publication and cleanup for these bytes
        // (issue: round 4 #4).
        return synchronized(OfflineFileCoordination.lockFor(userDrive, file.id)) {
            runCatching {
                val fileModifiedAt = Date(offlineFile.lastModified())
                val snapshotUri = UploadFile.createDurableOfflineSnapshot(
                    context = context,
                    source = offlineFile,
                    fileModifiedAt = fileModifiedAt,
                    fileId = file.id,
                    driveId = userDrive.driveId,
                ) ?: return@synchronized false
                UploadFile(
                    uri = snapshotUri.toString(),
                    driveId = userDrive.driveId,
                    fileModifiedAt = fileModifiedAt,
                    fileName = file.name,
                    fileSize = offlineFile.length(),
                    remoteFolder = file.parentId,
                    type = UploadFile.Type.SYNC_OFFLINE.name,
                    userId = userDrive.userId,
                ).store()
                context.syncImmediately(isAutomaticTrigger = false)
                true
            }.onFailure { exception ->
                SentryLog.e(TAG, "Failed to snapshot a locally modified offline file", exception)
            }.getOrDefault(false)
        }
    }

    private fun deleteOfflineCopy(
        context: Context,
        file: File,
        offlineFile: IOFile,
        userDrive: UserDrive,
        result: RemovalResult,
    ) {
        runCatching { offlineFile.delete() }.onFailure { exception ->
            SentryLog.e(TAG, "Failed to delete offline file ${offlineFile.path}", exception)
        }.onSuccess {
            if (file.isMedia()) file.deleteInMediaScan(context, userDrive)
            result.removedOfflineFilesCount++
        }
    }

    private fun clearInheritedFlags(realm: Realm, file: File) {
        realm.executeTransaction {
            if (file.isValid) {
                file.isInOfflineFolder = false
                file.isMarkedAsOffline = false
            }
        }
    }

    private val TAG = OfflineFolderController::class.java.simpleName
}
