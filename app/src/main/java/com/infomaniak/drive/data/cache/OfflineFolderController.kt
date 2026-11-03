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
                val isCoveredByAncestorRoot = isInsideAnotherRoot(realm, folderId)

                realm.executeTransaction { if (root.isValid) root.deleteFromRealm() }

                if (isCoveredByAncestorRoot) {
                    RemovalResult(rootExisted = true)
                } else {
                    clearCoverageRecursively(context, realm, folderId, userDrive)
                        .also { result ->
                            result.rootExisted = true
                            if (hasProtectedFilesRecursively(folderId, context, userDrive, realm)) {
                                // Protected copies remain (pending uploads or unsynchronized
                                // edits): the record stays as the durable owner of the deferred
                                // cleanup until the offline sync finishes it (issue #1902).
                                realm.executeTransaction {
                                    getOfflineRoot(folderId, customRealm = realm)?.let { pendingRoot ->
                                        if (pendingRoot.isValid) pendingRoot.isPendingRemoval = true
                                    }
                                }
                            } else {
                                // Nothing protected remains: the removal completes now.
                                realm.executeTransaction {
                                    getOfflineRoot(folderId, customRealm = realm)?.let { finishedRoot ->
                                        if (finishedRoot.isValid) finishedRoot.deleteFromRealm()
                                    }
                                }
                            }
                        }
                }
            }
        }
        return customRealm?.let(block) ?: FileController.getRealmInstance(userDrive).use(block)
    }

    /**
     * Whether a root record exists and is active (not pending removal). Only active roots
     * establish coverage and validate downloads (issue #1902).
     */
    fun hasActiveOfflineRoot(folderId: Int, userDrive: UserDrive = UserDrive(), customRealm: Realm? = null): Boolean {
        val root = getOfflineRoot(folderId, userDrive, customRealm) ?: return false
        return !root.isPendingRemoval
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
     * Whether [folderId] has an ancestor — other than itself — which is itself an
     * offline root, i.e. whether the folder stays covered after removing one of its own roots.
     * Works for files too: it walks the durable parentId chain.
     */
    fun isInsideAnotherRoot(realm: Realm, folderId: Int): Boolean {
        val visited = mutableSetOf(folderId)
        var currentId: Int? = folderId
        while (currentId != null && currentId != ROOT_ID) {
            val folder = FileController.getFileById(realm, currentId) ?: return false
            if (folder.id != folderId && getOfflineRoot(folder.id, customRealm = realm) != null) return true
            currentId = folder.parentId.takeIf { visited.add(it) }
        }
        return false
    }

    /**
     * Whether a file (or folder) is still covered by any offline root, i.e. its inherited state
     * and offline copy must be preserved even when its explicit pin is removed.
     */
    fun isFileCoveredByAnyRoot(fileId: Int, userDrive: UserDrive = UserDrive(), customRealm: Realm? = null): Boolean {
        val block: (Realm) -> Boolean = { realm -> isInsideAnotherRoot(realm, fileId) }
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
     * Revisit of roots whose removal was deferred because protected copies remained: re-run the
     * coverage clearing, then delete the record once nothing protected is left. Called by the
     * offline sync so the deferred intent always has a durable owner (issue #1902).
     */
    fun processPendingRemovalRoots(context: Context, userDrive: UserDrive, customRealm: Realm) {
        getOfflineRoots(customRealm = customRealm).filter { it.isPendingRemoval }.forEach { root ->
            clearCoverageRecursively(context, customRealm, root.folderId, userDrive)
            val stillProtected = hasProtectedFilesRecursively(root.folderId, context, userDrive, customRealm)
            if (!stillProtected) {
                customRealm.executeTransaction {
                    getOfflineRoot(root.folderId, customRealm = customRealm)?.let { pendingRoot ->
                        if (pendingRoot.isValid) pendingRoot.deleteFromRealm()
                    }
                }
            }
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
    private fun clearCoverageRecursively(context: Context, realm: Realm, rootFolderId: Int, userDrive: UserDrive): RemovalResult {
        val result = RemovalResult()
        val visited = mutableSetOf(rootFolderId)
        val skippedSubtreeIds = mutableSetOf<Int>()
        val queue = ArrayDeque(listOf(rootFolderId))

        while (queue.isNotEmpty()) {
            val folderId = queue.removeFirst()
            if (folderId in skippedSubtreeIds) continue

            val folder = FileController.getFileById(realm, folderId)
            if (folder == null || !folder.isUsable()) continue

            if (folderId != rootFolderId && getOfflineRoot(folderId, customRealm = realm) != null) {
                // Subtree covered by another remaining root: skip it entirely.
                skippedSubtreeIds.add(folderId)
                continue
            }

            clearFileCoverage(context, realm, file = folder, userDrive = userDrive, result = result, isFolder = true)

            realm.where(File::class.java)
                .equalTo(File::parentId.name, folderId)
                .findAll()
                .createSnapshot()
                .forEach { child ->
                    if (child.isFolder()) {
                        if (visited.add(child.id)) queue.add(child.id)
                    } else {
                        clearFileCoverage(context, realm, file = child, userDrive = userDrive, result = result, isFolder = false)
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
    ) {
        if (file.isOffline) return

        if (isFolder) {
            clearInheritedFlags(realm, file)
            return
        }

        val offlineFile = file.getOfflineFile(context, userDrive.userId) ?: return
        if (!offlineFile.exists()) {
            clearInheritedFlags(realm, file)
            return
        }

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
                    return
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

    private enum class CopyProtection { NONE, PENDING_UPLOAD, UPLOADED, DIRTY }

    /**
     * Preservation state of an existing offline copy, shared with the deferred-cleanup
     * completion check (issue #1902): [PENDING_UPLOAD] and [DIRTY] copies must survive cleanup,
     * [UPLOADED] copies are content-safe duplicates, [NONE] copies are clearable.
     */
    private fun protectionStateOf(file: File, offlineFile: IOFile, userDrive: UserDrive): CopyProtection {
        val fileModifiedAt = Date(offlineFile.lastModified())
        return when {
            UploadFile.hasPendingUpload(uri = Uri.fromFile(offlineFile).toString()) -> CopyProtection.PENDING_UPLOAD
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

    private fun registerDirtySnapshot(context: Context, file: File, offlineFile: IOFile, userDrive: UserDrive): Boolean {
        return runCatching {
            val fileModifiedAt = Date(offlineFile.lastModified())
            val snapshotUri = UploadFile.createDurableOfflineSnapshot(
                context = context,
                source = offlineFile,
                fileModifiedAt = fileModifiedAt,
                fileId = file.id,
                driveId = userDrive.driveId,
            ) ?: return@runCatching false
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
