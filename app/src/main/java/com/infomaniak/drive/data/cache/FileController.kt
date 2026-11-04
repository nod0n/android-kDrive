/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2022-2026 Infomaniak Network SA
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
import com.infomaniak.core.auth.networking.AuthHttpClientProvider
import com.infomaniak.core.legacy.utils.removeAccents
import com.infomaniak.core.network.models.ApiResponse
import com.infomaniak.core.network.models.exceptions.NetworkException
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.BuildConfig
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.CursorApiResponse
import com.infomaniak.drive.data.models.CancellableAction
import com.infomaniak.drive.data.models.DropBox
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.File.Companion.IS_PRIVATE_SPACE
import com.infomaniak.drive.data.models.File.SortType
import com.infomaniak.drive.data.models.File.Type
import com.infomaniak.drive.data.models.FileActivity
import com.infomaniak.drive.data.models.MqttAction
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.models.file.FileExternalImport
import com.infomaniak.drive.data.models.file.FileExternalImport.FileExternalImportStatus
import com.infomaniak.drive.data.models.file.SpecialFolder.Favorites
import com.infomaniak.drive.data.models.file.SpecialFolder.Gallery
import com.infomaniak.drive.data.models.file.SpecialFolder.MyShares
import com.infomaniak.drive.data.models.file.SpecialFolder.RecentChanges
import com.infomaniak.drive.data.models.file.SpecialFolder.SharedWithMe
import com.infomaniak.drive.data.services.MqttClientWrapper
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.IOFile
import com.infomaniak.drive.utils.OfflineFileCoordination
import com.infomaniak.drive.utils.RealmModules
import com.infomaniak.drive.utils.Utils.ROOT_ID
import io.realm.Realm
import io.realm.RealmConfiguration
import io.realm.RealmList
import io.realm.RealmQuery
import io.realm.RealmResults
import io.realm.Sort
import io.realm.exceptions.RealmPrimaryKeyConstraintException
import io.realm.kotlin.oneOf
import io.realm.kotlin.toFlow
import io.sentry.Sentry
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.invoke
import kotlinx.coroutines.launch
import okhttp3.OkHttpClient
import java.util.Calendar

object FileController {

    private const val REALM_DB_FILE = "kDrive-%d-%d.realm"
    private const val REALM_DB_SHARES_WITH_ME = "kDrive-%d-shares.realm"

    private val minDateToIgnoreCache = Calendar.getInstance().apply { add(Calendar.MONTH, -2) }.timeInMillis / 1000 // 3 month

    fun getFileById(realm: Realm, fileId: Int): File? {
        return realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()
    }

    fun getSharedDrive(userId: Int, fileId: Int): File? {
        getRealmInstance(UserDrive(userId = userId, sharedWithMe = true)).use { realm ->
            return realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.let {
                realm.copyFromRealm(it, 1)
            }
        }
    }

    // https://github.com/realm/realm-java/issues/1862
    fun emptyList(realm: Realm): RealmResults<File> = realm.where(File::class.java).alwaysFalse().findAll()

    fun getParentFileProxy(fileId: Int, userDrive: UserDrive? = null, realm: Realm? = null): File? {
        val block: (Realm) -> File? = { currentRealm ->
            getFileById(currentRealm, fileId)?.localParent?.let { parents ->
                parents.firstOrNull { it.id > 0 }
            }
        }

        return realm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }

    fun getParentFile(fileId: Int, userDrive: UserDrive? = null, realm: Realm? = null): File? {
        val block: (Realm) -> File? = { currentRealm ->
            getParentFileProxy(fileId, userDrive, currentRealm)?.let { parent ->
                currentRealm.copyFromRealm(parent, 0)
            }
        }
        return realm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }

    fun getIdOfChildrenFileWithName(folderId: Int, name: String): List<Int> {
        return getRealmInstance().use { realm ->
            realm.where(File::class.java)
                .equalTo(File::parentId.name, folderId)
                .equalTo(File::name.name, name)
                .equalTo(File::type.name, Type.DIRECTORY.value)
                .findAll()
                .map { it.id }
        }
    }

    fun generateAndSavePath(fileId: Int, userDrive: UserDrive): String {
        return getRealmInstance(userDrive).use { realm ->
            getFileById(realm, fileId)?.let { file ->
                file.path.ifEmpty {
                    val generatedPath = generatePath(file, userDrive)
                    if (generatedPath.isNotBlank()) {
                        CoroutineScope(Dispatchers.IO).launch { savePath(userDrive, fileId, generatedPath) }
                    }
                    generatedPath
                }
            } ?: ""
        }
    }

    private fun savePath(userDrive: UserDrive, fileId: Int, generatedPath: String) {
        getRealmInstance(userDrive).use { realm ->
            getFileById(realm, fileId)?.let { file ->
                // Coordinated AND conditional on the ancestry the path was computed from
                // (issue: round 11 #3): the async save used to land unconditionally, so a move
                // between path generation and persistence would persist a chain the row no
                // longer hangs from — poisoning offline path resolution. The per-file
                // coordination spans the re-derivation and the write, so an identity writer
                // cannot interleave, and the write happens only when the CURRENT ancestry
                // still generates the same path.
                OfflineFileCoordination.withFileLock(userDrive, fileId) {
                    val currentPath = generatePath(file, userDrive)
                    if (currentPath == generatedPath) {
                        realm.executeTransaction {
                            if (file.isValid) file.path = generatedPath
                        }
                    }
                }
            }
        }
    }

    private tailrec fun generatePath(file: File, userDrive: UserDrive, recPath: StringBuilder = StringBuilder()): String {
        // id > 0 to exclude other root parents, home root has priority
        val folder = file.localParent?.createSnapshot()?.firstOrNull { it.id > 0 }
        return when {
            folder == null -> ""
            folder.id == ROOT_ID -> recPath.insert(0, "/${file.name}").toString()
            else -> generatePath(folder, userDrive, recPath.insert(0, "/${file.name}"))
        }
    }

    fun getFileProxyById(fileId: Int, userDrive: UserDrive? = null, customRealm: Realm? = null): File? {
        val block: (Realm) -> File? = { realm ->
            realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()
        }
        return customRealm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }

    fun getFileByUidOrId(fileId: Int, userDrive: UserDrive? = null): File? {
        return userDrive?.let { getFileByUid(fileId, userDrive = it) } ?: getFileById(fileId)
    }

    private fun getFileById(fileId: Int): File? {
        return getRealmInstance().use { realm ->
            realm.where(File::class.java).equalTo(File::id.name, fileId).findFirst()?.let {
                realm.copyFromRealm(it, 1)
            }
        }
    }

    private fun getFileByUid(fileId: Int, userDrive: UserDrive): File? {
        val uid = "${fileId}_${userDrive.driveId}"
        return getRealmInstance(userDrive).use { realm ->
            realm.where(File::class.java)
                .equalTo(File::uid.name, uid)
                .findFirst()
                ?.let { realm.copyFromRealm(it, 1) }
        }
    }

    suspend fun hasFile(fileId: Int, userDrive: UserDrive? = null): Boolean = forRealm(userDrive) {
        where(File::class.java)
            .equalTo(File::id.name, fileId)
            .count() > 0
    }

    fun hasFile(realm: Realm, fileId: Int): Boolean {
        return realm.where(File::class.java)
            .equalTo(File::id.name, fileId)
            .count() > 0
    }

    private suspend fun <R> forRealm(userDrive: UserDrive?, block: Realm.() -> R): R = Dispatchers.IO {
        getRealmInstance(userDrive).use(block)
    }

    fun getFolderOfflineFilesCount(folderId: Int): Long {
        return getRealmInstance().use { realm ->
            realm.where(File::class.java)
                .equalTo(File::parentId.name, folderId)
                .equalTo(File::isOffline.name, true)
                .count()
        }
    }

    fun getFolderFilesFlow(realm: Realm, folderId: Int): Flow<RealmResults<File>> {
        return realm.where(File::class.java)
            .equalTo(File::parentId.name, folderId)
            .findAllAsync()
            .toFlow()
    }

    fun getRecentFolders(realm: Realm, recentFolderNumber: Int): Flow<List<File>> {
        return realm.where(File::class.java)
            .equalTo(File::type.name, Type.DIRECTORY.value)
            .greaterThan(File::id.name, ROOT_ID)
            .greaterThan(File::parentId.name, ROOT_ID)
            .sort(File::lastModifiedAt.name, Sort.DESCENDING)
            .limit(recentFolderNumber.toLong())
            .sort(File::lastModifiedAt.name, Sort.ASCENDING)
            .findAllAsync()
            .toFlow()
    }

    //region isMarkedAsOffline
    fun hasOfflineIntentFiles(folderId: Int): Boolean {
        // Explicit pins carry durable download intent too (issue: round 5 #15): the enqueue
        // gate must not drop a selection whose pending markers an overlapping inherited walk
        // already cleared.
        return getRealmInstance().use { realm ->
            realm.where(File::class.java)
                .equalTo(File::parentId.name, folderId)
                .beginGroup()
                .equalTo(File::isMarkedAsOffline.name, true)
                .or()
                .equalTo(File::isOffline.name, true)
                .endGroup()
                .notEqualTo(File::type.name, Type.DIRECTORY.value)
                .count() > 0
        }
    }

    fun getFolderOfflineFilesId(folderId: Int, sortType: SortType, userDrive: UserDrive? = null): List<Int> {
        // Enumerate in the caller's captured account scope, not the currently active one
        // (issue: round 5 #16).
        return getRealmInstance(userDrive).use { realm ->
            // Durable explicit-pin intent (issue: round 5 #15) is enumerated alongside the
            // shared pending marker: a concurrent inherited walk can clear the markers while
            // this batch loads, and an explicitly pinned file must still be downloaded. Files
            // whose offline copy is already intact are skipped inside the transfer.
            val realmFiles = realm.where(File::class.java)
                .equalTo(File::parentId.name, folderId)
                .beginGroup()
                .equalTo(File::isMarkedAsOffline.name, true)
                .or()
                .equalTo(File::isOffline.name, true)
                .endGroup()
                .getSortQueryByOrder(sortType)
                .findAll()
            realmFiles.filter { !it.isFolder() }.map { it.id }
        }
    }

    private fun markFileAsOfflineQuery(realm: Realm, fileId: Int, isMarkedAsOffline: Boolean, setExplicitPin: Boolean) {
        realm.where(File::class.java)
            .equalTo(File::id.name, fileId)
            .findFirst()?.let { file ->
                file.isMarkedAsOffline = isMarkedAsOffline
                // Durable explicit-pin intent (issue: round 5 #15): the user's request must not
                // depend on the shared pending marker, which an overlapping inherited walk can
                // clear. Inherited completion neither consumes nor suppresses this flag, and
                // root removal never strips explicitly pinned files.
                if (setExplicitPin && isMarkedAsOffline) file.isOffline = true
            }
    }

    fun markFilesAsOffline(
        customRealm: Realm? = null,
        filesId: List<Int>,
        isMarkedAsOffline: Boolean,
        userDrive: UserDrive? = null,
        setExplicitPin: Boolean = false,
    ) {
        if (setExplicitPin) {
            // Explicit-pin writes serialize with destructive cleanup on each file's
            // coordination lock, taken BEFORE the Realm write (issue: round 7 #9).
            val lockedBlock: (Realm) -> Unit? = { realm ->
                filesId.forEach { fileId ->
                    OfflineFileCoordination.withFileLock(userDrive ?: UserDrive(), fileId) {
                        realm.executeTransaction {
                            markFileAsOfflineQuery(realm, fileId, isMarkedAsOffline, setExplicitPin)
                        }
                    }
                }
            }
            customRealm?.let(lockedBlock) ?: getRealmInstance(userDrive).use(lockedBlock)
            return
        }
        val block: (Realm) -> Unit? = { realm ->
            realm.executeTransaction {
                filesId.forEach { fileId ->
                    markFileAsOfflineQuery(realm, fileId, isMarkedAsOffline, setExplicitPin)
                }
            }
        }

        customRealm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }

    fun markFileAsOffline(customRealm: Realm? = null, fileId: Int, isMarkedAsOffline: Boolean, userDrive: UserDrive? = null) {
        val block: (Realm) -> Unit? = { realm ->
            realm.executeTransaction {
                markFileAsOfflineQuery(realm, fileId, isMarkedAsOffline, setExplicitPin = false)
            }
        }
        customRealm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }
    //endregion

    fun removeFile(
        fileId: Int,
        keepFileCaches: ArrayList<Int> = arrayListOf(),
        keepFiles: ArrayList<Int> = arrayListOf(),
        customRealm: Realm? = null,
        recursive: Boolean = true,
        userDrive: UserDrive? = null,
    ) {
        val block: (Realm) -> Unit? = { realm ->
            getFileById(realm, fileId)?.let { file ->
                if (recursive) {
                    file.children.createSnapshot().forEach {
                        // The captured account flows through the recursion (issue: round 5 #6):
                        // a descendant resolving UserDrive from the active account would
                        // inspect, snapshot or delete another account's bytes while removing
                        // this account's metadata.
                        if (!keepFiles.contains(it.id)) {
                            removeFile(it.id, keepFileCaches, keepFiles, realm, userDrive = userDrive)
                        }
                    }
                }
                runCatching {
                    val resolvedUserDrive = userDrive ?: UserDrive(driveId = file.driveId)
                    // Row removal is a mutation owner of the offline copy: preservation, cache
                    // deletion and row deletion are one coordinated section (issue: round 4 #4).
                    OfflineFileCoordination.withFileLock(resolvedUserDrive, fileId) {
                        // Required ancestry (issue: round 5 #10): a folder row whose descendants
                        // remain locally owned must survive — deleting it strands the preserved
                        // copies' metadata and breaks every parentId traversal (deferred
                        // cleanup, orphan sweep, offline path resolution). Applies to both the
                        // recursive and the nonrecursive removal paths.
                        val folderStillNeeded = file.isFolder() &&
                                OfflineFolderController.hasLocallyOwnedDescendant(
                                    folderId = fileId,
                                    realm = realm,
                                )
                        if (!keepFileCaches.contains(fileId)) {
                            // Inherited copies are preserved before any deletion (issue: round 3 #6):
                            // a pending upload keeps its bytes, a dirty copy is snapshotted first.
                            when (OfflineFolderController.preserveInheritedCopyBeforeRemoval(file, resolvedUserDrive)) {
                                // Content safe (or nothing to preserve): bytes and row can go.
                                OfflineFolderController.PreservationOutcome.CONTENT_SAFE -> {
                                    if (!keepFileCaches.contains(fileId)) {
                                        file.deleteCaches(Realm.getApplicationContext()!!, resolvedUserDrive.userId)
                                    }
                                    if (!keepFiles.contains(fileId) && !folderStillNeeded) {
                                        realm.executeTransaction { if (file.isValid) file.deleteFromRealm() }
                                    }
                                }
                                // Preservation pending: retain bytes AND the row (with its durable
                                // metadata and ancestry) until the upload completes; the next sync
                                // re-processes the remote deletion then (issue: round 4 #6).
                                OfflineFolderController.PreservationOutcome.PRESERVED -> Unit
                            }
                        } else if (!keepFiles.contains(fileId) && !folderStillNeeded) {
                            realm.executeTransaction { if (file.isValid) file.deleteFromRealm() }
                        }
                    }
                }.onFailure { exception ->
                    Sentry.captureException(exception) { scope ->
                        scope.setExtra("with custom realm", "${customRealm != null}")
                        scope.setExtra("recursive", "$recursive")
                    }
                }
                Unit
            }
        }
        customRealm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }

    fun renameFile(
        file: File,
        newName: String,
        realm: Realm? = null,
        userDrive: UserDrive? = null,
    ): ApiResponse<CancellableAction> {
        val apiResponse = ApiRepository.renameFile(file, newName)
        if (apiResponse.isSuccess()) {
            // Preservation BEFORE the identity replacement (issue: round 6 #4): the owned copy
            // at the old name's path is reconciled to the new name first, and an incomplete
            // reconciliation keeps the OLD name until a later pass retries.
            val preserved = preserveRenameBeforeMetadataChange(file, newName, userDrive, realm)
            if (preserved) {
                updateFile(file.id, realm, userDrive = userDrive) { localFile ->
                    localFile.name = newName
                }
            }
        }
        return apiResponse
    }

    /**
     * Reconciles the owned copy of [file] at its OLD name's path to [newName], BEFORE the row's
     * name is replaced (issue: round 6 #4). The offline path's file component is always the
     * row's name, so the post-commit location is the same directory with the new name, for
     * rows with a stored path and for derived ones alike. Returns true when the old location
     * is fully handled (or nothing owned exists there); false means the caller must NOT
     * replace the identity yet.
     */
    private fun preserveRenameBeforeMetadataChange(
        file: File,
        newName: String,
        userDrive: UserDrive?,
        customRealm: Realm?,
    ): Boolean {
        val context = Realm.getApplicationContext() ?: return false
        val scope = userDrive ?: UserDrive()
        // A lambda stored in a variable has no implicit label: the explicit [renameBlock]
        // label is what the early returns below target (round-6 compile fix).
        val block: (Realm) -> Boolean = renameBlock@{ realm ->
            val liveRow = getFileProxyById(file.id, customRealm = realm) ?: return@renameBlock true
            if (liveRow.isFolder()) return@renameBlock true
            val oldOffline = liveRow.getOfflineFile(context, scope.userId)?.takeIf { it.exists() }
                ?: return@renameBlock true
            val newOffline = oldOffline.parentFile?.let { parent -> IOFile(parent, newName) }
                ?: return@renameBlock false
            if (newOffline.path == oldOffline.path) return@renameBlock true
            // The dirty-copy snapshot registers under the NEW name: the edit belongs to the
            // renamed file, and uploading it under the old name would recreate that name
            // remotely.
            val snapshotIdentity = File(id = liveRow.id, driveId = scope.driveId).apply {
                parentId = liveRow.parentId
                name = newName
            }
            OfflineFolderController.reconcileMovedInheritedCopy(
                context = context,
                oldOfflineFile = oldOffline,
                oldIdentityName = liveRow.name,
                oldIdentityParentId = liveRow.parentId,
                oldLastModifiedAt = liveRow.lastModifiedAt,
                newIdentity = snapshotIdentity,
                userDrive = scope,
                newOfflineTarget = newOffline,
            )
        }
        return customRealm?.let(block) ?: getRealmInstance(scope).use(block)
    }

    fun updateFolderColor(file: File, color: String, userDrive: UserDrive): ApiResponse<Boolean> {
        return ApiRepository.updateFolderColor(file, color).also {
            if (it.isSuccess()) updateFile(file.id, userDrive = userDrive) { localFile -> localFile.color = color }
        }
    }

    tailrec fun deleteFileAndChildrenRec(
        filesToDelete: MutableList<File>,
        realm: Realm? = null,
        userDrive: UserDrive? = null,
        context: Context,
    ) {
        if (filesToDelete.isEmpty()) return

        val file = filesToDelete.removeAt(0)
        val children = getFileByUidOrId(file.id, userDrive)?.children ?: emptyList()

        filesToDelete.addAll(children)
        file.deleteCaches(context)
        updateFile(file.id, realm, userDrive) { localFile -> localFile.deleteFromRealm() }

        deleteFileAndChildrenRec(filesToDelete, realm, userDrive, context)
    }

    fun deleteFile(
        file: File,
        realm: Realm? = null,
        userDrive: UserDrive? = null,
        context: Context,
        onSuccess: ((fileId: Int) -> Unit)? = null,
    ): ApiResponse<CancellableAction> = ApiRepository.deleteFile(file).also { apiResponse ->
        if (apiResponse.isSuccess()) {
            runCatching {
                deleteFileAndChildrenRec(filesToDelete = mutableListOf(file), realm, userDrive, context)
                onSuccess?.invoke(file.id)
            }.onFailure(Sentry::captureException)
        }
    }

    fun updateIsOfflineForFiles(
        fileIds: List<Int>,
        customRealm: Realm? = null,
        userDrive: UserDrive? = null,
        isOffline: Boolean,
    ) {
        val block: (Realm) -> Unit? = { currentRealm ->
            fileIds.forEach { fileId ->
                // Per-file pin/unpin writes serialize with destructive cleanup on the same
                // coordination lock, taken BEFORE the Realm write (issue: round 6 #6).
                OfflineFileCoordination.withFileLock(userDrive ?: UserDrive(), fileId) {
                    currentRealm.executeTransaction {
                        getFileById(currentRealm, fileId)?.let { file ->
                            file.isOffline = isOffline
                            if (!isOffline) {
                                // Removing the explicit pin must materialize the current coverage
                                // result (issue #1902): a file still covered by a root keeps its
                                // inherited state (even when the pin was the only flag so far),
                                // while an uncovered file loses any stale inherited flag.
                                file.isInOfflineFolder = OfflineFolderController.isInsideActiveRoot(currentRealm, fileId)
                                // The pending download marker is part of the removed intent
                                // (issue: round 5 #15): a stale marker would otherwise resurrect
                                // the pin through the sync's intent check after the next download.
                                file.isMarkedAsOffline = false
                            }
                        }
                    }
                }
            }
        }
        customRealm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }

    /**
     * Folders that still hold durable offline download intent in [userDrive]'s scope — an
     * explicit pin or a pending download marker (issue: round 5 #15/#17): used to re-drive
     * unfinished pin batches after a constraint reconfiguration or an app restart. Intact
     * copies are skipped inside the worker, so re-driving an already-satisfied intent is cheap.
     */
    fun getFoldersWithOfflineIntent(userDrive: UserDrive? = null, customRealm: Realm? = null): List<Int> {
        val block: (Realm) -> List<Int> = { realm ->
            realm.where(File::class.java)
                .beginGroup()
                .equalTo(File::isMarkedAsOffline.name, true)
                .or()
                .equalTo(File::isOffline.name, true)
                .endGroup()
                .notEqualTo(File::type.name, Type.DIRECTORY.value)
                .distinct(File::parentId.name)
                .findAll()
                .map { it.parentId }
        }
        return customRealm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }

    fun updateFile(fileId: Int, realm: Realm? = null, userDrive: UserDrive? = null, transaction: (file: File) -> Unit) {
        try {
            val block: (Realm) -> Unit? = { currentRealm ->
                getFileById(currentRealm, fileId)?.let { file ->
                    // Row-field writers share the per-file coordination (issue: round 10 #3):
                    // the direct rename's identity write must serialize with a publication's
                    // check-and-rename. Callers without an explicit scope fall back to the
                    // default-scope key — their writes are non-identity fields (cursors,
                    // colors); the rename and every graph writer pass their captured scope.
                    OfflineFileCoordination.withFileLock(userDrive ?: UserDrive(), fileId) {
                        currentRealm.executeTransaction {
                            if (file.isValid) transaction(file)
                        }
                    }
                }
            }
            realm?.let(block) ?: getRealmInstance(userDrive).use(block)
        } catch (exception: Exception) {
            exception.printStackTrace()
            Sentry.captureException(exception) { scope -> scope.setExtra("custom realm", "${realm != null}") }
        }
    }

    fun upsertActionFile(realm: Realm, folderId: Int, actionFile: File, userDrive: UserDrive) {
        // Shared upsert boundary (issue: round 5 #5): an activity refresh can carry a
        // rename/move; the owned copy at the old location is preserved before the metadata
        // replacement, and a deferred reconciliation keeps the OLD row authoritative.
        // Authoritative-change flow (the activity event IS the change): the generation-only
        // gate — an identity difference at equal revisions is the rename being applied, and a
        // superseded ACTION is already prevented upstream by the activity ordering.
        val preservedActionFile = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
            realm = realm,
            remoteFiles = listOf(actionFile),
            userDrive = userDrive,
            isStale = { live, incoming -> FileController.isSupersededGeneration(live, incoming) },
        ).first()
        realm.where(File::class.java).equalTo(File::id.name, folderId).findFirst()?.let { realmFolder ->
            if (!realmFolder.children.contains(preservedActionFile)) {
                // Coordinated child-add (issue: round 11 #3): the add mutates the new child's
                // graph identity (it gains its parent) and the container's children list, so
                // both locks precede the transaction — under the root-intent monitor, since
                // the add establishes ancestry that coverage resolution walks (issue: round
                // 11 #7). Order: intent monitor → file locks → transaction.
                synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    OfflineFileCoordination.withLocksFor(userDrive, listOf(folderId, preservedActionFile.id)) {
                        realm.executeTransaction { realmFolder.children.add(preservedActionFile) }
                    }
                }
            } else {
                updateFileFromActivity(realm, preservedActionFile, realmFolder.id, userDrive)
            }
        }
    }

    fun updateShareLinkWithRemote(fileId: Int) {
        getRealmInstance().use { realm ->
            getFileProxyById(fileId, customRealm = realm)?.let { fileProxy ->
                ApiRepository.getShareLink(fileProxy).data?.let { shareLink ->
                    realm.executeTransaction { fileProxy.shareLink = shareLink }
                }
            }
        }
    }

    fun updateDropBox(fileId: Int, newDropBox: DropBox?) {
        updateFile(fileId) { fileProxy ->
            fileProxy.dropbox = newDropBox
        }
    }

    fun updateOfflineStatus(fileId: Int, isOffline: Boolean, userDrive: UserDrive? = null) {
        // The pin write serializes on the same per-file coordination the destructive cleanup
        // holds, taken BEFORE the Realm write, so a cleanup cannot delete a copy whose pin
        // request is being written (issue: round 6 #6).
        OfflineFileCoordination.withFileLock(userDrive ?: UserDrive(), fileId) {
            updateFile(fileId, userDrive = userDrive) { file ->
                file.isOffline = isOffline
            }
        }
    }

    fun updateExistingFile(
        newFile: File,
        realm: Realm,
        userDrive: UserDrive,
    ) {
        getFileById(realm, newFile.id)?.let { localFile ->
            insertOrUpdateFile(realm, newFile, localFile, userDrive)
        }
    }

    /**
     * Whether [incoming] metadata describes a STRICTLY OLDER SERVER GENERATION than the live
     * row and must be rejected by every conditional path (issues: round 10 #4, round 11 #4/#6).
     *
     * [File.revisedAt] is the server's revision sequence in SECONDS (every re-upload creates a
     * new revision), unlike the modification time, which a re-upload can legitimately move
     * backwards — a strictly older revisedAt is superseded, a strictly newer one is accepted
     * even with a backward mtime. Legacy rows whose revisedAt was never populated fall back to
     * the modification baseline, and a revision-less incoming row never replaces a row that
     * holds a server revision.
     *
     * This generation-only rule governs flows where the incoming identity IS the change being
     * applied (the sync's rename/move actions): an identity difference at equal revisions is
     * the action's purpose there, not staleness.
     */
    internal fun isSupersededGeneration(live: File, incoming: File): Boolean {
        // Authoritative version identifier where supplied (issue: round 12 #6): a strictly
        // lower version number is an older content revision even when revisedAt seconds tie —
        // two revisions sharing a nonzero timestamp interval are ordered by it.
        val liveVersion = live.version?.number ?: 0
        val incomingVersion = incoming.version?.number ?: 0
        if (liveVersion > 0 && incomingVersion > 0 && liveVersion > incomingVersion) return true
        return when {
            live.revisedAt > incoming.revisedAt -> true
            live.revisedAt == 0L && incoming.revisedAt == 0L -> incoming.lastModifiedAt < live.lastModifiedAt
            else -> false
        }
    }

    /**
     * Whether [incoming] metadata must be rejected by a REPLACEMENT boundary — a path whose
     * incoming row would overwrite the live one wholesale (listings, the download manager's
     * refresh, graph imports). Beyond [isSupersededGeneration], the SERVER MUTATION timestamp
     * ([File.updatedAt] — bumped by every rename/move/metadata change and copied monotonically
     * onto the row) orders two observations of the same content revision (issue: round 12 #3):
     * an observation from an OLDER server state is stale, while a response describing a LATER
     * server mutation — a CURRENT rename at an unchanged content revision — is ACCEPTED. The
     * previous equal-revision identity rejection refused exactly that response on every
     * listing and refetch (refetching returns the same comparison), permanently rejecting
     * legitimate renames and stranding already-moved bytes.
     */
    internal fun isStaleReplacement(live: File, incoming: File): Boolean = when {
        isSupersededGeneration(live, incoming) -> true
        live.updatedAt > incoming.updatedAt -> true
        else -> false
    }

    /**
     * Conditional metadata replacement (issue: round 10 #4, refined round 11 #4/#5/#6): the
     * incoming row commits only when it is not stale against the LIVE row read INSIDE the
     * committing transaction — the caller's Realm view can lag a commit from another thread
     * (lock wait, no-Looper realm), so the acceptance decision must not be made against a
     * pre-transaction read. The check and the commit share the per-file coordination, so a
     * delayed older response can no longer roll back a newer committed generation between the
     * publication's supersession check and its rename. Returns false (nothing written) when
     * the incoming row is stale or the row vanished.
     */
    fun updateExistingFileIfNotOlder(newFile: File, realm: Realm, userDrive: UserDrive): Boolean {
        return insertOrUpdateFile(
            realm = realm,
            newFile = newFile,
            oldFile = getFileById(realm, newFile.id),
            userDrive = userDrive,
            // Must EXIST (issue: round 12 #2): a worker refresh without a live row has nothing
            // to refresh — absence is rejection, not insertion.
            acceptIf = { live -> live != null && !isStaleReplacement(live, newFile) },
        )
    }

    /**
     * Conditional metadata replacement for AUTHORITATIVE CHANGE flows (the offline sync's
     * rename/move/download actions): the generation-only gate of [isSupersededGeneration] —
     * the action's identity difference at equal revisions is the change being applied, and a
     * stale ACTION is already prevented upstream by the activity-cursor ordering, so only a
     * strictly newer committed revision may reject it.
     */
    fun updateExistingFileIfNotSuperseded(
        newFile: File,
        realm: Realm,
        userDrive: UserDrive,
        requireActionAtOrAfter: Long = Long.MAX_VALUE,
    ): Boolean {
        return insertOrUpdateFile(
            realm = realm,
            newFile = newFile,
            oldFile = getFileById(realm, newFile.id),
            userDrive = userDrive,
            // Must EXIST (issue: round 12 #2), generation-only, and the row's activity cursor
            // must not already be PAST the action being applied (issue: round 12 #5) — a
            // delayed action's commit must not overwrite a location a later action already
            // committed, nor rewind anything.
            acceptIf = { live ->
                live != null && !isSupersededGeneration(live, newFile) && live.lastActionAt <= requireActionAtOrAfter
            },
        )
    }

    private fun insertOrUpdateFile(
        realm: Realm,
        newFile: File,
        oldFile: File? = null,
        userDrive: UserDrive,
        moreTransaction: (() -> Unit)? = null,
        acceptIf: ((live: File?) -> Boolean)? = null,
    ): Boolean {
        // Identity/revision metadata writers share the publisher's per-file coordination
        // (issue: round 9 #7): without it, a writer could commit a newer identity or baseline
        // between the publisher's supersession check and its rename — the publisher's
        // check-and-rename is only a CAS if every identity/revision writer takes the same
        // lock. Reentrant locks reenter, so writers already holding the file's lock
        // (upload registration, move reconciliation) nest safely.
        return OfflineFileCoordination.withFileLock(userDrive, newFile.id) {
            var accepted = false
            realm.executeTransaction {
                // Conditional acceptance INSIDE the committing transaction (issue: round 11 #5):
                // the decision reads the transaction-fresh live row, so a response that was
                // fresh when the caller fetched it is still revalidated against whatever
                // committed during the coordination wait. The gate receives NULL for a
                // row-absence so each caller distinguishes conditional UPSERT (absence
                // inserts — single-file fetches after a cache miss) from conditional
                // EXISTING-ROW UPDATE (must exist — worker refreshes) (issue: round 12 #2).
                if (acceptIf != null) {
                    val live = getFileById(realm, newFile.id)
                    if (!acceptIf(live)) return@executeTransaction
                }
                if (oldFile?.isUsable() == true) newFile.keepOldLocalFilesData(oldFile)
                moreTransaction?.invoke()
                it.insertOrUpdate(newFile)
                accepted = true
            }
            accepted
        }
    }

    fun addChild(localFolderId: Int, newFile: File, realm: Realm, userDrive: UserDrive? = null) {
        getFileById(realm, localFolderId)?.let { localFolder ->
            if (!localFolder.children.contains(newFile)) {
                // The child-add is an identity mutation of the new child (the UI move calls
                // this with a changed parent/path) and of the container's children list
                // (issue: round 11 #3): both coordination locks precede the transaction, in
                // the captured scope of the ACTUAL realm.
                OfflineFileCoordination.withLocksFor(
                    userDrive ?: UserDrive(),
                    listOf(localFolderId, newFile.id),
                ) {
                    realm.executeTransaction { runCatching { localFolder.children.add(newFile) } }
                }
            }
        }
    }

    fun createSharedWithMeFolderIfNeeded(userDrive: UserDrive?) {
        getRealmInstance(userDrive ?: UserDrive(sharedWithMe = true)).use {
            getFileById(realm = it, SharedWithMe.id) ?: saveFiles(SharedWithMe.file, emptyList(), realm = it)
        }
    }

    fun saveFavoritesFiles(
        files: List<File>,
        replaceOldData: Boolean = false,
        realm: Realm? = null,
        userDrive: UserDrive,
    ) {
        // The Favorites request's captured scope flows through realm, preservation and
        // commit (issue: round 8 #4): a default active-account scope would read one
        // account's metadata while checking another account's filesystem.
        saveFiles(Favorites.file, files, replaceOldData, realm, userDrive)
    }

    private fun saveMySharesFiles(userDrive: UserDrive, files: ArrayList<File>, replaceOldData: Boolean) {
        val keepCaches = arrayListOf<Int>()
        val keepFiles = arrayListOf<Int>()
        val appContext = Realm.getApplicationContext()
        getRealmInstance(userDrive).use { realm ->
            files.forEachIndexed { index, file ->
                realm.where(File::class.java).equalTo(File::id.name, file.id).findFirst()?.let { oldFile ->
                    realm.executeTransaction {
                        file.children = oldFile.children
                        keepFiles.add(file.id)
                    }

                    // Offline state belongs to the OLD row, resolved under the request's
                    // captured account (issue: round 8 #3): the incoming API object carries
                    // default offline flags, so the previous intact check ALWAYS failed and
                    // eagerly deleted the existing copy — destroying unregistered edits
                    // before the preservation boundary ever ran. No filesystem mutation
                    // happens here anymore: an unkept copy's fate is decided by the
                    // preservation policies (removeFile's per-file preservation, the shared
                    // boundary in saveFiles). Inherited coverage is not converted into an
                    // explicit pin: only an old row that was itself pinned restores the pin
                    // intent.
                    val offlineFile = appContext?.let { oldFile.getOfflineFile(it, userDrive.userId) }
                    if (offlineFile != null && oldFile.isOfflineAndIntact(offlineFile)) {
                        files[index].isOffline = oldFile.isOffline
                        keepCaches.add(file.id)
                    }
                }
            }

            if (replaceOldData) removeFile(MyShares.id, keepCaches, keepFiles, realm, userDrive = userDrive)
            saveFiles(MyShares.file, files, replaceOldData, realm, userDrive)
        }
    }

    private fun saveFiles(
        folder: File,
        files: List<File>,
        replaceOldData: Boolean = false,
        realm: Realm? = null,
        userDrive: UserDrive? = null
    ) {
        val block: (Realm) -> Unit = { currentRealm ->
            // Coordinated commit boundary BEFORE the boundary AND the transaction (issues:
            // round 10 #3, round 11 #2): the container folder and every incoming child,
            // ascending, share the publication lock set across the shared upsert boundary's
            // validation/preservation AND the commit — one coordinated operation, so no
            // identity writer can interleave between a file's freshness validation and its
            // commit.
            OfflineFileCoordination.withLocksFor(
                userDrive ?: UserDrive(),
                listOf(folder.id) + files.map { it.id },
            ) {
                // Shared upsert boundary OUTSIDE the write transaction (issue: round 7 #6):
                // Favorites/MyShares listings replace identities too; a stale response is
                // rejected here BEFORE any preservation side effect (issue: round 11 #4).
                val committableFiles = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                    realm = currentRealm,
                    remoteFiles = files,
                    userDrive = userDrive ?: UserDrive(),
                )
                currentRealm.executeTransaction { realm ->
                    val newMySharesFolder = if (replaceOldData) {
                        realm.copyToRealmOrUpdate(folder)
                    } else {
                        currentRealm.where(File::class.java).equalTo(File::id.name, folder.id).findFirst()
                            ?: realm.copyToRealmOrUpdate(folder)
                    }
                    newMySharesFolder?.children?.addAll(realm, committableFiles)
                }
            }
        }

        realm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }

    private fun getDriveFileName(userDrive: UserDrive): String {
        val realmDb = if (userDrive.sharedWithMe) REALM_DB_SHARES_WITH_ME else REALM_DB_FILE
        return realmDb.run {
            if (userDrive.sharedWithMe) format(userDrive.userId) else format(userDrive.userId, userDrive.driveId)
        }
    }

    fun getRealmConfiguration(userDrive: UserDrive?) = getRealmConfiguration(getDriveFileName(userDrive ?: UserDrive()))

    fun getRealmInstance(userDrive: UserDrive? = null): Realm = Realm.getInstance(getRealmConfiguration(userDrive))

    private fun getRealmConfiguration(dbName: String): RealmConfiguration {
        return RealmConfiguration.Builder()
            .schemaVersion(FileMigration.DB_VERSION) // Must be bumped when the schema changes
            .migration(FileMigration())
            .modules(RealmModules.LocalFilesModule())
            .name(dbName)
            .build()
    }

    /**
     * Delete all drive data cache for ne user
     * @param userId User Id
     * @param driveId Drive Id (null if all user drive)
     */
    fun deleteUserDriveFiles(userId: Int, driveId: Int? = null) {
        val filesDir = Realm.getApplicationContext()!!.filesDir
        filesDir.listFiles()?.forEach { file ->
            val match = Regex("(\\d+)-(\\d+)").find(file.name)
            match?.destructured?.let {
                val (fileUserId, fileDriveId) = it
                if (fileUserId.toInt() == userId && (driveId == null || fileDriveId.toInt() == driveId)) {
                    if (file.isDirectory) file.deleteRecursively() else file.delete()
                }
            }
        }
        DriveInfosController.deleteDrives(userId)
    }

    fun getFilesFromCache(folderId: Int, userDrive: UserDrive? = null, order: SortType = SortType.NAME_AZ): ArrayList<File> {
        return getRealmInstance(userDrive).use { currentRealm ->
            currentRealm
                .where(File::class.java)
                .equalTo(File::id.name, folderId)
                .findFirst()?.children?.where()?.getSortQueryByOrder(order)?.findAll()?.let { children ->
                    ArrayList(currentRealm.copyFromRealm(children, 0))
                }
        } ?: ArrayList()
    }

    fun getFileDetails(fileId: Int, userDrive: UserDrive = UserDrive()): File? {
        fun getRemoteParentDetails(remoteFile: File, userDrive: UserDrive, realm: Realm) {
            // Shared upsert boundary (issue: round 6 #4): the details cascade can also replace
            // the file's identity; preserve the owned copy at the old location first.
            val preservedFile = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                realm = realm,
                remoteFiles = listOf(remoteFile),
                userDrive = userDrive,
            ).first()
            ApiRepository.getFileDetails(File(id = remoteFile.parentId, driveId = userDrive.driveId)).data?.let {
                it.children = RealmList(preservedFile)
                // The parent upsert CASCADES the embedded child row (issue: round 11 #3):
                // both affected identities are locked before the transaction — under the
                // root-intent monitor, since the parent row's commit can re-parent a folder
                // (issue: round 11 #7). Order: intent monitor → file locks → transaction.
                synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    OfflineFileCoordination.withLocksFor(userDrive, listOf(it.id, preservedFile.id)) {
                        insertOrUpdateFile(realm, it, userDrive = userDrive)
                    }
                }
            }
        }

        val apiResponse = ApiRepository.getFileDetails(File(id = fileId, driveId = userDrive.driveId))
        return if (apiResponse.isSuccess()) {
            apiResponse.data?.let { remoteFile ->
                getRealmInstance(userDrive).use { realm ->
                    val localParentProxy = getParentFile(fileId, realm = realm)
                    if (localParentProxy == null) {
                        getRemoteParentDetails(remoteFile, userDrive, realm)
                    } else {
                        // Shared upsert boundary (issue: round 6 #4): details responses carry
                        // name/path metadata that would replace the old identity without
                        // preserving the owned copy at its old location.
                        val preservedFile = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                            realm = realm,
                            remoteFiles = listOf(remoteFile),
                            userDrive = userDrive,
                        ).first()
                        insertOrUpdateFile(
                            realm,
                            preservedFile,
                            getFileProxyById(fileId, customRealm = realm),
                            userDrive,
                            // Must exist — the details refresh has nothing to refresh otherwise
                            // (issue: round 12 #2).
                            acceptIf = { live -> live != null && !isStaleReplacement(live, preservedFile) },
                        )
                    }
                }
                remoteFile
            }
        } else {
            null
        }
    }

    @Throws(NetworkException::class)
    fun getRemoteFile(fileId: Int, driveId: Int, okHttpClient: OkHttpClient): File? {
        val apiResponse = ApiRepository.getFileDetails(File(id = fileId, driveId = driveId), okHttpClient)
        (apiResponse.error?.exception as? NetworkException)?.let { throw it }
        return apiResponse.data?.takeIf { apiResponse.isSuccess() }
    }

    tailrec suspend fun getMySharedFiles(
        userDrive: UserDrive,
        sortType: SortType,
        cursor: String? = null,
        onlyLocal: Boolean = false,
        transaction: (files: ArrayList<File>, isComplete: Boolean) -> Unit,
        isFirstPage: Boolean = true,
    ) {
        if (onlyLocal) {
            transaction(getFilesFromCache(MyShares.id, userDrive, sortType), true)
        } else {
            val apiResponse = ApiRepository.getMySharedFiles(
                AccountUtils.getHttpClient(userDrive.userId), userDrive.driveId, sortType, cursor
            )
            if (apiResponse.isSuccess()) {
                val apiResponseData = apiResponse.data
                when {
                    apiResponseData.isNullOrEmpty() -> transaction(arrayListOf(), true)
                    apiResponse.hasMoreAndCursorExists -> {
                        saveMySharesFiles(userDrive, apiResponseData, isFirstPage)
                        transaction(apiResponseData, false)
                        getMySharedFiles(userDrive, sortType, apiResponse.cursor, false, transaction, isFirstPage = false)
                    }
                    else -> {
                        saveMySharesFiles(userDrive, apiResponseData, isFirstPage)
                        transaction(apiResponseData, true)
                    }
                }
            } else if (isFirstPage) transaction(getFilesFromCache(MyShares.id, userDrive, sortType), true)
        }
    }

    tailrec suspend fun cloudStorageSearch(
        userDrive: UserDrive,
        query: String,
        onResponse: (files: ArrayList<File>) -> Unit,
        cursor: String? = null,
    ) {
        val order = SortType.NAME_AZ
        val apiResponse = ApiRepository.searchFiles(
            driveId = userDrive.driveId,
            query = query,
            sortType = order,
            cursor = cursor,
            okHttpClient = AccountUtils.getHttpClient(userDrive.userId)
        )

        if (apiResponse.isSuccess()) {
            val apiResponseData = apiResponse.data
            when {
                apiResponseData.isNullOrEmpty() -> onResponse(arrayListOf())
                apiResponse.hasMoreAndCursorExists -> {
                    onResponse(apiResponseData)
                    cloudStorageSearch(userDrive, query, onResponse, apiResponse.cursor)
                }
                else -> {
                    onResponse(apiResponseData)
                }
            }
        } else if (cursor == null) {
            onResponse(searchFiles(query, order, userDrive))
        }
    }

    fun getActivities(): ArrayList<FileActivity> {
        val activityResults = arrayListOf<FileActivity>()
        getRealmInstance().use { realm ->
            realm.where(FileActivity::class.java)
                .sort(FileActivity::createdAt.name, Sort.DESCENDING)
                .findAll()?.forEach { fileActivity ->
                    fileActivity.userId?.let { userId ->
                        activityResults.add(realm.copyFromRealm(fileActivity, 1).apply {
                            user = DriveInfosController.getUsers(arrayListOf(userId)).firstOrNull()
                        })
                    }
                }
        }
        return activityResults
    }

    fun getGalleryDrive(customRealm: Realm? = null): ArrayList<File> {
        val operation: (Realm) -> ArrayList<File> = { realm ->
            realm.where(File::class.java).equalTo(File::id.name, Gallery.id).findFirst()?.let { galleryFolder ->
                realm.copyFromRealm(galleryFolder.children, 1) as ArrayList<File>
            } ?: arrayListOf()
        }
        return customRealm?.let(operation) ?: getRealmInstance().use(operation)
    }

    fun getRecentChanges(): ArrayList<File> {
        return getRealmInstance().use { realm ->
            realm.where(File::class.java).equalTo(File::id.name, RecentChanges.id).findFirst()?.let { folder ->
                ArrayList(realm.copyFromRealm(folder.children, 0))
            } ?: arrayListOf()
        }
    }

    fun storeRecentChanges(files: ArrayList<File>, isFirstPage: Boolean = false, userDrive: UserDrive) {
        getRealmInstance(userDrive).use {
            // Shared upsert boundary OUTSIDE the write transaction (issue: round 7 #6): an
            // activity-driven rename reaching Recent Changes must not replace the identity
            // before the owned copy at the old location is preserved; deferred files commit
            // their OLD row. The request's captured scope flows through realm and
            // preservation (issue: round 8 #4).
            val committableFiles = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(it, files, userDrive)
            // Coordinated commit boundary before the transaction (issue: round 10 #3).
            OfflineFileCoordination.withLocksFor(userDrive, listOf(RecentChanges.id) + committableFiles.map { file -> file.id }) {
                it.executeTransaction { realm ->
                    val folder = realm.where(File::class.java).equalTo(File::id.name, RecentChanges.id).findFirst()
                        ?: realm.copyToRealm(RecentChanges.file)
                    if (isFirstPage) {
                        folder.children = RealmList()
                    }
                    committableFiles.forEach { file ->
                        realm.where(File::class.java).equalTo(File::id.name, file.id).findFirst()?.let { realmFile ->
                            file.keepOldLocalFilesData(realmFile)
                        }
                        folder.children.add(file)
                    }
                }
            }
        }
    }

    fun storeFileActivities(fileActivities: ArrayList<FileActivity>, userDrive: UserDrive) {
        getRealmInstance(userDrive).use { realm ->
            // Coordinated commit boundary BEFORE the boundary AND the transaction (issues:
            // round 10 #3, round 11 #2): the activity rows cascade their embedded file rows,
            // so every affected file id joins the lock set across the shared upsert
            // boundary's validation/preservation AND the commit — one coordinated operation.
            // The ROOT-INTENT monitor precedes the lock set (issue: round 11 #7): an activity
            // can carry a folder rename/move, changing ancestry that coverage resolution
            // walks. Order: intent monitor → ascending file locks → transaction.
            val affectedFileIds = fileActivities.mapNotNull { fileActivity -> fileActivity.file?.id }
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withLocksFor(userDrive, affectedFileIds) {
                // Shared upsert boundary OUTSIDE the transaction (issue: round 7 #6): activity
                // files are upserted recursively; an owned copy at a renamed/moved old location
                // is preserved first, and a deferred activity keeps its OLD row. The request's
                // captured scope flows through realm and preservation (issue: round 8 #4).
                fileActivities.forEach { fileActivity ->
                    fileActivity.file?.let { file ->
                        fileActivity.file = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                            realm = realm,
                            remoteFiles = listOf(file),
                            userDrive = userDrive,
                        ).first()
                    }
                }
                realm.beginTransaction()
                fileActivities.forEach { fileActivity ->
                    fileActivity.userId = fileActivity.user?.id
                    val activityFile = fileActivity.file
                    val liveFile = activityFile?.let { file ->
                        realm.where(File::class.java).equalTo(File::id.name, file.id).findFirst()
                    }
                    // Conditional acceptance INSIDE the committing transaction (issue: round
                    // 11 #5): a delayed activity carrying an older generation of a file whose
                    // newer revision another writer already committed is skipped entirely —
                    // inserting it would cascade the stale embedded row over the live one.
                    if (activityFile == null || liveFile == null || !isStaleReplacement(liveFile, activityFile)) {
                        liveFile?.let { file -> activityFile?.keepOldLocalFilesData(file) }
                        realm.insertOrUpdate(fileActivity)
                    }
                }
                realm.commitTransaction()
                }
            }
        }
    }

    fun storeGalleryDrive(
        mediaList: List<File>,
        isFirstPage: Boolean = false,
        userDrive: UserDrive,
        customRealm: Realm? = null,
    ) {
        val block: (Realm) -> Unit = { currentRealm ->
            // Coordinated commit boundary BEFORE the boundary AND the transaction (issues:
            // round 10 #3, round 11 #2): the Gallery container and every incoming media row,
            // ascending, share the lock set across the shared upsert boundary's
            // validation/preservation AND the commit — one coordinated operation.
            OfflineFileCoordination.withLocksFor(userDrive, listOf(Gallery.id) + mediaList.map { media -> media.id }) {
                // Shared upsert boundary OUTSIDE the write transaction (issue: round 7 #6). The
                // request's captured scope flows through realm and preservation (issue: round 8 #4).
                val committableMedia = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(currentRealm, mediaList, userDrive)
                currentRealm.executeTransaction { realm ->
                    val galleryFolder = realm.where(File::class.java).equalTo(File::id.name, Gallery.id).findFirst()
                        ?: realm.copyToRealm(Gallery.file)
                    if (isFirstPage) galleryFolder.children = RealmList()
                    galleryFolder.children.addAll(realm, committableMedia)
                }
            }
        }
        customRealm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }

    private fun RealmList<File>.addAll(realm: Realm, files: List<File>) {
        files.forEach { file ->
            val managedFile = realm.where(File::class.java).equalTo(File::id.name, file.id).findFirst()
            // Conditional acceptance INSIDE the committing transaction (issue: round 11 #5):
            // a response that was fresh when its listing landed is revalidated against the
            // transaction-fresh row; a stale one does not relabel or roll it back — the live
            // row stays linked in the container instead of the incoming entry.
            if (managedFile != null && isStaleReplacement(managedFile, file)) {
                add(managedFile)
            } else {
                managedFile?.also { file.keepOldLocalFilesData(it) }
                add(file)
            }
        }
    }

    fun removeOrphanAndActivityFiles(customRealm: Realm? = null) {
        val block: (Realm) -> Unit = { realm ->
            realm.executeTransaction {
                realm.where(FileActivity::class.java).findAll().deleteAllFromRealm()
            }
            removeOrphanFiles(realm)
        }
        customRealm?.let(block) ?: getRealmInstance().use(block)
    }

    fun removeOrphanFiles(customRealm: Realm? = null) {
        val block: (Realm) -> Unit = { realm ->
            realm.executeTransaction {
                // Offline-owned rows (explicit pins, inherited coverage, pending markers) and the
                // ancestor chain they are reached through survive presentation-cache collection:
                // deleting them would strand durable offline copies and their metadata, which no
                // reconciliation can recover (issue: round 3 #15).
                val ownedRows = realm.where(File::class.java)
                    .beginGroup()
                    .equalTo(File::isOffline.name, true)
                    .or()
                    .equalTo(File::isInOfflineFolder.name, true)
                    .or()
                    .equalTo(File::isMarkedAsOffline.name, true)
                    .endGroup()
                    .greaterThan(File::id.name, ROOT_ID)
                    .findAll()

                val protectedIds = mutableSetOf<Int>()
                ownedRows.createSnapshot().forEach { owned ->
                    protectedIds.add(owned.id)
                    val visited = mutableSetOf(owned.id)
                    var parentId: Int = owned.parentId
                    while (parentId > ROOT_ID && visited.add(parentId)) {
                        protectedIds.add(parentId)
                        parentId = getFileById(realm, parentId)?.parentId ?: 0
                    }
                }

                realm.where(File::class.java)
                    .greaterThan(File::id.name, ROOT_ID)
                    .isEmpty(File::localParent.name)
                    .findAll()
                    .createSnapshot()
                    .filter { it.id !in protectedIds }
                    .forEach { it.deleteFromRealm() }
            }
        }
        customRealm?.let(block) ?: getRealmInstance().use(block)
    }

    fun getRealmLiveFiles(
        parentId: Int,
        realm: Realm,
        order: SortType?,
        withVisibilitySort: Boolean = true,
        isFavorite: Boolean = false
    ): RealmResults<File> {
        realm.refresh()
        return getRealmLiveSortedFiles(getFileById(realm, parentId), order, withVisibilitySort, isFavorite = isFavorite)
            ?: emptyList(realm)
    }

    fun getFilesFromIdList(realm: Realm, idList: Array<Int>, order: SortType = SortType.NAME_AZ): RealmResults<File>? {
        return realm.where(File::class.java)
            .oneOf(File::id.name, idList)
            .getSortQueryByOrder(order)
            .findAll()
    }

    fun saveRemoteFiles(
        realm: Realm,
        localFolderProxy: File?,
        remoteFolder: File?,
        apiResponse: CursorApiResponse<List<File>>,
        isFirstPage: Boolean,
        isCompleteFolder: Boolean,
        userDrive: UserDrive,
    ) {
        val remoteFiles = apiResponse.data ?: return

        // Save remote folder if it doesn't exist locally
        var newLocalFolderProxy: File? = null
        if (localFolderProxy == null && remoteFolder != null) {
            runCatching {
                realm.executeTransaction { newLocalFolderProxy = it.copyToRealm(remoteFolder) }
            }.onFailure { exception ->
                if (exception is RealmPrimaryKeyConstraintException) {
                    SentryLog.i("FileController", "Exception while adding a folder to Realm", exception)
                } else {
                    throw exception
                }
            }
        }
        // Coordinated commit boundary BEFORE the shared boundary AND the transaction (issues:
        // round 10 #3, round 11 #2): the folder row (its properties and children list are
        // identity-adjacent state) and every incoming child, ascending — one coordinated
        // operation spans the shared boundary's validation/preservation and the commit, so no
        // identity writer can interleave between a file's freshness validation and its commit.
        // The ROOT-INTENT monitor precedes the lock set (issue: round 11 #7): a listing can
        // re-parent a folder row (a subfolder moved into this folder), which changes the
        // ancestry that coverage resolution walks — the same intent protocol as cleanup, so
        // an ancestor move cannot commit between a coverage decision and a deletion. Order:
        // intent monitor → ascending file locks → transaction.
        synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withLocksFor(
                userDrive,
                listOfNotNull(localFolderProxy?.id, remoteFolder?.id) + remoteFiles.map { file -> file.id },
            ) {
            // Shared upsert boundary (issue: round 5 #5): owned copies at renamed/moved locations
            // are preserved BEFORE any metadata replacement — ordinary listings included, not only
            // the walk. Deferred files keep their OLD row in the commit list; a stale response is
            // rejected before any preservation side effect (issue: round 11 #4). This runs OUTSIDE
            // the write transaction below: reconciliation coordinates per-file ownership and
            // performs file I/O.
            val committableFiles = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                realm = realm,
                remoteFiles = remoteFiles,
                userDrive = userDrive,
            )
            realm.executeTransaction {
                // Restore same children data
                val acceptedChildren = keepSubFolderChildren(realm, localFolderProxy?.children, committableFiles)
                // Save to realm
                (localFolderProxy ?: newLocalFolderProxy)?.let { folderProxy ->
                    // Remove old children
                    if (isFirstPage) folderProxy.children.clear()
                    // Add children
                    folderProxy.children.addAll(acceptedChildren)
                    // Update folder properties
                    folderProxy.isComplete = isCompleteFolder
                    folderProxy.responseAt = apiResponse.responseAt
                    folderProxy.cursor = apiResponse.cursor
                    folderProxy.versionCode = BuildConfig.VERSION_CODE
                }
            }
        }
        }
    }

    /**
     * Applies the pre-commit child restoration INSIDE the listing transaction and returns the
     * children to link: a child whose response is stale against the transaction-fresh live row
     * (issue: round 11 #5) is substituted by that live row — the container keeps referencing
     * the newer generation instead of the delayed response's, and the stale metadata never
     * relabels it.
     */
    private fun keepSubFolderChildren(
        realm: Realm,
        localFolderChildren: List<File>?,
        remoteFolderChildren: List<File>,
    ): List<File> {
        val oldChildren = localFolderChildren?.associateBy { it.id }
        return remoteFolderChildren.map { newFile ->
            newFile.sortedName = newFile.sortedName.lowercase().removeAccents()

            val oldChild = oldChildren?.get(newFile.id) ?: getFileById(realm, newFile.id)
            if (oldChild != null && isStaleReplacement(oldChild, newFile)) {
                oldChild
            } else {
                oldChild?.let { oldFile ->
                    newFile.keepOldLocalFilesData(oldFile)
                }

                // If the new file is an external import, start MQTT listening to receive its status updates.
                if (newFile.isImporting()) MqttClientWrapper.start(newFile.externalImport?.id)
                newFile
            }
        }
    }

    private fun getRealmLiveSortedFiles(
        localFolder: File?,
        order: SortType?,
        withVisibilitySort: Boolean = true,
        localChildren: RealmResults<File>? = null,
        isFavorite: Boolean = false
    ): RealmResults<File>? {
        val children = localChildren ?: localFolder?.children
        return children?.where()
            ?.apply {
                order?.let {
                    getSortQueryByOrder(it)
                    if (isFavorite) equalTo(File::isFavorite.name, true)
                    if (withVisibilitySort) sort(File::visibility.name, Sort.DESCENDING)
                    sort(File::type.name, Sort.ASCENDING)
                    distinct(File::id.name)
                }
            }?.findAll()
    }

    fun getLocalSortedFolderFiles(
        localFolder: File?,
        order: SortType,
        localChildren: RealmResults<File>? = null,
        currentRealm: Realm? = null
    ): ArrayList<File> {
        val files = getRealmLiveSortedFiles(localFolder, order, localChildren = localChildren)?.let { realmFiles ->
            localFolder?.realm?.copyFromRealm(realmFiles, 1) ?: currentRealm?.copyFromRealm(realmFiles, 1)
        }
        return files?.let { ArrayList(it) } ?: arrayListOf()
    }

    fun getOfflineFiles(
        order: SortType?,
        userDrive: UserDrive = UserDrive(),
        customRealm: Realm? = null
    ): RealmResults<File> {
        val block: (Realm) -> RealmResults<File> = { realm ->
            // Explicit pins and files covered by an offline folder root (issue #1902) are both
            // maintained by the offline sync.
            realm.where(File::class.java)
                .beginGroup()
                .equalTo(File::isOffline.name, true)
                .or()
                .equalTo(File::isInOfflineFolder.name, true)
                .endGroup()
                .notEqualTo(File::type.name, Type.DIRECTORY.value)
                .findAll()?.let { files ->
                    if (order == null) files
                    else getRealmLiveSortedFiles(localFolder = null, order = order, localChildren = files)
                } ?: emptyList(realm)
        }
        return customRealm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }

    fun searchFiles(
        query: String,
        order: SortType,
        userDrive: UserDrive = UserDrive(),
        customRealm: Realm? = null
    ): ArrayList<File> {
        val block: (Realm) -> ArrayList<File> = { currentRealm ->
            currentRealm.where(File::class.java).like(File::name.name, "*$query*").findAll()?.let { files ->
                getLocalSortedFolderFiles(null, order, files, currentRealm)
            } ?: arrayListOf()
        }
        return customRealm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }

    private fun updateFileFromActivity(realm: Realm, remoteFile: File, folderId: Int, userDrive: UserDrive) {
        // Branch split on row EXISTENCE only (the conditional commit's result must not select
        // the child-add path): a stale response keeps the live row and must not re-add it as a
        // child of the container.
        val localFile = getFileProxyById(remoteFile.id, customRealm = realm)
        if (localFile != null) {
            // Authoritative-change commit (activity event): generation-only gate (issue:
            // round 11 #4) — an identity difference at equal revisions is the rename being
            // applied, not a rollback.
            insertOrUpdateFile(
                realm,
                remoteFile,
                localFile,
                userDrive,
                // Must exist — this branch already resolved the live row (issue: round 12 #2).
                acceptIf = { live -> live != null && !isSupersededGeneration(live, remoteFile) },
            )
        } else {
            // Coordinated commit boundary (issues: round 10 #3, round 11 #3/#7): the child-add
            // is an identity mutation of the new child and of the container's children list,
            // and it establishes ancestry — the root-intent monitor precedes the file locks.
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withLocksFor(userDrive, listOf(folderId, remoteFile.id)) {
                    realm.executeTransaction {
                        realm.where(File::class.java).equalTo(File::id.name, folderId).findFirst()?.children?.add(remoteFile)
                    }
                }
            }
        }
    }

    fun addFileTo(parentFolderId: Int, file: File, userDrive: UserDrive? = null) {
        getRealmInstance(userDrive).use { realm ->
            val localFolder = realm.where(File::class.java).equalTo(File::id.name, parentFolderId).findFirst()
            if (localFolder != null) {
                realm.executeTransaction {
                    localFolder.children.add(file)
                }
            }
        }
    }

    suspend fun saveRemoteFileToDb(
        remoteFile: File,
        userDrive: UserDrive,
        okHttpClient: OkHttpClient = AuthHttpClientProvider.authOkHttpClient,
    ) = Dispatchers.IO {
        getRealmInstance(userDrive).use { realm ->
            // Shared upsert boundary (issue: round 6 #4): preserve the owned copy at the old
            // location before the metadata replacement; a deferred reconciliation commits the
            // OLD row instead of the new identity.
            val preservedFile = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                realm = realm,
                remoteFiles = listOf(remoteFile),
                userDrive = userDrive,
            ).first()
            val localFile = getFileById(realm, preservedFile.id)
            // Conditional UPSERT (issue: round 12 #2): absence INSERTS (playback and deep-link
            // flows call this after a cache miss), while a stale response against an existing
            // newer row is rejected.
            val committed = insertOrUpdateFile(
                realm,
                preservedFile,
                localFile,
                userDrive,
                acceptIf = { live -> live == null || !isStaleReplacement(live, preservedFile) },
            )
            // Propagate rejection (issue: round 12 #2): a rejected response left the newer
            // live row authoritative — continuing the ancestor linking would attach the LIVE
            // row under the STALE response's parent, corrupting the graph with the loser's
            // ancestry.
            if (committed) {
                val driveId = userDrive.driveId
                resolveAndLinkFileAncestorsChain(realm, preservedFile.parentId, preservedFile.id, driveId, userDrive, okHttpClient)
            }
        }
    }

    private tailrec fun resolveAndLinkFileAncestorsChain(
        realm: Realm,
        parentId: Int,
        childId: Int,
        driveId: Int,
        userDrive: UserDrive,
        okHttpClient: OkHttpClient,
    ) {
        if (parentId == 0) return
        if (tryLinkToExistingParent(realm, parentId, childId)) return

        val remoteParent = saveRemoteFileAncestorToDb(realm, parentId, childId, driveId, userDrive, okHttpClient) ?: return
        resolveAndLinkFileAncestorsChain(realm, remoteParent.parentId, parentId, driveId, userDrive, okHttpClient)
    }

    private fun saveRemoteFileAncestorToDb(
        realm: Realm,
        parentId: Int,
        childId: Int,
        driveId: Int,
        userDrive: UserDrive,
        okHttpClient: OkHttpClient,
    ): File? {
        val remoteParent = getRemoteFile(fileId = parentId, driveId = driveId, okHttpClient = okHttpClient) ?: return null
        insertOrUpdateFile(realm, remoteParent, userDrive = userDrive)
        linkChildToParent(realm, parentId = parentId, childId = childId)
        return remoteParent
    }

    private fun tryLinkToExistingParent(realm: Realm, parentId: Int, childId: Int): Boolean {
        return hasFile(realm, parentId).also { _ ->
            linkChildToParent(realm = realm, parentId = parentId, childId = childId)
        }
    }

    private fun linkChildToParent(realm: Realm, parentId: Int, childId: Int) {
        getFileById(realm, childId)?.let { childProxy ->
            addChild(localFolderId = parentId, newFile = childProxy, realm = realm)
        }
    }

    suspend fun createFolder(name: String, parentId: Int, onlyForMe: Boolean, userDrive: UserDrive?): ApiResponse<File> {
        val okHttpClient =
            userDrive?.userId?.let { AccountUtils.getHttpClient(it) } ?: AuthHttpClientProvider.authOkHttpClient
        val driveId = userDrive?.driveId ?: AccountUtils.currentDriveId
        return ApiRepository.createFolder(okHttpClient, driveId, parentId, name, onlyForMe)
    }

    suspend fun createCommonFolder(name: String, forAllUsers: Boolean, userDrive: UserDrive?): ApiResponse<File> {
        val okHttpClient =
            userDrive?.userId?.let { AccountUtils.getHttpClient(it) } ?: AuthHttpClientProvider.authOkHttpClient
        val driveId = userDrive?.driveId ?: AccountUtils.currentDriveId
        return ApiRepository.createTeamFolder(okHttpClient, driveId, name, forAllUsers)
    }

    private fun File.keepOldLocalFilesData(oldFile: File) {
        // We update the children here, as this method is called from different places in the app.
        oldFile.children.forEach { it.sortedName = it.sortedName.lowercase().removeAccents() }
        children = oldFile.children
        cursor = oldFile.cursor
        isComplete = oldFile.isComplete
        isOffline = oldFile.isOffline
        isInOfflineFolder = oldFile.isInOfflineFolder
        responseAt = oldFile.responseAt
        if (path.isEmpty() && oldFile.path.isNotEmpty()) path = oldFile.path
        // NO revisedAt transplant (issue: round 11 #4): the incoming row carries its own
        // server revision; copying the live row's newer timestamp onto a stale incoming
        // object would LABEL old content as the newer generation — the publication
        // comparison and every later conditional acceptance would then treat that stale
        // metadata as current. Stale responses are rejected by the acceptance gate before
        // this point instead.
        if (oldFile.lastActionAt > lastActionAt) lastActionAt = oldFile.lastActionAt
        if (oldFile.updatedAt > updatedAt) updatedAt = oldFile.updatedAt
        versionCode = oldFile.versionCode
        isMarkedAsOffline = oldFile.isMarkedAsOffline
    }

    private fun RealmQuery<File>.getSortQueryByOrder(order: SortType): RealmQuery<File> {
        return when (order) {
            SortType.NAME_AZ -> sort(File::sortedName.name, Sort.ASCENDING)
            SortType.NAME_ZA -> sort(File::sortedName.name, Sort.DESCENDING)
            SortType.OLDER -> sort(File::lastModifiedAt.name, Sort.ASCENDING)
            SortType.RECENT -> sort(File::lastModifiedAt.name, Sort.DESCENDING)
            SortType.OLDEST_ADDED -> sort(File::addedAt.name, Sort.ASCENDING)
            SortType.MOST_RECENT_ADDED -> sort(File::addedAt.name, Sort.DESCENDING)
            SortType.OLDER_TRASHED -> sort(File::deletedAt.name, Sort.ASCENDING)
            SortType.RECENT_TRASHED -> sort(File::deletedAt.name, Sort.DESCENDING)
            SortType.SMALLER -> sort(File::size.name, Sort.ASCENDING)
            SortType.BIGGER -> sort(File::size.name, Sort.DESCENDING)
            // Because we don't know how to compute relevance here, we fallback
            // on the 'last modified' sorting for this pretty unusual case.
            SortType.LEAST_RELEVANT -> sort(File::lastModifiedAt.name, Sort.ASCENDING)
            SortType.MOST_RELEVANT -> sort(File::lastModifiedAt.name, Sort.DESCENDING)
            // SortType.EXTENSION -> sort(File::convertedType.name, Sort.ASCENDING) // TODO: Implement
        }
    }

    fun updateExternalImport(driveId: Int, importId: Int, action: MqttAction) {
        when (action) {
            MqttAction.EXTERNAL_IMPORT_FINISHED -> FileExternalImportStatus.DONE
            MqttAction.EXTERNAL_IMPORT_CANCELED -> FileExternalImportStatus.CANCELED
            MqttAction.EXTERNAL_IMPORT_ERROR -> FileExternalImportStatus.FAILED
            else -> null
        }?.let { status -> updateExternalImportStatus(driveId, importId, status) }
    }

    fun updateExternalImportStatus(driveId: Int, importId: Int, newStatus: FileExternalImportStatus) {
        getRealmInstance(UserDrive(driveId = driveId)).use { realm ->
            realm.where(File::class.java)
                .equalTo("${File::externalImport.name}.${FileExternalImport::id.name}", importId)
                .findFirst()?.let { file -> realm.executeTransaction { file.externalImport?.status = newStatus.value } }
        }
    }

    fun getPrivateFolder(): File? {
        return getRealmInstance().use { realm ->
            realm.where(File::class.java)
                .equalTo(File::visibility.name, IS_PRIVATE_SPACE)
                .findFirst()
                ?.let {
                    realm.copyFromRealm(it, Int.MAX_VALUE)
                }
        }
    }
}
