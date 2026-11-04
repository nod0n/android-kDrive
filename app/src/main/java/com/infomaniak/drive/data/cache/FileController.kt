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

    /** Ancestry resolutions per gallery pass (issue: round 19 #27) — bounded, retried next refresh. */
    private const val GALLERY_ANCESTRY_RESOLUTION_BUDGET = 4

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
            getFileById(currentRealm, fileId)?.let { file ->
                // The AUTHORITATIVE parent relationship resolves the proxy (issues: round 13
                // #12, round 14 #11): the backlink matching parentId wins, a missing backlink
                // resolves BY ID from the KNOWN parent — every positive id, ROOT included —
                // and when that parent is absent the resolution returns UNAVAILABLE rather
                // than trusting a stale extra backlink. The first-positive backlink is the
                // legacy fallback ONLY for rows without a known parent id (virtual
                // ancestry).
                file.localParent?.firstOrNull { it.id == file.parentId && it.id > 0 }
                    ?: file.parentId.takeIf { it > 0 }?.let { getFileById(currentRealm, it) }
                    ?: file.localParent?.firstOrNull { it.id > 0 }?.takeIf { file.parentId <= 0 }
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
            // Ancestry coordination + TRANSACTION-FRESH derivation (issue: round 12 #7): the
            // previous check derived the path from a pre-lock, un-refreshed Realm view, so a
            // move committed during the lock wait compared against old ancestry A and then
            // wrote A's path over ancestry B's row. Ancestor changes also require more than
            // the leaf's lock — the root-intent monitor serializes with ancestry writers.
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withFileLock(userDrive, fileId) {
                    runCatching { realm.refresh() }
                    var persisted = false
                    realm.executeTransaction {
                        val liveRow = getFileById(realm, fileId) ?: return@executeTransaction
                        // Persist only when the CURRENT transaction-fresh ancestry still
                        // derives exactly the captured candidate.
                        if (generatePath(liveRow, userDrive) == generatedPath) {
                            liveRow.path = generatedPath
                            persisted = true
                        }
                    }
                    persisted
                }
            }
        }
    }

    private tailrec fun generatePath(file: File, userDrive: UserDrive, recPath: StringBuilder = StringBuilder()): String {
        // The AUTHORITATIVE parent relationship generates the fallback path (issue: round 12
        // #12): a moved row can carry a stale extra backlink, and choosing the first
        // positive-id localParent would derive — and the path saver then persist — the
        // ABANDONED location's path. The parent identified by [File.parentId] wins; the
        // first positive-id backlink is only the legacy fallback.
        val parents = file.localParent?.createSnapshot()
        val folder = parents?.firstOrNull { it.id == file.parentId && it.id > 0 }
            // A known authoritative parent with NO matching backlink DEFERS the derivation
            // (issues: round 13 #12, round 14 #11): falling back to an inconsistent
            // real-folder backlink would derive the abandoned location's path. ROOT_ID is a
            // REAL parent id and defers like any other; rows without a known parent id
            // (parentId <= 0, virtual containers) keep the legacy first-positive fallback.
            ?: parents?.firstOrNull { it.id > 0 }?.takeIf { file.parentId <= 0 }
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
            // Explicit-pin writes join the ROOT-INTENT protocol (issue: round 15 #13) and
            // serialize with destructive cleanup on each file's coordination lock, taken
            // BEFORE the Realm write (issue: round 7 #9).
            val lockedBlock: (Realm) -> Unit? = { realm ->
                synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    filesId.forEach { fileId ->
                        OfflineFileCoordination.withFileLock(userDrive ?: UserDrive(), fileId) {
                            realm.executeTransaction {
                                markFileAsOfflineQuery(realm, fileId, isMarkedAsOffline, setExplicitPin)
                            }
                        }
                    }
                }
            }
            customRealm?.let(lockedBlock) ?: getRealmInstance(userDrive).use(lockedBlock)
            return
        }
        // Marker writes authorize publication and completion: they take the intent monitor
        // too (issue: round 15 #13).
        val block: (Realm) -> Unit? = { realm ->
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                realm.executeTransaction {
                    filesId.forEach { fileId ->
                        markFileAsOfflineQuery(realm, fileId, isMarkedAsOffline, setExplicitPin)
                    }
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
        originFolderId: Int? = null,
    ) {
        val block: (Realm) -> Unit? = { realm ->
            // Child recursion runs BEFORE this row's coordination (no nested lock
            // acquisition): each child re-runs the full protocol under its own section,
            // and the captured account flows through the recursion (issue: round 5 #6).
            // Every descendant carries the ORIGINAL removal boundary (issue: round 21 #5):
            // the top call's own id — so each recursive section can revalidate that its
            // row is still inside the subtree this removal was authorized for.
            if (recursive) {
                getFileById(realm, fileId)?.children?.createSnapshot()?.forEach {
                    if (!keepFiles.contains(it.id)) {
                        removeFile(
                            it.id,
                            keepFileCaches,
                            keepFiles,
                            realm,
                            userDrive = userDrive,
                            originFolderId = originFolderId ?: fileId,
                        )
                    }
                }
            }
            runCatching {
                // Lock-scope derivation from a read-only pre-read (a row's id/driveId
                // identity is stable); every DECISION below re-reads the row inside the
                // section. No row and no scope: nothing to remove.
                val lockScope = userDrive ?: getFileById(realm, fileId)?.driveId?.let { UserDrive(driveId = it) }
                    ?: return@runCatching
                // Removal is a destructive effect (issue: round 19 #7): the whole decision
                // section runs under the intent monitor → file lock → refresh → CURRENT row,
                // then byte effects only on confirmed current-path ownership. The previous
                // order read the row and its descendants BEFORE the lock and preserved or
                // deleted from that stale view.
                synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    OfflineFileCoordination.withFileLock(lockScope, fileId) {
                        // A refresh failure STOPS the removal (issue: round 17 #9 doctrine):
                        // preservation and deletion must never run on a stale Realm; the row
                        // stays and a later pass retries.
                        runCatching { realm.refresh() }.onFailure { return@withFileLock }
                        val file = getFileById(realm, fileId) ?: return@withFileLock
                        // ORIGINAL REMOVAL AUTHORITY (issue: round 21 #5): a recursive call
                        // is authorized by the ORIGINAL boundary's removal only while the
                        // row is still INSIDE that boundary's subtree — a child that moved
                        // out between its collection and its coordination (its recursive
                        // call waited on the locks) is no longer this removal's to delete:
                        // its current copy and row are another location's content now.
                        // Fresh path ownership alone never establishes deletion authority.
                        if (originFolderId != null && !ancestryReachesBoundary(realm, file.parentId, originFolderId)) {
                            return@withFileLock
                        }
                        val resolvedUserDrive = userDrive ?: UserDrive(driveId = file.driveId)
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
                            when (OfflineFolderController.preserveInheritedCopyBeforeRemoval(file, resolvedUserDrive, realm)) {
                                // Content safe (or nothing to preserve): bytes and row can go.
                                OfflineFolderController.PreservationOutcome.CONTENT_SAFE -> {
                                    val context = Realm.getApplicationContext()
                                    // CURRENT-PATH byte ownership (issue: round 19 #7): an
                                    // offline-scoped file row's bytes go only when the row still
                                    // EXCLUSIVELY owns its resolved path — a rename, move or
                                    // duplicate that committed while this removal waited leaves
                                    // the bytes to another identity's claim, and the byte
                                    // effects defer instead of deleting another row's copy.
                                    val offlinePath = context?.let {
                                        file.getOfflineFile(it, resolvedUserDrive.userId)?.path
                                    }
                                    val pathOwned = context == null || file.isFolder() ||
                                        !(file.isOffline || file.isInOfflineFolder) ||
                                        offlinePath == null ||
                                        OfflineFolderController.exclusivePathOwner(
                                            realm,
                                            context,
                                            resolvedUserDrive,
                                            offlinePath,
                                        ) == file.id
                                    if (pathOwned) {
                                        file.deleteCaches(context!!, resolvedUserDrive.userId)
                                        if (!keepFiles.contains(fileId) && !folderStillNeeded) {
                                            realm.executeTransaction { if (file.isValid) file.deleteFromRealm() }
                                        }
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
                }
            }.onFailure { exception ->
                Sentry.captureException(exception) { scope ->
                    scope.setExtra("with custom realm", "${customRealm != null}")
                    scope.setExtra("recursive", "$recursive")
                }
            }
            Unit
        }
        customRealm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }

    /**
     * Whether the live ancestry from [fromParentId] still reaches [boundaryId] (issue: round
     * 21 #5): the original removal authority covers only rows still inside its subtree.
     * Bounded walk over the live parent chain; a missing ancestor or the root ends it.
     */
    private fun ancestryReachesBoundary(realm: Realm, fromParentId: Int, boundaryId: Int): Boolean {
        var currentId: Int? = fromParentId
        var hops = 0
        while (currentId != null && currentId > 0 && hops < 64) {
            if (currentId == boundaryId) return true
            currentId = getFileById(realm, currentId)?.parentId
            hops++
        }
        return false
    }

    fun renameFile(
        file: File,
        newName: String,
        realm: Realm? = null,
        userDrive: UserDrive? = null,
    ): ApiResponse<CancellableAction> {
        // IMMUTABLE pre-HTTP request observation (issue: round 16 #7): the identity the rename
        // was issued against, captured before the request — a delayed successful response must
        // not apply an old requested name over a newer committed identity.
        val observedName = file.name
        val observedParentId = file.parentId
        val apiResponse = ApiRepository.renameFile(file, newName)
        if (apiResponse.isSuccess()) {
            // Preservation BEFORE the identity replacement (issue: round 6 #4): the owned copy
            // at the old name's path is reconciled to the new name first, and an incomplete
            // reconciliation keeps the OLD name until a later pass retries. The rename is an
            // ancestry-changing identity commit (issue: round 13 #5): the intent monitor
            // precedes the file lock, and both the preservation and the name write hold them.
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withFileLock(userDrive ?: UserDrive(), file.id) {
                    // Delayed-success validation (issue: round 16 #7): another rename/move that
                    // committed while this response was in flight owns the row's identity now —
                    // preserving toward the obsolete target and writing the old requested name
                    // would roll the row back. The stale operation defers; the authoritative
                    // current identity stays and a later listing/details pass reconciles.
                    if (renameRequestStillCurrent(file.id, observedName, observedParentId, userDrive, realm)) {
                        val preserved = preserveRenameBeforeMetadataChange(file, newName, userDrive, realm)
                        if (preserved) {
                            // The SAME immutable observation is rechecked INSIDE the
                            // committing transaction (issue: round 17 #9): a newer committed
                            // identity aborts the write instead of rolling it back.
                            updateFile(file.id, realm, userDrive = userDrive) { localFile ->
                                if (localFile.name == observedName && localFile.parentId == observedParentId) {
                                    localFile.name = newName
                                }
                            }
                        }
                    } else {
                        SentryLog.i(
                            "FileController",
                            "Deferring rename of ${file.id}: a newer identity committed while the rename response was in flight",
                        )
                    }
                }
            }
        }
        return apiResponse
    }

    /**
     * Whether the live row still carries the identity a rename/move request was issued
     * against (issue: round 16 #7): a missing row defers to the caller's own missing-row
     * handling, and a row whose name or parent changed since the request belongs to a newer
     * committed operation.
     */
    private fun renameRequestStillCurrent(
        fileId: Int,
        observedName: String,
        observedParentId: Int,
        userDrive: UserDrive?,
        customRealm: Realm?,
    ): Boolean {
        val block: (Realm) -> Boolean = renameBlock@{ realmInstance ->
            // REFRESH under the coordination the caller holds (issue: round 17 #9): the SAF
            // caller supplies a Realm opened before the HTTP request — without a refresh its
            // stale view passes the identity check while another rename/move has committed.
            runCatching { realmInstance.refresh() }.onFailure { return@renameBlock false }
            val liveRow = getFileProxyById(fileId, customRealm = realmInstance)
            liveRow == null || (liveRow.name == observedName && liveRow.parentId == observedParentId)
        }
        return customRealm?.let(block) ?: getRealmInstance(userDrive ?: UserDrive()).use(block)
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
            // Same refresh-before-source-resolution contract (issue: round 17 #9): a refresh
            // failure STOPS the rename — preservation must never run on a stale Realm.
            runCatching { realm.refresh() }.onFailure { return@renameBlock false }
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
            // Historical provenance anchored BEFORE the relocation (issue: round 17 #3).
            OfflineFolderController.anchorHistoricalBaseline(context, realm, liveRow, oldOffline, scope)
            OfflineFolderController.reconcileMovedInheritedCopy(
                context = context,
                oldOfflineFile = oldOffline,
                oldIdentityName = liveRow.name,
                oldIdentityParentId = liveRow.parentId,
                // The bytes' provenance baseline (issue: round 16 #2), never bare server
                // metadata a replacement may have moved away from the bytes.
                oldLastModifiedAt = liveRow.effectivePublishedBaseline,
                newIdentity = snapshotIdentity,
                userDrive = scope,
                newOfflineTarget = newOffline,
                realm = realm,
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

    fun upsertActionFile(
        realm: Realm,
        folderId: Int,
        actionFile: File,
        userDrive: UserDrive,
        actionAt: Long? = null,
        observed: File? = null,
    ): Boolean {
        // ONE caller-level coordinated operation (issue: round 13 #2): the intent monitor
        // precedes the ordered lock set, and the shared boundary's validation/preservation
        // runs INSIDE it — reentrant — immediately before the commit, with no lock release
        // between them. Authoritative-change flow (the activity event IS the change): the
        // action gate — newer generations AND newer non-Sync metadata reject the activity,
        // and its nullable timestamp bounds the row's activity cursor (issue: round 13 #3);
        // the request-observed anchor resolves ordering ties (issue: round 15 #10).
        var handled = true
        synchronized(OfflineFileCoordination.rootIntentMonitor) {
            // COMPLETE RESULTING MEMBERSHIP DECIDED BEFORE PRESERVATION (issue: round 18
            // #9): the payload's own parentId is the authoritative resulting location, and
            // the decision runs FIRST — with the destination's lock taken into the same
            // ascending set — so preservation never moves or snapshots bytes for a payload
            // the membership decision then rejects. The previous order preserved FIRST and
            // checked `actionFile.parentId == folderId` inside the adding transaction: a
            // fresh F payload whose resulting parent was Q moved F's bytes to Q, then the
            // membership check rejected the insertion and the old metadata stayed at its
            // prior location.
            val membershipOk = actionFile.parentId == folderId
            val destinationContainerId = if (membershipOk) null else actionFile.parentId
            // A mismatched payload whose destination row is absent has no container to link
            // into: defer WITHOUT side effects — no preservation runs (issue: round 18 #9).
            // COMPLETE FRESH DESTINATION ANCESTRY BEFORE PRESERVATION (issue: round 21 #6):
            // the container row's EXISTENCE is not a complete chain — anchoring, snapshot
            // and relocation must never run for a destination whose ancestry is missing
            // locally (an unchanged-path dirty snapshot previously registered before the
            // ancestry rejection); this deferral is side-effect-free and the resolving pass
            // repairs the chain.
            val destinationReady = if (membershipOk) {
                destinationChainExists(realm, folderId)
            } else {
                getFileById(realm, destinationContainerId ?: 0) != null &&
                    destinationChainExists(realm, destinationContainerId ?: 0)
            }
            if (!destinationReady) {
                handled = false
            } else OfflineFileCoordination.withLocksFor(
                userDrive,
                buildList {
                    add(folderId)
                    add(actionFile.id)
                    destinationContainerId?.let(::add)
                },
            ) {
                val preservedActionFile = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                    realm = realm,
                    remoteFiles = listOf(actionFile),
                    userDrive = userDrive,
                    isStale = { live, incoming, _ ->
                        FileController.isStaleForAuthoritativeAction(live, incoming, actionAt, observed)
                    },
                ).first()
                // A boundary substitution means the activity's own identity did not take —
                // preservation or an ambiguous tie deferred it (issue: round 15 #10). The
                // deferred action performs NO further write (issue: round 16 #4): the raw
                // payload must not be inserted past a rejected/substituted boundary — the
                // original payload describes the revision the boundary refused to apply.
                if (preservedActionFile !== actionFile) handled = false
                if (handled && !membershipOk) {
                    // ACCEPTED MOVE OUT (issue: round 18 #9): the payload is authoritative
                    // for F's resulting location — ONE transaction commits the payload's
                    // destination metadata, links it into the named container and removes
                    // the OBSOLETE backlink from this container (and any other prior one),
                    // under the same ascending locks. The previous existing-row branch
                    // (updateFileFromActivity) updated such a row without removing this
                    // container's obsolete backlink.
                    val destinationRow = getFileById(realm, destinationContainerId ?: return@withLocksFor)
                    if (destinationRow == null) {
                        handled = false
                    } else if (!destinationChainExists(realm, destinationContainerId ?: return@withLocksFor)) {
                        // COMPLETE FRESH ANCESTRY before the link (issue: round 19 #8): the
                        // container exists but an ancestor row does not — linking here
                        // strands the row's traversal (offline path resolution, deferred
                        // cleanup, orphan sweeps all walk parentId). Defer WITHOUT side
                        // effects; a later pass with the resolved ancestry re-applies.
                        handled = false
                    } else {
                        realm.executeTransaction {
                            val freshRow = getFileById(realm, actionFile.id)
                            val accepted = when {
                                freshRow == null -> true
                                else -> !FileController.isStaleForAuthoritativeAction(freshRow, actionFile, actionAt, observed)
                            }
                            if (accepted) {
                                val previousParentId = freshRow?.parentId
                                if (freshRow != null) actionFile.keepOldLocalFilesData(freshRow)
                                // The payload's METADATA commits even when the destination
                                // backlink already exists (issue: round 19 #9): the previous
                                // order committed the payload only through the list-add, so
                                // an existing backlink skipped the metadata upsert and the
                                // row kept its pre-move identity while the obsolete
                                // backlinks were removed.
                                it.insertOrUpdate(actionFile)
                                // SEPARATELY link and unlink (issue: round 19 #9): the
                                // backlink's presence decides only the LINK half; the
                                // obsolete backlinks are removed regardless.
                                if (destinationRow.children.none { it.id == actionFile.id }) {
                                    destinationRow.children.add(actionFile)
                                }
                                if (freshRow != null && previousParentId != null && previousParentId != destinationRow.id) {
                                    getFileById(realm, previousParentId)?.children
                                        ?.removeAll { it.id == actionFile.id }
                                }
                                getFileById(realm, folderId)?.children
                                    ?.removeAll { it.id == actionFile.id }
                            } else {
                                handled = false
                            }
                        }
                    }
                } else if (handled) realm.where(File::class.java).equalTo(File::id.name, folderId).findFirst()?.let { realmFolder ->
                    // COMPLETE FRESH ANCESTRY before the insert (issue: round 19 #8): the
                    // same-container path derives the bytes' location from THIS folder's
                    // chain — a missing ancestor yields a wrong derived path for the
                    // committed identity, and the row would strand beneath a broken
                    // traversal. Defer WITHOUT side effects.
                    if (!destinationChainExists(realm, folderId)) {
                        handled = false
                    } else
                    if (!realmFolder.children.contains(preservedActionFile) &&
                        // Parent-consistent membership (issue: round 13 #12): a
                        // boundary-substituted MANAGED row whose authoritative parent is NOT
                        // this container must not gain a wrong backlink here either; a fresh
                        // unmanaged action row keeps the upstream add semantics.
                        (!preservedActionFile.isManaged || preservedActionFile.parentId == folderId)
                    ) {
                        // Coordinated child-add (issues: round 11 #3, round 12 #12): the add
                        // mutates the new child's graph identity and the container's children
                        // list; the locks are already held by this section. The complete
                        // fresh action predicate applies INSIDE the adding transaction
                        // (issues: round 15 #4, round 16 #4) — the resulting membership was
                        // decided BEFORE preservation (issue: round 18 #9), so the rejected
                        // branch can no longer strand preserved bytes.
                        realm.executeTransaction {
                            val freshRow = getFileById(realm, actionFile.id)
                            val accepted = when {
                                freshRow == null -> true
                                else -> !FileController.isStaleForAuthoritativeAction(freshRow, actionFile, actionAt, observed)
                            }
                            if (accepted) {
                                val previousParentId = freshRow?.parentId
                                if (freshRow != null) actionFile.keepOldLocalFilesData(freshRow)
                                realmFolder.children.add(actionFile)
                                if (freshRow != null && previousParentId != folderId && previousParentId != null) {
                                    getFileById(realm, previousParentId)?.children
                                        ?.removeAll { it.id == actionFile.id }
                                }
                            } else {
                                handled = false
                            }
                        }
                    } else if (realmFolder.children.contains(preservedActionFile)) {
                        handled = updateFileFromActivity(realm, preservedActionFile, realmFolder.id, userDrive, actionAt, observed) && handled
                    }
                }
            }
        }
        return handled
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
        // The pin write joins the ROOT-INTENT protocol (issue: round 15 #13) and serializes
        // on the same per-file coordination the destructive cleanup holds, taken BEFORE the
        // Realm write, so a cleanup cannot delete a copy whose pin request is being written
        // (issue: round 6 #6).
        synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive ?: UserDrive(), fileId) {
                updateFile(fileId, userDrive = userDrive) { file ->
                    file.isOffline = isOffline
                    // A removed intent cannot be reauthorized by its stale pending marker
                    // (issue: round 15 #12): unpinning clears both.
                    if (!isOffline) file.isMarkedAsOffline = false
                }
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
     * refresh, graph imports). Beyond [isSupersededGeneration] and the [File.updatedAt]
     * ordering (issue: round 12 #3), an IDENTITY TIE — all ordering fields equal but name or
     * parent differing — is accepted ONLY when the live row still matches the
     * request-observed state [observed] exactly (issue: round 13 #1): a rename inside the
     * timestamp interval ties on every server timestamp, so a delayed response naming the
     * abandoned location is indistinguishable from a current one by timestamps alone.
     * Accepting such a tie let preservation register a current edit under the OBSOLETE
     * identity and delete the original — a misdirected SYNC_OFFLINE upload that no later
     * listing can undo. Without an observed basis the tie is rejected (deferred): renames
     * reach the row through the authoritative action path, and a deferral never spends
     * preservation side effects. With a basis, a tied rename against an UNCHANGED
     * observation is the server's current truth and is accepted.
     */
    internal fun isStaleReplacement(live: File, incoming: File, observed: File? = null): Boolean = when {
        isSupersededGeneration(live, incoming) -> true
        live.updatedAt > incoming.updatedAt -> true
        isSameIdentity(live, incoming) -> false
        // Identity difference the incoming row demonstrates as STRICTLY NEWER: a legitimate
        // rename/move committed after the live row's state — the observation requirement
        // applies to actual ordering TIES only (issue: round 14 #5); rejecting these blocked
        // ordinary listing/details progress forever.
        live.updatedAt < incoming.updatedAt -> false
        // Ordering tie with a differing identity: ambiguous — an unchanged request-time
        // observation accepts it, otherwise reject (issue: round 13 #1).
        observed == null || !isSameObservation(live, observed) -> true
        else -> false
    }

    /** Whether two rows describe the same identity/generation observation of one file. */
    internal fun isSameObservation(a: File, b: File): Boolean =
        a.revisedAt == b.revisedAt &&
            a.updatedAt == b.updatedAt &&
            (a.version?.number ?: 0) == (b.version?.number ?: 0) &&
            a.parentId == b.parentId &&
            a.name == b.name

    private fun isSameIdentity(live: File, incoming: File): Boolean =
        live.parentId == incoming.parentId && live.name == incoming.name

    /**
     * Whether an AUTHORITATIVE ACTION (the offline sync's rename/move/download/upload
     * registration, folder activities) is stale against the live row (issues: round 12 #5,
     * round 13 #3/#10): a strictly newer committed generation rejects it, and so does newer
     * metadata committed OUTSIDE Sync — listings and details can move a file without
     * advancing its activity cursor, so [File.updatedAt] ordering applies here too, not only
     * the Sync cursor. [actionAt] is the action's nullable ordering token; an unavailable
     * timestamp (null or 0, legacy data) applies no cursor bound — the generation and
     * updatedAt gates remain the fallback both before preservation and at commit.
     */
    internal fun isStaleForAuthoritativeAction(
        live: File,
        incoming: File,
        actionAt: Long?,
        observed: File? = null,
    ): Boolean = when {
        isSupersededGeneration(live, incoming) -> true
        live.updatedAt > incoming.updatedAt -> true
        // Identity difference the action demonstrates as STRICTLY NEWER: the move/rename the
        // event carries applies (issue: round 14 #2/#5).
        !isSameIdentity(live, incoming) && live.updatedAt < incoming.updatedAt -> false
        // Identity difference at an ordering tie: ambiguous — a delayed event against a
        // current row must NOT snapshot the row's edit under the event's identity; only an
        // unchanged request anchor (the state the action request was issued against) resolves
        // it (issues: round 13 #1, round 14 #2). An action response alone is not proof of
        // ordering.
        !isSameIdentity(live, incoming) -> observed == null || !isSameObservation(live, observed)
        // Same identity: the activity-cursor bound (issue: round 13 #3/#10).
        else -> actionAt != null && actionAt > 0 && live.lastActionAt >= actionAt
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
    fun updateExistingFileIfNotOlder(
        newFile: File,
        realm: Realm,
        userDrive: UserDrive,
        observed: File? = null,
    ): Boolean {
        return insertOrUpdateFile(
            realm = realm,
            newFile = newFile,
            oldFile = getFileById(realm, newFile.id),
            userDrive = userDrive,
            // Must EXIST (issue: round 12 #2): a worker refresh without a live row has nothing
            // to refresh — absence is rejection, not insertion. [observed] carries the
            // request-observed row state for tied-timestamp acceptance (issue: round 13 #1).
            acceptIf = { live -> live != null && !isStaleReplacement(live, newFile, observed) },
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
        requireActionAtOrAfter: Long? = null,
        observed: File? = null,
    ): Boolean {
        return insertOrUpdateFile(
            realm = realm,
            newFile = newFile,
            oldFile = getFileById(realm, newFile.id),
            userDrive = userDrive,
            // Must EXIST (issue: round 12 #2). The action bound is NULLABLE (issue: round 13
            // #10) — an unavailable timestamp applies no cursor constraint instead of a
            // zero bound that would reject every row with a positive cursor — and the gate
            // accounts for newer metadata committed by non-Sync writers via the updatedAt
            // ordering (issue: round 13 #3). The request ANCHOR is forwarded (issue: round
            // 14 #2): a tie the pre-preservation gate resolved through it must not be
            // re-rejected at the commit — that would defer a preserved rename forever.
            acceptIf = { live ->
                live != null && !isStaleForAuthoritativeAction(live, newFile, requireActionAtOrAfter, observed)
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

    fun addChild(
        localFolderId: Int,
        newFile: File,
        realm: Realm,
        userDrive: UserDrive? = null,
        revalidateExistingRow: Boolean = true,
    ) {
        // Authoritative-parent membership (issue: round 13 #12): a real-folder child addition
        // links a row only when the row's own parent relationship names THIS folder — the
        // linking caller's basis can be stale (the child may have moved while an ancestor
        // HTTP fetch was pending, after the child's lock was released), and a wrong backlink
        // would re-attach an abandoned location. The move caller sets parentId before
        // calling; the chain resolvers defer the link instead of forcing it.
        if (newFile.parentId != localFolderId) return
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
                    realm.executeTransaction {
                        // TRANSACTION-FRESH membership (issue: round 14 #11): re-read the
                        // committed row under the locks. EXISTING-ROW LINKING (the chain
                        // resolvers) requires the committed row to still name this container
                        // — its basis can predate another writer's move. An add that also
                        // COMMITS the child's post-move identity (the UI move) passes
                        // [revalidateExistingRow] = false: the passed object IS the intended
                        // new identity and the row still holds the source parent.
                        val freshChild = getFileById(realm, newFile.id)
                        val links = when {
                            freshChild == null -> true
                            !revalidateExistingRow -> true
                            else -> freshChild.parentId == localFolderId
                        }
                        if (links) runCatching { localFolder.children.add(newFile) }
                    }
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
                    if (offlineFile != null && oldFile.isOfflineAndReadable(appContext, offlineFile, userDrive)) {
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
            // round 10 #3, round 11 #2, round 12 #8): the container folder and every incoming
            // child, ascending, share the publication lock set across the shared upsert
            // boundary's validation/preservation AND the commit — one coordinated operation,
            // so no identity writer can interleave between a file's freshness validation and
            // its commit. The ROOT-INTENT monitor precedes the lock set: a Favorites/MyShares
            // replacement can reparent a folder row, changing ancestry that coverage
            // resolution walks.
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
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
            // HTTP FIRST (issue: round 13 #2): the parent response is fetched BEFORE any
            // preservation, so validation, preservation and the commit run in ONE coordinated
            // operation — the old order preserved the child's copy, released the lock, waited
            // for the second HTTP response, then committed, leaving a window where a newer
            // response could commit and strand the migrated bytes or an upload targeting
            // them. The intent monitor precedes the ordered lock set (issue: round 13 #5):
            // the parent row's commit can re-parent a folder.
            val parentResponse = ApiRepository.getFileDetails(File(id = remoteFile.parentId, driveId = userDrive.driveId)).data
                ?: return
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withLocksFor(userDrive, listOf(parentResponse.id, remoteFile.id)) {
                    // Validate the COMPLETE graph operation BEFORE child preservation (issue:
                    // round 14 #6): when the parent's fresh live row would REJECT the cascade
                    // (a newer committed parent), preserving the child's bytes toward A's
                    // location would strand them — the newer parent's own flow owns the child.
                    runCatching { realm.refresh() }
                    val parentOldRow = getFileById(realm, parentResponse.id)
                    if (parentOldRow != null && isStaleReplacement(parentOldRow, parentResponse)) {
                        return@withLocksFor
                    }
                    // Shared upsert boundary (issue: round 6 #4): the details cascade can also
                    // replace the file's identity; preserve the owned copy at the old location,
                    // inside the same coordination as the commit.
                    val preservedFile = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                        realm = realm,
                        remoteFiles = listOf(remoteFile),
                        userDrive = userDrive,
                    ).first()
                    // Authoritative-parent membership (issue: round 13 #12): the cascade may
                    // attach the child to A's children list only when the child's live
                    // authoritative parent still IS A — a row that moved away while the parent
                    // HTTP response was in flight must not gain a second, wrong backlink
                    // through the cascade (the same rule the folder listing's substitution
                    // applies). The narrowing to the single cascaded child is the established
                    // base design of this path.
                    parentResponse.children =
                        if (preservedFile.parentId == parentResponse.id) RealmList(preservedFile) else RealmList()
                    // The parent upsert CASCADES the embedded child row (issues: round 11 #3,
                    // round 13 #4): the PARENT's acceptance is also checked against the
                    // transaction-fresh row with its local fields preserved, and the accepted
                    // child is EXPLICITLY merged below (issue: round 14 #6) — the local-field
                    // preservation replaces this list with the parent's existing children and
                    // must not silently discard the cascade's child.
                    insertOrUpdateFile(
                        realm,
                        parentResponse,
                        parentOldRow,
                        userDrive = userDrive,
                        // Absence inserts — the cascade legitimately creates the missing
                        // ancestor (issue: round 13 #4). Unreachable-false under the held
                        // locks (the prospective gate above passed on the same row), kept as
                        // the conditional-commit contract.
                        acceptIf = { live -> live == null || !isStaleReplacement(live, parentResponse) },
                        moreTransaction = {
                            val liveChild = getFileById(realm, preservedFile.id)
                            val index = parentResponse.children.indexOfFirst { child -> child.id == preservedFile.id }
                            when {
                                // The boundary returned a live row that lives elsewhere — its
                                // replacement was rejected or deferred, so no link in this
                                // cascade: the row's membership stays with the parent its
                                // authoritative parentId names.
                                preservedFile.parentId != parentResponse.id ->
                                    parentResponse.children.removeAll { it.id == preservedFile.id }
                                // The child row does not exist yet: add the accepted cascade
                                // child back when its authoritative parent is this container.
                                liveChild == null ->
                                    if (index < 0) {
                                        parentResponse.children.add(preservedFile)
                                    }
                                // The ACCEPTED child commits explicitly with its current
                                // local fields preserved (issue: round 15 #5), then links
                                // under its RESULTING parent — an accepted move whose live
                                // row still names the old parent is committed and kept, not
                                // dropped, so metadata never points at the emptied old
                                // location. The child's acceptance is REVALIDATED inside the
                                // committing transaction (issue: round 16 #8): parent
                                // acceptance never authorizes a stale child replacement — a
                                // row that moved or advanced while the parent response was
                                // being applied keeps its current state, and the cascade
                                // drops its entry. The old child is substituted only when
                                // its replacement was actually rejected or deferred (the
                                // branch above).
                                else -> {
                                    if (isStaleReplacement(liveChild, preservedFile)) {
                                        parentResponse.children.removeAll { it.id == preservedFile.id }
                                    } else {
                                        preservedFile.keepOldLocalFilesData(liveChild)
                                        if (index >= 0) {
                                            parentResponse.children[index] = preservedFile
                                        } else {
                                            parentResponse.children.add(preservedFile)
                                        }
                                    }
                                }
                            }
                        },
                    )
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
                        // ONE caller-level coordinated operation (issue: round 13 #2): the
                        // HTTP response is already fetched; the intent monitor precedes the
                        // file lock (issue: round 13 #5 — the commit can reparent a
                        // directory), and validation, preservation and the conditional commit
                        // run before the lock is released.
                        synchronized(OfflineFileCoordination.rootIntentMonitor) {
                            OfflineFileCoordination.withFileLock(userDrive, fileId) {
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
                                    // Must exist — the details refresh has nothing to refresh
                                    // otherwise (issue: round 12 #2).
                                    acceptIf = { live -> live != null && !isStaleReplacement(live, preservedFile) },
                                )
                            }
                        }
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
        getRealmInstance(userDrive).use { currentRealm ->
            // One coordinated operation (issues: round 11 #2, round 12 #4/#8): the intent
            // monitor precedes the ascending lock set (a Recent activity can carry a folder
            // rename/move — ancestry that coverage resolution walks), the shared boundary's
            // validation/preservation runs INSIDE it, and every affected row's acceptance is
            // re-checked in the committing transaction — preservation can no longer commit
            // side effects for a response that a concurrent newer generation then rejects.
            // Recent Changes is a replacement boundary: the full acceptance rule applies.
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withLocksFor(
                    userDrive,
                    listOf(RecentChanges.id) + files.map { file -> file.id },
                ) {
                val committableFiles = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(currentRealm, files, userDrive)
                currentRealm.executeTransaction { realm ->
                    val folder = realm.where(File::class.java).equalTo(File::id.name, RecentChanges.id).findFirst()
                        ?: realm.copyToRealm(RecentChanges.file)
                    if (isFirstPage) {
                        folder.children = RealmList()
                    }
                    committableFiles.forEach { file ->
                        val liveFile = realm.where(File::class.java).equalTo(File::id.name, file.id).findFirst()
                        // Fresh acceptance inside the transaction (issue: round 12 #4): a
                        // delayed R1 response must not overwrite an R2 that committed during
                        // the coordination wait — the live row keeps its place instead.
                        if (liveFile != null && isStaleReplacement(liveFile, file)) {
                            folder.children.add(liveFile)
                        } else {
                            liveFile?.let { file.keepOldLocalFilesData(it) }
                            folder.children.add(file)
                        }
                    }
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
        // SUSPEND CLIENT ACQUISITION (round-19 build fix): the ancestor-resolution fetch
        // needs the account's HTTP client, whose lookup is suspending — the SUSPEND caller
        // acquires it once (outside every coordinated scope) and hands it in; this
        // non-suspend writer never bridges into a coroutine itself.
        okHttpClient: OkHttpClient,
    ) {
        val block: (Realm) -> Unit = { currentRealm ->
            // COMPLETE PROTOCOL for the Gallery commit (issue: round 18 #24): the ROOT-INTENT
            // monitor precedes the ordered lock set and holds through validation,
            // preservation and commit — the previous section took only file locks, so a
            // monitored ancestry decision (SAF holding the intent monitor, checking that D
            // has no owned descendants and deleting D) could interleave between this
            // section's reads and its commit, and the Gallery transaction then committed F
            // beneath the missing ancestor: its file lock does not serialize with the
            // ancestry decision. The realm is refreshed first, and every media row's
            // COMPLETE destination ancestry is validated BEFORE the preservation boundary —
            // media-file reparenting included, not only directory payloads.
            // OWNED-MOVE CLASSIFICATION (issue: round 19 #27) — a lock-free HINT read: a
            // row whose destination chain is broken and which carries durable offline
            // state requires ancestry; a NEW UNCACHED payload defers harmlessly. The
            // committing section below revalidates everything under the full protocol.
            runCatching { currentRealm.refresh() }
            val ownedMovesRequiringAncestry = mediaList.filter { media ->
                media.parentId > 0 && !destinationChainExists(currentRealm, media.parentId) &&
                    getFileById(currentRealm, media.id)?.let { existing ->
                        existing.isOffline || existing.isInOfflineFolder || existing.isMarkedAsOffline
                    } == true
            }
            // EXPLICIT ANCESTRY RESOLUTION for owned-move payloads (issues: round 19 #27,
            // round 17 #5 doctrine) — OUTSIDE the intent monitor and file locks: the
            // blocking details fetch must never retain them while another coordinated
            // writer waits. Each pass fetches the FIRST missing ancestor per payload
            // (details routes carry the expansion) and commits it through the shared
            // boundary (which takes its own locks), growing the chain toward the
            // destination. The budget bounds the pass; an unresolved payload keeps its
            // row (the re-link below) and retries on the next gallery refresh — never a
            // silent discard.
            var ancestryBudget = GALLERY_ANCESTRY_RESOLUTION_BUDGET
            ownedMovesRequiringAncestry.forEach { media ->
                if (ancestryBudget <= 0) return@forEach
                val missingAncestorId = firstMissingAncestorId(currentRealm, media.parentId) ?: return@forEach
                ancestryBudget--
                val ancestorDetails = runCatching {
                    ApiRepository.getFileDetails(
                        file = File(id = missingAncestorId, driveId = userDrive.driveId),
                        okHttpClient = okHttpClient,
                    )
                }.getOrNull()?.data?.takeIf { it.id == missingAncestorId } ?: return@forEach
                val committableAncestor = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                    currentRealm,
                    listOf(ancestorDetails),
                    userDrive,
                ).first()
                if (committableAncestor === ancestorDetails) {
                    updateExistingFileIfNotOlder(committableAncestor, currentRealm, userDrive)
                }
            }
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withLocksFor(userDrive, listOf(Gallery.id) + mediaList.map { media -> media.id }) {
                    runCatching { currentRealm.refresh() }
                    // DESTINATION ANCESTRY BEFORE PRESERVATION (issues: round 18 #24, round
                    // 19 #27): a row whose destination chain is broken is not committed
                    // this pass — its existing identity and gallery backlink survive in
                    // the re-link below, and a later gallery refresh retries the payload
                    // once the ancestor exists (the resolution above may have completed
                    // the chain since the hint classification).
                    val chainOkMedia = mediaList.filter { media ->
                        media.parentId <= 0 || destinationChainExists(currentRealm, media.parentId)
                    }
                    // Shared upsert boundary OUTSIDE the write transaction (issue: round 7 #6). The
                    // request's captured scope flows through realm and preservation (issue: round 8 #4).
                    val committableMedia = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(currentRealm, chainOkMedia, userDrive)
                    currentRealm.executeTransaction { realm ->
                        val galleryFolder = realm.where(File::class.java).equalTo(File::id.name, Gallery.id).findFirst()
                            ?: realm.copyToRealm(Gallery.file)
                        if (isFirstPage) galleryFolder.children = RealmList()
                        galleryFolder.children.addAll(realm, committableMedia)
                        val chainOkIds = chainOkMedia.map { media -> media.id }.toSet()
                        mediaList.filter { media -> media.id !in chainOkIds }.forEach { deferred ->
                            // Harmless new-uncached payloads have no row to re-link; an
                            // unresolved owned move re-links its EXISTING row so its
                            // identity and gallery presence survive to the next refresh.
                            realm.where(File::class.java).equalTo(File::id.name, deferred.id).findFirst()?.let { existing ->
                                if (galleryFolder.children.none { child -> child.id == existing.id }) {
                                    galleryFolder.children.add(existing)
                                }
                            }
                        }
                    }
                }
            }
        }
        customRealm?.let(block) ?: getRealmInstance(userDrive).use(block)
    }

    /**
     * Whether [parentId]'s COMPLETE ancestor chain currently exists (issue: round 18 #24):
     * every ancestor of a destination — not just the immediate parent, whose own ancestry
     * could be missing — must be present before a commit may link rows beneath it.
     */
    internal fun destinationChainExists(realm: Realm, parentId: Int): Boolean {
        var currentId: Int? = parentId
        val visited = mutableSetOf<Int>()
        while (currentId != null && currentId > 0) {
            if (!visited.add(currentId)) return false
            val row = getFileById(realm, currentId) ?: return false
            currentId = row.parentId
        }
        return true
    }

    /**
     * The id of the FIRST missing ancestor in [parentId]'s chain (issue: round 19 #27): the
     * explicit ancestry-resolution entry point — one details fetch per pass grows the
     * chain toward a destination whose payload requires ancestry. Null when the chain is
     * complete (or cyclic).
     */
    private fun firstMissingAncestorId(realm: Realm, parentId: Int): Int? {
        var currentId: Int? = parentId
        val visited = mutableSetOf<Int>()
        while (currentId != null && currentId > 0) {
            if (!visited.add(currentId)) return null
            val row = getFileById(realm, currentId) ?: return currentId
            currentId = row.parentId
        }
        return null
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

    /**
     * Explicit per-file commit outcomes (issue: round 14 #8): ACCEPTED ids are absent from
     * both sets; a DEFERRED id is unfinished reconciliation — an ambiguous identity tie or a
     * preservation deferral — and keeps the walk incomplete; a SUPERSEDED id is a PROVEN
     * newer live generation (the listing lost the race and the next listing refetches it).
     */
    data class ListingCommitOutcomes(val deferredIds: Set<Int>, val supersededIds: Set<Int>)

    /**
     * Commits a folder listing and returns the [ListingCommitOutcomes] of the children whose
     * row did NOT take the listing's identity at commit (issues: round 13 #8, round 14 #8).
     *
     * [observedById] carries the COMPLETE PRE-REQUEST observations (issue: round 16 #16): the
     * container's children as they stood before the listing HTTP request, detached. They anchor
     * tied identity changes through the shared boundary AND the in-transaction restoration —
     * without them a legitimate same-second directory rename was rejected on every pass, and
     * the offline sync (which excludes directories) provided no recovery route.
     */
    fun saveRemoteFiles(
        realm: Realm,
        localFolderProxy: File?,
        remoteFolder: File?,
        apiResponse: CursorApiResponse<List<File>>,
        isFirstPage: Boolean,
        isCompleteFolder: Boolean,
        userDrive: UserDrive,
        observedById: Map<Int, File> = emptyMap(),
    ): ListingCommitOutcomes {
        val remoteFiles = apiResponse.data ?: return ListingCommitOutcomes(emptySet(), emptySet())

        // Save remote folder if it doesn't exist locally
        var newLocalFolderProxy: File? = null
        if (localFolderProxy == null && remoteFolder != null) {
            runCatching {
                // Initial folder insertion establishes ancestry (issue: round 13 #5): the
                // intent monitor precedes it.
                synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    realm.executeTransaction { newLocalFolderProxy = it.copyToRealm(remoteFolder) }
                }
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
        // The outcome set outlives the coordination section: it is declared in the function
        // scope, filled inside the section and returned after it (issue: round 13 #8).
        val substitutedOrDroppedIds = mutableSetOf<Int>()
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
                observedById = observedById,
            )
            // Explicit per-file outcomes (issue: round 13 #8), part 1 — the shared boundary:
            // it maps the incoming list 1:1 and returns the OLD row in place of an entry it
            // deferred (stale gate or unfinished preservation), the SAME reference otherwise,
            // so reference identity detects every boundary substitution positionally.
            committableFiles.forEachIndexed { index, committable ->
                if (index < remoteFiles.size && committable !== remoteFiles[index]) {
                    substitutedOrDroppedIds.add(remoteFiles[index].id)
                }
            }
            realm.executeTransaction {
                // Restore same children data
                val acceptedChildren = keepSubFolderChildren(
                    realm,
                    localFolderProxy?.id ?: newLocalFolderProxy?.id,
                    localFolderProxy?.children,
                    committableFiles,
                    substitutedOrDroppedIds,
                    observedById,
                )
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
        // Explicit outcome classification (issue: round 14 #8): each non-accepted id is
        // judged from the committed live row against the LISTED row — a PROVEN newer
        // generation (server generation or strictly newer updatedAt) is superseded;
        // everything else (an ambiguous identity tie, a preservation deferral) stays
        // UNRESOLVED and keeps the reconciliation incomplete. Under this section's locks
        // the committed rows cannot change between the transaction and this read.
        val deferredIds = mutableSetOf<Int>()
        val supersededIds = mutableSetOf<Int>()
        for (id in substitutedOrDroppedIds) {
            val liveRow = getFileById(realm, id)
            val listed = remoteFiles.firstOrNull { it.id == id }
            if (liveRow == null || listed == null) {
                deferredIds.add(id)
            } else if (isSupersededGeneration(liveRow, listed) || liveRow.updatedAt > listed.updatedAt) {
                supersededIds.add(id)
            } else {
                deferredIds.add(id)
            }
        }
        return ListingCommitOutcomes(deferredIds, supersededIds)
    }

    /**
     * Applies the pre-commit child restoration INSIDE the listing transaction and returns the
     * children to link: a child whose response is stale against the transaction-fresh live row
     * (issue: round 11 #5) is substituted by that live row — but ONLY when the live row's
     * authoritative parent is still this container (issue: round 12 #12): a row that MOVED
     * away must not gain a second, wrong backlink, so the entry is dropped entirely and the
     * row's membership stays with the parent [File.parentId] names. The same parent
     * consistency governs boundary-substituted managed rows reaching the else branch. Every
     * substitution/drop is recorded in [substitutedOrDroppedIds] (issue: round 13 #8).
     */
    private fun keepSubFolderChildren(
        realm: Realm,
        containerId: Int?,
        localFolderChildren: List<File>?,
        remoteFolderChildren: List<File>,
        substitutedOrDroppedIds: MutableSet<Int>,
        observedById: Map<Int, File> = emptyMap(),
    ): List<File> {
        val oldChildren = localFolderChildren?.associateBy { it.id }
        return remoteFolderChildren.mapNotNull { newFile ->
            newFile.sortedName = newFile.sortedName.lowercase().removeAccents()

            val oldChild = oldChildren?.get(newFile.id) ?: getFileById(realm, newFile.id)
            // The pre-request observation resolves tied identity changes (issue: round 16 #16):
            // a legitimate same-second rename against an unchanged observation is the server's
            // current truth, not an ambiguous tie to defer forever.
            if (oldChild != null && isStaleReplacement(oldChild, newFile, observedById[newFile.id])) {
                // Explicit per-file outcomes (issue: round 13 #8), part 2 — the in-transaction
                // restoration: a substituted or dropped entry did not take the listing's
                // identity, whether the live row stays linked here or moved away.
                substitutedOrDroppedIds.add(newFile.id)
                oldChild.takeIf { it.parentId == containerId }
            } else if (newFile.isManaged && containerId != null && newFile.parentId != containerId) {
                substitutedOrDroppedIds.add(newFile.id)
                null
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

    private fun updateFileFromActivity(
        realm: Realm,
        remoteFile: File,
        folderId: Int,
        userDrive: UserDrive,
        actionAt: Long? = null,
        observed: File? = null,
    ): Boolean {
        // Branch split on row EXISTENCE only (the conditional commit's result must not select
        // the child-add path): a stale response keeps the live row and must not re-add it as a
        // child of the container.
        val localFile = getFileProxyById(remoteFile.id, customRealm = realm)
        if (localFile != null) {
            // Authoritative-change commit (activity event): the COMPLETE action predicate
            // (issue: round 14 #2) — generation AND newer non-Sync metadata AND the nullable
            // cursor bound, with the request anchor resolving ties (issue: round 15 #10) —
            // gates BOTH branches, including DIRECTORY rows the shared boundary skips (a
            // stale directory response must not restore obsolete ancestry). The commit can
            // REPARENT a folder (issue: round 12 #8), so the root-intent monitor precedes
            // the file lock.
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                return insertOrUpdateFile(
                    realm,
                    remoteFile,
                    localFile,
                    userDrive,
                    // Must exist — this branch already resolved the live row (issue: round 12 #2).
                    acceptIf = { live ->
                        live != null && !isStaleForAuthoritativeAction(live, remoteFile, actionAt, observed)
                    },
                )
            }
        } else {
            // Coordinated commit boundary (issues: round 10 #3, round 11 #3/#7): the child-add
            // is an identity mutation of the new child and of the container's children list,
            // and it establishes ancestry — the root-intent monitor precedes the file locks.
            // Parent-consistent membership (issue: round 14 #2) and the complete fresh action
            // predicate apply INSIDE the adding transaction (issue: round 15 #4): an
            // unmanaged response must not bypass them, and the current row's local fields
            // are preserved when accepting over it.
            if (remoteFile.parentId != folderId) return false
            var accepted = false
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withLocksFor(userDrive, listOf(folderId, remoteFile.id)) {
                    realm.executeTransaction {
                        val freshRow = getFileById(realm, remoteFile.id)
                        // No same-parent bypass (issue: round 16 #4) AND resulting-parent
                        // consistency independent of freshness (issue: round 17 #18): a
                        // payload naming another container never links here; an accepted
                        // move removes the obsolete backlink from its prior container.
                        val membershipOk = remoteFile.parentId == folderId
                        accepted = membershipOk && when {
                            freshRow == null -> true
                            else -> !isStaleForAuthoritativeAction(freshRow, remoteFile, actionAt, observed)
                        }
                        if (accepted && freshRow != null && freshRow.parentId != folderId) {
                            getFileById(realm, freshRow.parentId)?.children
                                ?.removeAll { it.id == remoteFile.id }
                        }
                        if (accepted) {
                            if (freshRow != null) remoteFile.keepOldLocalFilesData(freshRow)
                            realm.where(File::class.java).equalTo(File::id.name, folderId).findFirst()?.children?.add(remoteFile)
                        }
                    }
                }
            }
            return accepted
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
            // ONE caller-level coordinated operation (issue: round 13 #2): the HTTP response
            // is already fetched by the caller; the shared boundary's validation/preservation
            // and the conditional commit run inside ONE per-file section — the old order
            // released the boundary's lock before acquiring the commit's, leaving a window
            // where a newer response could strand migrated bytes or an upload targeting
            // them. The ancestor linking runs AFTER the section (it fetches further HTTP
            // responses and takes other rows' locks — holding this file's lock across those
            // acquisitions could cycle against ascending multi-lock commits).
            var linkedParentId = 0
            var linkedChildId = 0
            // The primary upsert joins the OUTER intent protocol (issue: round 14 #10): the
            // deep-link caller accepts directories, and inserting a missing directory row
            // ESTABLISHES ancestry — without the monitor, such an insert can commit while
            // cleanup holds the monitor and a descendant's lock, and newly covered bytes can
            // be deleted after cleanup's last check. Order: intent monitor → file lock.
            val committed = synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withFileLock(userDrive, remoteFile.id) {
                // Shared upsert boundary (issue: round 6 #4): preserve the owned copy at the old
                // location before the metadata replacement; a deferred reconciliation commits the
                // OLD row instead of the new identity.
                val preservedFile = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                    realm = realm,
                    remoteFiles = listOf(remoteFile),
                    userDrive = userDrive,
                ).first()
                val localFile = getFileById(realm, preservedFile.id)
                // Conditional UPSERT (issue: round 12 #2): absence INSERTS (playback and
                // deep-link flows call this after a cache miss), while a stale response
                // against an existing newer row is rejected.
                val accepted = insertOrUpdateFile(
                    realm,
                    preservedFile,
                    localFile,
                    userDrive,
                    acceptIf = { live -> live == null || !isStaleReplacement(live, preservedFile) },
                )
                linkedParentId = preservedFile.parentId
                linkedChildId = preservedFile.id
                accepted
                }
            }
            // Propagate rejection (issue: round 12 #2): a rejected response left the newer
            // live row authoritative — continuing the ancestor linking would attach the LIVE
            // row under the STALE response's parent, corrupting the graph with the loser's
            // ancestry.
            if (committed) {
                val driveId = userDrive.driveId
                resolveAndLinkFileAncestorsChain(realm, linkedParentId, linkedChildId, driveId, userDrive, okHttpClient)
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
        if (tryLinkToExistingParent(realm, parentId, childId, userDrive)) return

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
        // Conditional ancestor upsert (issue: round 13 #4): the HTTP fetch precedes it, the
        // transaction-fresh live row gates acceptance (a newer committed ancestor wins),
        // absence inserts (the chain is being built), and the live row's local fields are
        // preserved. The insert establishes ancestry — the intent monitor precedes the file
        // lock (issue: round 13 #5).
        val parentOldRow = getFileById(realm, remoteParent.id)
        synchronized(OfflineFileCoordination.rootIntentMonitor) {
            insertOrUpdateFile(
                realm,
                remoteParent,
                parentOldRow,
                userDrive = userDrive,
                acceptIf = { live -> live == null || !isStaleReplacement(live, remoteParent) },
            )
        }
        linkChildToParent(realm, parentId = parentId, childId = childId, userDrive = userDrive)
        return remoteParent
    }

    private fun tryLinkToExistingParent(realm: Realm, parentId: Int, childId: Int, userDrive: UserDrive): Boolean {
        return hasFile(realm, parentId).also { _ ->
            linkChildToParent(realm = realm, parentId = parentId, childId = childId, userDrive = userDrive)
        }
    }

    private fun linkChildToParent(realm: Realm, parentId: Int, childId: Int, userDrive: UserDrive) {
        getFileById(realm, childId)?.let { childProxy ->
            // The CAPTURED scope keys the locks (issue: round 14 #11): a scope-less default
            // would key another account's lock namespace after an account switch.
            addChild(localFolderId = parentId, newFile = childProxy, realm = realm, userDrive = userDrive)
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
        // LOCAL byte-provenance travels with the row (issue: round 15 #6): the baseline
        // describes the bytes at the row's path, which metadata acceptance does not replace
        // — only a successful publication advances it.
        publishedCleanBaseline = oldFile.publishedCleanBaseline
        responseAt = oldFile.responseAt
        if (path.isEmpty() && oldFile.path.isNotEmpty()) path = oldFile.path
        // NO version transplant (issue: round 15 #8): metadata equality — identity, both
        // server timestamps, even matching size and modification baseline — does not prove
        // identical CONTENT, and an inherited version N would falsely prove equality with an
        // in-flight transfer of the old generation. UNKNOWN version status is preserved: the
        // publication's both-known comparison and its exactly-one-known ambiguity clause
        // handle a null-version row safely (a version-bearing transfer against it DEFERS and
        // the version-bearing details routes restore the identifier on the retry), so no
        // timestamp equality ever manufactures a version identifier.
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
