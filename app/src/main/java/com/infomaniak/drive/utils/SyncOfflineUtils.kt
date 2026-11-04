/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2022-2025 Infomaniak Network SA
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
import android.net.Uri
import androidx.concurrent.futures.await
import androidx.work.Operation
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.R
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.cache.DriveInfosController
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.cache.OfflineFolderController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.FileActivityType
import com.infomaniak.drive.data.models.FileActivityType.FILE_MOVE_OUT
import com.infomaniak.drive.data.models.FileActivityType.FILE_RENAME
import com.infomaniak.drive.data.models.FileActivityType.FILE_RENAME_ALIAS
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.models.file.FileLastActivityBody
import com.infomaniak.drive.data.models.file.LastFileAction
import com.infomaniak.drive.utils.MediaUtils.deleteInMediaScan
import com.infomaniak.drive.utils.MediaUtils.isMedia
import com.infomaniak.drive.utils.SyncUtils.syncImmediately
import io.realm.Realm
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.ensureActive
import java.util.Date

object SyncOfflineUtils {

    /** Maximum number of files that can be sent to the api */
    private const val API_LIMIT_FILES_ACTION_BODY = 500
    private const val API_V3_ROOT_FOLDER_NAME = "Private"
    private const val API_V3_COMMON_DOCUMENTS_NAME = "Common documents"

    private val apiV3RootFolders = setOf(API_V3_ROOT_FOLDER_NAME, API_V3_COMMON_DOCUMENTS_NAME)

    private val renameActions = setOf(FILE_RENAME, FILE_RENAME_ALIAS, FILE_MOVE_OUT)

    /**
     * A cursor advance whose enqueue persistence is still unconfirmed (issues: round 8 #12,
     * round 9 #2): the sync awaits the WorkManager operation OUTSIDE the file Realm session —
     * no managed object or Realm instance survives the suspension — then revalidates on a
     * fresh Realm in the captured scope before advancing.
     */
    private class PendingCursorAdvance(
        val fileId: Int,
        val lastActionAt: Long,
        val operation: Operation,
    )

    /** Outcome of the download branch: handled, deferred, or handled with a pending advance. */
    private class DownloadActionOutcome(
        val handled: Boolean,
        val pendingOperation: Operation? = null,
    )

    suspend fun startSyncOffline(context: Context) = coroutineScope {
        // Delete all offline storage files prior to APIv3. For more info, see deleteLegacyOfflineFolder kDoc
        deleteLegacyOfflineFolder(context)
        DriveInfosController.getDrives(AccountUtils.currentUserId).forEach { drive ->
            ensureActive()
            val userDrive = UserDrive(driveId = drive.id)
            val pendingAdvances = mutableListOf<PendingCursorAdvance>()

            FileController.getRealmInstance(userDrive).use { realm ->
                // INFO(offline): We can get remote files when the local files path is empty to prevent sync bug
                val localFiles = FileController.getOfflineFiles(order = null, customRealm = realm)

                // The api doesn't support sending a list of files that exceeds a certain limit,
                // so we chunk the files in relation to this limit.
                localFiles.chunked(API_LIMIT_FILES_ACTION_BODY).forEach {
                    ensureActive()
                    processChunk(
                        context = context,
                        coroutineScope = this,
                        userDrive = userDrive,
                        // IMMUTABLE DETACHED observations (issue: round 15 #1): the map rows
                        // are the request anchors — managed objects would be refreshed
                        // together with the live rows inside the per-action coordination,
                        // collapsing the historical state into the current one and defeating
                        // the stale-action gate's tie resolution.
                        localFilesMap = it.associateBy({ file -> file.id }) { file ->
                            realm.copyFromRealm(file, 0)
                        },
                        realm = realm,
                        pendingAdvances = pendingAdvances,
                    )
                }

                // Reconcile the offline folder roots (issue #1902): resume interrupted walks and
                // periodically refresh the coverage so remote additions/deletions are reflected.
                ensureActive()
                OfflineFolderUtils.enqueueStaleOfflineFolderSyncs(context, userDrive, realm)
            }

            // Enqueue persistence is awaited AFTER the Realm session closed (issue: round 9 #2):
            // the await suspends and its continuation may resume on another IO thread, which a
            // thread-confined Realm (or its managed objects) must never survive. Each cursor
            // advance revalidates on a fresh Realm in the captured scope, so an action already
            // consumed by a concurrent sync neither rewinds nor duplicates the cursor; a failed
            // or cancelled await retains it.
            pendingAdvances.forEach { pending ->
                ensureActive()
                val persisted = try {
                    pending.operation.result.await()
                    true
                } catch (cancellation: CancellationException) {
                    throw cancellation
                } catch (exception: Exception) {
                    false
                }
                if (persisted) {
                    FileController.getRealmInstance(userDrive).use { freshRealm ->
                        freshRealm.executeTransaction {
                            FileController.getFileById(freshRealm, pending.fileId)?.let { live ->
                                if (live.lastActionAt < pending.lastActionAt) live.lastActionAt = pending.lastActionAt
                            }
                        }
                    }
                }
            }
        }
    }

    /**
     * After the migration from API V2 to API V3, offline files were saved in a different folder called "Private". Because we
     * cannot know if we're in a migration or not, we just delete old files and we marked previously isOffline files as
     * isMarkedAsOffline to let the BulkDownloadWorker redownload files.
     */
    private fun deleteLegacyOfflineFolder(context: Context) {
        val userDrive = UserDrive()
        val offlineFolder = IOFile(File.getOfflineFolder(context), "${userDrive.userId}/${userDrive.driveId}")
        offlineFolder.listFiles()?.forEach { file ->
            if (file.name !in apiV3RootFolders) file.deleteRecursively()
        }
    }

    private fun processChunk(
        context: Context,
        coroutineScope: CoroutineScope,
        userDrive: UserDrive,
        localFilesMap: Map<Int, File>,
        realm: Realm,
        pendingAdvances: MutableList<PendingCursorAdvance>,
    ) {
        if (localFilesMap.isNotEmpty()) {
            val fileActionsBody = localFilesMap.values.map { file ->
                FileLastActivityBody.FileActionBody(
                    id = file.id,
                    fromDate = file.lastActionAt.takeIf { it > 0 } ?: file.updatedAt,
                )
            }
            val lastFilesActions = ApiRepository.getFilesLastActivities(
                driveId = userDrive.driveId,
                body = FileLastActivityBody(files = fileActionsBody),
            ).data

            // When a local file changes without a corresponding fileAction, we need to synchronize it differently.
            // We store file IDs of the processed fileActions to track what has already been handled,
            // so we only need to process files that don't have a fileAction.
            val fileActionsIds = mutableSetOf<Int>()

            lastFilesActions?.forEach { fileAction ->
                coroutineScope.ensureActive()
                fileActionsIds.add(fileAction.fileId)
                handleFileAction(context, fileAction, localFilesMap, userDrive, realm, pendingAdvances)
            }

            // Check if any of the files that don't have fileActions require synchronization.
            handleFilesWithoutActions(context, localFilesMap, fileActionsIds, userDrive, realm, coroutineScope)
        }
    }

    private fun handleFilesWithoutActions(
        context: Context,
        localFilesMap: Map<Int, File>,
        fileActionsIds: Set<Int>,
        userDrive: UserDrive,
        realm: Realm,
        coroutineScope: CoroutineScope,
    ) {
        for (file in localFilesMap.values) {
            coroutineScope.ensureActive()
            if (fileActionsIds.contains(file.id)) continue
            val ioFile = file.getOfflineFile(context, userDrive.userId) ?: continue

            migrateOfflineIfNeeded(context, file, ioFile, userDrive)

            if (ioFile.lastModified() > file.revisedAtInMillis) {
                uploadFile(
                    context = context,
                    localFile = file,
                    remoteFile = null,
                    userDrive = userDrive,
                    realm = realm,
                )
            }
        }
    }

    private fun handleFileAction(
        context: Context,
        fileAction: LastFileAction,
        localFilesMap: Map<Int, File>,
        userDrive: UserDrive,
        realm: Realm,
        pendingAdvances: MutableList<PendingCursorAdvance>,
    ) {
        val localFile = localFilesMap[fileAction.fileId]
        val ioFile = localFile?.getOfflineFile(context, userDrive.userId) ?: return

        migrateOfflineIfNeeded(context, localFile, ioFile, userDrive)

        when (fileAction.lastAction) {
            FileActivityType.FILE_DELETE, FileActivityType.FILE_TRASH -> {
                // Deletion joins the per-action freshness protocol (issue: round 15 #3):
                // intent monitor → file lock → fresh Realm BEFORE any byte or coverage side
                // effect — an old trash response arriving after a restore, a move under
                // another active root, or a redownload must not delete the current clean
                // copy and coverage. When the action carries a file row, the complete
                // action predicate gates it (the detached [localFile] is the request
                // anchor); a legacy payload-less action applies the cursor bound alone, and
                // ambiguous authority defers (nothing is cleared).
                synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    OfflineFileCoordination.withFileLock(userDrive, fileAction.fileId) {
                        runCatching { realm.refresh() }
                        val freshRow = FileController.getFileById(realm, fileAction.fileId)
                            ?: return@withFileLock
                        val actionAt = fileAction.lastActionAt
                        val stale =
                            (fileAction.file != null && FileController.isStaleForAuthoritativeAction(
                                freshRow,
                                fileAction.file,
                                actionAt,
                                observed = localFile,
                            )) ||
                                (actionAt != null && actionAt > 0 && freshRow.lastActionAt >= actionAt)
                        if (stale) return@withFileLock
                        val freshIoFile = freshRow.getOfflineFile(context, userDrive.userId)
                        if (freshRow.isOffline) {
                            // Explicit pins keep the existing remote-wins behavior.
                            freshIoFile?.delete()
                        } else {
                            // Inherited copies (issue #1902) go through the same preservation
                            // policy as root removal: pending uploads and locally modified
                            // copies are protected (snapshotted into the upload queue when
                            // needed) before bytes are dropped. The coverage clearing
                            // reenters this section's monitor and lock.
                            if (freshIoFile == null) return@withFileLock
                            OfflineFolderController.clearFileCoverage(
                                context = context,
                                realm = realm,
                                file = freshRow,
                                userDrive = userDrive,
                                result = OfflineFolderController.RemovalResult(),
                                isFolder = false,
                            )
                        }
                    }
                }
            }
            else -> updateFile(context, ioFile, localFile, fileAction, userDrive, realm, pendingAdvances)
        }
    }

    private fun updateFile(
        context: Context,
        ioFile: IOFile,
        localFile: File,
        fileAction: LastFileAction,
        userDrive: UserDrive,
        realm: Realm,
        pendingAdvances: MutableList<PendingCursorAdvance>,
    ): Boolean {
        val remoteFile = fileAction.file
        if (remoteFile == null) {
            SentryLog.e("SyncOffline", "Expect remote file instead of null file") { scope ->
                scope.setExtra("fileAction", "${fileAction.lastAction}")
            }
            return false
        }
        var downloadOutcome = DownloadActionOutcome(handled = false)
        // ONE coordinated operation per action (issue: round 14 #1): the ROOT-INTENT monitor
        // precedes the file lock (the move branch re-parents), the Realm REFRESHES under
        // them, and the CURRENT row, byte baseline and action ordering select the branch
        // BEFORE any side effect. Every side effect and every commit re-applies the same
        // action predicate against the fresh row; an action is consumed only when its
        // rejection was side-effect-free. [localFile] is the request anchor (issue: round 14
        // #2): the actions were requested FROM each row's cursor, so a live row still
        // matching it resolves an otherwise ambiguous ordering tie.
        val actionAt = fileAction.lastActionAt
        val handled = synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive, localFile.id) {
                runCatching { realm.refresh() }
                val liveRow = FileController.getFileById(realm, localFile.id)
                // Stale-action consumption BEFORE any side effect, on the fresh row (issue:
                // round 12 #5 kept): a row already at or past this action has processed it.
                if (actionAt != null && actionAt > 0 && liveRow != null && liveRow.lastActionAt >= actionAt) {
                    true
                } else {
                    val ioFileLastModified = ioFile.lastModified()
                    when {
                        ioFileLastModified > remoteFile.revisedAtInMillis ->
                            uploadFile(context, localFile, remoteFile, userDrive, realm, actionAt)
                        ioFileLastModified < remoteFile.revisedAtInMillis -> {
                            downloadOutcome = downloadOfflineFile(context, localFile, remoteFile, userDrive, realm, actionAt)
                            downloadOutcome.handled
                        }
                        fileAction.lastAction in renameActions -> {
                            // Conditional acceptance BEFORE preservation side effects (issues:
                            // round 11 #2/#4, round 14 #1/#2): an action superseded by a newer
                            // generation, newer non-Sync metadata, or an unresolved identity
                            // tie must not move the bytes to its obsolete target; the anchor
                            // resolves legitimate tied renames.
                            when {
                                liveRow == null -> true
                                FileController.isStaleForAuthoritativeAction(liveRow, remoteFile, actionAt, observed = localFile) -> true
                                // FRESH source (issue: round 15 #2): the bytes live at the
                                // live row's current path — the pre-pass ioFile can be stale
                                // or reused by another file.
                                liveRow.getOfflineFile(context, userDrive.userId)?.let { freshSource ->
                                    freshSource.exists() && !OfflineFolderController.reconcileMovedInheritedCopy(
                                        context = context,
                                        oldOfflineFile = freshSource,
                                        oldIdentityName = localFile.name,
                                        oldIdentityParentId = localFile.parentId,
                                        oldLastModifiedAt = localFile.lastModifiedAt,
                                        newIdentity = remoteFile,
                                        userDrive = userDrive,
                                    )
                                } == true
                                ->
                                    // Preservation incomplete (pending upload, snapshot
                                    // failed, protected destination): keep the old bytes and
                                    // metadata, and do not consume the action so a later sync
                                    // retries it (issue: round 6 #11).
                                    false
                                else ->
                                    // Bytes are safely at (or durably registered for) the new
                                    // location: commit the new identity so the row and the
                                    // offline path agree. A rejected commit is PROPAGATED
                                    // (issue: round 13 #10) — under this section's lock the
                                    // gate and commit read the same row, so a false here
                                    // means the row vanished mid-section; the action stays
                                    // pending and the retry's ordering gates consume it.
                                    FileController.updateExistingFileIfNotSuperseded(
                                        newFile = remoteFile,
                                        realm = realm,
                                        userDrive = userDrive,
                                        requireActionAtOrAfter = actionAt,
                                        observed = localFile,
                                    )
                            }
                        }
                        else -> true
                    }
                }
            }
        }

        when {
            !handled -> {
                // The activity cursor is NOT advanced for deferred operations (issue: round 6 #11):
                // the action stays pending, so a later sync re-processes it instead of the retry
                // depending on another mechanism.
                SentryLog.i("SyncOffline", "Deferring file action ${fileAction.lastAction} for ${localFile.id}")
            }
            downloadOutcome.pendingOperation != null -> {
                // The cursor advance waits for the enqueue's WorkManager persistence OUTSIDE
                // this Realm session (issues: round 8 #12, round 9 #2): only detached values
                // (file id, action date, operation) cross the suspension.
                pendingAdvances += PendingCursorAdvance(
                    fileId = localFile.id,
                    lastActionAt = fileAction.lastActionAt ?: 0,
                    operation = downloadOutcome.pendingOperation,
                )
            }
            else -> FileController.updateFile(localFile.id, realm) { mutableFile ->
                // No cursor rewind (issue: round 12 #5): a delayed action completing after a
                // later one must never move the row's activity cursor backwards.
                if ((actionAt ?: 0) > mutableFile.lastActionAt) mutableFile.lastActionAt = actionAt ?: 0
            }
        }
        return handled
    }

    /**
     * Migrate old V1 offline files to the V2 offline structure
     */
    private fun migrateOfflineIfNeeded(context: Context, file: File, ioFile: IOFile, userDrive: UserDrive) {
        val offlineDir = context.getString(R.string.EXPOSED_OFFLINE_DIR)
        val oldPath = IOFile(context.filesDir, "$offlineDir/${userDrive.userId}/${userDrive.driveId}/${file.id}")
        if (oldPath.exists()) oldPath.renameTo(ioFile)
    }

    /**
     * Update the remote file with the local file
     */
    private fun uploadFile(
        context: Context,
        localFile: File,
        remoteFile: File?,
        userDrive: UserDrive,
        realm: Realm,
        actionLastActionAt: Long? = null,
    ): Boolean {
        // Upload-source registration is a mutation owner of these bytes: it holds the shared
        // per-file coordination so a publication or cleanup cannot interleave (issue:
        // round 4 #4). The action predicate applies BEFORE the snapshot side effect (issue:
        // round 14 #1): a stale action must not register available bytes under an obsolete
        // identity — the anchor ([localFile], the state the action request was issued
        // against) resolves ordering ties.
        OfflineFileCoordination.withFileLock(userDrive, localFile.id) {
            runCatching { realm.refresh() }
            val liveRow = FileController.getFileById(realm, localFile.id)
            if (liveRow == null) {
                // The row is gone (removed by another flow): consume with NO side effect —
                // a later action or listing owns the identity.
                return true
            }
            // The ACTION path's fresh acceptance BEFORE the snapshot side effect (issue:
            // round 14 #1): a stale action must not register available bytes under an
            // obsolete identity — the anchor ([localFile], the state the action request was
            // issued against) resolves ordering ties. The no-action sweep ([remoteFile] ==
            // null) has no action identity to gate; it registers under the CURRENT row's
            // identity, read fresh under this lock.
            if (remoteFile != null &&
                FileController.isStaleForAuthoritativeAction(liveRow, remoteFile, actionLastActionAt, observed = localFile)
            ) {
                return true
            }
            // FRESH source resolution INSIDE coordination (issue: round 15 #2): the caller's
            // ioFile was resolved from the pre-pass row and its timestamps captured before
            // the lock — the live row may have moved meanwhile, and the old path can even be
            // reused by another file, so registering bytes read there under this file's
            // fresh identity would upload foreign content. Existence, size, modification
            // date and the upload decision are re-read here, for the action and no-action
            // branches alike.
            val freshSource = liveRow.getOfflineFile(context, userDrive.userId)
            if (freshSource == null || !freshSource.exists()) {
                // Nothing owned at the current location: consume with no side effect.
                return true
            }
            val uri = Uri.fromFile(freshSource)
            val fileModifiedAt = Date(freshSource.lastModified())
            if (!UploadFile.canUpload(uri, fileModifiedAt)) {
                // Not schedulable right now: the action stays pending for a later sync
                // (issues: round 6 #11, round 7 #4). With per-revision snapshot registration
                // this only trips for a legacy record still keyed by the original URI with
                // the same modification date.
                return false
            }
            // Revision dedup under the file lock (issue: round 8 #7): the identity of a
            // registration is the captured owner + content revision + destination (name,
            // folder, drive, user, modification date) — NOT the uri, since each registration
            // stores a fresh snapshot URI the legacy lookup above can never find. Without
            // this gate every no-action sync re-registered the same pending revision as a
            // new full snapshot, and a cancelled revision was silently reactivated on the
            // next sync.
            val registrationName = remoteFile?.name ?: liveRow.name
            val registrationFolder = remoteFile?.parentId ?: liveRow.parentId
            if (UploadFile.hasUploadRecordFor(
                    fileName = registrationName,
                    remoteFolder = registrationFolder,
                    driveId = userDrive.driveId,
                    userId = userDrive.userId,
                    fileModifiedAt = fileModifiedAt,
                    pendingOnly = false,
                ) || UploadFile.hasCancelledUploadRecordFor(
                    fileName = registrationName,
                    remoteFolder = registrationFolder,
                    driveId = userDrive.driveId,
                    userId = userDrive.userId,
                    fileModifiedAt = fileModifiedAt,
                )
            ) {
                // The revision is already registered — live (pending or sent) or retained
                // after a cancellation. The existing record keeps owning the snapshot bytes;
                // registering again would duplicate pending work or reactivate the
                // cancellation. The action is handled: durable ownership exists either way.
                return true
            }
            // Immutable per-revision source (issue: round 7 #4): ordinary offline edits used to
            // register the MUTABLE original URI as their primary identity — store() replaces
            // the row with that key, and an older upload finishing later marked and removed
            // whichever revision then occupied the URI, erasing the newer revision's durable
            // ownership without uploading it. Each registration now copies the bytes to a
            // unique durable snapshot and owns THAT identity, exactly like removal-time
            // snapshots: completion, error and cancellation all target the exact registered
            // generation, and the original-path copy stays protected through the record's
            // content identity (fileName, folder, modification date).
            val snapshotUri = UploadFile.createDurableOfflineSnapshot(
                context = context,
                source = freshSource,
                fileModifiedAt = fileModifiedAt,
                fileId = localFile.id,
                driveId = userDrive.driveId,
            ) ?: return false

            UploadFile(
                uri = snapshotUri.toString(),
                driveId = userDrive.driveId,
                fileModifiedAt = fileModifiedAt,
                // The record carries the POST-COMMIT identity (issue: round 7 #5): when the
                // same action renames/moves the file, uploading under the source name/parent
                // would recreate the old location remotely. The sweep path uses the FRESH
                // row's identity (issue: round 14 #1), not the pre-pass map's snapshot.
                fileName = remoteFile?.name ?: liveRow.name,
                fileSize = freshSource.length(),
                remoteFolder = remoteFile?.parentId ?: liveRow.parentId,
                type = UploadFile.Type.SYNC_OFFLINE.name,
                userId = userDrive.userId,
            ).store()

            if (remoteFile != null) {
                remoteFile.lastModifiedAt = freshSource.lastModified() / 1000
                remoteFile.size = freshSource.length()
                // Conditional advance (issues: round 12 #4, round 13 #3, round 14 #1): the
                // uploaded snapshot's baseline must not roll back a strictly newer generation
                // OR newer non-Sync metadata committed meanwhile; the action's nullable
                // timestamp bounds the cursor the same way as the other action commits. Under
                // this section's lock the acceptance gate and this commit read the same row,
                // so a rejection here means the row vanished mid-section: the snapshot's
                // durable ownership exists, the action is consumed, and the rejection is
                // logged rather than ignored.
                val advanced = FileController.updateExistingFileIfNotSuperseded(
                    newFile = remoteFile,
                    realm = realm,
                    userDrive = userDrive,
                    requireActionAtOrAfter = actionLastActionAt,
                    observed = localFile,
                )
                if (!advanced) {
                    SentryLog.i("SyncOffline", "Upload advance rejected for ${localFile.id}; snapshot ownership retained")
                }
            }

            context.syncImmediately(isAutomaticTrigger = false)
        }
        return true
    }

    /**
     * Update the local file with the remote file
     */
    private fun downloadOfflineFile(
        context: Context,
        file: File,
        remoteFile: File,
        userDrive: UserDrive,
        realm: Realm,
        actionLastActionAt: Long?,
    ): DownloadActionOutcome {
        // Conditional acceptance BEFORE any preservation side effect (issues: round 11 #2/#4,
        // round 12 #5, round 13 #3/#10): a refresh superseded by an already-committed NEWER
        // GENERATION, by newer non-Sync metadata (updatedAt ordering), or whose row's
        // activity cursor already moved past this action (when the action carries a
        // timestamp — legacy null applies no cursor bound) must not move the bytes or
        // relabel the live row; the newer generation's own flow owns this file's identity,
        // so the action is consumed with no side effect. The authoritative revalidation
        // still happens inside the committing transaction below.
        val liveRow = FileController.getFileById(realm, file.id)
        if (liveRow == null) {
            // The row is gone: nothing to refresh — consume.
            return DownloadActionOutcome(handled = true)
        }
        if (FileController.isStaleForAuthoritativeAction(liveRow, remoteFile, actionLastActionAt, observed = file)) {
            return DownloadActionOutcome(handled = true)
        }
        // FRESH row state and source path INSIDE the coordination (issue: round 15 #2): the
        // caller's ioFile, marker and flags were resolved from the pre-pass row — the bytes
        // live at the live row's current path and the marker/origin flags are the live
        // row's, not the detached observation's.
        val freshOffline = liveRow.getOfflineFile(context, userDrive.userId)
            ?: return DownloadActionOutcome(handled = false)
        val remoteOfflineFile = remoteFile.getOfflineFile(context, userDrive.userId)
            ?: return DownloadActionOutcome(handled = false)
        val pathChanged = freshOffline.path != remoteOfflineFile.path

        if (pathChanged) {
            // A pending download marker leaves this refresh to the queued worker, which runs
            // its own protection checks: moving or deleting the bytes here would race it. The
            // action stays pending for a later sync (issue: round 6 #11).
            if (liveRow.isMarkedAsOffline) return DownloadActionOutcome(handled = false)
            if (freshOffline.exists() && !OfflineFolderController.reconcileMovedInheritedCopy(
                    context = context,
                    oldOfflineFile = freshOffline,
                    oldIdentityName = file.name,
                    oldIdentityParentId = file.parentId,
                    oldLastModifiedAt = file.lastModifiedAt,
                    newIdentity = remoteFile,
                    userDrive = userDrive,
                )
            ) {
                // Preservation incomplete: keep the old bytes and metadata; a later sync
                // retries the refresh (issues: round 5 #4, round 6 #11).
                return DownloadActionOutcome(handled = false)
            }
            // The old location no longer holds this file's content (migrated, snapshotted
            // under the new identity, deleted as uploaded, or never existed): drop its stale
            // media index entry.
            if (liveRow.isMedia()) liveRow.deleteInMediaScan(context, userDrive)
        }

        if (!liveRow.isMarkedAsOffline && (!remoteFile.isOfflineAndIntact(remoteOfflineFile) || pathChanged)) {
            // Preservation against the OLD row baseline BEFORE the metadata replacement
            // (issue: round 8 #1): with baseline T1, an unregistered local edit T2 and a newer
            // remote revision T3 (T1 < T2 < T3), committing T3 metadata first would advance the
            // baseline, and the subsequent download would then classify the T2 bytes as clean
            // and replace them. The shared boundary judges the existing copy against the old
            // row, snapshots a dirty edit into the durable upload queue, and returns the OLD row
            // when preservation fails — in that case the action stays deferred and the cursor
            // is retained.
            val commitFile = OfflineFolderController.reconcileOwnedCopiesBeforeCommit(
                realm = realm,
                remoteFiles = listOf(remoteFile),
                userDrive = userDrive,
                // The COMPLETE action predicate (issue: round 14 #1/#2): generation, newer
                // non-Sync metadata, the nullable cursor bound, and the request anchor
                // ([file]) resolving ordering ties — the generation-only gate let ambiguous
                // identities through to preservation.
                isStale = { live, incoming, _ ->
                    FileController.isStaleForAuthoritativeAction(live, incoming, actionLastActionAt, observed = file)
                },
            ).first()
            if (commitFile !== remoteFile) return DownloadActionOutcome(handled = false)
            // Conditional commit (issues: round 11 #4/#5, round 12 #5, round 14 #1): a newer
            // generation committed in the acceptance window wins, and a row whose cursor
            // moved past this action rejects it. A REJECTED commit is PROPAGATED: the
            // enqueue happens only for a committed identity — enqueueing work for obsolete
            // metadata would download a superseded generation.
            val committed = FileController.updateExistingFileIfNotSuperseded(
                newFile = remoteFile,
                realm = realm,
                userDrive = userDrive,
                requireActionAtOrAfter = actionLastActionAt,
                observed = file,
            )
            if (!committed) return DownloadActionOutcome(handled = false)
            // Preserve the download origin: a refresh of a file covered by an offline folder is
            // an inherited download, so its completion can never manufacture an explicit pin
            // after an unmark (issue #1902). The enqueue's Operation is returned UNAWAITED
            // (issues: round 8 #12, round 9 #2): awaiting here would suspend inside the
            // thread-confined Realm session; the caller awaits it after the session closed.
            val operation = Utils.downloadAsOfflineFileOperation(
                context = context,
                file = remoteFile,
                userDrive = userDrive,
                isInheritedDownload = liveRow.isInOfflineFolder && !liveRow.isOffline,
            )
            return DownloadActionOutcome(handled = true, pendingOperation = operation)
        }
        if (liveRow.isMarkedAsOffline && !remoteFile.isOfflineAndIntact(remoteOfflineFile)) {
            // A pending marker alone does not prove that any worker owns the refresh (issue:
            // round 7 #16): it can outlive failed or cancelled work, and reporting this action
            // handled without durable work would advance the sync cursor past a missing or
            // stale pin. Idempotently enqueue the scoped refresh — the download chain is
            // serialized, so a worker that already owns the file simply completes first. The
            // Operation is returned unawaited for the same out-of-session await (round 9 #2);
            // an enqueue that fails its persistence leaves the cursor retained.
            val operation = Utils.downloadAsOfflineFileOperation(
                context = context,
                file = remoteFile,
                userDrive = userDrive,
                isInheritedDownload = liveRow.isInOfflineFolder && !liveRow.isOffline,
            )
            return DownloadActionOutcome(handled = true, pendingOperation = operation)
        }
        // The local state already matches the remote revision, or a queued worker durably owns
        // the refresh: nothing left to do for this action (issue: round 6 #11).
        return DownloadActionOutcome(handled = true)
    }
}
