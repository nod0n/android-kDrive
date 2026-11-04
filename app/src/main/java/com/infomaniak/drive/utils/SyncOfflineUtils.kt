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
                        localFilesMap = it.associateBy { file -> file.id },
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
                    ioFile = ioFile,
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
                if (localFile.isOffline) {
                    // Explicit pins keep the existing remote-wins behavior.
                    ioFile.delete()
                } else {
                    // Inherited copies (issue #1902) go through the same preservation policy as
                    // root removal: pending uploads and locally modified copies are protected
                    // (snapshotted into the upload queue when needed) before bytes are dropped.
                    OfflineFolderController.clearFileCoverage(
                        context = context,
                        realm = realm,
                        file = localFile,
                        userDrive = userDrive,
                        result = OfflineFolderController.RemovalResult(),
                        isFolder = false,
                    )
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
        val ioFileLastModified = ioFile.lastModified()
        var downloadOutcome = DownloadActionOutcome(handled = false)
        val handled = when {
            remoteFile == null -> {
                SentryLog.e("SyncOffline", "Expect remote file instead of null file") { scope ->
                    scope.setExtra("fileAction", "${fileAction.lastAction}")
                }
                false
            }
            ioFileLastModified > remoteFile.revisedAtInMillis ->
                uploadFile(context, localFile, remoteFile, ioFile, userDrive, realm)
            ioFileLastModified < remoteFile.revisedAtInMillis -> {
                downloadOutcome = downloadOfflineFile(context, localFile, remoteFile, ioFile, userDrive, realm)
                downloadOutcome.handled
            }
            fileAction.lastAction in renameActions -> synchronized(OfflineFileCoordination.rootIntentMonitor) {
                // Ancestry-changing writers participate in the ROOT-INTENT protocol (issue:
                // round 11 #7): the move's commit can re-parent a folder, changing the
                // ancestry that coverage resolution walks — it serializes with root removal's
                // coverage decisions, taken BEFORE the file locks below.
                // Conditional acceptance BEFORE preservation side effects (issues: round 11
                // #2/#4): an action superseded by an already-committed NEWER GENERATION of the
                // file must not move the bytes to its obsolete target or relabel the live row;
                // the newer generation's own flow owns this file's identity, so the action is
                // consumed with no side effect. Generation-only: an identity difference at
                // equal revisions is the change this action applies, not staleness.
                val liveRow = FileController.getFileById(realm, localFile.id)
                if (liveRow != null && FileController.isSupersededGeneration(liveRow, remoteFile)) {
                    true
                } else if (ioFile.exists() && !OfflineFolderController.reconcileMovedInheritedCopy(
                        context = context,
                        oldOfflineFile = ioFile,
                        oldIdentityName = localFile.name,
                        oldIdentityParentId = localFile.parentId,
                        oldLastModifiedAt = localFile.lastModifiedAt,
                        newIdentity = remoteFile,
                        userDrive = userDrive,
                    )
                ) {
                    // Preservation incomplete (pending upload, snapshot failed, protected
                    // destination): keep the old bytes and metadata, and do not consume the
                    // action so a later sync retries it (issue: round 6 #11).
                    false
                } else {
                    // Bytes are safely at (or durably registered for) the new location: commit
                    // the new identity so the row and the offline path agree. Conditional
                    // (issue: round 11 #4): a newer generation committed in the acceptance
                    // window wins; the action is consumed either way — its preservation is
                    // done, and re-queueing a superseded rename would retry forever.
                    FileController.updateExistingFileIfNotSuperseded(newFile = remoteFile, realm = realm, userDrive = userDrive)
                    true
                }
            }
            else -> true
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
                mutableFile.lastActionAt = fileAction.lastActionAt ?: 0
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
        ioFile: IOFile,
        userDrive: UserDrive,
        realm: Realm,
    ): Boolean {
        val uri = Uri.fromFile(ioFile)
        val fileModifiedAt = Date(ioFile.lastModified())

        if (!UploadFile.canUpload(uri, fileModifiedAt)) {
            // Not schedulable right now: the action stays pending for a later sync (issues:
            // round 6 #11, round 7 #4). With per-revision snapshot registration this only
            // trips for a legacy record still keyed by the original URI with the same
            // modification date.
            return false
        }
        // Upload-source registration is a mutation owner of these bytes: it holds the shared
        // per-file coordination so a publication or cleanup cannot interleave (issue:
        // round 4 #4).
        OfflineFileCoordination.withFileLock(userDrive, localFile.id) {
            // Revision dedup under the file lock (issue: round 8 #7): the identity of a
            // registration is the captured owner + content revision + destination (name,
            // folder, drive, user, modification date) — NOT the uri, since each registration
            // stores a fresh snapshot URI the legacy lookup above can never find. Without
            // this gate every no-action sync re-registered the same pending revision as a
            // new full snapshot, and a cancelled revision was silently reactivated on the
            // next sync.
            val registrationName = remoteFile?.name ?: localFile.name
            val registrationFolder = remoteFile?.parentId ?: localFile.parentId
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
                source = ioFile,
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
                // would recreate the old location remotely.
                fileName = remoteFile?.name ?: localFile.name,
                fileSize = ioFile.length(),
                remoteFolder = remoteFile?.parentId ?: localFile.parentId,
                type = UploadFile.Type.SYNC_OFFLINE.name,
                userId = userDrive.userId,
            ).store()

            if (remoteFile != null) {
                remoteFile.lastModifiedAt = ioFile.lastModified() / 1000
                remoteFile.size = ioFile.length()
                FileController.updateExistingFile(newFile = remoteFile, realm = realm, userDrive = userDrive)
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
        offlineFile: IOFile,
        userDrive: UserDrive,
        realm: Realm,
    ): DownloadActionOutcome {
        val remoteOfflineFile = remoteFile.getOfflineFile(context, userDrive.userId) ?: return DownloadActionOutcome(handled = false)
        // Conditional acceptance BEFORE any preservation side effect (issues: round 11 #2/#4):
        // a refresh superseded by an already-committed NEWER GENERATION must not move the
        // bytes or relabel the live row; the newer generation's own flow owns this file's
        // identity, so the action is consumed with no side effect. Generation-only: an
        // identity difference at equal revisions is the change this action applies. The
        // authoritative revalidation still happens inside the committing transaction below.
        val liveRow = FileController.getFileById(realm, file.id)
        if (liveRow != null && FileController.isSupersededGeneration(liveRow, remoteFile)) {
            return DownloadActionOutcome(handled = true)
        }
        val pathChanged = offlineFile.path != remoteOfflineFile.path

        if (pathChanged) {
            // A pending download marker leaves this refresh to the queued worker, which runs
            // its own protection checks: moving or deleting the bytes here would race it. The
            // action stays pending for a later sync (issue: round 6 #11).
            if (file.isMarkedAsOffline) return DownloadActionOutcome(handled = false)
            if (offlineFile.exists() && !OfflineFolderController.reconcileMovedInheritedCopy(
                    context = context,
                    oldOfflineFile = offlineFile,
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
            if (file.isMedia()) file.deleteInMediaScan(context, userDrive)
        }

        if (!file.isMarkedAsOffline && (!remoteFile.isOfflineAndIntact(remoteOfflineFile) || pathChanged)) {
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
                // Authoritative-change flow: the generation-only gate (issue: round 11 #4) —
                // an identity difference at equal revisions is the move this action applies.
                isStale = { live, incoming -> FileController.isSupersededGeneration(live, incoming) },
            ).first()
            if (commitFile !== remoteFile) return DownloadActionOutcome(handled = false)
            // Conditional commit (issue: round 11 #4/#5): a newer generation committed in the
            // acceptance window wins; the enqueued download revalidates everything itself, so
            // the action is consumed either way.
            FileController.updateExistingFileIfNotSuperseded(newFile = remoteFile, realm = realm, userDrive = userDrive)
            // Preserve the download origin: a refresh of a file covered by an offline folder is
            // an inherited download, so its completion can never manufacture an explicit pin
            // after an unmark (issue #1902). The enqueue's Operation is returned UNAWAITED
            // (issues: round 8 #12, round 9 #2): awaiting here would suspend inside the
            // thread-confined Realm session; the caller awaits it after the session closed.
            val operation = Utils.downloadAsOfflineFileOperation(
                context = context,
                file = remoteFile,
                userDrive = userDrive,
                isInheritedDownload = file.isInOfflineFolder && !file.isOffline,
            )
            return DownloadActionOutcome(handled = true, pendingOperation = operation)
        }
        if (file.isMarkedAsOffline && !remoteFile.isOfflineAndIntact(remoteOfflineFile)) {
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
                isInheritedDownload = file.isInOfflineFolder && !file.isOffline,
            )
            return DownloadActionOutcome(handled = true, pendingOperation = operation)
        }
        // The local state already matches the remote revision, or a queued worker durably owns
        // the refresh: nothing left to do for this action (issue: round 6 #11).
        return DownloadActionOutcome(handled = true)
    }
}
