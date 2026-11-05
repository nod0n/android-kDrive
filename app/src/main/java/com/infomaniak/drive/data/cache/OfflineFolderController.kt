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
import com.infomaniak.drive.data.models.StoredOfflineCopyState
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.DownloadOfflineFileManager
import com.infomaniak.drive.utils.DownloadOfflineFileManager.Companion.ProofRelocationFlow
import com.infomaniak.drive.utils.IOFile
import com.infomaniak.drive.utils.MediaUtils.deleteInMediaScan
import com.infomaniak.drive.utils.MediaUtils.isMedia
import com.infomaniak.drive.utils.OfflineFileCoordination
import com.infomaniak.drive.utils.OfflineFolderPolicy
import com.infomaniak.drive.utils.SyncUtils.syncImmediately
import com.infomaniak.drive.utils.Utils
import com.infomaniak.drive.utils.Utils.ROOT_ID
import io.realm.Realm
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * Realm data access for the durable offline folder roots (issue #1902).
 *
 * A root is a folder the user explicitly marked as available offline. Coverage of the descendants
 * is materialized on [File.isInOfflineFolder] (as opposed to the explicit [File.isOffline] pin),
 * so that removing a root can never strip a separately pinned file or a file still covered by
 * another overlapping root.
 */
object OfflineFolderController {

    // Process-local monotonic root-intent generations (issue: round 12 #9): every root state
    // change — creation, revival, pending-removal marking, record deletion — records a
    // strictly increasing generation per folderId. Wall-clock creation time missed revivals
    // (which retain their original createdAt) and broke on same-millisecond or
    // backward-adjusted clocks. Process-local by design: after a restart the map rebuilds
    // empty and pre-existing roots are conservatively treated as predating any traversal,
    // preserving the listing-maintenance semantics; every intra-process race the freshness
    // horizon exists for is covered.
    private val rootIntentGenerations = ConcurrentHashMap<Int, Long>()
    private val rootIntentGenerationClock = AtomicLong()

    /** Records a root state change for [folderId]; callers invoke it at every record mutation. */
    internal fun noteRootIntentChange(folderId: Int) {
        rootIntentGenerations.merge(folderId, rootIntentGenerationClock.incrementAndGet()) { current, next -> maxOf(current, next) }
    }

    /** The generation stamp a cleanup traversal takes BEFORE its collection checks. */
    internal fun currentRootIntentGeneration(): Long = rootIntentGenerationClock.get()

    /**
     * The per-root intent generation at OBSERVATION time (issue: round 18 #20): a walk
     * captures it BEFORE its first request so its completion can be compared against it —
     * an old batch must not complete newly revived intent.
     */
    internal fun rootIntentGeneration(folderId: Int): Long = rootIntentGenerations[folderId] ?: 0L

    internal fun rootIntentChangedSince(folderId: Int, sinceGeneration: Long): Boolean =
        (rootIntentGenerations[folderId] ?: 0L) > sinceGeneration

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
            // Root-intent writes serialize with destructive cleanup on the GLOBAL root-intent
            // monitor FIRST, then the root folder's coordination lock, then the Realm write
            // (issue: round 10 #5) — one consistent intent → locks → transaction order.
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                OfflineFileCoordination.withFileLock(userDrive, folder.id) {
                    // Refresh AFTER acquiring coordination (issue: round 13 #7): the Realm can
                    // be opened before the monitor wait — a removal completing while this
                    // re-mark queued must be visible, and the absence/active/pending decision
                    // re-reads the record inside its own transaction, so the decision is
                    // never made on stale state and the caller's re-mark cannot be silently
                    // discarded by an "already active" answer derived from it.
                    runCatching { realm.refresh() }
                    var absent = false
                    var pendingRevival = false
                    realm.executeTransaction {
                        val existingRoot = getOfflineRoot(folder.id, customRealm = realm)
                        absent = existingRoot == null
                        pendingRevival = existingRoot != null && existingRoot.isPendingRemoval
                    }
                    var stateChanged = false
                    if (absent) {
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
                        stateChanged = true
                    } else if (pendingRevival) {
                        // Re-marking a root whose deferred cleanup is still running revives it.
                        val existingRoot = getOfflineRoot(folder.id, customRealm = realm)
                        realm.executeTransaction { if (existingRoot != null && existingRoot.isValid) existingRoot.isPendingRemoval = false }
                        updateRootFolderFlags(folder.id, realm, isInOfflineFolder = true, isMarkedAsOffline = true)
                        stateChanged = true
                    }
                    // The generation is recorded ONLY for a committed state change (issue:
                    // round 13 #7): an "already active" answer must not masquerade as a
                    // revival, and a false return here is now a reliable fresh answer.
                    if (stateChanged) noteRootIntentChange(folder.id)
                    stateChanged
                }
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
                // Root-intent writes serialize with revival on the GLOBAL root-intent monitor.
                // The root folder's FILE lock is deliberately NOT held here (issue: round 11 #1):
                // it was kept across clearCoverageRecursively, which acquires ARBITRARY
                // descendant locks inside the walk, so a listing holding a descendant lock and
                // waiting for the root's lock deadlocked against this section waiting for that
                // descendant. The intent monitor alone serializes root add/remove/revival, and
                // the walk's per-file clearing takes each descendant's lock one at a time in the
                // consistent per-file order.
                synchronized(OfflineFileCoordination.rootIntentMonitor) {
                    // If the folder is still covered by an ancestor root, everything below stays
                    // covered: only the root record goes away.
                    val isCoveredByAncestorRoot = isInsideActiveRoot(realm, folderId)

                    if (isCoveredByAncestorRoot) {
                        realm.executeTransaction { if (root.isValid) root.deleteFromRealm() }
                        noteRootIntentChange(folderId)
                        RemovalResult(rootExisted = true)
                    } else {
                        // Persist the deferred-cleanup intent BEFORE any cleanup runs: the record is
                        // the durable owner and must exist while protected copies remain (issue:
                        // round 3 #8).
                        realm.executeTransaction { if (root.isValid) root.isPendingRemoval = true }
                        noteRootIntentChange(folderId)

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
                            noteRootIntentChange(folderId)
                        }
                        result
                    }
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

    /**
     * Whether an ACTIVE root record covering [folderId] — its own row or any ancestor —
     * changed intent after [sinceGeneration] (issues: round 11 #8, round 12 #9): the
     * freshness gate for remote-listing cleanup, keyed on the monotonic root-intent
     * generations rather than wall-clock creation time, so a REVIVED root's renewed intent
     * is caught too. Roots whose intent predates the traversal's stamp remain the listing's
     * maintenance responsibility (the walk must keep clearing stale rows beneath them),
     * while a root created or revived between the traversal's collection checks and its
     * destructive decisions is a newly authoritative owner whose state those checks never
     * saw — it stops those decisions.
     */
    internal fun hasActiveRootCoveringSince(folderId: Int, realm: Realm, sinceGeneration: Long): Boolean {
        getOfflineRoot(folderId, customRealm = realm)?.let { ownRoot ->
            if (ownRoot.isValid && !ownRoot.isPendingRemoval && rootIntentChangedSince(folderId, sinceGeneration)) return true
        }
        return walkAncestryForRoot(realm, folderId) { root ->
            root.isValid && !root.isPendingRemoval && rootIntentChangedSince(root.folderId, sinceGeneration)
        }
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
                    // UNPROVEN_HISTORICAL (issue: round 19 #2): unproven bytes are preserved
                    // through destructive operations — their folder is required ancestry.
                    CopyProtection.PENDING_UPLOAD, CopyProtection.DIRTY, CopyProtection.UNPROVEN_HISTORICAL -> return true
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
            // The deferred cleanup serializes with revival/addition on the GLOBAL root-intent
            // monitor (issue: round 10 #5) — a nested-root addition commits only outside this
            // whole section. The root folder's FILE lock is deliberately NOT held (issue:
            // round 11 #1): it was kept across clearCoverageRecursively, which acquires
            // ARBITRARY descendant locks inside the walk, so a listing holding a descendant
            // lock and waiting for the root's lock deadlocked against this section waiting for
            // that descendant. The intent monitor alone serializes root revival, and the walk's
            // per-file clearing takes each descendant's lock one at a time in the consistent
            // per-file order.
            synchronized(OfflineFileCoordination.rootIntentMonitor) {
                // Re-validate against the live record: the root may have been revived since
                // the detached list was built (issue: round 4 #11).
                val stillPending = customRealm.where(OfflineRoot::class.java)
                    .equalTo(OfflineRoot::folderId.name, root.folderId)
                    .findFirst()
                    ?.takeIf { it.isValid && it.isPendingRemoval } != null
                if (!stillPending) return@synchronized

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
                if (hasProtectedFilesRecursively(root.folderId, context, userDrive, customRealm)) return@synchronized

                customRealm.executeTransaction {
                    customRealm.where(OfflineRoot::class.java)
                        .equalTo(OfflineRoot::folderId.name, root.folderId)
                        .findFirst()
                        ?.takeIf { it.isValid && it.isPendingRemoval }
                        ?.deleteFromRealm()
                }
                noteRootIntentChange(root.folderId)
            }
        }
    }

    /**
     * Plain identity of the row owning a destination path, built while the Realm is open
     * (issue: round 7 #1): a managed object — even from createSnapshot(), which snapshots
     * collection membership but not the objects — escapes a closed Realm as a detached
     * failure waiting to happen.
     */
    internal class DestinationOwner(
        val id: Int,
        val lastModifiedAt: Long,
        // The owning row's PUBLISHED byte baseline travels with the DTO (issue: round 15
        // #6): edit detection against the owned bytes judges this, not accepted server
        // metadata — the DTO must not lose the field the row carries.
        val publishedCleanBaseline: Long? = null,
    ) {
        // The SAME computed baseline rule as [File.effectivePublishedBaseline] (issue: round
        // 18 compile corrections): the DTO must expose the effective value itself — zero is
        // not provenance and must fall back to the accepted metadata exactly like an unset
        // baseline, instead of every consumer re-deriving (or forgetting) the rule.
        val effectivePublishedBaseline: Long
            get() = publishedCleanBaseline?.takeIf { it > 0 } ?: lastModifiedAt
    }

    /**
     * The id of the row that EXCLUSIVELY claims [claimedPath] as its derived offline path, or
     * null when no row claims it or — decisively — when SEVERAL rows claim it (issue: round 16
     * #3): a first-match among multiple claimants is not proof of byte ownership. Callers must
     * defer ambiguous paths (no preservation, snapshot or deletion against those bytes) until a
     * later pass resolves the duplicate; a single claimant is the proven byte owner.
     *
     * Candidates are every non-directory row whose name matches the path's file component —
     * both derived-path claimants and rows still carrying a stale stored path that resolves
     * here.
     */
    internal fun exclusivePathOwner(
        realm: Realm,
        context: Context,
        userDrive: UserDrive,
        claimedPath: String,
    ): Int? {
        val nameComponent = claimedPath.substringAfterLast('/')
        val candidates = realm.where(File::class.java)
            .equalTo(File::name.name, nameComponent)
            .notEqualTo(File::type.name, File.Type.DIRECTORY.value)
            .findAll()
            .createSnapshot()
        var claimantId: Int? = null
        for (candidate in candidates) {
            val candidatePath = candidate.getOfflineFile(context, userDrive.userId)?.path ?: continue
            if (candidatePath == claimedPath) {
                if (claimantId != null && claimantId != candidate.id) return null
                claimantId = candidate.id
            }
        }
        return claimantId
    }

    /**
     * Resolves the row that owns the existing bytes at [target], by derived offline path, among
     * candidates sharing the incoming file's parent and name (the incoming file's own row
     * included). Returns null when no row claims that path: the bytes' ownership is unknown and
     * must be preserved, not replaced (issue: round 6 #3) — and null TOO when multiple rows
     * claim the path (issue: round 16 #3): a first match is not proof of ownership, and an
     * ambiguous path defers instead of publishing over whichever bytes happen to sit there.
     * The returned owner is a plain DTO, safe to read after the Realm session closes (issue:
     * round 7 #1).
     */
    internal fun destinationOwnerRow(
        realm: Realm,
        context: Context,
        userDrive: UserDrive,
        incoming: File,
        target: IOFile,
    ): DestinationOwner? {
        val claimantId = exclusivePathOwner(realm, context, userDrive, target.path) ?: return null
        val candidates = realm.where(File::class.java)
            .equalTo(File::parentId.name, incoming.parentId)
            .equalTo(File::name.name, incoming.name)
            .findAll()
            .createSnapshot()
        for (candidate in candidates) {
            val candidatePath = candidate.getOfflineFile(context, userDrive.userId)?.path ?: continue
            if (candidatePath == target.path && candidate.id == claimantId) {
                return DestinationOwner(
                    id = candidate.id,
                    lastModifiedAt = candidate.lastModifiedAt,
                    publishedCleanBaseline = candidate.publishedCleanBaseline,
                )
            }
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
     * reliable identity for the old bytes); [oldLastModifiedAt] is the PUBLISHED byte-provenance
     * baseline of those bytes — callers pass `publishedCleanBaseline ?: lastModifiedAt`, never
     * bare replacement metadata (issue: round 16 #2); [newIdentity] is the fresh remote File.
     * Directory rows are not file content and must not be passed here (issue: round 4 #12).
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
        realm: Realm? = null,
    ): Boolean {
        // SOURCE-OWNERSHIP GATE before any snapshot or deletion (issue: round 16 #3): when the
        // caller supplies its Realm, the old path must be EXCLUSIVELY claimed by this file's
        // row (the post-move identity keeps the row's id, so [newIdentity.id] is the expected
        // owner). An ambiguous or foreign path defers the whole reconciliation — the bytes
        // there may be another file's edit, and the caller keeps the old metadata for a later
        // retry. Callers that run inside the shared boundary already ran the same gate; the
        // check is re-applied here so every direct caller (sync actions, UI moves, SAF moves,
        // renames) is covered too.
        if (realm != null && exclusivePathOwner(realm, context, userDrive, oldOfflineFile.path) != newIdentity.id) {
            return false
        }
        // Copy ownership and mutation coordination (issue: round 4 #4): move reconciliation
        // competes with publication, cleanup and upload registration for the same bytes.
        OfflineFileCoordination.withFileLock(userDrive, newIdentity.id) {
            // DEFENSIVE RECOVERY EXCLUSION before any relocation effect (issue: round 32
            // #2): a direct caller may have skipped the shared boundary's fresh check — an
            // unresolved durable recovery governing this identity or the old path owns the
            // bytes and their location, and relocating them before the sweep resolves
            // leaves the restore unable to re-establish the recorded owner/location. The
            // whole reconciliation defers (the caller retains the old row/location).
            if (DownloadOfflineFileManager.hasUnresolvedRecovery(
                    context,
                    userDrive,
                    newIdentity.id,
                    oldOfflineFile.absolutePath,
                )
            ) {
                return false
            }
            val fileModifiedAt = Date(oldOfflineFile.lastModified())
            // ONE FRESH SCOPE+OWNERSHIP+CONTENT CLASSIFICATION decides the destructive
            // arms (issues: round 22 #4, round 23 #4) — computed BEFORE any destructive
            // arm runs:
            //  - a positively foreign record on the old path DEFERS — the bytes are
            //    another identity's content: never snapshotted under this identity,
            //    never deleted, never renamed; the foreign check PRECEDES the
            //    completed-upload deletion (a path attributed to F but recorded as G
            //    was previously deletable under F);
            //  - ONLY a record-identified, HASH-FULL record whose hash MISMATCHES the
            //    bytes is a positively established edit — registered before the
            //    relocation; a HASHLESS record proves nothing about content (a newer
            //    mtime under it is not an edit), and CERTIFIED published bytes are
            //    never an edit, not when the Realm baseline lags;
            //  - UNPROVEN historical bytes (no identifying record, or an unanchored
            //    baseline a backward-dated revision re-labels) DEFER: never
            //    auto-uploaded, never deleted; verification resolves their generation.
            val relocationRecord = DownloadOfflineFileManager.readPublishedProvenance(context, oldOfflineFile)
            val relocationIdentifies = relocationRecord?.identifies(newIdentity, userDrive) == true
            val relocationBytesHash = UploadFile.sha256OrNull(oldOfflineFile)
            val relocationCertified = relocationIdentifies &&
                relocationRecord.contentHash != null &&
                relocationBytesHash != null &&
                relocationRecord.contentHash == relocationBytesHash
            val relocationProvenDrift = relocationIdentifies &&
                relocationRecord.contentHash != null &&
                relocationRecord.contentHash != relocationBytesHash
            return when {
                // A pending upload owns the bytes at the old path until it completes (issue:
                // round 5 #9): committing the new metadata now would leave the upload reading a
                // location the walk no longer tracks. The same applies to an UNSENT record
                // retained after failure/cancellation (issue: round 6 #1). Deferring keeps the
                // old row and location authoritative; the caller retries reconciliation after
                // the upload finished.
                UploadFile.hasPendingUpload(uri = Uri.fromFile(oldOfflineFile).toString()) ||
                    UploadFile.hasRetainedUnsentUpload(uri = Uri.fromFile(oldOfflineFile).toString()) ||
                    contentHashOf(oldOfflineFile)?.let { hash ->
                        UploadFile.hasUploadRecordFor(
                            fileName = oldIdentityName,
                            remoteFolder = oldIdentityParentId,
                            driveId = userDrive.driveId,
                            userId = userDrive.userId,
                            fileModifiedAt = fileModifiedAt,
                            pendingOnly = true,
                            contentHash = hash,
                            originalFileId = newIdentity.id,
                        )
                    } == true -> false
                relocationRecord?.positivelyForeignTo(newIdentity, userDrive) == true -> false
                contentHashOf(oldOfflineFile)?.let { hash ->
                    UploadFile.hasUploadRecordFor(
                        fileName = oldIdentityName,
                        remoteFolder = oldIdentityParentId,
                        driveId = userDrive.driveId,
                        userId = userDrive.userId,
                        fileModifiedAt = fileModifiedAt,
                        pendingOnly = false,
                        contentHash = hash,
                        originalFileId = newIdentity.id,
                    )
                } == true -> {
                    // Every byte deletion clears matching provenance (issue: round 18 #12).
                    DownloadOfflineFileManager.deletePublishedProvenance(context, oldOfflineFile)
                    runCatching { oldOfflineFile.delete() }.isSuccess
                }
                relocationProvenDrift -> {
                    val registered = registerDirtySnapshot(context, newIdentity, oldOfflineFile, userDrive)
                    if (registered) {
                        // Every byte deletion clears matching provenance (issue: round 18 #12).
                        DownloadOfflineFileManager.deletePublishedProvenance(context, oldOfflineFile)
                        runCatching { oldOfflineFile.delete() }.isSuccess
                    } else false
                }
                !relocationCertified && OfflineFolderPolicy.isLocallyModified(
                    offlineFileLastModifiedMillis = oldOfflineFile.lastModified(),
                    remoteLastModifiedAtSeconds = oldLastModifiedAt,
                ) -> false
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
                                // THE RELOCATION IS PREPARED AND ITS PROOF/OBSERVATION
                                // VALIDATED BEFORE THE BYTES MOVE (issue: round 32 #4): the
                                // previous shape renamed the bytes FIRST and treated the
                                // proof move as best-effort — a failed proof/observation left
                                // bytes at B with the provenance stranded at A and NO
                                // prepared relocation recovery. The observation requirement
                                // is evidence-based here: an ABSENT observation of a
                                // verified-uploaded/version-less record is legitimate, while
                                // a PRESENT but unbindable one fails the relocation BEFORE
                                // any byte moves.
                                if (!DownloadOfflineFileManager.prepareProofRelocation(
                                        context,
                                        oldOfflineFile,
                                        target,
                                        expectedObservationLocation = oldOfflineFile.absolutePath,
                                        requireObservation = false,
                                        // THE CAPTURED SCOPE IS EXPLICIT (review-33 #5): the
                                        // operation's sidecar records the caller's ACTUAL realm
                                        // flavor and the relocation's offline destination kind —
                                        // a silently-defaulted ordinary Realm made a shared-scope
                                        // relocation's replay unvalidatable forever.
                                        sharedWithMe = userDrive.sharedWithMe,
                                        storageKind = "offline",
                                        // THE PUBLISHED-SOURCE RELOCATION CONTRACT (review-33
                                        // #4): the source bytes have not moved yet — a
                                        // preparation failure preserves the source proof and
                                        // disposes only the destination preparation. THE
                                        // COMPANION-NESTED TYPE IS IMPORTED (review-33 compile
                                        // repair): Kotlin resolves companion callables through
                                        // the class name but not companion-nested TYPES — the
                                        // same convention as Companion.PublicationOutcome.
                                        flow = ProofRelocationFlow.PUBLISHED_SOURCE_RELOCATION,
                                        // THE RECORDED SOURCE IDENTITY (review-33 #3): the v2
                                        // relocation record carries the old location so the
                                        // sweep's conditional move-completion can validate the
                                        // recorded old identity when the live row has not
                                        // advanced to the destination yet.
                                        recordedSourcePath = oldOfflineFile.absolutePath,
                                    )
                                ) {
                                    return@runCatching false
                                }
                                val renamed = oldOfflineFile.renameTo(target)
                                if (!renamed) {
                                    // The rename touched nothing; the prepared destination
                                    // state is inert (a record without bytes) and the sweep's
                                    // missing-target arm disposes the retained relocation
                                    // state with the source proof intact.
                                    return@runCatching false
                                }
                                // THE PROOF AND OBSERVATION COMPLETE TOGETHER (issue: round
                                // 32 #4): a failure here RETAINS the durable relocation state
                                // (the sweep replays it onto the already-moved bytes) — never
                                // bytes-without-provenance and no prepared recovery.
                                DownloadOfflineFileManager.completePreparedProofMove(context, oldOfflineFile, target)
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
        isStale: (live: File, incoming: File, observed: File?) -> Boolean = { live, incoming, observed ->
            // The request-observed basis is FORWARDED (issue: round 14 #5): discarding it made
            // the download manager's supplied observation unusable and re-broke tied renames
            // it was supplied for.
            FileController.isStaleReplacement(live, incoming, observed)
        },
        observedById: Map<Int, File> = emptyMap(),
    ): List<File> {
        val context = Realm.getApplicationContext() ?: return remoteFiles
        if (remoteFiles.isEmpty()) return remoteFiles
        return remoteFiles.map { remoteFile ->
            // Fresh validate–preserve under coordination (issue: round 12 #1): the boundary
            // acquires each file's lock ITSELF — reentrant when a coordinated caller already
            // holds the set — and REFRESHES the Realm before reading the source
            // identity/baseline, so standalone callers (the sync, the details cascade,
            // single-file upserts) get the same freshness and serialization as the graph
            // commits, and no preservation decision is made on a stale Realm view.
            OfflineFileCoordination.withFileLock(userDrive, remoteFile.id) {
                runCatching { realm.refresh() }
                val oldRow = FileController.getFileById(realm, remoteFile.id) ?: return@withFileLock remoteFile
                // Stale-response gate BEFORE ANY preservation side effect (issue: round 11 #2/#4)
                // and BEFORE the directory early return (issue: round 16 #8): directories
                // previously returned the incoming payload before any freshness decision, and
                // the details cascade then merged it as if accepted — a delayed D-at-A response
                // restored obsolete ancestry over D's current row. EVERY row type is gated
                // first; a stale directory returns the OLD row in place like a stale file.
                // When the live row already holds a newer server generation — or, for
                // replacement boundaries, a different location identity at an equal revision —
                // the incoming response describes a revision that no longer exists; reconciling
                // its bytes would snapshot a current edit under the obsolete identity, delete
                // the original, and then be rejected at the metadata commit, leaving the live
                // row's bytes absent and an upload targeting the old location. The OLD row is
                // returned in place: the caller commits (or defers to) the live state, and
                // nothing on the filesystem or in the upload queue is touched.
                // Authoritative-change callers (the offline sync's actions) pass the action
                // predicate — an identity difference at equal revisions is the change being
                // applied there, not staleness.
                if (isStale(oldRow, remoteFile, observedById[remoteFile.id])) return@withFileLock oldRow
                if (oldRow.isFolder()) return@withFileLock remoteFile
                // RECOVERY BEFORE BOTH SOURCE BRANCHES (issues: round 30 #1, round 31 #2):
                // an old location that is ABSENT (an interrupted recovery can hold the
                // sole retained revision in a durable backup while the path itself is
                // missing) — or one that EXISTS with a governing unresolved recovery — is
                // recovery-managed state: an existing certified source must not be
                // relocated or committed over before the sweep resolves (the recovery's
                // restore requires the live owner to still resolve to the recorded path;
                // a relocation/commit here strands the retained revision, and the
                // identity-based exclusion then blocks the row's later publications
                // indefinitely). The check runs by identity AND the available path,
                // BEFORE the existing and the missing-source branch and before any
                // relocation effect.
                val resolvedOldOffline = oldRow.getOfflineFile(context, userDrive.userId)
                if (DownloadOfflineFileManager.hasUnresolvedRecovery(
                        context,
                        userDrive,
                        oldRow.id,
                        resolvedOldOffline?.absolutePath,
                    )
                ) {
                    return@withFileLock oldRow
                }
                val oldOffline = resolvedOldOffline?.takeIf { it.exists() }
                if (oldOffline == null) {
                    return@withFileLock remoteFile
                }
                // SOURCE-OWNERSHIP GATE, before ANY preservation side effect (issue: round 16
                // #3): the bytes at the old row's path must be EXCLUSIVELY claimed by this
                // file's row. A path reused by another row (a stale duplicate claiming the
                // same derived path) makes the owner ambiguous — snapshotting or deleting
                // there would capture or destroy another file's edit; the whole replacement
                // defers (the OLD row is returned, nothing on disk or in the upload queue is
                // touched) and a later pass retries once the duplicate resolves.
                val pathOwnerId = exclusivePathOwner(realm, context, userDrive, oldOffline.path)
                if (pathOwnerId != oldRow.id) return@withFileLock oldRow
                // COMPLETE FRESH DESTINATION ANCESTRY BEFORE THE FIRST PRESERVATION EFFECT
                // (issue: round 22 #9): the anchor below and every preservation arm after it
                // run only once the INCOMING payload's parent chain is complete and fresh —
                // otherwise a payload with a populated destination path anchors, snapshots or
                // moves bytes beneath locally missing ancestors and the metadata commit then
                // fails, stranding them. The deferral is side-effect-free; missing ancestors
                // resolve outside coordination (listings/details/firstMissingAncestorId) and
                // a later pass retries the whole decision. The location-changed branch
                // REVALIDATES the chain after the unchanged-path arms (continuously through
                // commit) below.
                if (remoteFile.parentId > 0 && !FileController.destinationChainExists(realm, remoteFile.parentId)) {
                    return@withFileLock oldRow
                }
                // Historical baseline ANCHOR (issues: round 16 #2, round 18 #6, round 19 #2):
                // a row whose bytes were published before the baseline field existed carries
                // UNSET (null — and, consistently, a non-positive repaired value). The anchor
                // is written ONLY from AUTHORITATIVE content proof — the published
                // generation's provenance record naming THIS row's accepted metadata — never
                // from a size/mtime comparison: a same-size local edit retaining the old
                // mtime was previously anchored CLEAN and destructive operations then
                // destroyed it without a snapshot. An unproven historical copy keeps unset
                // and is PRESERVED (protection classifies it unproven-historical); the
                // replacement flow's protection and the upload completion's exact-revision
                // reconciliation gate the destructive paths, and a genuine edit still
                // snapshots below.
                if ((oldRow.publishedCleanBaseline ?: 0L) <= 0L &&
                    oldRow.storedOfflineCopyState(context, oldOffline, userDrive) == StoredOfflineCopyState.PROVEN_CURRENT
                ) {
                    runCatching {
                        realm.executeTransaction { oldRow.publishedCleanBaseline = oldRow.lastModifiedAt }
                    }
                }
                // Location changes are detected on the RESOLVED offline paths (issue: round 5 #7).
                val newOffline = remoteFile.getOfflineFile(context, userDrive.userId)
                val locationChanged = newOffline == null || newOffline.path != oldOffline.path
                if (!locationChanged) {
                    // Pre-existing bytes are judged against the OLD baseline BEFORE the incoming
                    // metadata replaces it (issue: round 7 #2): a freshly upserted row cannot
                    // establish the cleanliness of bytes it did not write. Baseline T1, unsent
                    // edit T2, incoming T3 (T1 < T2 < T3): committing T3 first would classify the
                    // T2 edit as clean and let publication or root removal destroy it. A pending
                    // or retained-unsent record already owns the copy. The PUBLISHED byte
                    // baseline — not the (possibly replaced) server metadata — is the
                    // comparison basis (issue: round 16 #2).
                    val targetUri = Uri.fromFile(oldOffline).toString()
                    if (UploadFile.hasPendingUpload(uri = targetUri) ||
                        UploadFile.hasRetainedUnsentUpload(uri = targetUri)
                    ) {
                        return@withFileLock remoteFile
                    }
                    // ONE FRESH SCOPE+OWNERSHIP+CONTENT CLASSIFICATION decides the
                    // unchanged-path boundary (issue: round 21 #2) — never the
                    // metadata-only mtime comparison alone:
                    //  - a copy the record CERTIFIES (immutable byte identity matches) is
                    //    this row's published generation or a verified retained one:
                    //    replaceable (a preserved future mtime does not make it an edit);
                    //  - a KNOWN record whose hash MISMATCHES the bytes is a PROVEN
                    //    same-mtime edit: snapshotted before the replacement (the
                    //    seconds-resolution mtime comparison alone still matched);
                    //  - a genuine edit against an ANCHORED (authoritative) baseline is
                    //    snapshotted as before — the anchor came from content proof, so
                    //    the mtime comparison is trustworthy;
                    //  - everything else — unproven historical bytes whose UNSET baseline
                    //    a backward-dated newer revision re-labels through the fallback,
                    //    hashless legacy records that cannot certify content, unreadable
                    //    bytes — is PRESERVED: never automatically uploaded, never
                    //    overwritten; the whole commit defers side-effect-free and the
                    //    verification path resolves the generation.
                    if (oldRow.storedOfflineCopyState(context, oldOffline, userDrive) ==
                        StoredOfflineCopyState.FOREIGN_CONFLICT
                    ) {
                        return@withFileLock oldRow
                    }
                    val record = DownloadOfflineFileManager.readPublishedProvenance(context, oldOffline)
                    val recordIdentifiesRow =
                        record != null && record.identifies(oldRow, userDrive)
                    val certified = recordIdentifiesRow && record.certifiesBytes(oldOffline)
                    val provenSameMtimeEdit = recordIdentifiesRow && record.contentHash != null &&
                        UploadFile.sha256OrNull(oldOffline) != record.contentHash
                    when {
                        certified -> Unit
                        provenSameMtimeEdit -> {
                            if (!registerDirtySnapshot(context, oldRow, oldOffline, userDrive)) {
                                return@withFileLock oldRow
                            }
                        }
                        (oldRow.publishedCleanBaseline ?: 0L) > 0L &&
                            OfflineFolderPolicy.isLocallyModified(
                                offlineFileLastModifiedMillis = oldOffline.lastModified(),
                                remoteLastModifiedAtSeconds = oldRow.effectivePublishedBaseline,
                            ) -> {
                            if (!registerDirtySnapshot(context, oldRow, oldOffline, userDrive)) {
                                return@withFileLock oldRow
                            }
                        }
                        else -> return@withFileLock oldRow
                    }
                    return@withFileLock remoteFile
                }
                // CONTINUOUS ANCESTRY REVALIDATION (issues: round 19 #8, round 22 #9): the
                // unchanged-path arms above perform durable snapshot registrations (file I/O
                // and upload-queue writes) during which an ancestor can be deleted locally —
                // the move's destination chain is validated AGAIN immediately before the
                // relocation, so no preservation effect ever runs against a chain that went
                // missing mid-section. The deferral stays side-effect-free; a later pass with
                // the resolved ancestry re-applies the payload.
                if (!FileController.destinationChainExists(realm, remoteFile.parentId)) {
                    return@withFileLock oldRow
                }
                if (reconcileMovedInheritedCopy(
                        context = context,
                        oldOfflineFile = oldOffline,
                        oldIdentityName = oldRow.name,
                        oldIdentityParentId = oldRow.parentId,
                        // The OLD bytes' provenance baseline (issue: round 16 #2), not the
                        // possibly-replaced server metadata.
                        oldLastModifiedAt = oldRow.effectivePublishedBaseline,
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
    }

    /** Outcome of the shared pre-replacement check for an offline-owned copy (issue: round 7 #3). */
    enum class OwnedCopyReplacementDecision {
        /** The copy backs an unsent upload (or its snapshot failed): it IS the usable local content. */
        KEEP_LOCAL,
        /** Nothing owned is lost by replacing the bytes (snapshot succeeded or content-safe). */
        REPLACEABLE,
        /**
         * FRESH ownership could not be established for these bytes (issue: round 18 #5): the
         * path is contested, ambiguous or foreign, or the row no longer binds it — NO snapshot
         * was taken and the bytes are NOT certified as this file's usable content. Callers
         * must neither complete over them nor delete them; a fresh download defers through
         * the coordinated publication instead.
         */
        DEFERRED_FOREIGN,
    }

    /**
     * Decision for replacing an offline-owned copy's bytes locally (preview re-downloads and
     * download-dialog writes, issue: round 7 #3): a size mismatch alone never authorizes
     * deleting an inherited edit. A copy backing a pending or retained-unsent upload is the
     * usable local content; a dirty copy is snapshotted into the durable upload queue first —
     * a failed registration keeps the local copy; content-safe copies are replaceable.
     *
     * FRESH exclusive ownership is MANDATORY inside this API (issue: round 18 #5): callers
     * that cannot supply a coordinated Realm — the presentation entry points (preview
     * conversion, download dialog), which previously omitted the argument and thereby
     * SKIPPED the ownership check — get the complete section here: intent monitor → file
     * lock → refreshed scoped Realm → current identity/path/ownership → snapshot.
     */
    fun decisionForReplacingOwnedCopy(
        file: File,
        offlineFile: IOFile,
        userDrive: UserDrive,
        realm: Realm? = null,
    ): OwnedCopyReplacementDecision {
        val context = Realm.getApplicationContext() ?: return OwnedCopyReplacementDecision.KEEP_LOCAL
        return if (realm != null) {
            decisionForOwnedCopyWithFreshOwnership(context, file, offlineFile, userDrive, realm)
        } else synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive, file.id) {
                FileController.getRealmInstance(userDrive).use { freshRealm ->
                    runCatching { freshRealm.refresh() }
                    val live = FileController.getFileById(freshRealm, file.id)
                        ?: return@withFileLock OwnedCopyReplacementDecision.DEFERRED_FOREIGN
                    // CURRENT identity and path (issue: round 18 #5): a row moved, renamed or
                    // re-published since the presentation row was captured no longer binds
                    // these bytes — the captured F/path pair must not authorize a snapshot.
                    val liveOfflinePath = live.getOfflineFile(context, userDrive.userId)?.path
                    if (liveOfflinePath != offlineFile.path) {
                        return@withFileLock OwnedCopyReplacementDecision.DEFERRED_FOREIGN
                    }
                    decisionForOwnedCopyWithFreshOwnership(context, live, offlineFile, userDrive, freshRealm)
                }
            }
        }
    }

    /**
     * Decision body run under the caller's coordination, with a live row and a Realm whose
     * view is current (issue: round 18 #5): the exclusive path owner is established BEFORE
     * any snapshot side effect, and a foreign or ambiguous path yields
     * [OwnedCopyReplacementDecision.DEFERRED_FOREIGN] — never [OwnedCopyReplacementDecision
     * .KEEP_LOCAL], which certifies the bytes as usable local content.
     */
    private fun decisionForOwnedCopyWithFreshOwnership(
        context: Context,
        file: File,
        offlineFile: IOFile,
        userDrive: UserDrive,
        realm: Realm,
    ): OwnedCopyReplacementDecision {
        if (exclusivePathOwner(realm, context, userDrive, offlineFile.path) != file.id) {
            return OwnedCopyReplacementDecision.DEFERRED_FOREIGN
        }
        // FOREIGN CONFLICT IS DISTINCT END-TO-END (issue: round 20 #11): the protection
        // classification folds foreign bytes into UNPROVEN_HISTORICAL, but the REPLACEMENT
        // decision must never answer KEEP_LOCAL for them — KEEP_LOCAL certifies the bytes
        // as this file's usable content, and the dialog would report 100% over another
        // identity's bytes. The conflict defers like any foreign ownership.
        if (file.storedOfflineCopyState(context, offlineFile, userDrive) == StoredOfflineCopyState.FOREIGN_CONFLICT) {
            return OwnedCopyReplacementDecision.DEFERRED_FOREIGN
        }
        return when (protectionStateOf(file, offlineFile, userDrive)) {
            CopyProtection.PENDING_UPLOAD -> OwnedCopyReplacementDecision.KEEP_LOCAL
            CopyProtection.DIRTY ->
                if (registerDirtySnapshot(context, file, offlineFile, userDrive)) {
                    OwnedCopyReplacementDecision.REPLACEABLE
                } else {
                    OwnedCopyReplacementDecision.KEEP_LOCAL
                }
            // Unproven historical bytes (issue: round 19 #2): preserved, never snapshotted
            // for upload; the replacement defers and the verification path resolves the
            // generation.
            CopyProtection.UNPROVEN_HISTORICAL -> OwnedCopyReplacementDecision.KEEP_LOCAL
            CopyProtection.UPLOADED, CopyProtection.NONE -> OwnedCopyReplacementDecision.REPLACEABLE
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
    fun preserveInheritedCopyBeforeRemoval(file: File, userDrive: UserDrive, realm: Realm? = null): PreservationOutcome {
        val context = Realm.getApplicationContext() ?: return PreservationOutcome.PRESERVED
        // THE UNRESOLVED-SHARED-PATH GUARD PRECEDES THE PIN SHORTCUT (issues: round 28
        // #7, round 29 #3): physical ownership is resolved WITH THE CAPTURED FLAVOR before
        // any classification shortcut — an explicit pin carrying BOTH inherited coverage
        // and an unresolved shared path defers removal (its inherited bytes cannot be
        // located or classified), never "nothing to preserve".
        val offlineFile = file.getOfflineFile(context, userDrive.userId, sharedWithMe = userDrive.sharedWithMe)
        if (file.isOffline || file.isFolder() || !file.isInOfflineFolder) {
            if (!file.isFolder() && userDrive.sharedWithMe && offlineFile == null) {
                return PreservationOutcome.PRESERVED
            }
            return PreservationOutcome.CONTENT_SAFE
        }
        if (offlineFile == null) {
            return if (userDrive.sharedWithMe) PreservationOutcome.PRESERVED else PreservationOutcome.CONTENT_SAFE
        }
        if (!offlineFile.exists()) return PreservationOutcome.CONTENT_SAFE
        // FRESH exclusive source ownership is MANDATORY inside this preservation API (issue:
        // round 18 #5, same class as the replacement decision): a caller without a Realm gets
        // the coordinated section here; an ambiguous or foreign path PRESERVES (defers) —
        // never snapshots or deletes bytes whose owner is contested.
        return if (realm != null) {
            preserveWithFreshOwnership(context, file, offlineFile, userDrive, realm)
        } else synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive, file.id) {
                FileController.getRealmInstance(userDrive).use { freshRealm ->
                    runCatching { freshRealm.refresh() }
                    val live = FileController.getFileById(freshRealm, file.id)
                        ?: return@withFileLock PreservationOutcome.PRESERVED
                    val liveOfflinePath = live.getOfflineFile(
                        context, userDrive.userId, sharedWithMe = userDrive.sharedWithMe,
                    )?.path
                    if (liveOfflinePath != offlineFile.path) return@withFileLock PreservationOutcome.PRESERVED
                    preserveWithFreshOwnership(context, live, offlineFile, userDrive, freshRealm)
                }
            }
        }
    }

    /** Preservation body under the caller's coordination with a current Realm (issue: round 18 #5). */
    private fun preserveWithFreshOwnership(
        context: Context,
        file: File,
        offlineFile: IOFile,
        userDrive: UserDrive,
        realm: Realm,
    ): PreservationOutcome {
        if (exclusivePathOwner(realm, context, userDrive, offlineFile.path) != file.id) {
            return PreservationOutcome.PRESERVED
        }
        return when (protectionStateOf(file, offlineFile, userDrive)) {
            CopyProtection.PENDING_UPLOAD -> PreservationOutcome.PRESERVED
            CopyProtection.DIRTY ->
                if (registerDirtySnapshot(context, file, offlineFile, userDrive)) {
                    PreservationOutcome.CONTENT_SAFE
                } else {
                    // Snapshot registration failed: keep bytes AND identity/metadata (round 4 #6).
                    PreservationOutcome.PRESERVED
                }
            // Unproven historical bytes (issue: round 19 #2): preserved — no snapshot, no
            // deletion — until the verification path resolves their generation.
            CopyProtection.UNPROVEN_HISTORICAL -> PreservationOutcome.PRESERVED
            CopyProtection.UPLOADED, CopyProtection.NONE -> PreservationOutcome.CONTENT_SAFE
        }
    }

    private fun updateRootFolderFlags(folderId: Int, realm: Realm, isInOfflineFolder: Boolean, isMarkedAsOffline: Boolean) {
        FileController.updateFile(folderId, realm) { file ->
            file.isInOfflineFolder = isInOfflineFolder
            file.isMarkedAsOffline = isMarkedAsOffline
            // THE MONOTONIC INTENT GENERATION (issue: round 26 #12): every intent writer
            // bumps — the root-folder flag writer included.
            file.bumpOfflineIntentGeneration()
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
            // Fresh coverage predicate (issue: round 8 #8): a nested root added after this
            // traversal's own subtree check stops the clearing inside the committing
            // transaction — the traversal-time skip at the top of the loop cannot see it.
            val removalClearable: (Realm, File) -> Boolean = { currentRealm, candidate ->
                getOfflineRoot(candidate.id, customRealm = currentRealm)
                    ?.takeIf { it.isValid && !it.isPendingRemoval } == null &&
                    !isInsideActiveRoot(currentRealm, candidate.id)
            }
            if (folder != null && folder.isUsable()) {
                clearFileCoverage(
                    context = context,
                    realm = realm,
                    file = folder,
                    userDrive = userDrive,
                    result = result,
                    isFolder = true,
                    isAborted = isAborted,
                    isStillClearable = removalClearable,
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
                            isStillClearable = removalClearable,
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
     *
     * [isStillClearable] is the context's fresh coverage predicate, re-evaluated INSIDE each
     * committing transaction (issue: round 8 #8): the root-removal cleanup rejects rows a
     * newly added nested root now covers, and the orphan sweep rejects rows a root added in
     * the meantime covers. Callers whose authority is the remote listing itself (the walk's
     * stale cleanup, the per-file sync deletion) pass nil and keep the pre-existing
     * semantics.
     */
    fun clearFileCoverage(
        context: Context,
        realm: Realm,
        file: File,
        userDrive: UserDrive,
        result: RemovalResult,
        isFolder: Boolean,
        isAborted: () -> Boolean = { false },
        isStillClearable: ((Realm, File) -> Boolean)? = null,
    ) {
        if (file.isOffline) return

        // Everything below mutates durable ownership: the coordination is held across the
        // whole read-decide-write section, and the final intent revalidation runs INSIDE the
        // committing transaction on a refreshed Realm (issue: round 7 #9) — a revival or a
        // pin committed by another thread during the lock wait or the snapshot copy is still
        // seen before any flag is cleared. Byte deletion stays inside the same coordinated
        // section, after the transaction committed the flags.
        if (isFolder) {
            OfflineFileCoordination.withFileLock(userDrive, file.id) {
                var cleared = false
                runCatching { realm.refresh() }
                realm.executeTransaction {
                    if (isAborted()) return@executeTransaction
                    val liveRow = FileController.getFileById(realm, file.id) ?: return@executeTransaction
                    if (liveRow.isOffline) return@executeTransaction
                    // Caller-supplied freshness predicate, evaluated on the transaction-fresh
                    // CURRENT row (issue: round 8 #8): each clearing context revalidates the
                    // ancestry that authorizes IT — the root-removal cleanup rejects rows a
                    // newly added nested root now covers, the orphan sweep rejects rows a
                    // root newly covers. A nil predicate keeps the pre-existing semantics:
                    // the walk's and the sync's remote-listing truth needs no ancestry gate.
                    isStillClearable?.let { clearable -> if (!clearable(realm, liveRow)) return@executeTransaction }
                    liveRow.isInOfflineFolder = false
                    liveRow.isMarkedAsOffline = false
                    liveRow.bumpOfflineIntentGeneration()
                    cleared = true
                }
                if (!cleared) result.keptOfflineFilesCount++
            }
            return
        }

        // EVERYTHING a file decision needs is resolved INSIDE coordination (issue: round 11
        // #9): the intent monitor precedes the file lock (one consistent order), the Realm is
        // refreshed, and the CURRENT row, its derived path, the bytes' existence and the
        // protection state are read under the lock — the caller's snapshot row can be stale
        // (a move reconciled the copy elsewhere since collection), and branch selection from
        // that stale view cleared a current row using an obsolete path's existence result and
        // judged protection against bytes the row no longer owns.
        synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive, file.id) {
                runCatching { realm.refresh() }
                val liveRow = FileController.getFileById(realm, file.id) ?: return@withFileLock
                if (liveRow.isOffline) return@withFileLock
                // Caller-supplied freshness predicate, evaluated on the refreshed CURRENT row
                // INSIDE coordination (issues: round 8 #8, round 11 #8/#9): a root committed
                // after the caller collected this row is seen here, before any flag or byte
                // is touched.
                isStillClearable?.let { clearable ->
                    if (!clearable(realm, liveRow)) {
                        result.keptOfflineFilesCount++
                        return@withFileLock
                    }
                }
                val offlineFile = liveRow.getOfflineFile(context, userDrive.userId) ?: return@withFileLock

                // EXCLUSIVE fresh source ownership before snapshot or deletion (issue: round
                // 17 #2): stale F and current G can both claim P — cleanup of F must never
                // snapshot or delete G's sole edited bytes; the ambiguous path is KEPT.
                if (exclusivePathOwner(realm, context, userDrive, offlineFile.path) != liveRow.id) {
                    result.keptOfflineFilesCount++
                    return@withFileLock
                }

                // DURABLE RECOVERY EXCLUSION AT THE COVERAGE BOUNDARY (issue: round 25 #7):
                // an unresolved recovery governing this row or its path — including the
                // MISSING-target clearing arm below and the classified-clean byte deletion
                // after the flags clear — keeps the intent and the bytes out of this
                // cleanup; the sweep owns their resolution. The check runs before both.
                if (DownloadOfflineFileManager.hasUnresolvedRecovery(
                        context,
                        userDrive,
                        liveRow.id,
                        offlineFile.absolutePath,
                    )
                ) {
                    result.keptOfflineFilesCount++
                    return@withFileLock
                }

                if (!offlineFile.exists()) {
                    var cleared = false
                    realm.executeTransaction {
                        if (isAborted()) return@executeTransaction
                        val committedRow = FileController.getFileById(realm, file.id) ?: return@executeTransaction
                        if (committedRow.isOffline) return@executeTransaction
                        isStillClearable?.let { clearable -> if (!clearable(realm, committedRow)) return@executeTransaction }
                        committedRow.isInOfflineFolder = false
                        committedRow.isMarkedAsOffline = false
                        committedRow.bumpOfflineIntentGeneration()
                        cleared = true
                    }
                    if (!cleared) result.keptOfflineFilesCount++
                    return@withFileLock
                }

                val needsSnapshot: Boolean
                when (protectionStateOf(liveRow, offlineFile, userDrive)) {
                    CopyProtection.PENDING_UPLOAD -> {
                        result.keptOfflineFilesCount++
                        return@withFileLock
                    }
                    // Unproven historical bytes (issue: round 19 #2): the coverage flags and
                    // the bytes are RETAINED — never snapshotted for upload, never cleared —
                    // until the verification path resolves the generation.
                    CopyProtection.UNPROVEN_HISTORICAL -> {
                        result.keptOfflineFilesCount++
                        return@withFileLock
                    }
                    CopyProtection.UPLOADED -> needsSnapshot = false
                    CopyProtection.DIRTY -> {
                        val uploadRegistered = registerDirtySnapshot(context, liveRow, offlineFile, userDrive)
                        if (!uploadRegistered) {
                            // Keep the bytes and the flags rather than losing the edit; a later attempt
                            // (next sync) retries the snapshot.
                            result.keptOfflineFilesCount++
                            return@withFileLock
                        }
                        needsSnapshot = true
                    }
                    CopyProtection.NONE -> needsSnapshot = false
                }

                // The committing transaction revalidates the removal intent (root revival via the
                // abort predicate), the live pin state AND the context's fresh coverage predicate
                // on the transaction-fresh row, immediately before the flags clear (issues: round
                // 7 #9, round 9 #6, round 11 #9).
                var cleared = false
                // THE OPERATION'S OWN EXPECTED MUTATION (issue: round 26 #13): the transaction
                // clears the marker and bumps the durable generation — the post-commit
                // re-validation normalizes a DETACHED comparison copy back to the PRE-COMMIT
                // marker/generation (the exact values this same predicate just validated on the
                // transaction-fresh row), so cleanup never rejects its own mutation.
                var preCommitMarker = false
                var preCommitStamp = 0L
                realm.executeTransaction {
                    if (isAborted()) return@executeTransaction
                    val committedRow = FileController.getFileById(realm, file.id) ?: return@executeTransaction
                    if (committedRow.isOffline) return@executeTransaction
                    isStillClearable?.let { clearable -> if (!clearable(realm, committedRow)) return@executeTransaction }
                    preCommitMarker = committedRow.isMarkedAsOffline
                    preCommitStamp = committedRow.offlineIntentStamp
                    committedRow.isInOfflineFolder = false
                    committedRow.isMarkedAsOffline = false
                    committedRow.bumpOfflineIntentGeneration()
                    cleared = true
                }
                if (cleared) {
                    // Intent AND freshness re-check AFTER the commit, BEFORE the bytes go (issues:
                    // round 8 #8, round 9 #6, round 10 #5): the flag commit and the byte deletion
                    // must share one intent protocol. The realm is refreshed first so commits from
                    // other threads are actually visible here, and the global intent monitor holds
                    // through this decision and the deletion — a nested-root addition cannot
                    // commit inside it. The predicate is applied against the NORMALIZED view: the
                    // marker/generation arms see the pre-commit values the transaction validated;
                    // every arm the operation did not touch (identity, publication token,
                    // ancestry) still reads the LIVE row, so an external invalidation in the
                    // commit→deletion window is still caught.
                    runCatching { realm.refresh() }
                    val normalized = FileController.getFileById(realm, file.id)?.let { live ->
                        realm.copyFromRealm(live, 1).apply {
                            isMarkedAsOffline = preCommitMarker
                            offlineIntentStamp = preCommitStamp
                        }
                    }
                    val intentRevoked = isAborted() ||
                        (isStillClearable != null && (normalized == null || isStillClearable(realm, normalized) == false))
                    if (!intentRevoked) {
                        deleteOfflineCopy(context, liveRow, offlineFile, userDrive, result)
                    } else {
                        result.keptOfflineFilesCount++
                    }
                } else {
                    result.keptOfflineFilesCount++
                }
            }
        }
    }

    internal enum class CopyProtection { NONE, PENDING_UPLOAD, UPLOADED, DIRTY, UNPROVEN_HISTORICAL }

    /**
     * Schedules generation VERIFICATION for a readable-but-unproven offline copy (issues:
     * round 17 #15, round 18 #15/#17): the pending-download marker drives the walk/worker
     * verification, so a legacy copy is neither blocked behind its missing provenance nor
     * silently treated as current. Coordinated like every intent write, idempotent.
     *
     * CONDITIONAL (issue: round 18 #15, round 19 #16): the marker is written only when the
     * CURRENT row still carries the durable intent that authorizes retention — an explicit
     * pin, or inherited coverage by a still-active root matching the inherited flag. The
     * write is NOT gated on the caller's retained-BYTE mtime (the previous
     * observed-generation gate): a row whose accepted metadata advanced past the retained
     * copy is exactly the stale-copy case that needs verification, the marker carries no
     * generation, and the worker's redrive verifies against the CURRENT generation.
     * Verification never CREATES offline authorization: the delayed call of a preview
     * whose copy was unpinned meanwhile (or whose root was removed) is a no-op and cannot
     * resurrect the removed intent.
     *
     * DURABLE ENQUEUE (issue: round 18 #17): after the conditional commit the scoped
     * verification/download work is enqueued — a marker alone guarantees no worker when an
     * explicit pin sits outside any walked root, so an unchanged legacy pin stayed
     * unverified indefinitely while the dialog reported completion. A failed enqueue keeps
     * the marker as its durable retry evidence (the sync's re-drive re-enqueues from it).
     */
    fun scheduleOfflineVerification(
        fileId: Int,
        userDrive: UserDrive,
        observedGeneration: Long? = null,
        /**
         * The pending request's ORIGIN (issue: round 23 #21): when supplied, it overrides
         * the `!isOffline` derivation — a marker-only EXPLICIT download must use the
         * marker-authorized explicit path (inherited work requires a pin or root coverage
         * to publish, which the marker-only recovery state can never satisfy), while
         * inherited verification stays dependent on active-root coverage.
         */
        forceExplicitDownload: Boolean? = null,
    ) {
        val appContext = Realm.getApplicationContext()
        synchronized(OfflineFileCoordination.rootIntentMonitor) {
            OfflineFileCoordination.withFileLock(userDrive, fileId) {
                val detachedRow = FileController.getRealmInstance(userDrive).use { realm ->
                    runCatching { realm.refresh() }
                    var marked = false
                    realm.executeTransaction {
                        val liveRow = FileController.getFileById(realm, fileId) ?: return@executeTransaction
                        if (liveRow.isMarkedAsOffline) {
                            marked = true
                            return@executeTransaction
                        }
                        // CURRENT durable intent only (issue: round 18 #15) — this ROW +
                        // INTENT identity check is the write's whole authorization (issue:
                        // round 19 #16).
                        val hasDurableIntent = liveRow.isOffline ||
                            (liveRow.isInOfflineFolder && isInsideActiveRoot(realm, fileId))
                        // The previous OBSERVED-GENERATION gate rejected the
                        // marker whenever the row's accepted metadata was NEWER than the
                        // caller's retained-BYTE mtime — exactly the normal
                        // newer-metadata case that NEEDS verification (the retained copy
                        // is stale). The marker carries no generation: the worker's
                        // redrive re-fetches the authoritative details and verifies
                        // against the CURRENT generation regardless of what the caller
                        // observed; a row with newer pending work is already marked (the
                        // early return above), and a delayed caller cannot resurrect a
                        // removed intent (the durable-intent check).
                        if (!hasDurableIntent) return@executeTransaction
                        liveRow.isMarkedAsOffline = true
                        liveRow.bumpOfflineIntentGeneration()
                        marked = true
                    }
                    if (marked) FileController.getFileById(realm, fileId)?.let { realm.copyFromRealm(it, 0) } else null
                }
                if (detachedRow != null && appContext != null) {
                    runCatching {
                        Utils.downloadAsOfflineFileOperation(
                            context = appContext,
                            file = detachedRow,
                            userDrive = userDrive,
                            // Follows the CURRENT authorization: a covered non-pinned row
                            // must never manufacture a pin through the verification work.
                            isInheritedDownload = forceExplicitDownload ?: !detachedRow.isOffline,
                            // VERIFICATION NEVER CANCELS (issue: round 19 #17): repeated
                            // scheduling keeps the existing account-scoped verification
                            // work and never kills in-flight downloads for the file.
                            cancelPendingWork = false,
                        )
                    }.onFailure { exception ->
                        SentryLog.e(
                            TAG,
                            "Failed to enqueue verification work for $fileId; marker retained as retry evidence",
                            exception,
                        )
                    }
                }
            }
        }
    }

    /**
     * Preservation state of an existing offline copy, shared with the deferred-cleanup
     * completion check (issue #1902): [PENDING_UPLOAD] and [DIRTY] copies must survive
     * cleanup, [UPLOADED] copies are content-safe duplicates, [NONE] copies are clearable,
     * and [CopyProtection.UNPROVEN_HISTORICAL] copies — bytes whose content is neither
     * provably the published generation nor a durably owned upload revision — are PRESERVED
     * through destructive operations without being snapshotted for upload (issue: round 19
     * #2).
     */
    internal fun protectionStateOf(file: File, offlineFile: IOFile, userDrive: UserDrive): CopyProtection {
        val fileModifiedAt = Date(offlineFile.lastModified())
        val context = Realm.getApplicationContext() ?: return CopyProtection.UNPROVEN_HISTORICAL
        // EXACT content identity (issue: round 18 #7): a record matching the destination
        // identity and modification date owns these bytes only when it provably holds THEIR
        // content — a same-mtime record of a different revision (an edit that retained the
        // mtime) must not certify protection for bytes it never held.
        val contentHash = UploadFile.sha256OrNull(offlineFile)
        return when {
            // A pending upload owns the bytes; so does an UNSENT record retained after a
            // failure or cancellation (soft-deleted): it is still the only copy of the edit,
            // regardless of the row's advanced clean baseline (issue: round 6 #1).
            UploadFile.hasPendingUpload(uri = Uri.fromFile(offlineFile).toString()) ||
                UploadFile.hasRetainedUnsentUpload(uri = Uri.fromFile(offlineFile).toString()) -> CopyProtection.PENDING_UPLOAD
            contentHash != null && UploadFile.hasUploadRecordFor(
                fileName = file.name,
                remoteFolder = file.parentId,
                driveId = userDrive.driveId,
                userId = userDrive.userId,
                fileModifiedAt = fileModifiedAt,
                pendingOnly = true,
                contentHash = contentHash,
                originalFileId = file.id,
            ) -> CopyProtection.PENDING_UPLOAD
            contentHash != null && UploadFile.hasUploadRecordFor(
                fileName = file.name,
                remoteFolder = file.parentId,
                driveId = userDrive.driveId,
                userId = userDrive.userId,
                fileModifiedAt = fileModifiedAt,
                pendingOnly = false,
                contentHash = contentHash,
                originalFileId = file.id,
            ) -> CopyProtection.UPLOADED
            // AUTHORITATIVE content proof decides cleanliness (issue: round 19 #2): bytes
            // are the published generation only when the provenance record names THIS row's
            // accepted metadata. Size and a not-newer mtime never do — a same-size local
            // edit retaining the old mtime previously classified as NONE and destructive
            // operations destroyed it without a snapshot.
            file.storedOfflineCopyState(context, offlineFile, userDrive) == StoredOfflineCopyState.PROVEN_CURRENT -> CopyProtection.NONE
            // VERIFIED RETAINED GENERATION (issues: round 20 #3, round 20 #24, round 20
            // #2): the bytes' IMMUTABLE content identity matches their provenance record
            // — these ARE the generation the record published, even when the row's
            // accepted metadata has moved on. Byte identity, not metadata currency, is
            // what destructive cleanliness requires: root removal may delete them WITHOUT
            // current remote content or recreated offline intent (the retained
            // older-generation copy converges instead of pending forever), and the
            // same-second same-size edit (hash mismatch) falls through to the preserved
            // classes below instead of returning NONE.
            DownloadOfflineFileManager.readPublishedProvenance(context, offlineFile)
                ?.takeIf { it.identifies(file, userDrive) && it.certifiesBytes(offlineFile) } != null ->
                CopyProtection.NONE
            // POSITIVELY FOREIGN provenance (issue: round 19 #12): the record names another
            // owner at this location — the bytes are another identity's content. Never
            // classified as this row's edit, never snapshotted into this row's upload queue,
            // never deleted: PRESERVED like unproven historical bytes until the conflict
            // resolves (a fresh publication SKIPPED_CONFLICTs).
            file.storedOfflineCopyState(context, offlineFile, userDrive) == StoredOfflineCopyState.FOREIGN_CONFLICT ->
                CopyProtection.UNPROVEN_HISTORICAL
            // PROVEN SAME-MTIME DRIFT (issue: round 22 #4): the row's own record whose
            // content hash does NOT match the bytes is a proven edit REGARDLESS of mtime —
            // selected for snapshot, never classified unproven-historical (the seconds-
            // resolution mtime test alone missed the same-mtime case).
            DownloadOfflineFileManager.readPublishedProvenance(context, offlineFile)?.takeIf {
                it.identifies(file, userDrive) && it.contentHash != null &&
                    UploadFile.sha256OrNull(offlineFile) != it.contentHash
            } != null -> CopyProtection.DIRTY
            OfflineFolderPolicy.isLocallyModified(
                offlineFileLastModifiedMillis = offlineFile.lastModified(),
                // The PUBLISHED byte baseline, not accepted server metadata (issue: round
                // 15 #6) — retained old-clean bytes after a backward-dated newer
                // generation commits are not a local edit. The baseline must also be
                // ANCHORED (issue: round 22 #4): a historical copy whose baseline is UNSET
                // falls back to accepted remote metadata, which a backward-dated newer
                // revision re-labels — unproven historical bytes are never auto-uploaded
                // through that fallback.
                remoteLastModifiedAtSeconds = file.effectivePublishedBaseline,
            ) && (file.publishedCleanBaseline ?: 0L) > 0L -> CopyProtection.DIRTY
            // Unproven historical bytes (issue: round 19 #2): not proven as the published
            // generation, not newer than their baseline — an ambiguous retained copy (or a
            // same-mtime edit). PRESERVED: destructive operations keep the bytes and the
            // row; they are never snapshotted for upload (uncertain content must not be
            // automatically uploaded over a newer revision), and the ordinary verification
            // path resolves their generation.
            else -> CopyProtection.UNPROVEN_HISTORICAL
        }
    }

    /**
     * Establishes a historical row's byte provenance BEFORE any relocation or metadata
     * change (issues: round 17 #3, round 18 #6, round 19 #2): the baseline is anchored ONLY
     * from AUTHORITATIVE content proof — the published generation's provenance record naming
     * THIS row's accepted metadata. UNSET is consistent everywhere: null AND a non-positive
     * repaired value. An unproven historical copy keeps unset — it is never anchored from a
     * size/mtime comparison (a same-size edit retaining the old mtime was previously
     * certified clean), and it is not automatically labeled and uploaded as a local edit:
     * destructive operations PRESERVE it until the verification path resolves the
     * generation.
     */
    internal fun anchorHistoricalBaseline(context: Context, realm: Realm, row: File, bytes: IOFile, userDrive: UserDrive) {
        if ((row.publishedCleanBaseline ?: 0L) > 0L) return
        // EXCLUSIVE CURRENT-PATH OWNERSHIP before the anchor (issue: round 19 #7): the
        // historical baseline relabels [bytes] as this row's published generation — on a
        // contested or foreign path those bytes may be another identity's copy, and
        // anchoring them would launder foreign content into this row's provenance.
        if (exclusivePathOwner(realm, context, userDrive, bytes.path) != row.id) return
        if (row.storedOfflineCopyState(context, bytes, userDrive) == StoredOfflineCopyState.PROVEN_CURRENT) {
            runCatching { realm.executeTransaction { row.publishedCleanBaseline = row.lastModifiedAt } }
        }
    }

    /** EXACT content identity of [file]'s bytes (issue: round 18 #7), or null when unreadable. */
    private fun contentHashOf(file: IOFile): String? = UploadFile.sha256OrNull(file)

    internal fun registerDirtySnapshot(
        context: Context,
        file: File,
        offlineFile: IOFile,
        userDrive: UserDrive,
        allowPendingRecoveryResolution: Boolean = false,
    ): Boolean {
        // PENDING-RECOVERY EXCLUSION (issue: round 22 #3): a file with an unresolved
        // durable publication recovery is excluded from ALL snapshot registration — its
        // bytes may be partial output of an interrupted restore, and snapshotting them
        // would upload an unknown generation as an edit. ONLY the reconciliation sweep's
        // explicit intervening-revision preservation passes true (it IS the resolution
        // step).
        // UNDER-FINAL-LOCKS RECHECK (issue: round 23 #3): the durable recovery state is
        // re-read HERE — inside the caller's mutation locks — not from a batch-captured
        // set: a recovery created during the HTTP/lock window is still an exclusion, and
        // an enumeration failure fails CLOSED. This gate covers every registration caller
        // (the sync's own snapshot registration included). Only the reconciliation sweep's
        // explicit intervening-revision preservation passes true (it IS the resolution
        // step), and the sweep itself validates the recorded owner/path first (round 23 #2).
        if (!allowPendingRecoveryResolution &&
            DownloadOfflineFileManager.hasUnresolvedRecovery(context, userDrive, file.id, offlineFile.absolutePath)
        ) {
            return false
        }
        // Upload-source registration competes with publication and cleanup for these bytes
        // (issue: round 4 #4).
        return OfflineFileCoordination.withFileLock(userDrive, file.id) {
            runCatching {
                val fileModifiedAt = Date(offlineFile.lastModified())
                val snapshotUri = UploadFile.createDurableOfflineSnapshot(
                    context = context,
                    source = offlineFile,
                    fileModifiedAt = fileModifiedAt,
                    fileId = file.id,
                    driveId = userDrive.driveId,
                ) ?: return@withFileLock false
                // EXACT content identity of the COMPLETED IMMUTABLE SNAPSHOT (issue: round
                // 19 #4): the previous order hashed the MUTABLE original and stored that
                // hash beside the later copy — an external editor holding write permission
                // (Utils.openWithIntentExceptkDrive does not take this lock) could change
                // the bytes between hashing and copying, producing a record that claims
                // content A while uploading B, and dedup/completion would trust the false
                // identity. The completed snapshot is hashed (and sized) itself; the record
                // therefore describes the bytes it will actually upload.
                val snapshotFile = UploadFile.durableSnapshotFileOf(snapshotUri)
                val contentHash = snapshotFile?.let { UploadFile.sha256OrNull(it) } ?: run {
                    runCatching { IOFile(snapshotUri.path ?: "").delete() }
                    return@withFileLock false
                }
                // REVALIDATION of the ORIGINAL against the captured revision (issues: round
                // 19 #4, round 20 #4): registration certifies ownership of the original's
                // CURRENT bytes only when they still are the snapshotted revision — an
                // edit that raced the copy leaves the original's bytes unowned by this
                // record. An UNREADABLE original never substitutes the snapshot's hash for
                // itself: unreadability is not equality, so revalidation fails.
                val originalStillRevision = offlineFile.exists() &&
                    offlineFile.lastModified() == fileModifiedAt.time &&
                    offlineFile.length() == snapshotFile.length() &&
                    UploadFile.sha256OrNull(offlineFile) == contentHash
                // Revision dedup under the file lock (issues: round 8 #7, round 20 #4/#5):
                // this exact revision may already be registered — live (pending or sent),
                // retained after a cancellation, or retained by an ID-LESS cancelled legacy
                // record. Re-snapshotting would duplicate the pending work or silently
                // reactivate the cancelled upload; the dedup hit reports the ORIGINAL's
                // revalidation state, never blanket success — "snapshot A is durable" is
                // distinct from "the original still contains A", and a raced original
                // (now B, owned by no record) defers the caller instead of certifying its
                // deletion.
                if (UploadFile.hasUploadRecordFor(
                        fileName = file.name,
                        remoteFolder = file.parentId,
                        driveId = userDrive.driveId,
                        userId = userDrive.userId,
                        fileModifiedAt = fileModifiedAt,
                        pendingOnly = false,
                        contentHash = contentHash,
                        originalFileId = file.id,
                    ) || UploadFile.hasCancelledUploadRecordFor(
                        fileName = file.name,
                        remoteFolder = file.parentId,
                        driveId = userDrive.driveId,
                        userId = userDrive.userId,
                        fileModifiedAt = fileModifiedAt,
                        contentHash = contentHash,
                        originalFileId = file.id,
                    )
                ) {
                    // A duplicate registration adds nothing; the fresh snapshot copy is
                    // redundant residue either way.
                    runCatching { IOFile(snapshotUri.path ?: "").delete() }
                    return@withFileLock originalStillRevision
                }
                // AMBIGUOUS LEGACY CANCELLATION DEFERS (issue: round 21 #4): an ID-LESS
                // cancelled record matching content/destination blocks automatic
                // re-registration but can NEVER establish that record's stable ownership
                // of this file — returning the original's revalidation state here would
                // authorize deleting the current original and its metadata on an
                // ambiguous match. Registration stays blocked; the caller's preservation
                // DEFERS and the current bytes and row are retained.
                if (UploadFile.hasLegacyCancelledUploadRecordFor(
                        fileName = file.name,
                        remoteFolder = file.parentId,
                        driveId = userDrive.driveId,
                        userId = userDrive.userId,
                        fileModifiedAt = fileModifiedAt,
                        contentHash = contentHash,
                    )
                ) {
                    runCatching { IOFile(snapshotUri.path ?: "").delete() }
                    return@withFileLock false
                }
                UploadFile(
                    uri = snapshotUri.toString(),
                    driveId = userDrive.driveId,
                    fileModifiedAt = fileModifiedAt,
                    fileName = file.name,
                    fileSize = snapshotFile.length(),
                    remoteFolder = file.parentId,
                    // Stable ORIGINAL identity for completion reconciliation (issue: round
                    // 17 #4).
                    originalFileId = file.id,
                    // EXACT content identity of the snapshot (issues: round 18 #7, round 19 #4).
                    contentHash = contentHash,
                    type = UploadFile.Type.SYNC_OFFLINE.name,
                    userId = userDrive.userId,
                ).store()
                context.syncImmediately(isAutomaticTrigger = false)
                originalStillRevision
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
        // The published generation's provenance sidecar dies with the bytes (issue: round
        // 16 #12): an orphaned sidecar must never lend its generation to later bytes.
        DownloadOfflineFileManager.deletePublishedProvenance(context, offlineFile)
        runCatching { offlineFile.delete() }.onFailure { exception ->
            SentryLog.e(TAG, "Failed to delete offline file ${offlineFile.path}", exception)
        }.onSuccess {
            if (file.isMedia()) file.deleteInMediaScan(context, userDrive)
            result.removedOfflineFilesCount++
        }
    }

    private val TAG = OfflineFolderController::class.java.simpleName
}
