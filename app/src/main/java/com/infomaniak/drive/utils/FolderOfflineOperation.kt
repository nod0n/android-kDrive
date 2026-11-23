/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2026 Infomaniak Network SA
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.infomaniak.drive.utils

import android.content.Context
import com.infomaniak.drive.R
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.OfflineFolderRoot
import com.infomaniak.drive.data.models.UserDrive
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.CoroutineStart
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlin.coroutines.coroutineContext

internal data class FolderOfflineState(val enabled: Boolean, val needsAttention: Boolean)

data class FolderOfflineResult(val requestedEnabled: Boolean, val completed: Boolean, val needsAttention: Boolean) {
    val messageResId: Int? get() = when {
        !completed -> if (requestedEnabled) R.string.anErrorHasOccurred else R.string.offlineRemovalRefused
        needsAttention -> R.string.offlineCopyPreserved
        else -> null
    }
}

/** Activity ViewModel ownership; the originating sheet may disappear during any IO phase. */
internal class FolderOfflineOperation(
    private val owner: CoroutineScope,
    private val data: FolderOfflineOperationData,
    private val io: CoroutineDispatcher = Dispatchers.IO,
) {
    private val requests = Mutex()

    fun request(folderId: Int, scope: UserDrive, enabled: Boolean, onResult: (FolderOfflineResult) -> Unit) = run {
        val captured = data.captureScope()
        // Enter/queue synchronously in call order, as with the existing lock-setting queue.
        owner.launch(start = CoroutineStart.UNDISPATCHED) {
            requests.withLock {
                val result = try {
                    withContext(io) {
                        val folder = data.withScope {
                            if (!isCurrent(captured, scope)) null else data.folder(folderId, scope)
                        } ?: return@withContext FolderOfflineResult(enabled, false, false)
                        val work = coroutineContext
                        val checkRequest = {
                            work.ensureActive()
                            if (!isCurrent(captured, scope)) throw CancellationException("Offline folder scope changed")
                        }
                        checkRequest()
                        val confirmed = data.change(folder, scope, enabled, checkRequest) { id ->
                            data.withScope {
                                checkRequest()
                                data.enqueue(id, scope)
                            }
                        }
                        checkRequest()
                        val state = data.state(folderId, scope)
                        FolderOfflineResult(enabled, confirmed && state.enabled == enabled, state.needsAttention)
                    }
                } catch (cancelled: CancellationException) {
                    throw cancelled
                } catch (_: Exception) {
                    FolderOfflineResult(enabled, false, false)
                }
                data.withScope { if (isCurrent(captured, scope)) onResult(result) }
            }
        }
    }

    private fun isCurrent(captured: AccountUtils.CurrentScope, scope: UserDrive) =
        !scope.sharedWithMe && captured.userId == scope.userId && captured.driveId == scope.driveId &&
            captured == data.captureScope()
}

/** IO boundaries only; existing walk/withdrawal/authority decisions are unchanged. */
internal open class FolderOfflineOperationData(private val context: Context) {
    open fun captureScope() = AccountUtils.captureCurrentScope()
    open fun <T> withScope(block: () -> T): T = AccountUtils.withCurrentScope(block)

    open fun folder(id: Int, scope: UserDrive): File? = OfflineFolderSyncUtils.certifiedFreshRealm(scope).use { realm ->
        realm.where(File::class.java).equalTo(File::id.name, id).equalTo(File::driveId.name, scope.driveId)
            .findFirst()?.takeIf { it.isFolder() }?.let { realm.copyFromRealm(it, 0) }
    }

    open suspend fun change(
        folder: File,
        scope: UserDrive,
        enabled: Boolean,
        checkRequest: () -> Unit,
        enqueue: (Int) -> Unit,
    ): Boolean = if (enabled) {
        OfflineFolderSyncUtils.markFolderOffline(context, folder, scope, checkRequest, enqueue)
    } else OfflineFolderSyncUtils.unmarkFolderOffline(context, folder, scope, checkRequest)

    open fun state(id: Int, scope: UserDrive): FolderOfflineState {
        val enabled = OfflineFolderSyncUtils.certifiedFreshRealm(scope).use { realm ->
            realm.where(OfflineFolderRoot::class.java).equalTo(OfflineFolderRoot::folderId.name, id)
                .equalTo(OfflineFolderRoot::driveId.name, scope.driveId).count() > 0
        }
        return FolderOfflineState(enabled, OfflineFolderSyncUtils.folderNeedsAttention(scope, id))
    }

    open fun enqueue(id: Int, scope: UserDrive) = Utils.enqueueBulkDownloadWorker(context, id, scope)
}
