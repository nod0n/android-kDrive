/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2026 Infomaniak Network SA
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.infomaniak.drive.utils

import android.content.Context
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import kotlin.coroutines.coroutineContext

/** Owned by MainViewModel, not the actions sheet which pops immediately after a click. */
internal class DirectOfflinePin(
    private val ownerScope: CoroutineScope,
    private val data: DirectOfflinePinData,
    private val ioDispatcher: CoroutineDispatcher = Dispatchers.IO,
) {
    fun request(fileId: Int, userDrive: UserDrive, onResult: (Boolean) -> Unit) = run {
        val captured = data.captureScope()
        ownerScope.launch {
            val completed = try {
                withContext(ioDispatcher) {
                    // Reacquire on this thread; never transport a managed UI Realm object.
                    val file = data.withScope {
                        if (!isCurrent(captured, userDrive)) null else data.commitPin(fileId, userDrive)
                    } ?: return@withContext false
                    val workContext = coroutineContext
                    workContext.ensureActive()
                    if (!isCurrent(captured, userDrive)) return@withContext false
                    when (data.publishCached(file, userDrive, captured)) {
                        DirectOfflinePinData.CacheResult.PUBLISHED -> true
                        DirectOfflinePinData.CacheResult.REFUSED -> false
                        DirectOfflinePinData.CacheResult.UNAVAILABLE -> data.withScope {
                            workContext.ensureActive()
                            if (!isCurrent(captured, userDrive)) false else {
                                data.enqueue(file, userDrive)
                                true
                            }
                        }
                    }
                }
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (_: Exception) {
                false
            }
            // Back on the owner's Main scope. Never publish into a replacement account/drive.
            data.withScope { if (isCurrent(captured, userDrive)) onResult(completed) }
        }
    }

    private fun isCurrent(captured: AccountUtils.CurrentScope, userDrive: UserDrive): Boolean =
        !userDrive.sharedWithMe && captured.userId == userDrive.userId && captured.driveId == userDrive.driveId &&
            captured == data.captureScope()
}

/** Concrete IO boundaries; the request above owns ordering, cancellation and UI delivery. */
internal open class DirectOfflinePinData(private val context: Context) {
    enum class CacheResult { UNAVAILABLE, PUBLISHED, REFUSED }

    open fun captureScope() = AccountUtils.captureCurrentScope()
    open fun <T> withScope(block: () -> T): T = AccountUtils.withCurrentScope(block)

    open fun commitPin(fileId: Int, userDrive: UserDrive): File? = OfflineCommitAuthority.withCommitAuthority {
        OfflineFolderSyncUtils.certifiedFreshRealm(userDrive).use { realm ->
            val row = FileController.getFileById(realm, fileId) ?: return@use null
            if (row.driveId != userDrive.driveId || row.isFolder() || Utils.getInvalidFileNameCharacter(row.name) != null) {
                return@use null
            }
            if (!FileController.setDirectlyPinned(realm, fileId, isDirectlyPinned = true)) return@use null
            realm.copyFromRealm(row, 1)
        }
    }

    open fun publishCached(file: File, userDrive: UserDrive, captured: AccountUtils.CurrentScope): CacheResult {
        val cache = file.getCacheFile(context, userDrive)
        if (!cache.exists() || file.path.isEmpty() || file.isObsoleteOrNotIntact(cache)) return CacheResult.UNAVAILABLE
        val offline = file.getOfflineFile(context, userDrive.userId) ?: return CacheResult.REFUSED
        // Staging/digest IO is outside the account lock. Final publication checks the same
        // revision under that lock, then takes the existing offline commit authority.
        return if (Utils.moveCacheFileToOffline(file, cache, offline, userDrive, captured)) {
            CacheResult.PUBLISHED
        } else CacheResult.REFUSED
    }

    open fun enqueue(file: File, userDrive: UserDrive) = Utils.downloadAsOfflineFile(context, file, userDrive)
}
