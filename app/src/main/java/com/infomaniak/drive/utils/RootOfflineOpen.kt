/* Copyright (C) 2026 Infomaniak Network SA. SPDX-License-Identifier: GPL-3.0-or-later */
package com.infomaniak.drive.utils

import android.content.Context
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import java.io.IOException

/** An original-file open must not become a second root publisher. Root maintenance owns
 * refresh and basis adoption. Uncovered direct pins retain their existing refresh semantics.
 * Body streaming is outside these short holds; scope, path and ownership are reacquired at
 * publication, including a root adopted while the request was in flight. */
internal class RootOfflineOpen(
    private val context: Context,
    private val fileId: Int,
    private val userDrive: UserDrive,
    private val destination: IOFile,
    private val data: OfflineOpenData,
    private val captured: AccountUtils.CurrentScope,
) {
    fun existingCopy(): IOFile? = checked(null)

    fun publish(staging: IOFile): IOFile = checked {
        if (!staging.renameTo(destination)) throw IOException("Could not publish offline original")
        destination
    } ?: throw IOException("Offline original publication refused")

    private fun checked(uncovered: (() -> IOFile)?): IOFile? = data.withScope {
        if (userDrive.sharedWithMe || captured.userId != userDrive.userId || captured.driveId != userDrive.driveId ||
            captured != data.captureScope()) throw IOException("Offline open scope changed")
        OfflineCommitAuthority.withCommitAuthorityForEvidence {
            data.realm(userDrive).use { realm ->
                val row = realm.where(File::class.java).equalTo(File::id.name, fileId)
                    .equalTo(File::driveId.name, userDrive.driveId).findFirst()
                    ?: throw IOException("Offline open row unavailable")
                val currentPath = row.getOfflineFile(context, userDrive.userId)
                if (currentPath?.absolutePath != destination.absolutePath) throw IOException("Offline open path changed")
                if (!OfflineFolderSyncUtils.hasLiveOfflineIntent(row, realm, userDrive)) {
                    throw IOException("Offline intent withdrawn")
                }
                if (OfflineFolderSyncUtils.isRootOwnedCopy(row, realm, userDrive)) {
                    val recorded = row.offlinePublishedPath
                    if ((!recorded.isNullOrBlank() && recorded != destination.absolutePath) || !destination.isFile) {
                        // Relocation and missing-copy publication belong to root maintenance, not a preview.
                        throw IOException("Root original requires maintenance")
                    }
                    val basis = if (row.offlineBasisModifiedAt > 0) OfflineFolderSyncUtils.PublicationBasis(
                        row.offlineBasisModifiedAt, row.offlineBasisSize, row.offlineContentDigest, row.serverContentHash,
                    ) else null
                    val state = OfflineFolderSyncUtils.classifyLocalCopy(basis, true, destination.length(), destination.lastModified() / 1000)
                    if (state != OfflineFolderSyncUtils.LocalCopyState.INTACT || basis?.digest == null) {
                        if (!row.offlineNeedsAttention) OfflineCommitAuthority.withCommitAuthority {
                            realm.executeTransaction { row.offlineNeedsAttention = true }
                        }
                    }
                    // Do not compare with remote mtime/size or rewrite the accepted basis here.
                    // Same-stat digest verification remains the maintenance path's responsibility.
                    destination
                } else {
                    if (!row.isOffline) throw IOException("Offline intent withdrawn")
                    uncovered?.let { OfflineCommitAuthority.withCommitAuthority(it) }
                }
            }
        }
    }
}
