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
package com.infomaniak.drive.data.models

import io.realm.RealmObject
import io.realm.annotations.PrimaryKey
import java.util.Date

/**
 * Durable binding between a local media item (its MediaStore uri) and the exact server file
 * created by uploading it. A pairing is ONLY created from the recorded upload response (the
 * finish-session reply carries the created [File]); nothing is inferred from names, sizes or
 * dates, so historical uploads without such a recording stay unpaired.
 *
 * Transitions (trash/restore on either side) are detected against this durable state, not
 * against MediaStore timestamps: [baselineTrashed] is the activation baseline, and
 * [lastObservedTrashed] / [lastLocalTrashReconciledAt] / [remoteTrashPendingSince] /
 * [lastRemoteTrashAppliedAt] carry the observed and propagated transition states (epoch
 * millis; 0 = never). Keeping the state here survives process death and re-arms the pair
 * after a restore, so operation completion never permanently disables it.
 */
open class PhotoPairing(
    @PrimaryKey var uri: String = "",
    var userId: Int = -1,
    var driveId: Int = -1,
    var remoteFileId: Int = -1,
    var fileName: String = "",
    var localSizeAtPairing: Long = 0L,
    var localModifiedAtPairing: Long = 0L, // local media lastModified (ms) bound at pairing
    /**
     * sha-256 of the uploaded source bytes, bound to the ACTUAL upload: the digest of the
     * direct-upload stream when it was available, otherwise a pairing-time sample (the
     * transfer-to-sample window is a disclosed residual race). null = unknown — there is NO
     * metadata-only fallback: an unbound pairing is never eligible for a destructive action
     * on either side (the destructive boundary re-verification and the consent arming both
     * require this binding).
     */
    var localContentDigest: String? = null,
    /**
     * Server content hash of the created file at pairing (the response's with=hash identity);
     * null = the response did not establish one. The remote side of every destructive
     * decision compares fresh remote hashes against this basis — an unbound basis is unknown
     * evidence and preserves.
     */
    var remoteContentHash: String? = null,
    var pairedAt: Date? = null,
    /** Activation baseline: trashed-state of the local item when the feature was enabled; null = baseline not yet captured. */
    var baselineTrashed: Boolean? = null,
    var lastObservedTrashed: Boolean = false,
    var lastLocalTrashReconciledAt: Long = 0L,
    var remoteTrashPendingSince: Long = 0L,
    var lastRemoteTrashAppliedAt: Long = 0L,
    var state: String = State.ACTIVE.name,
) : RealmObject() {

    fun isActive() = state == State.ACTIVE.name

    enum class State {
        ACTIVE,
        /** The local media changed since pairing; never propagate against it automatically. */
        CONFLICT_LOCAL_CHANGED,
        /** The server file changed since pairing; never propagate against it automatically. */
        CONFLICT_REMOTE_CHANGED,
        /** Last observation was inconclusive (missing volume, revoked access, transient error). */
        INCONCLUSIVE,
        /**
         * The user declined the pending cloud->device trash consent. The denial is respected
         * until the remote file is restored (ClearPending expires it) or the local content
         * changes (a conflict re-evaluates the pairing) — without a remote trash timestamp,
         * re-asking on every pass would be a consent nagging loop.
         */
        DENIED,
    }
}
