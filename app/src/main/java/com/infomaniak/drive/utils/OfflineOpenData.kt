/* Copyright (C) 2026 Infomaniak Network SA. SPDX-License-Identifier: GPL-3.0-or-later */
package com.infomaniak.drive.utils

import com.infomaniak.drive.data.models.UserDrive

/** Account/Realm IO boundaries shared by the two original-file open paths. */
internal open class OfflineOpenData {
    open fun captureScope() = AccountUtils.captureCurrentScope()
    open fun <T> withScope(block: () -> T): T = AccountUtils.withCurrentScope(block)
    open fun realm(userDrive: UserDrive) = OfflineFolderSyncUtils.certifiedFreshRealm(userDrive)
}
