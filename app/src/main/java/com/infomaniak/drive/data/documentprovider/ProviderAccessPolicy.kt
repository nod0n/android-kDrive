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
package com.infomaniak.drive.data.documentprovider

/**
 * Pure decision policy for external access to [CloudStorageProvider].
 *
 * External access (Android Files app and any other app reaching the DocumentsProvider) is blocked
 * while kDrive's app lock is enabled: the provider cannot verify that the user unlocked the app,
 * and app/Core lock state is neither persisted nor provider-visible, so no unlock session is
 * trusted here. The user opt-out switch only matters while app lock is disabled.
 */
object ProviderAccessPolicy {

    enum class Reason {
        /** App lock is enabled: external access is disabled regardless of the opt-out switch. */
        BLOCKED_BY_APP_LOCK,

        /** The user disabled the provider from kDrive's security settings. */
        BLOCKED_BY_SETTING,

        /** External access is permitted. */
        ALLOWED,
    }

    /**
     * @param appLockEnabled durable app-lock value; null when the store cannot be read, which
     *   fails closed (deny) rather than trusting an unknown state
     * @param providerDisabledByUser the independent provider opt-out, honored only while app lock is off
     */
    fun evaluate(appLockEnabled: Boolean?, providerDisabledByUser: Boolean): Reason = when {
        appLockEnabled == null || appLockEnabled -> Reason.BLOCKED_BY_APP_LOCK
        providerDisabledByUser -> Reason.BLOCKED_BY_SETTING
        else -> Reason.ALLOWED
    }
}
