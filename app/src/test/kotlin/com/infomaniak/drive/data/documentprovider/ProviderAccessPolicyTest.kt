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

import com.infomaniak.drive.data.documentprovider.ProviderAccessPolicy.Reason
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Test

/**
 * Pure policy tests for [ProviderAccessPolicy]: no Android dependencies, no execution environment
 * required. They encode the security contract expected from issue #1865.
 */
class ProviderAccessPolicyTest {

    @Test
    fun `app lock enabled blocks external access even when provider switch is on`() {
        val reason = ProviderAccessPolicy.evaluate(appLockEnabled = true, providerDisabledByUser = false)

        assertEquals(Reason.BLOCKED_BY_APP_LOCK, reason)
    }

    @Test
    fun `app lock enabled takes precedence over the user opt-out`() {
        val reason = ProviderAccessPolicy.evaluate(appLockEnabled = true, providerDisabledByUser = true)

        assertEquals(Reason.BLOCKED_BY_APP_LOCK, reason)
    }

    @Test
    fun `user opt-out blocks external access while app lock is disabled`() {
        val reason = ProviderAccessPolicy.evaluate(appLockEnabled = false, providerDisabledByUser = true)

        assertEquals(Reason.BLOCKED_BY_SETTING, reason)
    }

    @Test
    fun `external access is allowed only when neither app lock nor opt-out blocks it`() {
        val reason = ProviderAccessPolicy.evaluate(appLockEnabled = false, providerDisabledByUser = false)

        assertEquals(Reason.ALLOWED, reason)
    }
}
