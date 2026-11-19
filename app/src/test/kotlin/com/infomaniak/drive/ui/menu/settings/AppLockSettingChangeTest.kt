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
package com.infomaniak.drive.ui.menu.settings

import com.infomaniak.drive.data.documentprovider.ProviderAccessPolicy
import com.infomaniak.drive.data.documentprovider.ProviderAccessPolicy.Reason
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class AppLockSettingChangeTest {

    // The actual success callback used by the Activity runs here. Authentication
    // and the asynchronous Realm transaction are controlled boundaries, not real
    // biometrics or disk IO. Completing a write models its committed value.
    private class Settings(private val providerDisabled: Boolean) {
        var durableLock = false
        var authenticateSuccess: ((Boolean) -> Unit)? = null
        val writes = mutableListOf<Pair<Boolean, () -> Unit>>()
        val notifications = mutableListOf<Pair<Boolean, Reason>>()
        var unlocks = 0

        fun request() = authenticateAppLockChange(
            authenticate = { authenticateSuccess = it },
            persistLock = { value, onCommitted -> writes += value to onCommitted },
            notifyProviderChanged = { notifications += durableLock to access() },
            unlock = { unlocks++ },
        )

        fun authenticate(value: Boolean) = requireNotNull(authenticateSuccess)(value)

        fun commit() {
            val (value, onCommitted) = writes.removeAt(0)
            durableLock = value
            onCommitted()
        }

        fun access() = ProviderAccessPolicy.evaluate(durableLock, providerDisabled)
    }

    @Test
    fun `provider off survives authenticated lock on and off`() {
        val settings = Settings(providerDisabled = true)
        settings.request()
        settings.authenticate(true)
        settings.commit()
        assertEquals(Reason.BLOCKED_BY_APP_LOCK, settings.access())
        settings.request()
        settings.authenticate(false)
        settings.commit()
        assertEquals(Reason.BLOCKED_BY_SETTING, settings.access())
        assertEquals(listOf(true, false), settings.notifications.map { it.first })
        assertEquals(1, settings.unlocks)
    }

    @Test
    fun `provider on remains independent while lock blocks even after unlock`() {
        val settings = Settings(providerDisabled = false)
        settings.request()
        settings.authenticate(true)
        settings.commit()
        assertEquals(1, settings.unlocks)
        assertEquals(Reason.BLOCKED_BY_APP_LOCK, settings.access())
        settings.request()
        settings.authenticate(false)
        settings.commit()
        assertEquals(Reason.ALLOWED, settings.access())
        assertEquals(listOf(Reason.BLOCKED_BY_APP_LOCK, Reason.ALLOWED), settings.notifications.map { it.second })
    }

    @Test
    fun `authentication success does not notify or unlock before durable commit`() {
        val settings = Settings(providerDisabled = false)
        settings.request()
        settings.authenticate(true)
        assertFalse(settings.durableLock)
        assertTrue(settings.notifications.isEmpty())
        assertEquals(0, settings.unlocks)
        assertEquals(listOf(true), settings.writes.map { it.first })
        settings.commit()
        assertEquals(listOf(true to Reason.BLOCKED_BY_APP_LOCK), settings.notifications)
        assertEquals(1, settings.unlocks)
    }

    @Test
    fun `lock off notification observes the newly committed unlocked value`() {
        val settings = Settings(providerDisabled = true).apply { durableLock = true }
        settings.request()
        settings.authenticate(false)
        assertTrue(settings.durableLock)
        assertTrue(settings.notifications.isEmpty())
        settings.commit()
        assertEquals(listOf(false to Reason.BLOCKED_BY_SETTING), settings.notifications)
        assertEquals(0, settings.unlocks)
    }

    @Test
    fun `failed authentication never commits or notifies`() {
        val settings = Settings(providerDisabled = true)
        settings.request()
        // Core does not invoke its success callback on authentication failure.
        assertTrue(settings.writes.isEmpty())
        assertFalse(settings.durableLock)
        assertTrue(settings.notifications.isEmpty())
        assertEquals(0, settings.unlocks)
        assertEquals(Reason.BLOCKED_BY_SETTING, settings.access())
    }

    @Test
    fun `cancelled authentication preserves an already enabled lock`() {
        val settings = Settings(providerDisabled = false).apply { durableLock = true }
        settings.request()
        // Cancellation likewise never invokes the success callback.
        assertTrue(settings.writes.isEmpty())
        assertTrue(settings.durableLock)
        assertTrue(settings.notifications.isEmpty())
        assertEquals(0, settings.unlocks)
        assertEquals(Reason.BLOCKED_BY_APP_LOCK, settings.access())
    }

    @Test
    fun `uncommitted write never publishes notification or unlock`() {
        val settings = Settings(providerDisabled = true)
        settings.request()
        settings.authenticate(true)
        // A failed transaction does not invoke the completion callback.
        assertFalse(settings.durableLock)
        assertTrue(settings.notifications.isEmpty())
        assertEquals(0, settings.unlocks)
        assertEquals(Reason.BLOCKED_BY_SETTING, settings.access())
    }
}
