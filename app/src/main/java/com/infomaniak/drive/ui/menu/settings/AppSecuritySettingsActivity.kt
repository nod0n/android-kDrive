/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2022-2026 Infomaniak Network SA
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

import android.os.Bundle
import androidx.appcompat.app.AppCompatActivity
import androidx.core.view.isGone
import androidx.core.view.isVisible
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.lifecycleScope
import androidx.lifecycle.repeatOnLifecycle
import com.infomaniak.core.applock.AppLockHelper.silentlyReverseSwitch
import com.infomaniak.core.applock.AppLockManager
import com.infomaniak.core.common.extensions.isNightModeEnabled
import com.infomaniak.core.ui.view.utils.SnackbarUtils.showSnackbar
import com.infomaniak.core.twofactorauth.front.TwoFactorAuthApprovalAutoManagedBottomSheet
import com.infomaniak.core.twofactorauth.front.addComposeOverlay
import com.infomaniak.drive.R
import com.infomaniak.drive.data.documentprovider.AppLockState
import com.infomaniak.drive.data.models.AppSettings
import com.infomaniak.drive.databinding.ViewSwitchSettingsBinding
import com.infomaniak.drive.twoFactorAuthManager
import kotlinx.coroutines.launch

class AppSecuritySettingsActivity : AppCompatActivity() {

    private val binding: ViewSwitchSettingsBinding by lazy { ViewSwitchSettingsBinding.inflate(layoutInflater) }

    override fun onCreate(savedInstanceState: Bundle?): Unit = with(binding) {
        super.onCreate(savedInstanceState)
        setContentView(root)
        addComposeOverlay {
            TwoFactorAuthApprovalAutoManagedBottomSheet(
                twoFactorAuthManager,
                isInDarkTheme = isNightModeEnabled()
            )
        }
        toolbar.setNavigationOnClickListener { onBackPressed() }

        switchSettingsTitle.text = getString(R.string.appSecurityTitle)
        description.text = getString(R.string.appSecurityDescription)
        image.setImageResource(R.drawable.ic_face_id_edit)
        enableFaceId.isVisible = true
        usersRecyclerView.isGone = true
        addUser.isGone = true

        enableFaceIdSwitch.apply {
            enableFaceId.setOnClickListener { isChecked = !isChecked }
            isChecked = AppSettings.appSecurityLock
            setOnCheckedChangeListener { switch, _ ->
                // Programmatic reconciliations set the tag to skip the interactive flow.
                if (switch.tag != null) return@setOnCheckedChangeListener
                // Reverse switch (before official parameter changed) by silent click
                silentlyReverseSwitch(this) { shouldLock ->
                    AppLockRequest.request(shouldLock)
                }
            }
        }

        // Pending/completion state is process-wide: every instance of this screen reconciles
        // with it, including one opened while a persistence is still running (R3-5).
        lifecycleScope.launch {
            repeatOnLifecycle(Lifecycle.State.STARTED) {
                AppLockRequest.state.collect { uiState -> renderAppLockRequestState(uiState) }
            }
        }
    }

    private fun renderAppLockRequestState(uiState: AppLockRequest.UiState) = with(binding) {
        when (uiState) {
            is AppLockRequest.UiState.Idle -> {
                enableFaceId.isEnabled = true
                enableFaceIdSwitch.isEnabled = true
                setSwitchCheckedSilently(AppSettings.appSecurityLock)
            }
            is AppLockRequest.UiState.Pending -> {
                // Disable the whole interactive row: the card click would otherwise flip the
                // switch while its change is still being persisted.
                enableFaceId.isEnabled = false
                enableFaceIdSwitch.isEnabled = false
            }
            is AppLockRequest.UiState.Completed -> {
                enableFaceId.isEnabled = true
                enableFaceIdSwitch.isEnabled = true
                // Acknowledge only the durably committed value; the published cache tracks it.
                setSwitchCheckedSilently(AppSettings.appSecurityLock)
                if (!uiState.persisted) {
                    showSnackbar(R.string.appSecurityPersistError)
                } else if (uiState.shouldLock) {
                    AppLockManager.unlock()
                }
                AppLockRequest.markDelivered()
            }
        }
    }

    private fun setSwitchCheckedSilently(checked: Boolean) = with(binding) {
        // silentlyReverseSwitch and the listener skip switches whose tag is non-null.
        enableFaceIdSwitch.tag = true
        enableFaceIdSwitch.isChecked = checked
        enableFaceIdSwitch.tag = null
    }
}
