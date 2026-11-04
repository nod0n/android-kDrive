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
package com.infomaniak.drive.ui.menu

import androidx.lifecycle.MutableLiveData
import androidx.lifecycle.ViewModel
import androidx.lifecycle.viewModelScope
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.api.CursorApiResponse
import com.infomaniak.drive.data.cache.FileController
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.AccountUtils
import com.infomaniak.drive.utils.IsComplete
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.launch

class GalleryViewModel : ViewModel() {

    val period = MutableStateFlow(GalleryPeriod.MONTH)

    private var getGalleryJob: Job? = null

    private var currentCursor: String? = null

    private var lastGalleryFiles = arrayListOf<File>()

    val galleryApiResult = MutableLiveData<Pair<ArrayList<File>, IsComplete>>()
    val needToRestoreFiles get() = galleryApiResult.isInitialized

    fun loadLastGallery(driveId: Int, ignoreCloud: Boolean) {
        lastGalleryFiles = arrayListOf()
        loadLastGallery(driveId, ignoreCloud, isFirstPage = true)
    }

    fun loadMoreGallery(driveId: Int, ignoreCloud: Boolean) {
        currentCursor?.let {
            loadLastGallery(driveId, ignoreCloud, isFirstPage = false, currentCursor)
        }
    }

    fun restoreGalleryFiles() {
        if (needToRestoreFiles) {
            val isComplete = galleryApiResult.value?.second ?: true
            galleryApiResult.value = lastGalleryFiles to isComplete
        }
    }

    private fun loadLastGallery(
        driveId: Int,
        ignoreCloud: Boolean,
        isFirstPage: Boolean,
        cursor: String? = null,
    ) {
        getGalleryJob?.cancel()
        getGalleryJob = viewModelScope.launch(Dispatchers.IO) {
            val result = getLastGallery(driveId, ignoreCloud, isFirstPage, cursor)
            galleryApiResult.postValue(result)
            lastGalleryFiles.addAll(result.first)
        }
    }

    private suspend fun getLastGallery(
        driveId: Int,
        ignoreCloud: Boolean,
        isFirstPage: Boolean,
        cursor: String?,
    ): Pair<ArrayList<File>, IsComplete> {
        getGalleryJob?.cancel()
        getGalleryJob = Job()

        return if (ignoreCloud) emitRealmGallery() else fetchApiGallery(driveId, isFirstPage, cursor)
    }

    private fun emitRealmGallery(): Pair<ArrayList<File>, Boolean> {
        currentCursor = null
        return FileController.getGalleryDrive() to true
    }

    private suspend fun fetchApiGallery(driveId: Int, isFirstPage: Boolean, cursor: String?): Pair<ArrayList<File>, Boolean> {
        // The COMPLETE scope — user AND drive — is captured BEFORE the call, and the request
        // runs on its HTTP client (issue: round 9 #4): only the drive was captured before, so
        // a response landing after an account switch was persisted into the new account's
        // realm and its cleanup inspected the wrong filesystem. Gallery's write has no
        // cancellation check that would prevent it.
        val capturedUserDrive = UserDrive(userId = AccountUtils.currentUserId, driveId = driveId)
        val apiResponse = ApiRepository.getLastGallery(
            driveId = driveId,
            cursor = cursor,
            okHttpClient = AccountUtils.getHttpClient(capturedUserDrive.userId),
        )
        return if (apiResponse.isSuccess()) {
            currentCursor = apiResponse.cursor
            emitApiGallery(apiResponse, isFirstPage, capturedUserDrive)
        } else {
            emitRealmGallery()
        }
    }

    private fun emitApiGallery(
        apiResponse: CursorApiResponse<ArrayList<File>>,
        isFirstPage: Boolean,
        capturedUserDrive: UserDrive,
    ): Pair<ArrayList<File>, Boolean> {
        val data = apiResponse.data

        val results = if (data.isNullOrEmpty()) {
            arrayListOf<File>() to true
        } else {
            // The request's captured scope scopes the persistence (issue: round 9 #4).
            FileController.storeGalleryDrive(data, isFirstPage, capturedUserDrive)
            val isComplete = !apiResponse.hasMore
            data to isComplete
        }

        if (isFirstPage) {
            // Associated cleanup in the captured scope (issue: round 9 #4).
            FileController.getRealmInstance(capturedUserDrive).use { capturedRealm ->
                FileController.removeOrphanFiles(capturedRealm)
            }
        }
        return results
    }

    override fun onCleared() {
        getGalleryJob?.cancel()
        super.onCleared()
    }
}
