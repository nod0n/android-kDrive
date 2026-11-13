/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2026 Infomaniak Network SA
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.infomaniak.drive.ui.fileList.preview

import androidx.fragment.app.FragmentManager
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.LifecycleRegistry
import androidx.test.platform.app.InstrumentationRegistry
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.OfflineFolderSyncUtils.RemovalResult
import io.mockk.every
import io.mockk.mockk
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class PreviewOfflineRemovalTest {
    private val scope = UserDrive(userId = 951, driveId = 751)

    private fun withAdapter(block: (PreviewSliderAdapter, File, File) -> Unit) {
        InstrumentationRegistry.getInstrumentation().runOnMainSync {
            val owner = mockk<LifecycleOwner>()
            val lifecycle = LifecycleRegistry(owner)
            every { owner.lifecycle } returns lifecycle
            val adapter = PreviewSliderAdapter(mockk<FragmentManager>(relaxed = true), lifecycle, scope)
            val selected = File(id = 610, driveId = scope.driveId, type = "file", isOffline = true)
            val nextPage = File(id = 611, driveId = scope.driveId, type = "file", isOffline = true)
            adapter.setFiles(arrayListOf(selected, nextPage))
            try {
                block(adapter, selected, nextPage)
            } finally {
                lifecycle.currentState = Lifecycle.State.CREATED
                lifecycle.currentState = Lifecycle.State.DESTROYED
            }
        }
    }

    @Test
    fun completionAfterSwipeUpdatesOnlyTheSelectedRowAndDoesNotRefreshTheNewPage() = withAdapter { adapter, selected, nextPage ->
        // The real completion seam is called after the await, with B now visible.
        assertFalse(adapter.applyOfflineRemoval(selected.id, scope, scope, nextPage, RemovalResult.PRESERVED))
        assertFalse(adapter.getFile(0).isOffline)
        assertTrue(adapter.getFile(0).offlineNeedsAttention)
        assertTrue(adapter.getFile(1).isOffline)
        assertFalse(adapter.getFile(1).offlineNeedsAttention)
        // When A remains visible, a refusal retains offline status and permits its UI.
        assertTrue(adapter.applyOfflineRemoval(selected.id, scope, scope, selected, RemovalResult.REFUSED))
        assertTrue(selected.isOffline && selected.offlineNeedsAttention)
    }

    @Test
    fun completionFromAnotherScopeDoesNotChangeEvenACollidingFileId() = withAdapter { adapter, selected, nextPage ->
        val otherScope = scope.copy(userId = scope.userId + 1)
        assertFalse(adapter.applyOfflineRemoval(selected.id, otherScope, scope, selected, RemovalResult.REMOVED))
        assertFalse(adapter.applyOfflineRemoval(selected.id, scope, otherScope, selected, RemovalResult.REMOVED))
        assertTrue(selected.isOffline && nextPage.isOffline)
        assertFalse(selected.offlineNeedsAttention || nextPage.offlineNeedsAttention)
    }
}
