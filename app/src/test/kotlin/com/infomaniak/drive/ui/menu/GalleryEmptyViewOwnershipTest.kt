/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2026 Infomaniak Network SA
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.infomaniak.drive.ui.menu

import android.widget.LinearLayout
import androidx.recyclerview.widget.RecyclerView
import com.infomaniak.drive.databinding.FragmentGalleryBinding
import com.infomaniak.drive.databinding.LayoutNoNetworkSmallBinding
import com.infomaniak.drive.views.NoItemsLayoutView
import io.mockk.mockk
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.Test

class GalleryEmptyViewOwnershipTest {
    @Test
    fun emptyStateTargetsTheCurrentScrollerAfterEveryViewRecreation() {
        val fragment = GalleryFragment()
        val bindingField = GalleryFragment::class.java.getDeclaredField("_binding").apply { isAccessible = true }
        val constructor = FragmentGalleryBinding::class.java.declaredConstructors.single().apply { isAccessible = true }

        repeat(3) {
            val currentScroller = mockk<RecyclerViewFastScroller>()
            // Use a real generated binding and the real fragment property. Only Android widgets
            // are substitutes: the host JVM cannot inflate/measure them. Null/replacement matches
            // the onDestroyView/onCreateView binding boundary used by preview navigation.
            val binding = constructor.newInstance(
                mockk<LinearLayout>(),
                currentScroller,
                mockk<RecyclerView>(),
                mockk<NoItemsLayoutView>(),
                mockk<LayoutNoNetworkSmallBinding>(),
            ) as FragmentGalleryBinding
            bindingField.set(fragment, binding)

            // NoItemsLayoutView uses this exact owner property both to hide the grid for an
            // empty result and to restore it for a nonempty result. A detached old grid is wrong.
            assertSame(currentScroller, fragment.noItemsInitialListView)
            bindingField.set(fragment, null)
        }
    }
}
