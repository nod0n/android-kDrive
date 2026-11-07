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
package com.infomaniak.drive.ui.menu

import com.infomaniak.drive.data.models.ExtensionType
import com.infomaniak.drive.data.models.File
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

class GalleryMediaFilterTest {

    @Test
    fun `filter maps to the expected server-side types`() {
        assertEquals(
            listOf(ExtensionType.IMAGE, ExtensionType.VIDEO),
            GalleryMediaFilter.ALL.apiTypes,
        )
        assertEquals(listOf(ExtensionType.IMAGE), GalleryMediaFilter.IMAGES.apiTypes)
        assertEquals(listOf(ExtensionType.VIDEO), GalleryMediaFilter.VIDEOS.apiTypes)
    }

    @Test
    fun `only the unfiltered query may replace the shared gallery cache`() {
        assertTrue(GalleryMediaFilter.ALL.writesSharedCache)
        assertFalse(GalleryMediaFilter.IMAGES.writesSharedCache)
        assertFalse(GalleryMediaFilter.VIDEOS.writesSharedCache)
    }

    @Test
    fun `matches keeps the filtered media types only`() {
        assertTrue(GalleryMediaFilter.ALL.matches(ExtensionType.IMAGE))
        assertTrue(GalleryMediaFilter.ALL.matches(ExtensionType.VIDEO))
        assertTrue(GalleryMediaFilter.IMAGES.matches(ExtensionType.IMAGE))
        assertFalse(GalleryMediaFilter.IMAGES.matches(ExtensionType.VIDEO))
        assertFalse(GalleryMediaFilter.VIDEOS.matches(ExtensionType.IMAGE))
        assertTrue(GalleryMediaFilter.VIDEOS.matches(ExtensionType.VIDEO))
    }

    @Test
    fun `file extension helper delegates to the filter decision`() {
        fun fileWith(extensionType: String) = File().apply {
            isFromUploads = false
            this.extensionType = extensionType
        }

        assertTrue(fileWith(ExtensionType.VIDEO.value).matchesMediaFilter(GalleryMediaFilter.VIDEOS))
        assertFalse(fileWith(ExtensionType.IMAGE.value).matchesMediaFilter(GalleryMediaFilter.VIDEOS))
        assertTrue(fileWith(ExtensionType.IMAGE.value).matchesMediaFilter(GalleryMediaFilter.ALL))
    }
}
