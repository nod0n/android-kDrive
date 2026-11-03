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
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** Pure JVM tests for the media-type filter mapping (no Realm, no Android runtime). */
class GalleryMediaFilterTest {

    @Test
    fun `all filter matches every type including unknown`() {
        ExtensionType.entries.forEach { type ->
            assertTrue(GalleryMediaFilter.ALL.matches(type), "ALL must match $type")
        }
        assertTrue(GalleryMediaFilter.ALL.matches(null))
    }

    @Test
    fun `images filter matches only image type`() {
        ExtensionType.entries.forEach { type ->
            assertEquals(type == ExtensionType.IMAGE, GalleryMediaFilter.IMAGES.matches(type))
        }
        assertEquals(false, GalleryMediaFilter.IMAGES.matches(null))
    }

    @Test
    fun `videos filter matches only video type`() {
        ExtensionType.entries.forEach { type ->
            assertEquals(type == ExtensionType.VIDEO, GalleryMediaFilter.VIDEOS.matches(type))
        }
        assertEquals(false, GalleryMediaFilter.VIDEOS.matches(null))
    }
}
