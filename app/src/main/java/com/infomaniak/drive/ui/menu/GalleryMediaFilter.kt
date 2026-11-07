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

import androidx.annotation.StringRes
import com.infomaniak.drive.R
import com.infomaniak.drive.data.models.ExtensionType
import com.infomaniak.drive.data.models.File

enum class GalleryMediaFilter(@StringRes val translation: Int) {
    ALL(R.string.galleryMediaFilterAll),
    IMAGES(R.string.allPictures),
    VIDEOS(R.string.allVideo);

    /** Server-side types[] argument for this filter, on the shared search route. */
    val apiTypes: List<ExtensionType>
        get() = when (this) {
            ALL -> listOf(ExtensionType.IMAGE, ExtensionType.VIDEO)
            IMAGES -> listOf(ExtensionType.IMAGE)
            VIDEOS -> listOf(ExtensionType.VIDEO)
        }

    /** Only the unfiltered query may replace the shared gallery cache. */
    val writesSharedCache: Boolean
        get() = this == ALL

    /** Whether a file of [type] belongs to this filter. */
    fun matches(type: ExtensionType?): Boolean {
        return when (this) {
            ALL -> true
            IMAGES -> type == ExtensionType.IMAGE
            VIDEOS -> type == ExtensionType.VIDEO
        }
    }
}

fun File.matchesMediaFilter(filter: GalleryMediaFilter): Boolean {
    return filter.matches(getFileType())
}
