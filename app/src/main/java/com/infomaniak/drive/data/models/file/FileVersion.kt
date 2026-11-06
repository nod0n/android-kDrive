/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2022-2024 Infomaniak Network SA
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
package com.infomaniak.drive.data.models.file

import android.os.Parcelable
import com.google.gson.annotations.SerializedName
import io.realm.RealmObject
import io.realm.annotations.RealmClass
import kotlinx.parcelize.Parcelize

@Parcelize
@RealmClass(embedded = true)
open class FileVersion(
    @SerializedName("is_multiple")
    var isMultiple: Boolean = false,
    /**
     * THE VERSION COUNT (official API: "Get number of version" — review-53 correction):
     * how many versions the file currently has, NOT a monotonic revision identity and
     * never a stable identifier of one generation. A count DIFFERENCE is a safe change
     * signal (the server saw at least one new version); count EQUALITY is not evidence
     * of the same content or generation (pruning, re-uploads and ABA edits can hold the
     * count steady). Every authority-bearing comparison must treat it accordingly.
     */
    var number: Int = 0,
    @SerializedName("total_size")
    var totalSize: Long = 0,
) : RealmObject(), Parcelable
