/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2026 Infomaniak Network SA
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.infomaniak.drive.data.sync

import com.google.gson.GsonBuilder
import com.infomaniak.drive.data.api.FileDeserialization
import com.infomaniak.drive.data.models.File
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.Test

class PhotoTrashHashMappingTest {
    private fun file(hashField: String): File = GsonBuilder().registerTypeAdapter(File::class.java, FileDeserialization()).create()
        .fromJson("""{"id":3,"drive_id":2$hashField}""", File::class.java)

    @Test
    fun omittedHashPreservesCreatedIdentityButDoesNotManufactureBasis() {
        val result = file("")
        assertEquals(3, result.id)
        assertEquals(2, result.driveId)
        assertEquals("3_2", result.uid)
        assertNull(result.serverContentHash)
    }

    @Test
    fun explicitNullHashRemainsUnbound() { assertNull(file(",\"hash\":null").serverContentHash) }

    @Test
    fun canonicalDetailsHashSurvivesTheActualFileAdapter() {
        assertEquals("xxh3:00ec7bec63e548df", file(",\"hash\":\"xxh3:00ec7bec63e548df\"").serverContentHash)
    }
}
