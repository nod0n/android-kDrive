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
package com.infomaniak.drive.data.cache

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.data.models.file.SpecialFolder.Gallery
import com.infomaniak.drive.utils.RealmModules
import io.realm.Realm
import io.realm.RealmConfiguration
import java.nio.file.Files
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** Real isolated Realm; no account overrides, shared cache path or credentials. */
class GalleryCacheMergeTest {
    private val scope = UserDrive(userId = 101, driveId = 201)

    private fun withRealm(block: (Realm) -> Unit) {
        val context = ApplicationProvider.getApplicationContext<Context>()
        Realm.init(context)
        val directory = Files.createTempDirectory(context.cacheDir.toPath(), "gallery-merge-").toFile()
        val config = RealmConfiguration.Builder().directory(directory).name("fixture.realm")
            .schemaVersion(FileMigration.DB_VERSION).migration(FileMigration())
            .modules(RealmModules.LocalFilesModule()).build()
        try {
            Realm.getInstance(config).use(block)
        } finally {
            assertTrue(Realm.deleteRealm(config))
            assertTrue(directory.deleteRecursively())
        }
    }

    private fun localFile(drive: Int = scope.driveId) = File(id = 7, driveId = drive, name = "fixture.png").apply {
        initUid()
        isOffline = true
        isMarkedAsOffline = true
        isDirectlyPinned = false
        offlineOwnedByRoot = true
        offlineNeedsAttention = true
    }

    @Test
    fun galleryWritePreservesOfflineStateAndNormalizesFolderSortKey() = withRealm { realm ->
        realm.executeTransaction { it.copyToRealm(localFile().apply { sortedName = "eclair.png" }) }
        val incoming = File(id = 7, driveId = scope.driveId, name = "Éclair.png", sortedName = "Éclair.png").apply {
            initUid()
        }
        FileController.storeGalleryDrive(listOf(incoming), isFirstPage = true, customRealm = realm)
        val stored = realm.where(File::class.java).equalTo(File::id.name, Gallery.id).findFirst()!!.children.single()
        assertTrue(stored.isOffline)
        assertTrue(stored.isMarkedAsOffline)
        assertTrue(stored.offlineOwnedByRoot)
        assertTrue(stored.offlineNeedsAttention)
        assertFalse(stored.isDirectlyPinned)
        assertEquals("eclair.png", stored.sortedName)
    }

    @Test
    fun filteredRowsUseLocalBadgesWithoutWritingRemoteMetadataOrMembership() = withRealm { realm ->
        realm.executeTransaction { it.copyToRealm(localFile()) }
        val incoming = File(id = 7, driveId = scope.driveId, name = "fresh server name", sortedName = "fresh server sort")
        FileController.restoreGalleryOfflineState(listOf(incoming), scope, customRealm = realm)
        assertTrue(incoming.isOffline)
        assertTrue(incoming.isMarkedAsOffline)
        assertTrue(incoming.offlineOwnedByRoot)
        assertTrue(incoming.offlineNeedsAttention)
        assertFalse(incoming.isDirectlyPinned)
        assertEquals("fresh server name", incoming.name)
        assertEquals("fresh server sort", incoming.sortedName)
        assertEquals(1L, realm.where(File::class.java).count())
        assertEquals("fixture.png", realm.where(File::class.java).findFirst()!!.name)
        assertEquals(0L, realm.where(File::class.java).equalTo(File::id.name, Gallery.id).count())
    }

    @Test
    fun collidingIdFromAnotherDriveCannotSupplyOfflineState() = withRealm { realm ->
        realm.executeTransaction { it.copyToRealm(localFile(drive = 202)) }
        val incoming = File(id = 7, driveId = scope.driveId)
        FileController.restoreGalleryOfflineState(listOf(incoming), scope, customRealm = realm)
        assertFalse(incoming.isOffline)
        assertFalse(incoming.offlineOwnedByRoot)
        assertFalse(incoming.offlineNeedsAttention)
        assertTrue(realm.where(File::class.java).findFirst()!!.isOffline)
    }

    @Test
    fun rowOutsideSelectedDriveIsNotHydrated() = withRealm { realm ->
        realm.executeTransaction { it.copyToRealm(localFile()) }
        val incoming = File(id = 7, driveId = 202)
        FileController.restoreGalleryOfflineState(listOf(incoming), scope, customRealm = realm)
        assertFalse(incoming.isOffline)
        assertEquals(1L, realm.where(File::class.java).count())
        assertTrue(realm.where(File::class.java).findFirst()!!.isOffline)
    }
}
