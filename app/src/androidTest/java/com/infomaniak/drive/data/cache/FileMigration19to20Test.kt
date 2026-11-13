/*
 * Infomaniak kDrive - Android
 * Copyright (C) 2026 Infomaniak Network SA
 * SPDX-License-Identifier: GPL-3.0-or-later
 */
package com.infomaniak.drive.data.cache

import android.content.Context
import androidx.test.core.app.ApplicationProvider
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.OfflineFolderRoot
import com.infomaniak.drive.utils.RealmModules
import io.realm.DynamicRealm
import io.realm.Realm
import io.realm.RealmConfiguration
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.nio.file.Files

class FileMigration19to20Test {
    // The persisted v19 field set, declared independently of the current Kotlin model.
    private val fieldsAt19 = setOf(
        "addedAt", "categories", "children", "color", "conversion", "createdAt", "createdBy",
        "cursor", "deletedAt", "deletedBy", "driveId", "dropbox", "extensionType",
        "externalImport", "id", "isComplete", "isFavorite", "isFromUploads",
        "isMarkedAsOffline", "isOffline", "lastActionAt", "lastModifiedAt", "name", "parentId",
        "path", "responseAt", "revisedAt", "rights", "shareLink", "size", "sortedName",
        "status", "supportedBy", "type", "uid", "updatedAt", "users", "version", "versionCode",
        "visibility", "isDirectlyPinned", "offlineBasisModifiedAt", "offlineBasisSize",
        "offlineContentDigest", "serverContentHash", "offlinePublishedPath", "offlineOwnedByRoot",
    )

    @Test
    fun trueVersion19AddsOnlyLocalAttentionAndPreservesRowsAndLinks() {
        val context = ApplicationProvider.getApplicationContext<Context>()
        Realm.init(context)
        val directory = Files.createTempDirectory(context.cacheDir.toPath(), "file19-upgrade-").toFile()
        fun config(version: Long) = RealmConfiguration.Builder().directory(directory).name("fixture.realm")
            .schemaVersion(version).modules(RealmModules.LocalFilesModule()).apply {
                if (version == FileMigration.DB_VERSION) migration(FileMigration())
            }.build()
        val old = config(19L)
        val current = config(FileMigration.DB_VERSION)
        try {
            // Provision the full module, then remove the future column dynamically. The
            // migration runs only after the exact old schema has been verified below.
            Realm.getInstance(old).use { realm ->
                realm.executeTransaction {
                    val folder = it.createObject(File::class.java, "610_751").apply {
                        id = 610
                        driveId = 751
                        name = "fixture-folder"
                        type = "dir"
                    }
                    val file = it.createObject(File::class.java, "611_751").apply {
                        id = 611
                        driveId = 751
                        parentId = 610
                        name = "fixture-copy"
                        path = "Private/fixture-copy"
                        type = "file"
                        size = 13L
                        isOffline = true
                        isMarkedAsOffline = true
                        isDirectlyPinned = true
                        offlineOwnedByRoot = true
                        offlineBasisModifiedAt = 1_700_000_000L
                        offlineBasisSize = 13L
                        offlineContentDigest = "fixture-sha"
                        serverContentHash = "fixture-server-hash"
                        offlinePublishedPath = "/synthetic/fixture-copy"
                    }
                    folder.children.add(file)
                    it.createObject(OfflineFolderRoot::class.java, "610_751_951").apply {
                        folderId = 610
                        driveId = 751
                        userId = 951
                    }
                }
            }
            DynamicRealm.getInstance(old).use { realm ->
                realm.executeTransaction { it.schema.get("File")!!.removeField("offlineNeedsAttention") }
                assertEquals(fieldsAt19, realm.schema.get("File")!!.fieldNames.toSet())
                assertFalse(realm.schema.get("File")!!.hasField("offlineNeedsAttention"))
                assertEquals(2L, realm.where("File").count())
                assertEquals(1L, realm.where("OfflineFolderRoot").count())
            }
            Realm.getInstance(current).use { realm ->
                val schema = realm.schema.get("File")!!
                assertEquals(fieldsAt19 + "offlineNeedsAttention", schema.fieldNames.toSet())
                assertFalse(schema.isNullable("offlineNeedsAttention"))
                assertEquals("uid", schema.primaryKey)
                assertEquals(2L, realm.where(File::class.java).count())
                val folder = realm.where(File::class.java).equalTo("uid", "610_751").findFirst()!!
                val file = realm.where(File::class.java).equalTo("uid", "611_751").findFirst()!!
                assertEquals(listOf("611_751"), folder.children.map { it.uid })
                assertEquals(751, file.driveId)
                assertEquals(610, file.parentId)
                assertEquals("fixture-copy", file.name)
                assertEquals("Private/fixture-copy", file.path)
                assertEquals(13L, file.size)
                assertTrue(file.isOffline && file.isMarkedAsOffline && file.isDirectlyPinned && file.offlineOwnedByRoot)
                assertEquals(1_700_000_000L, file.offlineBasisModifiedAt)
                assertEquals(13L, file.offlineBasisSize)
                assertEquals("fixture-sha", file.offlineContentDigest)
                assertEquals("fixture-server-hash", file.serverContentHash)
                assertEquals("/synthetic/fixture-copy", file.offlinePublishedPath)
                assertFalse(file.offlineNeedsAttention)
                val root = realm.where(OfflineFolderRoot::class.java).findFirst()!!
                assertEquals("610_751_951", root.uid)
                assertEquals(610, root.folderId)
                assertEquals(751, root.driveId)
                assertEquals(951, root.userId)
                realm.executeTransaction { file.offlineNeedsAttention = true }
            }
            Realm.getInstance(current).use { realm ->
                assertTrue(realm.where(File::class.java).equalTo("id", 611).findFirst()!!.offlineNeedsAttention)
            }
        } finally {
            Realm.deleteRealm(current)
            assertTrue(directory.deleteRecursively(), "only this exclusively created fixture directory is removed")
        }
    }
}
