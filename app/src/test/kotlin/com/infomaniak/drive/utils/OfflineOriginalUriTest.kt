/* Copyright (C) 2026 Infomaniak Network SA. SPDX-License-Identifier: GPL-3.0-or-later */
package com.infomaniak.drive.utils

import android.content.ContentResolver
import android.content.Context
import android.content.Intent
import android.net.Uri
import android.webkit.MimeTypeMap
import androidx.core.content.FileProvider
import com.infomaniak.drive.R
import com.infomaniak.drive.data.documentprovider.CloudStorageProvider
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.Utils.openWithIntentExceptkDrive
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.mockkObject
import io.mockk.mockkStatic
import io.mockk.unmockkAll
import io.mockk.verify
import io.realm.RealmList
import java.nio.file.Files
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertArrayEquals
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertSame
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/** Production path/URI selection and intent construction; only Android platform boundaries are replaced. */
class OfflineOriginalUriTest {
    private val root = Files.createTempDirectory("offline-original-uri").toFile()
    private val context = mockk<Context>()
    private val resolver = mockk<ContentResolver>()
    private val mimeMap = mockk<MimeTypeMap>()
    private val scope = UserDrive(userId = 7, driveId = 42)
    private val cloudUri = mockk<Uri>()
    private val contentUri = mockk<Uri>()
    private val fileUri = mockk<Uri>()
    private val originalBytes = "original-plaintext-nonce\n".toByteArray()
    private val convertedBytes = "%PDF-1.4\nnot-the-original\n%%EOF".toByteArray()
    private var providerFile: java.io.File? = null
    private var legacyFile: java.io.File? = null
    private var intentData: Uri? = null
    private var intentMime: String? = null
    private var intentFlags = 0
    private var intentAction: String? = null

    @BeforeEach
    fun setup() {
        every { context.externalMediaDirs } returns arrayOf(root.resolve("external-media"))
        every { context.cacheDir } returns root.resolve("cache")
        every { context.getString(R.string.EXPOSED_OFFLINE_DIR) } returns "offline_storage"
        every { context.getString(R.string.FILE_AUTHORITY) } returns "synthetic.fileprovider"
        every { context.contentResolver } returns resolver
        mockkObject(CloudStorageProvider.Companion)
        every { CloudStorageProvider.createShareFileUri(context, any(), scope) } returns cloudUri
        mockkStatic(MimeTypeMap::class, Uri::class, FileProvider::class)
        every { MimeTypeMap.getSingleton() } returns mimeMap
        val mimes = mapOf(
            "txt" to "text/plain",
            "docx" to "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "xlsx" to "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "pptx" to "application/vnd.openxmlformats-officedocument.presentationml.presentation",
            "png" to "image/png",
        )
        every { mimeMap.getMimeTypeFromExtension(any()) } answers { mimes[firstArg<String>()] }
        every { FileProvider.getUriForFile(context, "synthetic.fileprovider", any()) } answers {
            providerFile = thirdArg(); contentUri
        }
        every { Uri.fromFile(any()) } answers { legacyFile = firstArg(); fileUri }
        every { contentUri.scheme } returns "content"
        every { fileUri.scheme } returns "file"
        // The local Android jar has no Intent implementation. Capture actual production API writes;
        // no URI helper, predicate, original path selection or intent-building function is mocked.
        mockkConstructor(Intent::class)
        every { anyConstructed<Intent>().setAction(any()) } answers { intentAction = firstArg(); self as Intent }
        every { anyConstructed<Intent>().setFlags(any()) } answers { intentFlags = firstArg(); self as Intent }
        every { anyConstructed<Intent>().setDataAndType(any(), any()) } answers {
            intentData = firstArg(); intentMime = secondArg(); self as Intent
        }
        every { anyConstructed<Intent>().action } answers { intentAction }
        every { anyConstructed<Intent>().flags } answers { intentFlags }
        every { anyConstructed<Intent>().data } answers { intentData }
        every { anyConstructed<Intent>().type } answers { intentMime }
    }

    @AfterEach
    fun teardown() {
        unmockkAll()
        root.deleteRecursively()
    }

    private fun file(extension: String, category: String, office: Boolean = true, offline: Boolean = true) = File(
        id = 83, driveId = scope.driveId, name = "synthetic.$extension", path = "Private/synthetic.$extension",
        lastModifiedAt = 1,
    ).apply {
        extensionType = category
        isOffline = offline
        size = originalBytes.size.toLong()
        if (office) supportedBy = RealmList("onlyoffice")
    }

    private fun original(file: File): java.io.File = requireNotNull(file.getStoredFile(context, scope)).apply {
        parentFile!!.mkdirs()
        writeBytes(originalBytes)
        setLastModified(1000)
    }

    private fun intent(file: File, mime: String): Intent {
        every { resolver.getType(cloudUri) } returns mime
        return context.openWithIntentExceptkDrive(file, scope).also {
            assertEquals(Intent.ACTION_VIEW, it.action)
            assertEquals(mime, it.type)
            assertEquals(
                Intent.FLAG_GRANT_READ_URI_PERMISSION or Intent.FLAG_GRANT_WRITE_URI_PERMISSION or
                    Intent.FLAG_GRANT_PERSISTABLE_URI_PERMISSION,
                it.flags,
            )
            verify(exactly = 1) { resolver.getType(cloudUri) }
            verify(exactly = 1) { anyConstructed<Intent>().setDataAndType(any(), mime) }
        }
    }

    @Test
    fun offlinePlainTextSharesOriginalThroughGrantableContentUriWithCorrectMime() {
        val file = file("txt", "text")
        val original = original(file)
        val converted = file.getConvertedPdfCache(context, scope).apply { writeBytes(convertedBytes) }
        val result = intent(file, "text/plain")
        assertSame(contentUri, result.data)
        assertEquals("content", result.data!!.scheme)
        assertEquals(original.canonicalFile, providerFile!!.canonicalFile)
        assertNotEquals(converted.canonicalFile, providerFile!!.canonicalFile)
        assertArrayEquals(originalBytes, providerFile!!.readBytes())
        assertArrayEquals(convertedBytes, converted.readBytes())
        verify(exactly = 1) { FileProvider.getUriForFile(context, "synthetic.fileprovider", original) }
        verify(exactly = 0) { Uri.fromFile(any()) }
    }

    private fun assertLegacyOffice(extension: String, category: String, mime: String) {
        val file = file(extension, category)
        val original = original(file)
        val converted = file.getConvertedPdfCache(context, scope).apply { writeBytes(convertedBytes) }
        val result = intent(file, mime)
        assertSame(fileUri, result.data)
        assertEquals("file", result.data!!.scheme)
        assertEquals(original.canonicalFile, legacyFile!!.canonicalFile)
        assertNotEquals(converted.canonicalFile, legacyFile!!.canonicalFile)
        assertArrayEquals(originalBytes, legacyFile!!.readBytes())
        verify(exactly = 0) { FileProvider.getUriForFile(any(), any(), any()) }
    }

    @Test
    fun offlineWordDocumentKeepsLegacyEditingUriDespiteTextCategory() = assertLegacyOffice(
        "docx", "text", "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
    )

    @Test
    fun offlineSpreadsheetKeepsLegacyEditingUri() = assertLegacyOffice(
        "xlsx", "spreadsheet", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    @Test
    fun offlinePresentationKeepsLegacyEditingUri() = assertLegacyOffice(
        "pptx", "presentation", "application/vnd.openxmlformats-officedocument.presentationml.presentation",
    )

    @Test
    fun nonOfflinePlainTextRetainsCloudRouteAndMime() {
        val result = intent(file("txt", "text", offline = false), "text/plain")
        assertSame(cloudUri, result.data)
        verify(exactly = 0) { FileProvider.getUriForFile(any(), any(), any()) }
        verify(exactly = 0) { Uri.fromFile(any()) }
    }

    @Test
    fun offlineNonOfficeFileRetainsExistingFileProviderRoute() {
        val file = file("png", "image", office = false)
        val original = original(file)
        val result = intent(file, "image/png")
        assertSame(contentUri, result.data)
        assertEquals(original.canonicalFile, providerFile!!.canonicalFile)
        verify(exactly = 1) { FileProvider.getUriForFile(context, "synthetic.fileprovider", original) }
        verify(exactly = 0) { Uri.fromFile(any()) }
    }
}
