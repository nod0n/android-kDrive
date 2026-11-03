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

import kotlinx.coroutines.CancellationException
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

/**
 * Pure JVM tests for the filtered pagination semantics of [GalleryPageScanner]. The scanner is
 * generic, so items are plain strings: the predicate keeps only "m" items.
 */
class GalleryPageScannerTest {

    private val keepM = GalleryPageScanner<String>(predicate = { it == "m" })

    private fun success(data: List<String>, nextCursor: String?, hasMore: Boolean = nextCursor != null) =
        GalleryPage.Success(data, nextCursor, hasMore)

    private fun fetcher(
        pages: List<GalleryPage<String>>,
        requestedCursors: MutableList<String?> = mutableListOf(),
    ): (String?) -> GalleryPage<String> {
        var index = 0
        return { cursor ->
            requestedCursors.add(cursor)
            check(index < pages.size) { "fetch went past the scripted pages" }
            pages[index++]
        }
    }

    @Test
    fun `a matching page with no more pages completes the scan`() {
        val rawPages = mutableListOf<List<String>>()
        val result = keepM.scan(
            onRawPage = { rawPages.add(it) },
            fetchPage = fetcher(listOf(success(listOf("m", "x"), nextCursor = null))),
        )

        assertTrue(result is GalleryScanResult.Complete)
        assertEquals(listOf("m"), (result as GalleryScanResult.Complete).items)
        assertEquals(listOf(listOf("m", "x")), rawPages)
    }

    @Test
    fun `a page with zero matches does not end the scan`() {
        val cursors = mutableListOf<String?>()
        val result = keepM.scan(
            fetchPage = fetcher(
                pages = listOf(
                    success(listOf("x", "y"), nextCursor = "c1"),
                    success(listOf("m"), nextCursor = null),
                ),
                requestedCursors = cursors,
            ),
        )

        assertTrue(result is GalleryScanResult.Complete)
        assertEquals(listOf("m"), (result as GalleryScanResult.Complete).items)
        assertEquals(listOf(null, "c1"), cursors)
    }

    @Test
    fun `scan suspends after ten consecutive matchless pages and preserves the resume cursor`() {
        val cursors = mutableListOf<String?>()
        val pages = (0 until 20).map { i -> success(listOf("x$i"), nextCursor = "c$i") }
        val result = keepM.scan(
            fetchPage = fetcher(pages, requestedCursors = cursors),
        )

        assertTrue(result is GalleryScanResult.Suspended)
        assertEquals(0, (result as GalleryScanResult.Suspended).items.size)
        assertEquals("c9", result.nextCursor)
        assertEquals(10, cursors.size)
    }

    @Test
    fun `a matching page is handed back immediately with its resume cursor`() {
        val cursors = mutableListOf<String?>()
        val pages = buildList {
            repeat(9) { i -> add(success(emptyList(), nextCursor = "x$i")) }
            add(success(listOf("m"), nextCursor = "y0"))
            add(success(listOf("m"), nextCursor = "y1"))
        }
        val result = keepM.scan(fetchPage = fetcher(pages, requestedCursors = cursors))

        // The first match (right after 9 matchless pages) ends the scan without walking the
        // rest of the drive; the resume cursor points at the following page.
        assertTrue(result is GalleryScanResult.Suspended)
        assertEquals(listOf("m"), (result as GalleryScanResult.Suspended).items)
        assertEquals("y0", result.nextCursor)
        assertEquals(10, cursors.size)
    }

    @Test
    fun `unreachable first page is not a completion`() {
        val rawPages = mutableListOf<List<String>>()
        val result = keepM.scan(
            onRawPage = { rawPages.add(it) },
            fetchPage = fetcher(listOf(GalleryPage.Error)),
        )

        assertEquals(GalleryScanResult.Unreachable, result)
        assertTrue(rawPages.isEmpty())
    }

    @Test
    fun `mid-scan failure keeps the matches and the retry cursor of the failed page`() {
        val cursors = mutableListOf<String?>()
        val result = keepM.scan(
            fetchPage = fetcher(
                pages = listOf(
                    success(emptyList(), nextCursor = "c1"),
                    GalleryPage.Error,
                ),
                requestedCursors = cursors,
            ),
        )

        assertTrue(result is GalleryScanResult.Interrupted)
        assertEquals(emptyList<String>(), (result as GalleryScanResult.Interrupted).items)
        assertEquals("c1", result.retryCursor)
        assertEquals(listOf(null, "c1"), cursors)
    }

    @Test
    fun `more pages without a next cursor completes instead of looping forever`() {
        var fetchCount = 0
        val result = keepM.scan(
            fetchPage = { _ ->
                fetchCount++
                success(listOf("m"), nextCursor = null, hasMore = true)
            },
        )

        assertTrue(result is GalleryScanResult.Complete)
        assertEquals(1, fetchCount)
    }

    @Test
    fun `cancellation aborts before the next fetch and yields no result`() {
        val cursors = mutableListOf<String?>()
        val checkActive = {
            if (cursors.size >= 2) throw CancellationException("job cancelled")
        }

        assertThrows<CancellationException> {
            keepM.scan(
                checkActive = checkActive,
                fetchPage = fetcher(
                    pages = (0 until 12).map { i -> success(emptyList(), nextCursor = "c$i") },
                    requestedCursors = cursors,
                ),
            )
        }

        // Thrown by the post-fetch check of the second page: no third fetch, no result.
        assertEquals(2, cursors.size)
    }

    @Test
    fun `cancellation after a blocking fetch aborts before the page reaches the cache`() {
        val rawPages = mutableListOf<List<String>>()
        var checkActiveCalls = 0
        val checkActive = {
            // 1st call: before the fetch. 2nd call: right after the blocking fetch returned.
            if (++checkActiveCalls >= 2) throw CancellationException("job cancelled")
        }

        assertThrows<CancellationException> {
            keepM.scan(
                onRawPage = { rawPages.add(it) },
                checkActive = checkActive,
                fetchPage = { _ -> success(listOf("m"), nextCursor = "c1") },
            )
        }

        // Two checks ran (pre-fetch, post-fetch); the page was never stored or returned.
        assertEquals(2, checkActiveCalls)
        assertTrue(rawPages.isEmpty())
    }
}
