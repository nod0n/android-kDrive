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

/**
 * One page handed to [GalleryPageScanner] by its fetch lambda.
 */
sealed interface GalleryPage<out T> {

    data class Success<out T>(
        val data: List<T>,
        val nextCursor: String?,
        val hasMore: Boolean,
    ) : GalleryPage<T>

    /** The page could not be fetched. Never implies the end of the remote collection. */
    data object Error : GalleryPage<Nothing>
}

/**
 * Outcome of a [GalleryPageScanner.scan].
 */
sealed interface GalleryScanResult<out T> {

    /** The server reported no more pages (or none could be resumed): [items] is the authoritative result. */
    data class Complete<out T>(val items: List<T>) : GalleryScanResult<T>

    /**
     * The scan hit its page budget after pages yielding no match. More matching files may exist
     * server-side: [nextCursor] must be preserved to resume where the scan stopped.
     */
    data class Suspended<out T>(val items: List<T>, val nextCursor: String?) : GalleryScanResult<T>

    /** The first page could not be fetched: nothing was scanned, nothing is known. */
    data object Unreachable : GalleryScanResult<Nothing>

    /**
     * A page failed after earlier successes. [items] holds the matches gathered so far and
     * [retryCursor] is the cursor the failed fetch was attempted with, so a retry resumes
     * from exactly the same page without skipping or duplicating anything.
     */
    data class Interrupted<out T>(val items: List<T>, val retryCursor: String?) : GalleryScanResult<T>
}

/**
 * Cursor-paged scanner that keeps only the items matching [predicate].
 *
 * A page with zero matches is NOT the end of the collection: the scan keeps walking pages
 * while the server reports more, until either a match is found or the page budget is spent.
 * This bounds the work done per call (per UI tick) without ever mistaking an empty filtered
 * page for an empty gallery.
 */
class GalleryPageScanner<T>(
    private val predicate: (T) -> Boolean,
    private val maxEmptyPagesPerScan: Int = DEFAULT_MAX_EMPTY_PAGES_PER_SCAN,
) {

    /**
     * @param startCursor cursor to resume from, or null for a first page.
     * @param onRawPage called for every successfully fetched page, matched or not, before filtering.
     * @param checkActive called before every page fetch; throwing here aborts the scan without
     * any result (e.g. coroutine cancellation via [kotlinx.coroutines.Job.ensureActive]).
     * @param fetchPage fetches the page starting at [cursor]; must never return null pages.
     */
    fun scan(
        startCursor: String? = null,
        onRawPage: (page: List<T>) -> Unit = {},
        checkActive: () -> Unit = {},
        fetchPage: (cursor: String?) -> GalleryPage<T>,
    ): GalleryScanResult<T> {
        var cursor: String? = startCursor
        val matches = ArrayList<T>()
        var emptyPagesInARow = 0
        var isFirstPage = true

        while (true) {
            checkActive()

            when (val page = fetchPage(cursor)) {
                is GalleryPage.Error -> {
                    return if (isFirstPage) GalleryScanResult.Unreachable
                    else GalleryScanResult.Interrupted(ArrayList(matches), retryCursor = cursor)
                }
                is GalleryPage.Success -> {
                    onRawPage(page.data)
                    val pageMatches = page.data.filter(predicate)
                    matches.addAll(pageMatches)

                    // No cursor to resume with cannot be walked further; treat it as the end
                    // rather than looping on the same page forever.
                    if (!page.hasMore || page.nextCursor == null) {
                        return GalleryScanResult.Complete(ArrayList(matches))
                    }

                    if (pageMatches.isEmpty()) {
                        emptyPagesInARow++
                        if (emptyPagesInARow >= maxEmptyPagesPerScan) {
                            return GalleryScanResult.Suspended(ArrayList(matches), nextCursor = page.nextCursor)
                        }
                    } else {
                        emptyPagesInARow = 0
                    }

                    cursor = page.nextCursor
                    isFirstPage = false
                }
            }
        }
    }

    companion object {
        const val DEFAULT_MAX_EMPTY_PAGES_PER_SCAN = 10
    }
}
