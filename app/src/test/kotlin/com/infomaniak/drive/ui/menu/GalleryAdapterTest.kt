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

import androidx.recyclerview.widget.RecyclerView
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.ui.fileList.multiSelect.MultiSelectManager
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test

/** Exercises the real Gallery/Legacy adapter, including loader rows and removal notifications. */
class GalleryAdapterTest {
    private val scope = UserDrive(userId = 101, driveId = 201)
    private fun page(vararg ids: Int, complete: Boolean = true, generation: Int = 1) = GalleryPage(
        ArrayList(ids.map { File(id = it, driveId = scope.driveId, name = "image-$it.png") }),
        complete, generation, scope, scopeRevision = 1,
    )

    private fun adapter(onRemoved: (Int, Int) -> Unit = { _, _ -> }) = galleryAdapterForTest(onRemoved)

    @Test
    fun `empty first page removes actual skeleton rows`() {
        val removed = mutableListOf<Pair<Int, Int>>()
        val adapter = adapter { start, count -> removed += start to count }
        adapter.showLoading()
        assertEquals(3, adapter.itemCount)
        assertTrue(adapter.completePage(page(), { true }))
        assertEquals(0, adapter.itemCount)
        assertEquals(listOf(0 to 3), removed)
        assertTrue(adapter.galleryList.isEmpty())
        assertTrue(adapter.isComplete)
    }

    @Test
    fun `empty next page preserves earlier rows and allows further pagination`() {
        val adapter = adapter()
        adapter.completePage(page(1, complete = false), { true })
        val rows = adapter.itemList.toList()
        adapter.showLoading()
        assertEquals(rows.size + 3, adapter.itemCount)
        adapter.completePage(page(complete = false), { true })
        assertEquals(rows, adapter.itemList)
        assertEquals(rows.size, adapter.itemCount)
        assertEquals(listOf(1), adapter.galleryList.map { it.id })
        assertFalse(adapter.isComplete)
    }

    @Test
    fun `sparse first page ends its loader without declaring pagination complete`() {
        val adapter = adapter()
        adapter.showLoading()
        adapter.completePage(page(complete = false), { true })
        assertEquals(0, adapter.itemCount)
        assertTrue(adapter.galleryList.isEmpty())
        assertFalse(adapter.isComplete)
    }

    @Test
    fun `completed empty error fallback removes loader without clearing earlier rows`() {
        val adapter = adapter()
        adapter.completePage(page(1, complete = false), { true })
        val rows = adapter.itemList.toList()
        adapter.showLoading()
        // API errors publish a current stamped Realm fallback, not an unscoped null event.
        adapter.completePage(page(complete = true), { true })
        assertEquals(rows.size, adapter.itemCount)
        assertEquals(rows, adapter.itemList)
        assertEquals(listOf(1), adapter.galleryList.map { it.id })
        assertTrue(adapter.isComplete)
    }

    @Test
    fun `superseded completion cannot hide a newer loader or change its rows`() {
        val adapter = adapter()
        adapter.showLoading()
        assertFalse(adapter.completePage(page(1, generation = 1), { it.generation == 2 }))
        assertEquals(3, adapter.itemCount)
        assertTrue(adapter.galleryList.isEmpty())
        assertFalse(adapter.isComplete)
    }

    @Test
    fun `unstamped null cannot finish the current request`() {
        val adapter = adapter()
        adapter.showLoading()
        assertFalse(adapter.completePage(null, { error("null must not reach scope validation") }))
        assertEquals(3, adapter.itemCount)
        assertFalse(adapter.isComplete)
    }

    @Test
    fun `recreated observer completes loading without duplicating retained page`() {
        val adapter = adapter()
        val delivered = page(1, complete = false)
        adapter.completePage(delivered, { true })
        val rows = adapter.itemList.toList()
        adapter.showLoading()
        assertTrue(adapter.completePage(delivered, { true }, appendFiles = false))
        assertEquals(rows.size, adapter.itemCount)
        assertEquals(rows, adapter.itemList)
        assertEquals(listOf(1), adapter.galleryList.map { it.id })
        assertFalse(adapter.isComplete)
    }
}

internal fun galleryAdapterForTest(onRemoved: (Int, Int) -> Unit = { _, _ -> }) =
    GalleryAdapter(MultiSelectManager(), GalleryPeriod.MONTH) {}.apply {
        numberItemLoader = 3
        // The SDK's mockable Observable has neither constructor initialization nor
        // registerObserver on the JVM. Supply its observer list only; the real AndroidX
        // notification dispatch, LoaderAdapter state and GalleryAdapter all execute.
        val observable = RecyclerView.Adapter::class.java.getDeclaredField("mObservable").apply {
            isAccessible = true
        }.get(this)
        android.database.Observable::class.java.getDeclaredField("mObservers").apply {
            isAccessible = true
        }.set(observable, arrayListOf(object : RecyclerView.AdapterDataObserver() {
            override fun onItemRangeRemoved(positionStart: Int, itemCount: Int) {
                onRemoved(positionStart, itemCount)
            }
        }))
    }
