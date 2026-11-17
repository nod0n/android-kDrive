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

import android.content.Context
import android.os.Bundle
import androidx.arch.core.executor.ArchTaskExecutor
import androidx.arch.core.executor.TaskExecutor
import androidx.lifecycle.Lifecycle
import androidx.lifecycle.LifecycleOwner
import androidx.lifecycle.LifecycleRegistry
import androidx.lifecycle.ViewModelStore
import androidx.navigation.FloatingWindow
import androidx.navigation.NavBackStackEntry
import androidx.navigation.NavDestination
import androidx.navigation.NavGraph
import androidx.navigation.NavGraphNavigator
import androidx.navigation.NavHostController
import androidx.navigation.NavOptions
import androidx.navigation.Navigator
import androidx.navigation.fragment.NavHostFragment
import com.infomaniak.core.legacy.utils.setBackNavigationResult
import com.infomaniak.drive.R
import io.mockk.every
import io.mockk.mockk
import io.mockk.mockkConstructor
import io.mockk.mockkObject
import io.mockk.spyk
import io.mockk.unmockkAll
import io.mockk.verify
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.test.UnconfinedTestDispatcher
import kotlinx.coroutines.test.resetMain
import kotlinx.coroutines.test.setMain
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNull
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test

/**
 * Production result registration and Core result writes on real Navigation entries and lifecycle owners.
 * Android rendering/transactions and empty process-restoration Bundles are the only platform substitutes.
 */
@OptIn(ExperimentalCoroutinesApi::class)
class GalleryNavigationResultTest {
    private class Owner : LifecycleOwner {
        override val lifecycle = LifecycleRegistry.createUnsafe(this)
    }

    // Only Android Fragment transactions are replaced; Navigation owns the real stack,
    // lifecycle transitions, saved-state handles and result writes/pops.
    @Navigator.Name("screen")
    private class Screens : Navigator<NavDestination>() {
        override fun createDestination() = NavDestination(this)
        override fun navigate(entries: List<NavBackStackEntry>, navOptions: NavOptions?, navigatorExtras: Extras?) {
            entries.forEach { state.pushWithTransition(it) }
        }
        override fun popBackStack(popUpTo: NavBackStackEntry, savedState: Boolean) = state.popWithTransition(popUpTo, savedState)
        fun complete() = state.transitionsInProgress.value.toList().forEach { state.markTransitionComplete(it) }
    }

    @Navigator.Name("sheet")
    private class Sheets : Navigator<Sheet>() {
        override fun createDestination() = Sheet(this)
        override fun navigate(entries: List<NavBackStackEntry>, navOptions: NavOptions?, navigatorExtras: Extras?) {
            entries.forEach { state.pushWithTransition(it) }
            complete()
        }
        override fun popBackStack(popUpTo: NavBackStackEntry, savedState: Boolean) {
            state.popWithTransition(popUpTo, savedState)
            complete()
        }
        // Model an entirely completed DialogFragmentNavigator show/dismiss. A
        // pending screen transition must not be confused with a pending dialog.
        private fun complete() = state.transitionsInProgress.value.toList().forEach { state.markTransitionComplete(it) }
    }

    private class Sheet(navigator: Sheets) : NavDestination(navigator), FloatingWindow
    private val host = Owner()
    private var view = Owner()
    private val store = ViewModelStore()
    private lateinit var nav: NavHostController
    private lateinit var screens: Screens
    private lateinit var vm: GalleryViewModel

    @BeforeEach
    fun setUp() {
        Dispatchers.setMain(UnconfinedTestDispatcher())
        ArchTaskExecutor.getInstance().setDelegate(object : TaskExecutor() {
            override fun executeOnDiskIO(runnable: Runnable) = runnable.run()
            override fun postToMainThread(runnable: Runnable) = runnable.run()
            override fun isMainThread() = true
        })
        // The JVM Android stub has no Bundle implementation. These tests have no
        // restored process state; saved-result storage remains the real handle.
        mockkConstructor(Bundle::class)
        every { anyConstructed<Bundle>().containsKey(any()) } returns false
        every { anyConstructed<Bundle>().keySet() } returns emptySet()
        nav = NavHostController(mockk<Context>(relaxed = true))
        nav.setLifecycleOwner(host)
        nav.setViewModelStore(store)
        screens = Screens()
        val sheets = Sheets()
        nav.navigatorProvider.addNavigator(screens)
        nav.navigatorProvider.addNavigator(sheets)
        nav.graph = NavGraph(NavGraphNavigator(nav.navigatorProvider)).apply {
            id = 1
            addDestination(screens.createDestination().apply { id = R.id.menuGalleryFragment })
            addDestination(screens.createDestination().apply { id = R.id.previewSliderFragment })
            addDestination(sheets.createDestination().apply { id = R.id.galleryMediaFilterBottomSheetDialog })
            addDestination(sheets.createDestination().apply { id = R.id.galleryPeriodBottomSheetDialog })
            setStartDestination(R.id.menuGalleryFragment)
        }
        host.lifecycle.currentState = Lifecycle.State.RESUMED
        view.lifecycle.currentState = Lifecycle.State.RESUMED
        screens.complete()
        vm = spyk(GalleryViewModel())
        mockkObject(NavHostFragment.Companion)
        bindView()
    }

    private fun bindView() {
        val fragment = spyk(MenuGalleryFragment())
        every { NavHostFragment.findNavController(fragment) } returns nav
        every { fragment.viewLifecycleOwner } returns view
        MenuGalleryFragment::class.java.getDeclaredField("galleryViewModel\$delegate").apply { isAccessible = true }
            .set(fragment, lazy { vm })
        MenuGalleryFragment::class.java.getDeclaredMethod("observeGallerySelections").apply { isAccessible = true }.invoke(fragment)
    }

    @AfterEach
    fun tearDown() {
        view.lifecycle.currentState = Lifecycle.State.DESTROYED
        host.lifecycle.currentState = Lifecycle.State.DESTROYED
        store.clear()
        unmockkAll()
        ArchTaskExecutor.getInstance().setDelegate(null)
        Dispatchers.resetMain()
    }

    private fun select(filter: GalleryMediaFilter) {
        nav.navigate(R.id.galleryMediaFilterBottomSheetDialog)
        returnFilter(filter)
    }

    private fun returnFilter(filter: GalleryMediaFilter) {
        val sheet = GalleryMediaFilterBottomSheetDialog()
        every { NavHostFragment.findNavController(sheet) } returns nav
        sheet.setBackNavigationResult(GalleryMediaFilterBottomSheetDialog.GALLERY_MEDIA_FILTER_KEY, filter)
    }

    private fun returnFromPreview(complete: Boolean = false) {
        view.lifecycle.currentState = Lifecycle.State.DESTROYED
        nav.navigate(R.id.previewSliderFragment)
        screens.complete()
        nav.popBackStack()
        if (complete) screens.complete()
        view = Owner().also { it.lifecycle.currentState = Lifecycle.State.STARTED }
        bindView()
    }

    @Test
    fun ordinaryFirstAndSuccessiveSelectionsConsumeBothResultKinds() {
        select(GalleryMediaFilter.IMAGES)
        assertEquals(GalleryMediaFilter.IMAGES, vm.mediaFilter.value)
        select(GalleryMediaFilter.VIDEOS)
        assertEquals(GalleryMediaFilter.VIDEOS, vm.mediaFilter.value)
        selectPeriod(GalleryPeriod.DAY)
        assertEquals(GalleryPeriod.DAY, vm.period.value)
        selectPeriod(GalleryPeriod.YEAR)
        assertEquals(GalleryPeriod.YEAR, vm.period.value)
        val results = nav.currentBackStackEntry!!.savedStateHandle
        assertNull(results.get<GalleryMediaFilter>(GalleryMediaFilterBottomSheetDialog.GALLERY_MEDIA_FILTER_KEY))
        assertNull(results.get<GalleryPeriod>(GalleryPeriodBottomSheetDialog.GALLERY_PERIOD_KEY))
    }

    private fun selectPeriod(period: GalleryPeriod) {
        nav.navigate(R.id.galleryPeriodBottomSheetDialog)
        val sheet = GalleryPeriodBottomSheetDialog()
        every { NavHostFragment.findNavController(sheet) } returns nav
        sheet.setBackNavigationResult(GalleryPeriodBottomSheetDialog.GALLERY_PERIOD_KEY, period)
    }

    @Test
    fun filterAfterPreviewDoesNotWaitForAnotherEntryLifecycleEvent() {
        select(GalleryMediaFilter.IMAGES)
        returnFromPreview()
        // Actual FragmentNavigator also pops with a transition. Until completion,
        // Gallery stays STARTED, including across the floating sheet's dismissal.
        assertEquals(Lifecycle.State.STARTED, nav.currentBackStackEntry!!.lifecycle.currentState)
        select(GalleryMediaFilter.VIDEOS)
        assertEquals(Lifecycle.State.STARTED, nav.currentBackStackEntry!!.lifecycle.currentState)
        assertEquals(GalleryMediaFilter.VIDEOS, vm.mediaFilter.value)
        screens.complete()
        verify(exactly = 1) { vm.setMediaFilter(GalleryMediaFilter.VIDEOS) }
    }

    @Test
    fun periodAfterPreviewUsesTheSameImmediateResultBoundary() {
        returnFromPreview()
        selectPeriod(GalleryPeriod.DAY)
        assertEquals(GalleryPeriod.DAY, vm.period.value)
    }

    @Test
    fun completedPreviewTransitionStillDeliversNormally() {
        returnFromPreview(complete = true)
        assertEquals(Lifecycle.State.RESUMED, nav.currentBackStackEntry!!.lifecycle.currentState)
        select(GalleryMediaFilter.VIDEOS)
        assertEquals(GalleryMediaFilter.VIDEOS, vm.mediaFilter.value)
    }

    @Test
    fun viewRecreatedUnderFloatingSheetStillObservesGalleryHandle() {
        nav.navigate(R.id.galleryMediaFilterBottomSheetDialog)
        view.lifecycle.currentState = Lifecycle.State.DESTROYED
        view = Owner().also { it.lifecycle.currentState = Lifecycle.State.STARTED }
        // The current entry is the dialog, but the selection belongs to Gallery.
        bindView()
        returnFilter(GalleryMediaFilter.VIDEOS)
        assertEquals(GalleryMediaFilter.VIDEOS, vm.mediaFilter.value)
    }

    @Test
    fun resultWaitsForNewViewToStartAndIsNotDeliveredToDestroyedView() {
        view.lifecycle.currentState = Lifecycle.State.DESTROYED
        select(GalleryMediaFilter.VIDEOS)
        assertEquals(GalleryMediaFilter.ALL, vm.mediaFilter.value)
        view = Owner().also { it.lifecycle.currentState = Lifecycle.State.CREATED }
        bindView()
        assertEquals(GalleryMediaFilter.ALL, vm.mediaFilter.value)
        view.lifecycle.currentState = Lifecycle.State.STARTED
        assertEquals(GalleryMediaFilter.VIDEOS, vm.mediaFilter.value)
        verify(exactly = 1) { vm.setMediaFilter(GalleryMediaFilter.VIDEOS) }
    }

    @Test
    fun consumedResultDoesNotReplayOnRestartOrViewRecreation() {
        select(GalleryMediaFilter.VIDEOS)
        view.lifecycle.currentState = Lifecycle.State.CREATED
        view.lifecycle.currentState = Lifecycle.State.RESUMED
        view.lifecycle.currentState = Lifecycle.State.DESTROYED
        view = Owner().also { it.lifecycle.currentState = Lifecycle.State.RESUMED }
        bindView()
        verify(exactly = 1) { vm.setMediaFilter(GalleryMediaFilter.VIDEOS) }
        select(GalleryMediaFilter.IMAGES)
        verify(exactly = 1) { vm.setMediaFilter(GalleryMediaFilter.IMAGES) }
    }

    @Test
    fun cancellingSheetAfterPreviewDoesNotInventASelection() {
        select(GalleryMediaFilter.IMAGES)
        returnFromPreview()
        nav.navigate(R.id.galleryMediaFilterBottomSheetDialog)
        nav.popBackStack()
        screens.complete()
        assertEquals(GalleryMediaFilter.IMAGES, vm.mediaFilter.value)
        verify(exactly = 1) { vm.setMediaFilter(GalleryMediaFilter.IMAGES) }
    }
}
