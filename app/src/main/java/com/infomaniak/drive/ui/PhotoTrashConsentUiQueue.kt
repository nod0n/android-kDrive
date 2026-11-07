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
package com.infomaniak.drive.ui

import com.infomaniak.drive.data.sync.PhotoTrashController

/**
 * The Main-confined state machine of the photo-trash consent dialog flow. EVERY field is
 * read and written ONLY on the main thread: all wake paths (lifecycle entry, ActivityResult,
 * MQTT events, controller emissions) funnel through [MainActivity.drainPhotoTrashConsents],
 * which dispatches to Main before any state is touched, so the single-outstanding guard is
 * EXACT rather than a cross-thread best effort, and no phase can strand without a
 * corresponding Main-thread release.
 *
 * Three phases per dialog, with distinct persistence semantics:
 *  - DRAINING: a poll is running on IO. Not persisted: a recreation simply starts a new
 *    drain (the durable pending row state IS the queue).
 *  - VALIDATING: a poll returned a request and its launch-time revalidation is running on
 *    IO. NOT persisted across recreation — no dialog was ever shown, so nothing can deliver
 *    a result; the durable row stays armed and the recreated activity re-surfaces it on its
 *    first drain. (Persisting this phase was the zombie-drain bug: a restored non-null
 *    field whose dialog was never launched suppressed every future drain, and each further
 *    recreation re-persisted it.)
 *  - LAUNCHED: the system dialog was actually launched. This is the ONLY phase that
 *    persists across recreation (saved instance state), because only a launched dialog can
 *    deliver a result, and that result must be attributed to exactly the shown request,
 *    exactly once.
 */
class PhotoTrashConsentUiQueue {

    var launchedRequest: PhotoTrashController.ConsentRequest? = null
        private set

    var validatingRequest: PhotoTrashController.ConsentRequest? = null
        private set

    private var draining = false

    /** Whether a new drain may begin right now (main thread only). */
    fun canBeginDrain(): Boolean = launchedRequest == null && validatingRequest == null && !draining

    fun onDrainBegan() {
        draining = true
    }

    /** Ends a drain that surfaced nothing: the guard releases. */
    fun onDrainEndedWithNothing() {
        draining = false
    }

    /** Ends a drain that surfaced [request]: the drain guard releases, validation begins. */
    fun onDrainEndedWithRequest(request: PhotoTrashController.ConsentRequest) {
        draining = false
        validatingRequest = request
    }

    /** A validating request expired, failed, or was refused by a gate: validation ends. Never touches a LAUNCHED request. */
    fun onValidationFailed() {
        validatingRequest = null
    }

    /**
     * The revalidation and gates passed and the system launch call returned: validation ends,
     * the LAUNCHED identity begins. Called AFTER [androidx.activity.result.ActivityResultLauncher.launch]
     * returned on Main — a result callback can only run on a LATER main-loop pass, so the
     * identity is in place before any result; if the launch call itself throws, the caller
     * reports [onValidationFailed] instead and nothing was ever launched.
     */
    fun onValidationLaunched(request: PhotoTrashController.ConsentRequest) {
        validatingRequest = null
        launchedRequest = request
    }

    /** Takes the launched request out: a result can be delivered at most once. */
    fun onResultDelivered(): PhotoTrashController.ConsentRequest? {
        val request = launchedRequest
        launchedRequest = null
        return request
    }

    /** Restores a LAUNCHED request from saved instance state (never a validating one). */
    fun onLaunchedRequestRestored(request: PhotoTrashController.ConsentRequest) {
        launchedRequest = request
    }

    /** The saved-instance-state payload: only a LAUNCHED request. */
    fun requestForSavedState(): PhotoTrashController.ConsentRequest? = launchedRequest

    companion object {

        /**
         * The final Main-side launch gate, evaluated synchronously in the SAME Main block
         * that launches the dialog — this closes the application-side window between the IO
         * revalidation and the queued Main continuation. In-memory only (no realm open, no
         * network, no hash work on Main):
         *  - the account check reads the in-memory current user;
         *  - [activationWitness] is ONE coherent capture of the activation authority
         *    ([PhotoTrashController.captureLaunchWitness], taken INSIDE the caller's launch
         *    section): null fails closed; a witness whose snapshot is missing, whose scope
         *    user differs, that is not enabled, or whose EPOCH differs from the request's
         *    fails closed — an activation cycle that landed after the IO validation is
         *    caught here even when it ended at identical Boolean values;
         *  - WITNESS SETTLEDNESS covers what two independent atomic reads cannot: that the
         *    snapshot the gate consumes IS the current settled committed authority — no
         *    activation write is unsettled (begun but not yet published or abandoned) and no
         *    write landed between the capture's reads (a complete disable landing there
         *    would otherwise pair a stale enabled snapshot with a settled flag). It also
         *    covers an abandoned write: rollback and post-commit publication failure leave
         *    the counter leading the mirror, the witness reads INCOHERENT and the gate
         *    refuses until a certified read has rebuilt the mirror. Refusing never erases
         *    anything: the durable row stays armed and the next drain re-surfaces the
         *    request;
         *  - the witness's BINDING side carries the request uri's process-local generation
         *    captured with the same coherence rules: a pairing replacement that landed after
         *    the IO validation (a NEW binding even at identical metadata) or during the
         *    capture is caught here;
         *  - a lifecycle that is not at least STARTED cannot show a dialog.
         * The caller holds [PhotoTrashController.tryBeginLaunchSection] around this
         * evaluation and the launch call: no authority writer can begin between the witness
         * capture and the completed launch, so the app-controlled check->launch interval is
         * closed in both orders (write wins -> the witness is incoherent/unsettled; launch
         * wins -> the writer observes a request already issued). Unknown evidence and
         * mismatched scope fail closed: nothing launches, nothing is erased, the durable row
         * stays armed for the next drain. The platform's own approval-time mutation race
         * (after the launch call returned, while the system dialog is on screen) spans no
         * app lock and stays disclosed in the controller.
         */
        fun launchAllowedNow(
            request: PhotoTrashController.ConsentRequest,
            currentUserId: Int,
            activationWitness: PhotoTrashController.LaunchWitness?,
            lifecycleStarted: Boolean,
        ): Boolean {
            val witness = activationWitness ?: return false
            val snapshot = witness.activationSnapshot ?: return false
            return lifecycleStarted &&
                currentUserId == request.userId &&
                // The captured bundle is coherent and settled: the snapshot the gate
                // consumes is the current committed authority, and the binding generation
                // was captured from a settled registry.
                witness.activationSettled &&
                witness.bindingSettled &&
                snapshot.userId == currentUserId &&
                snapshot.optIn &&
                snapshot.activationEpoch == request.activationEpoch &&
                witness.bindingGeneration == request.bindingGeneration
        }
    }
}
