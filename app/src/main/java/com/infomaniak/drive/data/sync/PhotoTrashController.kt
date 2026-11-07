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
package com.infomaniak.drive.data.sync

import android.content.Context
import android.os.Build
import android.provider.MediaStore
import com.infomaniak.core.network.models.exceptions.NetworkException
import com.infomaniak.core.sentry.SentryLog
import com.infomaniak.drive.data.api.ApiRepository
import com.infomaniak.drive.data.models.File
import com.infomaniak.drive.data.models.PhotoPairing
import com.infomaniak.drive.data.models.UploadFile
import com.infomaniak.drive.data.models.UserDrive
import com.infomaniak.drive.utils.AccountUtils
import io.realm.Realm
import io.realm.RealmConfiguration
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.ensureActive
import kotlinx.coroutines.flow.MutableSharedFlow
import kotlinx.coroutines.withContext
import java.util.Date
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference

/**
 * Orchestrates photo trash propagation (opt-in) on top of [PhotoTrashRules]. Only pairings
 * recorded from an authoritative upload response participate; only trash (never permanent
 * deletion) is propagated; every destructive action is preceded by fresh checks on BOTH
 * sides, and uncertainty is never treated as deletion.
 *
 * The local state read (MediaStore) and the cloud->device consent flow are Android-bound;
 * the local->remote pass runs at the end of the existing upload worker pass.
 */
object PhotoTrashController {

    private const val TAG = "PhotoTrashController"

    /** Pairings whose cloud->device trash just became pending user consent (UI shows the dialog). */
    val pendingConsents = MutableSharedFlow<PhotoPairing>(extraBufferCapacity = 4)

    /**
     * Main-readable ACTIVATION AUTHORITY of the photo-trash feature, for the dialog launch
     * gate: one immutable snapshot of (scope user, activation epoch, opt-in, revision).
     *
     * Revision protocol (a bare @Volatile Boolean is not one). One monotone revision
     * counter, one unsettled-write marker, one strict high-water mirror:
     *  - A write seam calls [beginActivationWrite] INSIDE its transaction, BEFORE any
     *    mutation: the revision is assigned (commit-ordered — Realm serializes write
     *    transactions, so a later commit always holds a higher revision) and the write is
     *    marked UNSETTLED. From that instant — before the write can commit — the authority
     *    is UNAVAILABLE TO DESTRUCTIVE LAUNCH ([activationWritesUnsettled] > 0), and it
     *    stays unavailable until the write SETTLES: its publish lands (the seam publishes
     *    FIRST, then settles) or the write is abandoned (rollback/exception: the seam
     *    settles in a `finally` WITHOUT a publish — the last published mirror remains the
     *    committed truth, and no assigned revision is stranded; certified reads resume
     *    immediately, without requiring another write).
     *  - The write seam publishes post-commit at its commit-assigned revision
     *    ([noteActivationWritten]); acceptance is a STRICT high-water mark (first
     *    observation or strictly newer revision) — invalidating snapshots follow the SAME
     *    ordering, so a stale fail-closed candidate can never rewind the authority, and a
     *    late publish of an earlier commit loses to a later commit's publish.
     *  - THE SERIALIZATION POINTS for an app-controlled write vs the final launch are the
     *    begin points under ONE monitor (the LAUNCH SECTION below): a writer may not begin
     *    while a Main launch section is active, and a section that begins later necessarily
     *    reads the writer's marks ([captureLaunchWitness]). The gate refuses an incoherent
     *    or unsettled bundle, so the mirror it consumes is the latest COMMITTED AND
     *    PUBLISHED state; the write that wins excludes the launch, and the launch that wins
     *    excludes any write from beginning until it completed. The interval this closes is
     *    APP-CONTROLLED check->launch; the only residue is the Android APPROVAL window
     *    (after the launch call returned, the system applies the trash at approval; no
     *    app-side lock spans the system dialog), which is disclosed as such.
     *  - A READER publishes ONLY a CERTIFIED observation ([beginActivationRead] /
     *    [isActivationReadCertified], applied by [UploadFile.getAppSyncSettings]): the
     *    read slot must BEGIN quiescent (no unsettled writes), read a FRESH snapshot (the
     *    getter calls [io.realm.Realm.refresh] — a long-lived thread-local instance may
     *    otherwise pin an old version, and matching process counters alone would not prove
     *    a freshly read committed database version), and FINISH with the counter unmoved
     *    and still no unsettled writes. Only then did the reader observe exactly the
     *    settled committed state of that revision. A reader that began while a write was
     *    in flight would read the PRE-commit row and could publish it at the writer's own
     *    revision — the strict high-water treats equal revisions as first-come, so that
     *    poisoned publish would block the writer's real publish indefinitely. Uncertified
     *    reads still RETURN the row they read (the caller's decision evidence); they only
     *    skip the mirror publication.
     *  - Scope: the snapshot carries the user it was observed for; the launch gate compares
     *    it with the current in-memory account.
     */
    data class ActivationSnapshot(val userId: Int, val activationEpoch: Long, val optIn: Boolean, val revision: Long)

    private val activationMirror = AtomicReference<ActivationSnapshot?>(null)
    private val mirrorRevision = AtomicLong(0L)
    private val unsettledActivationWrites = AtomicInteger(0)

    /** The current activation authority snapshot (null = nothing observed yet: fail-closed). */
    val currentActivationSnapshot: ActivationSnapshot?
        get() = activationMirror.get()

    /**
     * Marks an activation write UNSETTLED and assigns its revision. MUST be called by the
     * write seam INSIDE its transaction, BEFORE any mutation: from this instant (before the
     * write can commit) the authority is unavailable to destructive launch until the write
     * settles — its publish lands, or the write is abandoned and [endActivationWrite] runs.
     *
     * The begin is EXCLUDED from a running Main launch section (see [tryBeginLaunchSection]):
     * the writer waits for the section to end, and its unsettled mark lands INSIDE the same
     * monitor hold, so a section that begins later necessarily reads the mark in its
     * [captureLaunchWitness]. An interruption while waiting marks nothing and strands
     * nothing (the seam's `began` stays false).
     */
    internal fun beginActivationWrite(): Long = synchronized(launchGate) {
        while (launchSectionActive) (launchGate as java.lang.Object).wait()
        unsettledActivationWrites.incrementAndGet()
        mirrorRevision.incrementAndGet()
    }

    /**
     * Settles an activation write. MUST be called by the write seam in a `finally`, exactly
     * once per [beginActivationWrite] — AFTER its publish when the transaction committed
     * (the publish must lead the settle, never trail it), and WITHOUT a publish when the
     * transaction was abandoned (rollback/exception: the last published mirror remains the
     * committed truth and no assigned revision is stranded).
     */
    internal fun endActivationWrite() {
        unsettledActivationWrites.decrementAndGet()
    }

    /** Activation writes begun but not yet settled (see [beginActivationWrite]). */
    internal fun activationWritesUnsettled(): Int = unsettledActivationWrites.get()

    /**
     * Opens a CERTIFIABLE read slot: the revision permit to publish under, or null while an
     * activation write is unsettled (the row a reader is about to see may predate that
     * write's commit). MUST be called BEFORE the reader opens the realm.
     */
    internal fun beginActivationRead(): Long? =
        mirrorRevision.get().takeIf { unsettledActivationWrites.get() == 0 }

    /**
     * The FINISH check of the read that began at [permit]: the counter is unmoved and no
     * write is unsettled, so the row just read is exactly the settled committed state of
     * [permit]. MUST be evaluated AFTER the row read, never before it; only a certified
     * read may publish an observation.
     */
    internal fun isActivationReadCertified(permit: Long?): Boolean =
        permit != null && mirrorRevision.get() == permit && unsettledActivationWrites.get() == 0

    /** Publishes a CERTIFIED reader's observation under its read-slot permit (see class kDoc). */
    internal fun noteActivationObserved(permit: Long, userId: Int, activationEpoch: Long, optIn: Boolean) {
        publishActivation(ActivationSnapshot(userId, activationEpoch, optIn, permit))
    }

    /** Publishes a writer's post-commit state under its COMMIT-assigned revision (see class kDoc). */
    internal fun noteActivationWritten(userId: Int, activationEpoch: Long, optIn: Boolean, revision: Long) {
        publishActivation(ActivationSnapshot(userId, activationEpoch, optIn, revision))
    }

    private fun publishActivation(candidate: ActivationSnapshot) {
        activationMirror.updateAndGet { current ->
            // STRICT high-water mark — for writes and readers alike, valid or invalidating:
            // a stale candidate (whatever it asserts) never rewinds the authority.
            if (current == null || candidate.revision > current.revision) candidate else current
        }
    }

    /**
     * ONE monitor shared by the Main LAUNCH SECTION and EVERY authority writer family's
     * BEGIN point ([beginActivationWrite], [beginBindingWrite]). The section is the
     * app-controlled check->launch exclusion:
     *  - Main holds the SECTION (a flag — it never waits for a writer) across the witness
     *    capture ([captureLaunchWitness]), the final gate evaluation and the system launch
     *    call, releasing it in a `finally` ([endLaunchSection]);
     *  - a writer cannot BEGIN while a section is active (it waits on [launchGate], notified
     *    by [endLaunchSection]), and a writer's marks that landed before the section began
     *    are necessarily read by the section's witness.
     * Both launch/write orders are therefore closed: WRITE WINS — the marks predate the
     * section, the witness reads them and the gate refuses; LAUNCH WINS — no disable can
     * begin, commit or publish between the witness capture and the completed launch, and the
     * writer proceeding after [endLaunchSection] observes a request ALREADY ISSUED. The
     * section runs on Main and does NO realm/network/waiting work; writers wait only for the
     * short section, never the reverse.
     */
    private val launchGate = Any()

    @Volatile
    private var launchSectionActive = false

    /**
     * Begins the Main launch section (fail-closed: refuses if one is somehow already
     * active). MUST be followed by [endLaunchSection] in a `finally`, on the same thread.
     */
    fun tryBeginLaunchSection(): Boolean = synchronized(launchGate) {
        if (launchSectionActive) {
            false
        } else {
            launchSectionActive = true
            true
        }
    }

    /** Ends the Main launch section and releases writers waiting at their begin points. */
    fun endLaunchSection() {
        synchronized(launchGate) {
            launchSectionActive = false
            (launchGate as java.lang.Object).notifyAll()
        }
    }

    /**
     * The launch gate's input: ONE coherent capture of both authorities, read in a fixed
     * order and RE-VERIFIED (the mirror identity re-read; the binding stamp bracketed around
     * the generation read), so an authority movement DURING the capture can never present a
     * coherent-looking stale bundle.
     *
     *  - [LaunchWitness.activationSettled] is the settledness OF THE SNAPSHOT THE GATE WILL
     *    USE: the snapshot's revision equals the counter, no activation write is unsettled,
     *    and the mirror still holds the read snapshot. Two independent atomic reads at a
     *    call site cannot provide this — a complete disable landing between them would pair
     *    a stale enabled snapshot with a settled flag (the launch would accept an opt-in the
     *    user just switched off BEFORE the gate was even evaluated). It also covers an
     *    abandoned write, whether rollback or a post-commit publication failure: in both the
     *    counter then leads the mirror, the witness reads INCOHERENT and the gate refuses
     *    until a certified read has republished the committed state — rollback and
     *    post-commit failure are not distinguished because neither may be trusted without
     *    that rebuild.
     *  - [LaunchWitness.bindingSettled] reports that no binding write is in flight and the
     *    stamp did not move across the generation read, so the captured
     *    [LaunchWitness.bindingGeneration] belongs to a fully settled registry (a write
     *    between the stamp reads either flips the in-flight marker — always marked before
     *    any bump — or moves the stamp).
     *
     * Evaluated INSIDE a launch section, no writer family can begin during or after the
     * capture until the section ends, so the captured bundle stays valid for the launch call.
     */
    data class LaunchWitness(
        val activationSnapshot: ActivationSnapshot?,
        val activationSettled: Boolean,
        val bindingGeneration: Long,
        val bindingSettled: Boolean,
    )

    /** Captures the coherent launch authority bundle for [uri] (see [LaunchWitness]). */
    fun captureLaunchWitness(uri: String): LaunchWitness {
        val activation = activationMirror.get()
        val revision = mirrorRevision.get()
        val unsettled = unsettledActivationWrites.get()
        val activationRecheck = activationMirror.get()
        val bindingStampBefore = bindingWrites.get()
        val bindingInFlight = bindingWritesInFlight.get()
        val generation = bindingGeneration(uri)
        val bindingStampAfter = bindingWrites.get()
        return LaunchWitness(
            activationSnapshot = activation,
            activationSettled = unsettled == 0 &&
                activation != null &&
                activation.revision == revision &&
                activationRecheck === activation,
            bindingGeneration = generation,
            bindingSettled = bindingInFlight == 0 && bindingStampBefore == bindingStampAfter,
        )
    }

    /**
     * Process-local revision of one uri's BINDING. Every binding write ([recordPairing]'s
     * replacement — bumped BEFORE the transaction, so no Main launch can read the old
     * identity after the new binding is visible — and [removeAllPairings]'s wipe) advances
     * it, so the Main-side launch gate can detect a pairing replacement that landed AFTER
     * the IO validation — without any DB or network work on Main. A replacement is a NEW
     * binding even when uri, user, file and metadata are all unchanged; the durable
     * pairedAt identity remains the authoritative check at validation and result time (this
     * registry covers only the launch window).
     *
     * The identity is NON-REUSED: [invalidateAllBindings] bumps a base epoch instead of
     * resetting counters, so no generation value can recur after a wipe (no ABA) and every
     * generation ever captured stays distinguishable. Process-local by design: a restored
     * (persisted) request is attributed through the row-level identity checks, never
     * through this registry.
     */
    private val bindingGenerations = ConcurrentHashMap<String, Long>()
    @Volatile
    private var bindingEpoch = 0L

    /**
     * Counts EVERY binding write (a replacement's pre-commit bump, a wipe's pre-commit
     * invalidation). The consent drain captures this stamp BEFORE it reads the rows and
     * re-checks it at hand-out: any binding write in between means the read rows' binding
     * identities can no longer be certified against this registry, so nothing may be
     * handed out (see [pollNextPendingConsent]).
     */
    private val bindingWrites = AtomicLong(0L)

    /**
     * Binding writes begun but not yet settled (pre-commit bump done, transaction still
     * open or its post-commit work pending). A write ALREADY IN FLIGHT before the drain
     * starts must make the registry uncertifiable too: its bump is already visible in
     * [bindingWrites] and [bindingGeneration], but its row is not committed — a hand-out
     * in that state would bless the OLD row with the NEW generation. The mark is acquired
     * through [beginBindingWrite], which is also the launch-section exclusion point.
     */
    private val bindingWritesInFlight = AtomicInteger(0)

    fun bindingGeneration(uri: String): Long = bindingEpoch + (bindingGenerations[uri] ?: 0L)

    private fun noteBindingWritten(uri: String) {
        bindingGenerations.merge(uri, 1L, Long::plus)
        bindingWrites.incrementAndGet()
    }

    private fun invalidateAllBindings() {
        // Base bump only — never clear: every uri's effective generation strictly grows,
        // so a wiped world cannot reuse an older world's generation value.
        bindingEpoch++
        bindingWrites.incrementAndGet()
    }

    /** All pairings of the current user, detached. */
    fun getPairings(customRealmConfig: RealmConfiguration? = null): List<PhotoPairing> {
        val block: (Realm) -> List<PhotoPairing> = { realm ->
            realm.where(PhotoPairing::class.java)
                .equalTo(PhotoPairing::userId.name, AccountUtils.currentUserId)
                .findAll()
                .map { realm.copyFromRealm(it, 0) }
        }
        return customRealmConfig?.let { Realm.getInstance(it).use(block) } ?: UploadFile.getRealmInstance().use(block)
    }

    fun getPairing(uri: String, customRealmConfig: RealmConfiguration? = null): PhotoPairing? {
        return getPairings(customRealmConfig).firstOrNull { it.uri == uri }
    }

    /**
     * Records the authoritative pairing of a completed upload: the finish-session response's
     * created server file bound to the uploaded local source. Bases are bound from the
     * ACTUAL upload and ONE MediaStore observation at pairing time — this is the activation
     * baseline's correct point (a trash after pairing is a transition, never a baseline),
     * and it keeps every later local comparison same-producer (a fallback producer's
     * timestamp precision used to latch conflicts for unchanged content). A later re-read
     * never re-blesses changed content: the recorded digest is re-verified only at the
     * destructive boundary. A pairing for a reused URI is replaced (the newest upload
     * identity wins); unknown evidence is recorded as null and preserves.
     *
     * [uploadedContentDigest] is the digest of the bytes the upload actually transferred,
     * captured by [com.infomaniak.drive.data.api.UploadTask] while the bytes were read for
     * the transfer (the direct-upload stream; the empty-body identity for empty uploads).
     * When the transfer produced no digest (chunked/resumed uploads read PARALLEL per-chunk
     * streams whose digests cannot be composed into a whole-body identity), the pairing
     * binds ONLY through equivalent validated server-to-source evidence: the created file's
     * server hash is over the bytes the server assembled from the upload, so a fresh local
     * read that MATCHES it establishes the source-to-server identity — a mismatch (the
     * source changed since the transfer, e.g. a same-metadata replacement before pairing)
     * records null and preserves. There is NO unconditional post-upload sample: a sample
     * alone cannot tell uploaded content from replaced content.
     * [remoteFile]'s server identity (hash/size/lastModifiedAt when the response established
     * them) becomes the REMOTE basis.
     */
    fun recordPairing(
        context: Context,
        uploadFile: UploadFile,
        remoteFile: File?,
        uploadedContentDigest: String?,
        customRealmConfig: RealmConfiguration? = null,
    ) {
        val remoteFileId = remoteFile?.id?.takeIf { it > 0 } ?: return
        val local = queryLocalMedia(context, uploadFile.uri)
        val baseline = when {
            // An ABSENT or unreadable row is UNKNOWN — it never starts as a
            // present-and-not-trashed baseline (a trashed-then-vanished item or an
            // unmounted volume would be misclassified as a fresh post-pairing
            // transition). It stays pending; a later capture re-observes.
            local == null || !local.exists -> null
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.R -> local.isTrashed
            else -> false // trash state is not observable below API 30; the feature is inert there
        }
        val localDigest = when {
            uploadedContentDigest != null -> uploadedContentDigest
            // Server-to-source validation (chunked/resumed/any transfer without a stream
            // digest): the server identity is parsed STRICTLY (only the documented
            // "xxh3:" + 16-hex form is supported — an unknown algorithm preserves), then
            // ONE single-stream pass over the source computes BOTH receipts (the server
            // algorithm's digest AND the local sha-256 receipt) from the same snapshot.
            // The binding lands only when the same-pass server-algorithm digest matches:
            // two separate reads could validate A and record a replaced B.
            remoteFile?.serverContentHash != null ->
                when (val parsed = PhotoTrashServerHash.parse(remoteFile.serverContentHash)) {
                    is PhotoTrashServerHash.Parsed.Supported ->
                        digestContentValidatedByServerHash(context, uploadFile.uri, parsed.canonical)
                    is PhotoTrashServerHash.Parsed.Absent, is PhotoTrashServerHash.Parsed.Unsupported -> null
                }
            else -> null
        }
        // A new pairing belongs to the CURRENT activation generation (a replacement is a
        // new binding, not a resurrection of the old one's activation evidence). Read
        // BEFORE the transaction: the settings seam opens its own instance and must never
        // run inside this write transaction.
        val pairingEpoch = UploadFile.getAppSyncSettings()?.photoTrashActivationEpoch ?: 0L
        // The launch-window binding invalidation lands BEFORE the transaction: from the
        // moment the new binding can become authoritative (its commit), no Main-side launch
        // may still read the previous generation. The write is marked UNSETTLED before its
        // pre-commit bump and settled in a finally: a poll that starts while this write is
        // in flight must hand out nothing (its bump is already visible in the registry but
        // its row is not committed — the old row must not be blessed with the new
        // generation).
        beginBindingWrite()
        try {
            noteBindingWritten(uploadFile.uri)
            withPairingRealm(customRealmConfig) { realm ->
                realm.executeTransaction { mutableRealm ->
                    val previous = mutableRealm.where(PhotoPairing::class.java)
                        .equalTo(PhotoPairing::uri.name, uploadFile.uri)
                        .findFirst()
                    // A replacement is a NEW binding with a STRICTLY LATER pairing moment: a
                    // same-millisecond Date() is not a guaranteed unique identity when every
                    // other field is identical, so the moment is forced past the previous
                    // binding's (pairing writes are in-process and serialized per uri).
                    val pairedAtMs = maxOf(System.currentTimeMillis(), (previous?.pairedAt?.time ?: 0L) + 1L)
                    previous?.deleteFromRealm()
                    mutableRealm.createObject(PhotoPairing::class.java, uploadFile.uri)?.apply {
                        userId = uploadFile.userId
                        driveId = uploadFile.driveId
                        this.remoteFileId = remoteFileId
                        fileName = uploadFile.fileName
                        activationEpoch = pairingEpoch
                        // An UNOBSERVABLE row binds NO metadata (the -1 sentinel): the pass
                        // compares these against MediaStore's own values — falling back to the
                        // UploadFile's different-producer values would latch a false
                        // CONFLICT_LOCAL_CHANGED the moment the row became visible. When the
                        // metadata is unbound the rules skip the metadata comparison entirely
                        // and rely on the content binding (or stay inconclusive without one).
                        localSizeAtPairing = local?.takeIf { it.exists }?.size ?: -1L
                        localModifiedAtPairing = local?.takeIf { it.exists }?.lastModified ?: -1L
                        localContentDigest = localDigest
                        remoteContentHash = remoteFile?.serverContentHash
                        baselineTrashed = baseline
                        lastObservedTrashed = baseline ?: false
                        pairedAt = Date(pairedAtMs)
                        state = PhotoPairing.State.ACTIVE.name
                    }
                }
            }
        } finally {
            bindingWritesInFlight.decrementAndGet()
        }
    }

    /**
     * Marks a binding write in flight, EXCLUDED from a running Main launch section (see
     * [tryBeginLaunchSection]): the mark lands inside the monitor hold that waited for the
     * section, so a section beginning later necessarily reads the in-flight marker at its
     * [captureLaunchWitness], and a section already running holds this begin until it ends.
     * A write's bumps ([noteBindingWritten] / [invalidateAllBindings]) follow the mark, so
     * the in-flight marker always precedes any visible registry movement.
     */
    private fun beginBindingWrite(): Int = synchronized(launchGate) {
        while (launchSectionActive) (launchGate as java.lang.Object).wait()
        bindingWritesInFlight.incrementAndGet()
    }

    /** Drops pairings and any pending destructive state when the option is turned off. */
    fun removeAllPairings(customRealmConfig: RealmConfiguration? = null) {
        // Pre-invalidation, matching recordPairing's pre-commit bump, and UNSETTLED marking
        // like every binding write: from the moment the wipe CAN become authoritative (its
        // commit) — and from the moment it has begun — no Main-side launch may still read
        // the previous generation, and a drain that starts or hands out while the wipe is
        // in flight must surface nothing.
        beginBindingWrite()
        try {
            invalidateAllBindings()
            withPairingRealm(customRealmConfig) { realm ->
                realm.executeTransaction { mutableRealm ->
                    mutableRealm.where(PhotoPairing::class.java).findAll().deleteAllFromRealm()
                }
            }
        } finally {
            bindingWritesInFlight.decrementAndGet()
        }
    }

    /**
     * Captures a pairing's activation baseline from the CURRENT MediaStore trashed-state —
     * the retry/recovery path for rows left pending (their pairing-time observation was
     * unavailable) and for pre-existing pairings at opt-in. The row is stamped with
     * [activationEpoch], the activation generation this capture belongs to: an activation
     * re-baseline that changes NO field still invalidates every older observation through
     * the epoch alone. An observation that arrives only LATE cannot tell a pre-activation
     * trash from a post-activation delete: a TRUE baseline captured here is therefore
     * recorded as a visible [PhotoPairing.State.INCONCLUSIVE] state, never silently
     * relabeled as history. An unavailable observation stays pending and is retried on a
     * later pass. Never call this on a blocking UI thread.
     */
    fun captureActivationBaseline(
        context: Context,
        pairing: PhotoPairing,
        activationEpoch: Long,
        customRealmConfig: RealmConfiguration? = null,
    ): Boolean {
        val local = queryLocalMedia(context, pairing.uri)
        val baseline = when {
            // An ABSENT or unreadable row is UNKNOWN: the baseline is reset to PENDING
            // (never assumed not-trashed) so a later capture re-observes when the row is
            // readable — an old populated baseline must not survive as eligible through
            // an observation that could not actually see the item.
            local == null || !local.exists -> null
            Build.VERSION.SDK_INT >= Build.VERSION_CODES.R -> local.isTrashed
            else -> false // trash state is not observable below API 30; baseline: not trashed
        }
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, pairing.uri)
                    .findFirst()?.takeIf { it.stillIsSameBinding(pairing) }?.let { row ->
                        row.activationEpoch = activationEpoch
                        row.baselineTrashed = baseline
                        if (baseline != null) {
                            row.lastObservedTrashed = baseline
                            if (baseline && row.state == PhotoPairing.State.ACTIVE.name) {
                                row.state = PhotoPairing.State.INCONCLUSIVE.name
                            }
                        }
                    }
            }
        }
        return baseline ?: false
    }

    /**
     * Activation point for the opt-in (the settings save runs it BEFORE the enabled flag is
     * written, on its existing IO context — a pass can only start once the flag is on, so
     * no pass can race this capture into propagating against a stale baseline): captures the
     * real MediaStore trashed-state for EVERY pairing of the current user, INCLUDING rows
     * whose pairing-time baseline predates this activation, and stamps each row with
     * [activationEpoch] — the generation the caller computed for THIS activation (the
     * settings write seam stamps the same value into the persisted settings, so the next
     * pass runs under it and every pre-activation observation is invalidated by the epoch
     * even when all captured fields return to identical values). An item trashed while the
     * option was off must not reclassify as a new post-activation transition: its late-TRUE
     * capture at activation is a visible [PhotoPairing.State.INCONCLUSIVE], never silent
     * history and never propagation. Only THIS activation moment re-baselines populated
     * rows; the regular pass still only fills null baselines (under its own current
     * generation). No blocking IO on the settings UI thread.
     */
    fun captureActivationBaselines(context: Context, activationEpoch: Long, customRealmConfig: RealmConfiguration? = null) {
        getPairings(customRealmConfig).forEach { pairing ->
            captureActivationBaseline(context, pairing, activationEpoch, customRealmConfig)
        }
    }

    /**
     * One local pass over the current user's pairings: captures pending baselines, then
     * decides and executes local->remote transitions, and doubles as the cloud->device
     * catch-up. Returns the number of propagated trashes.
     */
    suspend fun runLocalReconciliation(context: Context, customRealmConfig: RealmConfiguration? = null): Int = withContext(Dispatchers.IO) {
        val settingsAtStart = UploadFile.getAppSyncSettings()
        if (settingsAtStart?.photoTrashSyncEnabled != true) return@withContext 0
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return@withContext 0
        // The activation generation this pass runs under: the FINAL pre-destructive gate
        // compares it with the live settings, so any activation cycle mid-pass (even one
        // ending at the same enabled value) stops the pass as stale work.
        val passEpoch = settingsAtStart.photoTrashActivationEpoch

        var propagated = 0
        val okHttpClient = AccountUtils.getHttpClient(AccountUtils.currentUserId, null)
        getPairings(customRealmConfig).forEach { pairing ->
            ensureActive()
            ensurePairingBaseline(context, pairing, passEpoch, customRealmConfig)

            val local = queryLocalMedia(context, pairing.uri)
            val remote = queryRemoteState(pairing, okHttpClient)
            val decision = PhotoTrashRules.decideLocalPropagation(
                baselineTrashed = pairing.baselineTrashed,
                lastObservedTrashed = pairing.lastObservedTrashed,
                lastLocalTrashReconciledAt = pairing.lastLocalTrashReconciledAt,
                local = local,
                remote = remote,
                pairingSize = pairing.localSizeAtPairing,
                pairingModifiedAt = pairing.localModifiedAtPairing,
                localDigestBound = pairing.localContentDigest != null,
                remoteBasisHash = pairing.remoteContentHash,
            )

            when (decision) {
                is PhotoTrashRules.LocalDecision.PropagateTrash -> {
                    // Destructive boundary: the recorded upload digest (when known) is
                    // re-verified against the CURRENT local bytes — the only local re-read
                    // that exists, so a changed source is caught here or never. A mismatch
                    // is a conflict, never an authority; an unreadable or unbound source is
                    // INCONCLUSIVE — unknown evidence preserves, it never authorizes.
                    when (verifyLocalDigestAtBoundary(context, pairing)) {
                        DigestBoundary.MISMATCH -> markConflict(pairing, PhotoPairing.State.CONFLICT_LOCAL_CHANGED, customRealmConfig)
                        DigestBoundary.UNREADABLE, DigestBoundary.NOT_BOUND -> markInconclusive(pairing, customRealmConfig)
                        DigestBoundary.VERIFIED -> {
                            // FINAL boundary recheck, immediately before the destructive
                            // call: the pass captured its row list and ran network/digest
                            // IO since its top-of-pass gate — the option could have been
                            // switched off, an activation cycle could have re-baselined
                            // everything (an OFF->ON cycle ends at the same Boolean but
                            // increments the epoch), or the account switched DURING the
                            // pass, and the ROW itself could have moved on (a same-URI
                            // re-pairing is a NEW binding; an activation re-baseline
                            // rewrites baseline/state/epoch). An unauthorized or stale pass
                            // stops WITHOUT marking any state and WITHOUT calling the API;
                            // the next authorized pass re-decides from fresh evidence.
                            //
                            // RESIDUAL EXTERNAL RACE (disclosed, not closable app-side and
                            // distinct from the stale-local-state gaps above): the remote
                            // observation was made earlier in the pass; a server-side
                            // replacement of the same file id between that observation and
                            // the delete call cannot be observed without a second read,
                            // which would only narrow, never close, the window. The bound
                            // local digest and the live-row witness close every LOCAL side;
                            // the remote-side window is bounded only by the observation's
                            // freshness.
                            val settingsNow = UploadFile.getAppSyncSettings()
                            val stillAuthorized = settingsNow?.photoTrashSyncEnabled == true &&
                                settingsNow.photoTrashActivationEpoch == passEpoch &&
                                pairing.userId == AccountUtils.currentUserId
                            val liveRowUnchanged = withPairingRealm(customRealmConfig) { realm ->
                                realm.where(PhotoPairing::class.java)
                                    .equalTo(PhotoPairing::uri.name, pairing.uri)
                                    .findFirst()?.stillIs(pairing) ?: false
                            }
                            val remoteFile = remote.file
                            when {
                                !stillAuthorized || !liveRowUnchanged -> return@withContext propagated
                                remoteFile == null -> markInconclusive(pairing, customRealmConfig)
                                ApiRepository.deleteFile(remoteFile).isSuccess() -> {
                                    markLocalTrashReconciled(pairing, observedTrashed = true, customRealmConfig = customRealmConfig)
                                    propagated++
                                }
                                else -> markInconclusive(pairing, customRealmConfig)
                            }
                        }
                    }
                }
                is PhotoTrashRules.LocalDecision.RecordObservedTrashed -> markLocalTrashReconciled(pairing, observedTrashed = true, customRealmConfig = customRealmConfig)
                is PhotoTrashRules.LocalDecision.MarkRestored -> markLocalTrashReconciled(pairing, observedTrashed = false, customRealmConfig = customRealmConfig)
                is PhotoTrashRules.LocalDecision.Conflict -> markConflict(pairing, decision.state, customRealmConfig)
                is PhotoTrashRules.LocalDecision.Inconclusive -> markInconclusive(pairing, customRealmConfig)
                is PhotoTrashRules.LocalDecision.None -> Unit
            }

            // The same pass doubles as the cloud->device catch-up. A remote trash that no
            // MQTT event delivered is armed here for consent; and because the app only ever
            // RECEIVES FILE_TRASH events (a restore has no event), a pending request or a
            // stored denial EXPIRES here, on the pass's own observation that the remote is
            // no longer trashed.
            if (remote.reachable) {
                if (!remote.isTrashed) {
                    if (pairing.remoteTrashPendingSince > 0L || pairing.state == PhotoPairing.State.DENIED.name) {
                        clearRemoteTrashPending(pairing, customRealmConfig)
                    }
                } else if (pairing.remoteTrashPendingSince == 0L) {
                    val remoteDecision = PhotoTrashRules.decideRemotePropagation(
                        pendingSince = pairing.remoteTrashPendingSince,
                        denied = pairing.state == PhotoPairing.State.DENIED.name,
                        local = local,
                        remote = remote,
                        pairingSize = pairing.localSizeAtPairing,
                        pairingModifiedAt = pairing.localModifiedAtPairing,
                        localDigestBound = pairing.localContentDigest != null,
                        remoteBasisHash = pairing.remoteContentHash,
                    )
                    if (remoteDecision is PhotoTrashRules.RemoteDecision.RequestConsent) {
                        armRemoteTrashPending(pairing, customRealmConfig)?.let { armed -> pendingConsents.tryEmit(armed) }
                    }
                }
            }
        }
        propagated
    }

    /**
     * Cloud->device side: called for a remote trash event (MQTT or catch-up). Verifies the
     * current remote state, checks the local content binding and either arms the pending
     * consent state or clears a stale one. The consent dialog itself is UI: it is launched
     * only through [pollNextPendingConsent]'s launch-time revalidation, never directly from
     * an event.
     */
    suspend fun onRemoteTrashEvent(
        context: Context,
        remoteFileId: Int,
        driveId: Int,
        customRealmConfig: RealmConfiguration? = null,
    ): PhotoPairing? {
        if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return null
        val pairing = getPairings(customRealmConfig).firstOrNull {
            it.remoteFileId == remoteFileId && it.driveId == driveId && it.userId == AccountUtils.currentUserId
        } ?: return null
        if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse()) return null

        val okHttpClient = runCatching { AccountUtils.getHttpClient(AccountUtils.currentUserId, null) }.getOrNull()
        val remote = okHttpClient?.let { queryRemoteState(pairing, it) } ?: return null
        val local = queryLocalMedia(context, pairing.uri)

        return when (val decision = PhotoTrashRules.decideRemotePropagation(
            pendingSince = pairing.remoteTrashPendingSince,
            denied = pairing.state == PhotoPairing.State.DENIED.name,
            local = local,
            remote = remote,
            pairingSize = pairing.localSizeAtPairing,
            pairingModifiedAt = pairing.localModifiedAtPairing,
            localDigestBound = pairing.localContentDigest != null,
            remoteBasisHash = pairing.remoteContentHash,
        )) {
            is PhotoTrashRules.RemoteDecision.RequestConsent -> armRemoteTrashPending(pairing, customRealmConfig)
            is PhotoTrashRules.RemoteDecision.ClearPending -> {
                clearRemoteTrashPending(pairing, customRealmConfig)
                null
            }
            is PhotoTrashRules.RemoteDecision.Conflict -> {
                markConflict(pairing, decision.state, customRealmConfig)
                null
            }
            is PhotoTrashRules.RemoteDecision.Inconclusive, is PhotoTrashRules.RemoteDecision.None -> null
        }
    }

    /**
     * THE SCOPED OPERATION IDENTITY of one cloud->device consent request: the pairing
     * (uri + owner account + drive + remote file + pairing moment) plus the consent stamp of
     * the exact outstanding request, the ACTIVATION generation the request belongs to, and
     * the process-local binding generation at hand-out. The identity is captured at the
     * poll, carried through the pre-launch revalidation and persisted across activity
     * recreation, and a dialog result applies only when the LIVE row still matches it in
     * full — never to a newer re-arm, a replaced pairing (a same-URI re-pairing is a NEW
     * binding with a new pairedAt), a newer activation cycle (even one ending at identical
     * values), or another account's row.
     */
    data class ConsentRequest(
        val uri: String,
        val userId: Int,
        val driveId: Int,
        val remoteFileId: Int,
        val pendingSince: Long,
        val pairedAt: Long,
        val activationEpoch: Long,
        val bindingGeneration: Long,
    )

    /**
     * The pre-launch revalidation of a captured [ConsentRequest] against the LIVE row and
     * gates, closing the APPLICATION-side window between a poll return and the dialog
     * launch (a switch/disable, an activation cycle or a binding replacement that happens
     * during this check is caught; the platform's own approval-time mutation race stays
     * unclosable and disclosed).
     */
    fun isConsentStillPending(request: ConsentRequest, customRealmConfig: RealmConfiguration? = null): Boolean {
        if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse()) return false
        if (request.userId != AccountUtils.currentUserId) return false
        return withPairingRealm(customRealmConfig) { realm ->
            realm.where(PhotoPairing::class.java)
                .equalTo(PhotoPairing::uri.name, request.uri)
                .findFirst()?.let { row ->
                    row.userId == request.userId &&
                        row.driveId == request.driveId &&
                        row.remoteFileId == request.remoteFileId &&
                        row.remoteTrashPendingSince == request.pendingSince &&
                        // A replaced pairing is a NEW binding (new pairing moment); a
                        // re-activation re-stamps the generation — an older request is
                        // invalidated by either, even at identical flag/baseline values.
                        row.pairedAt?.time == request.pairedAt &&
                        row.activationEpoch == request.activationEpoch &&
                        row.state != PhotoPairing.State.DENIED.name
                } ?: false
        }
    }

    /**
     * Records the user's consent result for a pending cloud->device trash. The result is
     * keyed to the SAME [ConsentRequest]: it applies only when the live row still matches
     * the request's full identity (account, drive, remote file, consent stamp) AND belongs
     * to the current account — a duplicate delivery, a result that raced a re-arm, a
     * replaced pairing, or one from a switched account is dropped, never applied to a
     * newer request. A grant re-activates the pairing; a DECLINE records the denial so the
     * same remote trash is not re-asked on every pass — the denial expires when the remote
     * is observed restored (the maintenance pass or [pollNextPendingConsent] clears the
     * pending state and resets a DENIED row to ACTIVE) or the local content changes (a
     * conflict state re-evaluates the pairing).
     */
    fun onRemoteTrashConsentResult(request: ConsentRequest, granted: Boolean, customRealmConfig: RealmConfiguration? = null) {
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, request.uri)
                    .findFirst()?.let { row ->
                        if (row.userId != AccountUtils.currentUserId) return@let
                        if (row.userId != request.userId ||
                            row.driveId != request.driveId ||
                            row.remoteFileId != request.remoteFileId ||
                            row.remoteTrashPendingSince != request.pendingSince ||
                            // A replaced pairing is a NEW binding: an answer belongs to the
                            // binding the dialog was shown for, never to its replacement.
                            (row.pairedAt?.time ?: 0L) != request.pairedAt ||
                            // Nor to a newer activation: an answer validated under an older
                            // generation gains no authority in a re-activated world (even
                            // when every captured value came back identical).
                            row.activationEpoch != request.activationEpoch
                        ) {
                            return@let
                        }
                        row.remoteTrashPendingSince = 0L
                        if (granted) {
                            row.lastRemoteTrashAppliedAt = System.currentTimeMillis()
                            row.state = PhotoPairing.State.ACTIVE.name
                        } else {
                            row.state = PhotoPairing.State.DENIED.name
                        }
                    }
            }
        }
    }

    /**
     * The durable serialized consent drain: the EXISTING pending row state
     * ([PhotoPairing.remoteTrashPendingSince]) IS the queue, so armed requests survive
     * process death and are re-surfaced on every app entry without any in-memory collector.
     * Returns the OLDEST armed request that is STILL valid right now, as its full-identity
     * [ConsentRequest] — fully revalidated at
     * launch time (opt-in still on, fresh remote state still trashed with an unchanged bound
     * identity, local content present with an UNCHANGED digest — the destructive boundary
     * check of the cloud->device direction). The rules are re-run AS IF arming now
     * (pendingSince = 0): the outstanding stamp is already this request's identity, and its
     * validity is decided from fresh evidence only. Every other outcome is resolved in
     * place (a remote restore expires the pending state and any denial; a changed local
     * source is a conflict; unreadable or unbound content is inconclusive; a local item
     * already trashed makes the request moot) and the next row is polled, so a drain hands
     * the caller at most ONE full-identity [ConsentRequest] and a stale request can never
     * reach the user. An unreachable remote is transient: the row stays armed for a later
     * entry. null = nothing to ask right now.
     *
     * THE UNCLOSEABLE APPROVAL RACE (disclosed, not closable app-side): arming a consent is
     * a permission grant — Android applies the MediaStore trash at the moment the user
     * APPROVES the system dialog, with no app pre-apply callback. The checks above are the
     * last app-controlled ones; content replaced after them (same metadata) would be
     * trashed by the approval itself. No lock or recheck can close a system-dialog window;
     * the residual window is bounded only by this fresh digest check at launch.
     *
     * BINDING CERTIFICATION OF THE HAND-OUT: the rows are read once at the drain's start;
     * a binding write that lands afterwards (a replacement's pre-commit bump, a wipe's
     * pre-commit invalidation — see the registry) would otherwise be invisible to the
     * hand-out, and a replacement that bumps the generation BEFORE its commit could hand
     * the OLD binding's request out under the ALREADY-BUMPED generation. The drain
     * therefore refuses to start while a binding write is IN FLIGHT (its bump is already
     * in the registry but its row is not committed), captures the process-global
     * binding-write stamp BEFORE the row read, and at the hand-out captures the uri's
     * generation FIRST and re-verifies the registry AFTER it: a write bumping between the
     * earlier stamp comparison and the generation read would otherwise bless the old row
     * with the new generation, and a write still unsettled makes the whole witness
     * uncertifiable. Any failed verification ends the drain with NOTHING surfaced (the
     * durable row state re-surfaces on the next entry). This is deliberately conservative:
     * the stamp is global, so a write to a DIFFERENT uri also ends the drain — binding
     * writes are rare (completed uploads, an opt-out wipe) and fail-closed retried.
     */
    suspend fun pollNextPendingConsent(context: Context, customRealmConfig: RealmConfiguration? = null): ConsentRequest? =
        withContext(Dispatchers.IO) {
            if (Build.VERSION.SDK_INT < Build.VERSION_CODES.R) return@withContext null
            if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse()) return@withContext null

            val okHttpClient = runCatching { AccountUtils.getHttpClient(AccountUtils.currentUserId, null) }.getOrNull()
            // A binding write already in flight makes the registry uncertifiable before the
            // drain even reads: refuse to start.
            if (bindingWritesInFlight.get() != 0) return@withContext null
            // The binding-write stamp of the moment the rows are read (see the kDoc above).
            val bindingStampAtRead = bindingWrites.get()
            val armedRows = getPairings(customRealmConfig)
                .filter { it.remoteTrashPendingSince > 0L && it.state != PhotoPairing.State.DENIED.name }
                .sortedBy { it.remoteTrashPendingSince }
            for (pairing in armedRows) {
                ensureActive()
                // LIVE recheck per row: the opt-in/account gate ran once before the IO
                // block, and this row's revalidation involves network + digest IO — the
                // option or account could have changed in between. A changed gate ends
                // the drain: nothing is returned, nothing is marked.
                if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse()) return@withContext null
                if (pairing.userId != AccountUtils.currentUserId) return@withContext null
                // Revalidation evaluates the request AS IF arming it now (pendingSince = 0):
                // the rules' "consent already outstanding -> None" guard exists for arming
                // paths, not for validating an outstanding one. An unreachable remote is a
                // TRANSIENT condition: the request stays armed and the next row is tried —
                // a network failure never rewrites a row's state.
                val remote = okHttpClient?.let { queryRemoteState(pairing, it) }
                if (remote?.reachable != true) continue
                val local = queryLocalMedia(context, pairing.uri)
                when (val decision = PhotoTrashRules.decideRemotePropagation(
                    pendingSince = 0L,
                    denied = false,
                    local = local,
                    remote = remote,
                    pairingSize = pairing.localSizeAtPairing,
                    pairingModifiedAt = pairing.localModifiedAtPairing,
                    localDigestBound = pairing.localContentDigest != null,
                    remoteBasisHash = pairing.remoteContentHash,
                )) {
                    is PhotoTrashRules.RemoteDecision.RequestConsent -> when (verifyLocalDigestAtBoundary(context, pairing)) {
                        DigestBoundary.MISMATCH -> {
                            markConflict(pairing, PhotoPairing.State.CONFLICT_LOCAL_CHANGED, customRealmConfig)
                            clearRemoteTrashPending(pairing, customRealmConfig)
                        }
                        DigestBoundary.UNREADABLE, DigestBoundary.NOT_BOUND -> {
                            markInconclusive(pairing, customRealmConfig)
                            clearRemoteTrashPending(pairing, customRealmConfig)
                        }
                        DigestBoundary.VERIFIED -> {
                            // FINAL gate at the hand-out point: the live checks ran before
                            // this row's remote/digest IO — the option or account could
                            // have changed DURING it. The request is handed out only when
                            // the gate still holds now; otherwise the drain ends with
                            // nothing surfaced and the row's state untouched.
                            if (!UploadFile.getAppSyncSettings()?.photoTrashSyncEnabled.orFalse() ||
                                pairing.userId != AccountUtils.currentUserId
                            ) {
                                return@withContext null
                            }
                            // THE BINDING WITNESS: the generation is captured FIRST, then
                            // the registry is re-verified — a write bumping between the
                            // earlier stamp comparison and the generation read would
                            // otherwise bless the old row with the new generation, and a
                            // write still unsettled (before OR after its commit) makes the
                            // whole witness uncertifiable.
                            val requestBindingGeneration = bindingGeneration(pairing.uri)
                            if (bindingWritesInFlight.get() != 0 || bindingWrites.get() != bindingStampAtRead) {
                                return@withContext null
                            }
                            return@withContext ConsentRequest(
                                uri = pairing.uri,
                                userId = pairing.userId,
                                driveId = pairing.driveId,
                                remoteFileId = pairing.remoteFileId,
                                pendingSince = pairing.remoteTrashPendingSince,
                                pairedAt = pairing.pairedAt?.time ?: 0L,
                                activationEpoch = pairing.activationEpoch,
                                bindingGeneration = requestBindingGeneration,
                            )
                        }
                    }
                    is PhotoTrashRules.RemoteDecision.ClearPending -> clearRemoteTrashPending(pairing, customRealmConfig)
                    is PhotoTrashRules.RemoteDecision.Conflict -> {
                        markConflict(pairing, decision.state, customRealmConfig)
                        clearRemoteTrashPending(pairing, customRealmConfig)
                    }
                    is PhotoTrashRules.RemoteDecision.Inconclusive -> {
                        markInconclusive(pairing, customRealmConfig)
                        clearRemoteTrashPending(pairing, customRealmConfig)
                    }
                    is PhotoTrashRules.RemoteDecision.None ->
                        // With the as-if-arming inputs the only None case is a local item
                        // already trashed: both sides are consistent, the request is moot.
                        clearRemoteTrashPending(pairing, customRealmConfig)
                }
            }
            null
        }

    //region Realm state helpers

    /**
     * Whether the LIVE row still IS the pairing the detached [observed] copy was read from:
     * the full observed BINDING AND ACTIVATION identity — owner, drive, remote file, the
     * pairing moment (a same URI/user/file re-pairing is a NEW binding with a new pairedAt),
     * both content-binding columns, the pairing metadata, and the baseline/observation/state
     * triple. The pending consent stamp is deliberately NOT compared here: it is the one
     * field designed to change concurrently (a consent arm between observation and write).
     * A stale detached observation must neither trash nor mutate a row that moved on — a
     * replacement, an activation re-baseline, a concurrent pass — so every transaction-time
     * write of the reconciliation pass verifies this witness first and no-ops on mismatch
     * (the next pass re-decides from fresh evidence; uncertainty is never overwritten).
     */
    private fun PhotoPairing.stillIs(observed: PhotoPairing): Boolean =
        userId == observed.userId &&
            driveId == observed.driveId &&
            remoteFileId == observed.remoteFileId &&
            pairedAt == observed.pairedAt &&
            activationEpoch == observed.activationEpoch &&
            localContentDigest == observed.localContentDigest &&
            remoteContentHash == observed.remoteContentHash &&
            localSizeAtPairing == observed.localSizeAtPairing &&
            localModifiedAtPairing == observed.localModifiedAtPairing &&
            baselineTrashed == observed.baselineTrashed &&
            lastObservedTrashed == observed.lastObservedTrashed &&
            state == observed.state

    /**
     * The binding-only witness for consent results: the row must still be the SAME pairing
     * (owner, drive, remote file, pairing moment) the request was handed out for — a
     * replacement binding never receives an older request's answer. Activation-state drift
     * (a re-baseline while the dialog was out) does NOT invalidate the user's answer about
     * this remote trash; the pending stamp check in the caller covers the request identity.
     */
    private fun PhotoPairing.stillIsSameBinding(observed: PhotoPairing): Boolean =
        userId == observed.userId &&
            driveId == observed.driveId &&
            remoteFileId == observed.remoteFileId &&
            pairedAt == observed.pairedAt

    /**
     * Runs [block] on a pairing realm. An injected [customRealmConfig] is opened and closed
     * HERE, on the calling thread, for the duration of ONE helper call only — a Realm
     * instance is thread-confined and must never be held across a suspension point (the
     * reconciliation pass runs on Dispatchers.IO and suspends for its network calls, so a
     * long-lived instance handed in from another thread would be an illegal cross-thread
     * access, and even one opened inside the pass could resume on a different IO worker).
     */
    private inline fun <T> withPairingRealm(customRealmConfig: RealmConfiguration?, block: (Realm) -> T): T =
        customRealmConfig?.let { Realm.getInstance(it).use(block) } ?: UploadFile.getRealmInstance().use(block)

    /** Persists the pending consent state and returns the (detached) pairing with the fresh stamp. */
    private fun armRemoteTrashPending(pairing: PhotoPairing, customRealmConfig: RealmConfiguration? = null): PhotoPairing? {
        val now = System.currentTimeMillis()
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, pairing.uri)
                    .findFirst()?.takeIf { row ->
                        row.stillIs(pairing) && row.remoteTrashPendingSince == pairing.remoteTrashPendingSince
                    }?.remoteTrashPendingSince = now
            }
        }
        pairing.remoteTrashPendingSince = now
        return pairing
    }

    private fun ensurePairingBaseline(context: Context, pairing: PhotoPairing, passEpoch: Long, customRealmConfig: RealmConfiguration? = null) {
        if (pairing.baselineTrashed != null) return
        // A late fill belongs to the CURRENT activation generation the pass runs under.
        captureActivationBaseline(context, pairing, passEpoch, customRealmConfig)
    }

    private fun markLocalTrashReconciled(observed: PhotoPairing, observedTrashed: Boolean, customRealmConfig: RealmConfiguration? = null) {
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, observed.uri)
                    .findFirst()?.takeIf { it.stillIs(observed) }?.let { row ->
                        row.lastObservedTrashed = observedTrashed
                        if (observedTrashed) row.lastLocalTrashReconciledAt = System.currentTimeMillis()
                        else row.lastLocalTrashReconciledAt = 0L
                        row.state = PhotoPairing.State.ACTIVE.name
                    }
            }
        }
    }

    private fun markConflict(observed: PhotoPairing, state: PhotoPairing.State, customRealmConfig: RealmConfiguration? = null) {
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, observed.uri)
                    .findFirst()?.takeIf { it.stillIs(observed) }?.let { row ->
                        row.state = state.name
                        row.lastObservedTrashed = true
                    }
            }
        }
    }

    private fun markInconclusive(observed: PhotoPairing, customRealmConfig: RealmConfiguration? = null) {
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, observed.uri)
                    .findFirst()?.takeIf { it.stillIs(observed) }?.let { row ->
                        row.state = PhotoPairing.State.INCONCLUSIVE.name
                    }
            }
        }
    }

    private fun clearRemoteTrashPending(observed: PhotoPairing, customRealmConfig: RealmConfiguration? = null) {
        withPairingRealm(customRealmConfig) { realm ->
            realm.executeTransaction { mutableRealm ->
                mutableRealm.where(PhotoPairing::class.java)
                    .equalTo(PhotoPairing::uri.name, observed.uri)
                    .findFirst()?.takeIf { row ->
                        // An expiry is ABOUT one exact outstanding request: the live row must
                        // still be the observed binding AND carry that same pending stamp —
                        // a newer re-arm on a moved-on row is never cleared by stale work.
                        row.stillIsSameBinding(observed) && row.remoteTrashPendingSince == observed.remoteTrashPendingSince
                    }?.let { row ->
                        row.remoteTrashPendingSince = 0L
                        // A remote restore EXPIRES a recorded denial: the row becomes
                        // eligible again, so a later re-delete re-arms a fresh consent —
                        // a stored DENIED state is never permanent.
                        if (row.state == PhotoPairing.State.DENIED.name) {
                            row.state = PhotoPairing.State.ACTIVE.name
                        }
                    }
            }
        }
    }

    //endregion

    //region Observations

    /**
     * Reads the current MediaStore state of one local media item. Null means the read failed
     * entirely (security/illegal argument); a missing row maps to exists=false. Below API 30
     * the trashed flag is not observable: it is reported false so the pass records a
     * conservative non-trashed observation instead of inventing a deletion.
     */
    fun queryLocalMedia(context: Context, uriString: String): PhotoTrashRules.LocalObservation? {
        val uri = runCatching { android.net.Uri.parse(uriString) }.getOrNull() ?: return null
        return runCatching {
            val projection = buildList {
                add(MediaStore.MediaColumns.SIZE)
                add(MediaStore.MediaColumns.DATE_MODIFIED)
                if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.R) add(MediaStore.MediaColumns.IS_TRASHED)
            }.toTypedArray()
            context.contentResolver.query(uri, projection, null, null, null)?.use { cursor ->
                if (!cursor.moveToFirst()) return PhotoTrashRules.LocalObservation(exists = false, isTrashed = false)
                val size = runCatching { cursor.getLong(0) }.getOrNull()
                val lastModified = runCatching { cursor.getLong(1) * 1_000L }.getOrNull()
                val trashed = Build.VERSION.SDK_INT >= Build.VERSION_CODES.R &&
                    runCatching { cursor.getInt(2) == 1 }.getOrDefault(false)
                PhotoTrashRules.LocalObservation(exists = true, isTrashed = trashed, size = size, lastModified = lastModified)
            }
        }.onFailure { exception ->
            // The exception class is folded into the message: the plain SentryLog.d overload
            // carries no extras.
            SentryLog.d(TAG, "Local media query failed for $uriString (treated as inconclusive): ${exception.javaClass.simpleName}")
        }.getOrNull()
    }

    private fun queryRemoteState(pairing: PhotoPairing, okHttpClient: okhttp3.OkHttpClient): PhotoTrashRules.RemoteObservation {
        val response = runCatching {
            ApiRepository.getFileDetailsWithHash(
                file = File(id = pairing.remoteFileId, driveId = pairing.driveId),
                okHttpClient = okHttpClient,
            )
        }.onFailure { exception ->
            if (exception !is NetworkException) SentryLog.e(TAG, "Remote details failed", exception)
        }.getOrNull()
        val file = response?.takeIf { it.isSuccess() }?.data
        return when {
            file == null -> PhotoTrashRules.RemoteObservation(reachable = false, isTrashed = false)
            else -> PhotoTrashRules.RemoteObservation(
                reachable = true,
                isTrashed = file.isTrashed(),
                size = file.size,
                lastModifiedAt = file.lastModifiedAt,
                serverHash = file.serverContentHash,
                file = file,
            )
        }
    }

    //endregion

    //region Content binding
    /** Result of the destructive-boundary re-verification of the local source. */
    private enum class DigestBoundary {
        /** The current bytes equal the recorded upload digest. */
        VERIFIED,
        /** The current bytes diverge from the recorded upload digest: a conflict, never an authority. */
        MISMATCH,
        /** The current bytes could not be read: inconclusive. */
        UNREADABLE,
        /** No digest was recorded at pairing: metadata binding only (disclosed). */
        NOT_BOUND,
    }

    /**
     * The ONLY re-read of the local source that exists: at the destructive boundary, the
     * pairing's recorded upload digest is compared to the CURRENT bytes. A digest sampled or
     * computed anywhere else could bless changed content; here a changed source is caught or
     * the binding stays unknown ([DigestBoundary.NOT_BOUND]).
     */
    private suspend fun verifyLocalDigestAtBoundary(context: Context, pairing: PhotoPairing): DigestBoundary = withContext(Dispatchers.IO) {
        val recorded = pairing.localContentDigest ?: return@withContext DigestBoundary.NOT_BOUND
        val current = digestContent(context, pairing.uri) ?: return@withContext DigestBoundary.UNREADABLE
        if (current == recorded) DigestBoundary.VERIFIED else DigestBoundary.MISMATCH
    }

    /** sha-256 of the CURRENT local content behind [uriString]; null when unreadable. */
    private fun digestContent(context: Context, uriString: String): String? = runCatching {
        val uri = android.net.Uri.parse(uriString)
        context.contentResolver.openInputStream(uri)?.use { input ->
            val digest = java.security.MessageDigest.getInstance("SHA-256")
            val buffer = ByteArray(DEFAULT_BUFFER_SIZE)
            while (true) {
                val read = input.read(buffer)
                if (read < 0) break
                digest.update(buffer, 0, read)
            }
            digest.digest().joinToString("") { "%02x".format(it) }
        }
    }.onFailure { exception ->
        SentryLog.w(TAG, "Local content digest failed for $uriString (binding unavailable)", exception)
    }.getOrNull()

    /**
     * ONE single-stream pass over the source computing BOTH receipts from the same
     * snapshot: the server-algorithm (XXH3-64) digest and the local sha-256 receipt. The
     * sha-256 receipt is returned ONLY when the same-pass XXH3 digest equals
     * [serverHashCanonical] — the server-to-source identity proven against the exact
     * bytes the receipt describes. A mismatch, an unreadable source, or a mid-read
     * failure records nothing (null = unknown preserves).
     */
    private fun digestContentValidatedByServerHash(context: Context, uriString: String, serverHashCanonical: String): String? = runCatching {
        val uri = android.net.Uri.parse(uriString)
        context.contentResolver.openInputStream(uri)?.use { input ->
            // Every byte the XXH3 pass reads also updates the sha-256 receipt: both
            // digests see ONE read sequence, so a mid-pass source change can neither
            // split the two receipts nor validate one and record the other.
            val localDigest = java.security.MessageDigest.getInstance("SHA-256")
            val dualDigestSource = object : java.io.FilterInputStream(input) {
                override fun read(b: ByteArray, off: Int, len: Int): Int {
                    val read = super.read(b, off, len)
                    if (read > 0) localDigest.update(b, off, read)
                    return read
                }

                override fun read(): Int {
                    val read = super.read()
                    if (read >= 0) localDigest.update(read.toByte())
                    return read
                }
            }
            val serverAlgorithmDigest = PhotoTrashServerHash.hashStreaming(dualDigestSource)
            val localReceipt = localDigest.digest().joinToString("") { "%02x".format(it) }
            localReceipt.takeIf { serverAlgorithmDigest == serverHashCanonical }
        }
    }.onFailure { exception ->
        SentryLog.w(TAG, "Server-to-source validation read failed for $uriString (binding unavailable)", exception)
    }.getOrNull()

    //endregion

    private fun Boolean?.orFalse() = this == true
}
