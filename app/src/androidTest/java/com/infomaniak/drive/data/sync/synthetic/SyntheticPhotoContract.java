/* Copyright (C) 2026 Infomaniak Network SA. SPDX-License-Identifier: GPL-3.0-or-later */
package com.infomaniak.drive.data.sync.synthetic;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

/** Fixed synthetic test contract, independent of every archived-copy entry/sidecar. */
public final class SyntheticPhotoContract {
    private SyntheticPhotoContract() {}
    public static final String MAIN_SHA = "974714a61eb14405abe6146fb47b7cccad90b0968de3ab5d5d8349b89b6794d0";
    public static final String PHOTO = "PHOTO_TRASH_22";
    public static final String UPLOAD = "UPLOAD_DIGEST_5";
    private static final String[][] PHOTO_METHODS = {
        {"successfulUploadWithoutHashAcquiresBasisThroughNormalReconciliationOnly", null},
        {"missingBasisRejectsMatchingReplacementOfBothSidesAfterUpload", null},
        {"refusedAcquisitionPreservesOrdinaryRestoreBookkeepingBeforeALaterEvidencePass", null},
        {"missingBasisTransactionRejectsEveryConcurrentBindingFieldChangeAndDeletion", null},
        {"missingBasisRejectsAccountAbaAndActivationOrSettingsDrift", null},
        {"cancelledMissingBasisQueryCannotWriteOrSwallowCancellation", null},
        {"invalidationsAfterValidationPreventActualEnqueue", "completed opt-out, rebinding, activation and account changes after validation each prevent actual enqueue"},
        {"enqueueWinsBeforeInvalidationButDoesNotWaitForItsResponse", "actual enqueue precedes a queued invalidation without holding the gate over the response wait"},
        {"trashAfterPairingBeforeTheFirstPassPropagates", "a trash after pairing and before the first pass is a transition, not a baseline"},
        {"deniedRowExpiresThroughTheRealCatchUpPathAndReArms", "a stored DENIED row expires on the pass's own restore observation and re-arms on re-delete"},
        {"pairingBindsThroughValidatedServerToSourceEvidence", "a pairing binds through validated server-to-source evidence, never an unconditional sample"},
        {"absentLocalRowStaysPending", "an absent or unreadable local row stays pending \u2014 never an assumed false baseline"},
        {"optionDisabledMidPassStopsBeforeTheDestructiveCall", "an option switched off DURING the pass stops it: zero delete calls, no state writes"},
        {"itemTrashedWhileOffIsInconclusiveAtActivation", "an item trashed while the option was off is inconclusive at activation, never a new transition"},
        {"enabledOptionBeforeTrashStillPropagatesAfterActivation", "an option enabled before a trash still propagates it after activation"},
        {"lateBaselineCaptureOfATrashedItemMarksInconclusive", "a late baseline capture of a trashed item is a visible inconclusive, never silent history"},
        {"unboundDigestNeverPropagates", "an unbound upload digest never propagates and never calls the server trash"},
        {"sameMetadataReplacementIsCaughtAtTheBoundary", "a same-metadata URI replacement is caught at the destructive boundary"},
        {"staleObservationCannotTrashAReplacementPairing", "a same-URI replacement between observation and effect is never trashed or mutated by the stale pass"},
        {"offOnActivationBetweenObservationAndEffectIsNeverBypassed", "an OFF->ON activation between observation and effect is never bypassed by the in-flight pass"},
        {"staleExpiryCannotClearAReplacedBindingFreshArming", "a stale expiry cannot clear a replaced binding's fresh arming"},
        {"absentRowMetadataBecomesVisibleWithoutFalseConflict", "a row absent at pairing and visible later rides the content binding, never a false metadata conflict"},
    };
    private static final String[][] UPLOAD_METHODS = {
        {"directUploadPassesTheTransferredBytesDigest", "a direct sync upload passes the digest of the bytes the transfer actually read"},
        {"emptyUploadPassesTheEmptyBodyIdentity", "an empty upload passes the empty-body identity, not a later sample"},
        {"freshChunkedUploadPassesNoStreamDigest", "a fresh chunked upload passes NO stream digest; the finish-session file is passed through"},
        {"resumedChunkedUploadPassesNoStreamDigest", "a resumed chunked upload passes NO stream digest and skips the already-valid chunks"},
        {"nonSyncUploadsNeverReachRecordPairing", "non-sync uploads never reach recordPairing: the digest work stays scoped to sync"},
    };

    public static void require(boolean valid) {
        if (!valid) throw new IllegalArgumentException("SYNTHETIC_CONTRACT_REJECTED");
    }
    public static void input(Map<String, String> args) {
        require(args != null && args.keySet().equals(new HashSet<>(Arrays.asList("syntheticEntry", "syntheticStamp", "mainSha", "testSha"))));
        methods(args.get("syntheticEntry"));
        require(args.get("syntheticStamp") != null && args.get("syntheticStamp").matches("q12-synthetic-[0-9]{8}-[0-9a-f]{12}"));
        require(MAIN_SHA.equals(args.get("mainSha")));
        require(args.get("testSha") != null && args.get("testSha").matches("[0-9a-f]{64}"));
    }
    public static void identity(String packageName, String applicationClass, int runningUid, int testUid, int mainUid,
                                boolean ownCanonicalData, String actualMainSha, String actualTestSha, String expectedTestSha) {
        require("com.infomaniak.drive.test".equals(packageName) && "android.app.Application".equals(applicationClass));
        require(runningUid >= 10000 && runningUid == testUid && mainUid >= 10000 && mainUid != testUid);
        require(ownCanonicalData && MAIN_SHA.equals(actualMainSha) && expectedTestSha != null && expectedTestSha.matches("[0-9a-f]{64}") && expectedTestSha.equals(actualTestSha));
    }
    public static File runDirectory(File ownData, File ownFiles, String stamp) throws IOException {
        require(stamp != null && stamp.matches("q12-synthetic-[0-9]{8}-[0-9a-f]{12}"));
        require(!Files.isSymbolicLink(ownFiles.toPath()) && ownFiles.getCanonicalFile().getParentFile().equals(ownData.getCanonicalFile()));
        File fresh = new File(ownFiles, stamp);
        require(fresh.mkdir()); // no writes into a preexisting, partial or linked stamp
        return fresh.getCanonicalFile();
    }
    private static String[][] rows(String entry) {
        if (PHOTO.equals(entry)) return PHOTO_METHODS;
        if (UPLOAD.equals(entry)) return UPLOAD_METHODS;
        throw new IllegalArgumentException("SYNTHETIC_ENTRY_REJECTED");
    }
    public static String className(String entry) {
        rows(entry);
        return PHOTO.equals(entry) ? "com.infomaniak.drive.data.sync.PhotoTrashOrchestrationTest" :
            "com.infomaniak.drive.data.api.UploadTaskDigestCaptureTest";
    }
    public static String[] methods(String entry) {
        return Arrays.stream(rows(entry)).map(row -> row[0]).toArray(String[]::new);
    }
    public static void inventory(String entry, String[] actual) {
        require(actual != null && actual.length == rows(entry).length && new HashSet<>(Arrays.asList(actual)).equals(new HashSet<>(Arrays.asList(methods(entry)))));
    }
    public static String bare(String entry, String className, String raw) {
        if (!className(entry).equals(className)) return null;
        for (String[] row : rows(entry)) if (row[0].equals(raw) || (row[0] + "()").equals(raw) || row[1] != null && row[1].equals(raw)) return row[0];
        return null;
    }

    /** Consumed by the real RunListener, not a replacement engine or manufactured status stream. */
    public static final class Events {
        private final String entry;
        private final Set<String> started = new LinkedHashSet<>(), finished = new LinkedHashSet<>(), failed = new LinkedHashSet<>(), skipped = new LinkedHashSet<>();
        private int invalid;
        public Events(String entry) { rows(entry); this.entry = entry; }
        private String key(String cls, String raw) {
            String key = bare(entry, cls, raw);
            if (key == null) invalid++;
            return key;
        }
        public synchronized void start(String cls, String raw) {
            String key = key(cls, raw);
            if (key != null && (finished.contains(key) || !started.add(key))) invalid++;
        }
        public synchronized void finish(String cls, String raw) {
            String key = key(cls, raw);
            if (key != null && (!started.contains(key) || !finished.add(key))) invalid++;
        }
        public synchronized void fail(String cls, String raw) {
            String key = key(cls, raw);
            if (key != null) { if (!started.contains(key) || finished.contains(key) || skipped.contains(key)) invalid++; failed.add(key); }
        }
        public synchronized void skip(String cls, String raw, boolean ignored) {
            String key = key(cls, raw);
            if (key != null) {
                if (failed.contains(key) || !skipped.add(key)) invalid++;
                if (ignored) { if (started.contains(key) || !finished.add(key)) invalid++; }
                else if (!started.contains(key)) invalid++;
            }
        }
        public synchronized int started() { return started.size(); }
        public synchronized int finished() { return finished.size(); }
        public synchronized int failed() { return failed.size(); }
        public synchronized int skipped() { return skipped.size(); }
        public synchronized int passed() { return (int) finished.stream().filter(k -> !failed.contains(k) && !skipped.contains(k)).count(); }
        public synchronized int invalid() { return invalid; }
        public synchronized boolean complete() { return invalid == 0 && finished.equals(new HashSet<>(Arrays.asList(methods(entry)))); }
    }
}
