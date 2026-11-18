package com.infomaniak.drive.data.cache.copyreader;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Fixed diagnostic contract and ancestry decisions; no Android or Realm initialization. */
public final class AggregateCopyGuard {
    private AggregateCopyGuard() {}

    public static String entry(String value) {
        if (value == null) return "readFiveArchivedRows";
        CopyInputGuard.require(value.equals("readFiveArchivedRows") || value.equals("readAggregateArchivedScope")
                || value.equals("readArchivedTransition"));
        return value;
    }

    public static void keys(Set<String> actual, String... expected) {
        CopyInputGuard.require(actual.equals(new HashSet<>(Arrays.asList(expected))));
    }

    public static void outputKeys(String section, Set<String> actual) {
        String allowed;
        switch (section) {
            case "result": allowed = "protocol entry certification status phase selection drives files legacy sync issues allInputShaUnchanged"; break;
            case "selection": allowed = "rows knownAccount onlyWifiOffline"; break;
            case "drives": allowed = "account registryIndex inputIndex shared maintenance selected"; break;
            case "files": allowed = "inputIndex rootCount baselineOffline marked direct claimed attention covered orphanCandidates foreignDetails ancestryDetails"; break;
            case "foreignDetails": allowed = "role type typePresent idIsDefault0 idIsRoot1 driveIsDefault0 driveIsKnownOtherRegistry driveScope parentIsNull parentIsDefault0 parentIsRoot1 selfParent isOffline isMarkedAsOffline isDirectlyPinned offlineOwnedByRoot offlineNeedsAttention hasAnyOfflineIntent baselinePredicate baselinePredicateKnown passStartClaimPredicate count"; break;
            case "ancestryDetails": allowed = "channel end depth cycleLength cycleStartOrdinal startKnown startType startIdIsDefault0 startIdIsRoot1 startIsRegisteredRoot startOffline startMarked startDirect startOwned startAttention registeredRootEncountered cycleContainsRegisteredRoot sameDriveConsistent sameDriveCheckComplete terminalKnown terminalIdIsDefault0 terminalIdIsRoot1 terminalParentIsNull terminalParentIsDefault0 terminalParentIsRoot1 terminalSelfParent count"; break;
            case "legacy": allowed = "archivedPrefixes legacyAllowed rootCovered unclassifiedDeletionCandidates"; break;
            case "sync": allowed = "uploads pendingGlobal uploadedSyncDeletionCandidates settings appSyncRowExists mediaFolders syncedMediaFolders pairings"; break;
            case "uploads": allowed = "account registryIndex inputIndex knownDrive type uploaded deleted count"; break;
            case "settings": allowed = "account registryIndex inputIndex knownDrive interval activationEpoch syncImmediately syncVideo deleteAfterSync onlyWifiSyncMedia photoTrashSyncEnabled"; break;
            case "pairings": allowed = "account registryIndex inputIndex knownDrive state pendingConsent epochMatches paired baselineUnknown localDigestPresent remoteHashPresent count"; break;
            default: throw new IllegalStateException("COPY_OUTPUT_REJECTED");
        }
        CopyInputGuard.require(new HashSet<>(Arrays.asList(allowed.split(" "))).containsAll(actual));
    }

    public static void input(String role, String staged, String original, long schema, long user, long drive) {
        switch (role) {
            case "APP": fixed(staged, original, schema, user, drive, "app.realm", "AppSettings.realm", 3); break;
            case "DRIVES": fixed(staged, original, schema, user, drive, "drives.realm", "DrivesInfos.realm", 4); break;
            case "SYNC": fixed(staged, original, schema, user, drive, "sync.realm", "Sync.realm", 13); break;
            case "FILES":
                CopyInputGuard.require(staged.matches("drive-([0-9]|1[0-5])\\.realm") && schema == 20);
                CopyInputGuard.scope(user, drive, original);
                break;
            default: CopyInputGuard.require(false);
        }
    }

    private static void fixed(String staged, String original, long schema, long user, long drive,
                              String name, String source, long version) {
        CopyInputGuard.require(staged.equals(name) && original.equals(source) && schema == version && user == 0 && drive == 0);
    }

    public enum Coverage { COVERED, OUTSIDE, MISSING_PARENT, CYCLE, DEPTH_LIMIT }

    public static Coverage coverage(Long parent, Map<Long, Long> parents, Set<Long> roots) {
        Set<Long> visited = new HashSet<>();
        while (parent != null) {
            if (visited.size() >= 256) return Coverage.DEPTH_LIMIT;
            if (!visited.add(parent)) return Coverage.CYCLE;
            if (roots.contains(parent)) return Coverage.COVERED;
            if (!parents.containsKey(parent)) return Coverage.MISSING_PARENT;
            parent = parents.get(parent);
        }
        return Coverage.OUTSIDE;
    }

    public static boolean pending(boolean uploadAtNull, boolean deletedAtNull) {
        return uploadAtNull && deletedAtNull;
    }

    /** Already-read primitive metadata only. Raw identities never enter a returned projection. */
    public static final class FileState {
        final long id, drive;
        final Long parent;
        final String type;
        final boolean offline, marked, direct, owned, attention;
        public FileState(long id, long drive, Long parent, String type, boolean offline,
                         boolean marked, boolean direct, boolean owned, boolean attention) {
            this.id = id; this.drive = drive; this.parent = parent; this.type = type;
            this.offline = offline; this.marked = marked; this.direct = direct;
            this.owned = owned; this.attention = attention;
        }
    }

    private static String kind(FileState row) {
        return row != null && ("file".equals(row.type) || "dir".equals(row.type)) ? row.type : "UNKNOWN";
    }

    public static boolean baselineCandidate(FileState row) { return row.offline && !"dir".equals(row.type); }
    public static boolean passStartClaimCandidate(FileState row, long drive) { return row.owned && row.drive == drive; }

    public static Map<String, Object> foreignDetail(FileState row, long drive, boolean knownOther) {
        CopyInputGuard.require(row.drive != drive);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("role", "FOREIGN_DRIVE_ROW"); out.put("type", kind(row)); out.put("typePresent", row.type != null);
        out.put("idIsDefault0", row.id == 0); out.put("idIsRoot1", row.id == 1);
        out.put("driveIsDefault0", row.drive == 0); out.put("driveIsKnownOtherRegistry", knownOther);
        out.put("driveScope", row.drive == 0 ? "DEFAULT_ZERO" : knownOther ? "KNOWN_OTHER_REGISTRY" : "OUTSIDE_REGISTRY");
        out.put("parentIsNull", row.parent == null); out.put("parentIsDefault0", Long.valueOf(0).equals(row.parent));
        out.put("parentIsRoot1", Long.valueOf(1).equals(row.parent)); out.put("selfParent", Long.valueOf(row.id).equals(row.parent));
        out.put("isOffline", row.offline); out.put("isMarkedAsOffline", row.marked); out.put("isDirectlyPinned", row.direct);
        out.put("offlineOwnedByRoot", row.owned); out.put("offlineNeedsAttention", row.attention);
        out.put("hasAnyOfflineIntent", row.offline || row.marked || row.direct || row.owned);
        // Baseline has no drive filter; pass-start claims do. NULL type is explicitly uncertified.
        out.put("baselinePredicate", baselineCandidate(row)); out.put("baselinePredicateKnown", row.type != null);
        out.put("passStartClaimPredicate", passStartClaimCandidate(row, drive));
        outputKeys("foreignDetails", out.keySet());
        return out;
    }

    /** Parent-walk depth excludes the start row. ROOT_ANCESTOR never exempts a sentinel loop. */
    public static Map<String, Object> ancestryDetail(String channel, long startId, long drive,
            Map<Long, FileState> rows, Set<Long> duplicates, Set<Long> roots, Set<Long> foreignIds) {
        CopyInputGuard.require(channel.equals("ROOT_ANCESTOR") || channel.equals("ACTIVE_INTENT"));
        FileState start = duplicates.contains(startId) ? null : rows.get(startId), terminal = start;
        long terminalId = startId;
        boolean registered = false, consistent = start == null || start.drive == drive, driveComplete = start != null;
        String end; int cycleLength = 0, cycleOrdinal = -1;
        Map<Long, Integer> visited = new LinkedHashMap<>();
        Long current = start == null ? null : start.parent;
        boolean cycleRoot = false;
        if (duplicates.contains(startId)) { end = "DUPLICATE_START"; terminal = null; }
        else if (start == null) { end = "MISSING_START"; consistent = !foreignIds.contains(startId); }
        else if (channel.equals("ACTIVE_INTENT") && !duplicates.isEmpty()) { end = "DUPLICATE_MAP"; terminal = null; }
        else {
            while (true) {
                if (current == null) { end = "NULL_PARENT"; break; }
                terminalId = current;
                terminal = rows.get(current);
                if (visited.size() >= 256) { end = "DEPTH_LIMIT"; driveComplete = false; break; }
                if (visited.containsKey(current)) {
                    end = "CYCLE"; cycleOrdinal = visited.get(current); cycleLength = visited.size() - cycleOrdinal;
                    for (Map.Entry<Long, Integer> node : visited.entrySet())
                        if (node.getValue() >= cycleOrdinal && roots.contains(node.getKey())) cycleRoot = true;
                    break;
                }
                visited.put(current, visited.size());
                registered |= roots.contains(current);
                if (terminal != null && terminal.drive != drive) consistent = false;
                if (terminal == null && foreignIds.contains(current)) consistent = false;
                if (terminal == null) driveComplete = false;
                if (channel.equals("ACTIVE_INTENT") && roots.contains(current)) { end = "REGISTERED_ROOT"; break; }
                if (duplicates.contains(current)) { end = "DUPLICATE_PARENT"; terminal = null; driveComplete = false; break; }
                if (terminal == null) { end = "MISSING_PARENT"; break; }
                if (terminal.drive != drive) { consistent = false; end = "FOREIGN_PARENT"; break; }
                current = terminal.parent;
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("channel", channel); out.put("end", end); out.put("depth", visited.size());
        out.put("cycleLength", cycleLength); out.put("cycleStartOrdinal", cycleOrdinal);
        out.put("startKnown", start != null && !duplicates.contains(startId)); out.put("startType", kind(start));
        out.put("startIdIsDefault0", startId == 0); out.put("startIdIsRoot1", startId == 1);
        out.put("startIsRegisteredRoot", roots.contains(startId));
        out.put("startOffline", start != null && start.offline); out.put("startMarked", start != null && start.marked);
        out.put("startDirect", start != null && start.direct); out.put("startOwned", start != null && start.owned);
        out.put("startAttention", start != null && start.attention); out.put("registeredRootEncountered", registered);
        out.put("cycleContainsRegisteredRoot", cycleRoot); out.put("sameDriveConsistent", consistent);
        out.put("sameDriveCheckComplete", driveComplete); out.put("terminalKnown", terminal != null);
        out.put("terminalIdIsDefault0", terminalId == 0);
        out.put("terminalIdIsRoot1", terminalId == 1);
        out.put("terminalParentIsNull", terminal != null && terminal.parent == null);
        out.put("terminalParentIsDefault0", terminal != null && Long.valueOf(0).equals(terminal.parent));
        out.put("terminalParentIsRoot1", terminal != null && Long.valueOf(1).equals(terminal.parent));
        out.put("terminalSelfParent", terminal != null && Long.valueOf(terminal.id).equals(terminal.parent));
        outputKeys("ancestryDetails", out.keySet());
        return out;
    }

    public static String prefix(String path) {
        CopyInputGuard.require(path != null && !path.isEmpty() && !path.startsWith("/") && !path.endsWith("/"));
        String[] parts = path.split("/", -1);
        CopyInputGuard.require(parts.length <= 64);
        for (String part : parts) CopyInputGuard.require(!part.isEmpty() && !part.equals(".") && !part.equals("..") && part.indexOf('\0') < 0);
        return parts[0];
    }
}
