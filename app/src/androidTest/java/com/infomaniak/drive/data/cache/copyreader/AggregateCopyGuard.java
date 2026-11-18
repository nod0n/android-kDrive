package com.infomaniak.drive.data.cache.copyreader;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/** Fixed diagnostic contract and ancestry decisions; no Android or Realm initialization. */
public final class AggregateCopyGuard {
    private AggregateCopyGuard() {}

    public static String entry(String value) {
        if (value == null) return "readFiveArchivedRows";
        CopyInputGuard.require(value.equals("readFiveArchivedRows") || value.equals("readAggregateArchivedScope"));
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
            case "files": allowed = "inputIndex rootCount baselineOffline marked direct claimed attention covered orphanCandidates"; break;
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

    public static String prefix(String path) {
        CopyInputGuard.require(path != null && !path.isEmpty() && !path.startsWith("/") && !path.endsWith("/"));
        String[] parts = path.split("/", -1);
        CopyInputGuard.require(parts.length <= 64);
        for (String part : parts) CopyInputGuard.require(!part.isEmpty() && !part.equals(".") && !part.equals("..") && part.indexOf('\0') < 0);
        return parts[0];
    }
}
