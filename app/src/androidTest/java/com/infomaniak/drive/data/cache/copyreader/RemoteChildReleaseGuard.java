package com.infomaniak.drive.data.cache.copyreader;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Closed guards for the one retained pre-G1/later File20 observation, not a release certificate. */
public final class RemoteChildReleaseGuard {
    private RemoteChildReleaseGuard() {}

    public static final String SCENARIO = "REMOTE_CHILD_RELEASE";

    public static void inputFields(Set<String> actual) {
        AggregateCopyGuard.keys(actual, "protocol", "scenario", "userId", "driveId", "originalRealmName",
                "rootName", "rootRelativePath", "childId", "files", "copies", "targetId", "targetParentId", "targetName");
    }

    public static void target(long id, long parent, String name) {
        CopyInputGuard.require(id == 19 && parent == 14 && name != null
                && name.matches("T0006-G1-[0-9]{8}-[0-9a-f]{12}-renamed\\.txt"));
    }

    public static void copy(String stage, int index, String role, long schema, String name,
                            File file, long size, String sha) throws Exception {
        CopyInputGuard.require(index >= 0 && index < 2 && stage.equals(index == 0 ? "before" : "after")
                && role.equals("FILES") && schema == 20 && name.equals(stage + "-drive-0.realm"));
        CopyInputGuard.matches(file, size, sha);
    }

    public static boolean identity(long id, long drive, Long parent, String name, String uid,
                                    long expectedDrive, String expectedName) {
        return id == 19 && drive == expectedDrive && parent != null && parent == 14
                && expectedName.equals(name) && ("19_" + expectedDrive).equals(uid);
    }

    public static void baselineCount(int count) {
        // This is the pre-G1 creation cohort, not an observed claimed-before-trash row.
        CopyInputGuard.require(count == 0);
    }

    public static String numberState(long value) {
        return value == 0 ? "ZERO" : value > 0 ? "POSITIVE" : "NEGATIVE";
    }

    private static final Set<String> BOOLEAN_FIELDS = new HashSet<>(Arrays.asList(
            "isOffline", "isMarkedAsOffline", "isDirectlyPinned", "offlineOwnedByRoot", "offlineNeedsAttention",
            "rowPathMatchesArchive"));
    private static final Set<String> STATE_FIELDS = new HashSet<>(Arrays.asList(
            "basisModifiedState", "basisSizeState", "digestState", "serverContentHashState", "publishedPathState"));

    public static Map<String, Object> observation(int count, boolean identityMatches, Map<String, Object> values) {
        CopyInputGuard.require(count == 0 || count == 1);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("presence", count == 0 ? "ABSENT" : "PRESENT");
        if (count == 0) {
            CopyInputGuard.require(!identityMatches && values.isEmpty());
            return out; // No invented false/null field observations for an absent row.
        }
        CopyInputGuard.require(identityMatches);
        Set<String> fields = new HashSet<>(BOOLEAN_FIELDS); fields.addAll(STATE_FIELDS);
        CopyInputGuard.require(values.keySet().equals(fields));
        for (String field : BOOLEAN_FIELDS) CopyInputGuard.require(values.get(field) instanceof Boolean);
        for (String field : STATE_FIELDS) {
            Object value = values.get(field);
            String allowed;
            switch (field) {
                case "basisModifiedState": allowed = "ZERO POSITIVE NEGATIVE"; break;
                case "basisSizeState": allowed = "NULL ZERO POSITIVE NEGATIVE"; break;
                case "publishedPathState": allowed = "NULL EMPTY MATCH OTHER"; break;
                default: allowed = "NULL EMPTY NONEMPTY";
            }
            CopyInputGuard.require(value instanceof String && Arrays.asList(allowed.split(" ")).contains(value));
        }
        out.put("identityMatches", true);
        out.putAll(values);
        return out;
    }
}
