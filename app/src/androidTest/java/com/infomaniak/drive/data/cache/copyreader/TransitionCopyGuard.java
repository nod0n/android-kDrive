package com.infomaniak.drive.data.cache.copyreader;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/** In-memory comparison only: stored keys/values never enter the returned projection. */
public final class TransitionCopyGuard {
    private TransitionCopyGuard() {}

    public enum Scenario { FRESH_CHILD_ADDED, UNCHANGED_CHILD_CADENCE }

    /** No discriminator is the original, separately guarded absent-to-present contract. */
    public static Scenario scenario(String value) {
        if (value == null) return Scenario.FRESH_CHILD_ADDED;
        CopyInputGuard.require(value.equals(Scenario.UNCHANGED_CHILD_CADENCE.name()));
        return Scenario.UNCHANGED_CHILD_CADENCE;
    }

    public static void inputFields(Set<String> actual, boolean explicitScenario) {
        Set<String> expected = new HashSet<>(Arrays.asList("protocol", "userId", "driveId", "originalRealmName",
                "rootName", "rootRelativePath", "childId", "files", "copies"));
        if (explicitScenario) expected.add("scenario");
        fields(actual, expected);
    }

    public static boolean beforePresent(Scenario scenario, int alias) {
        CopyInputGuard.require(scenario != null && alias >= 0 && alias < 6);
        return alias < 5 || scenario == Scenario.UNCHANGED_CHILD_CADENCE;
    }

    public static void bodyPair(Scenario scenario, int alias, boolean before, boolean after, boolean equal) {
        CopyInputGuard.require(before == beforePresent(scenario, alias) && after);
        if (before) CopyInputGuard.require(equal);
    }

    public static String hashState(String value) {
        if (value != null && !value.isEmpty()) {
            boolean nonBlank = false;
            for (char c : value.toCharArray()) {
                nonBlank |= !Character.isWhitespace(c) && !Character.isSpaceChar(c);
            }
            CopyInputGuard.require(nonBlank);
        }
        return value == null ? "NULL" : value.isEmpty() ? "EMPTY" : "NONEMPTY";
    }

    public static String hashTransition(String before, String after) {
        hashState(before);
        hashState(after);
        boolean a = before != null && !before.isEmpty(), b = after != null && !after.isEmpty();
        if (a && b) return before.equals(after) ? "SAME_NONEMPTY" : "DIFFERENT_NONEMPTY";
        if (a) return "NONEMPTY_TO_ABSENT";
        if (b) return "ABSENT_TO_NONEMPTY";
        return Objects.equals(before, after) ? "SAME_ABSENT" : "ABSENT_STATE_CHANGED";
    }

    public static boolean bodyPublicationValuesEqual(List<String> before, List<String> after) {
        CopyInputGuard.require(before.size() == 11 && after.size() == 11);
        // Basis mtime/size, digest, publication path and row path; hash identity and flags are separate.
        return before.subList(0, 5).equals(after.subList(0, 5));
    }

    public static void fields(Set<String> actual, Set<String> expected) {
        CopyInputGuard.require(actual.equals(expected));
    }

    public static void persistedFields(Set<String> actual, Set<String> persisted, Set<String> computed) {
        fields(actual, persisted);
        CopyInputGuard.require(Collections.disjoint(persisted, computed));
    }

    public static void backlink(String alias, Set<String> persisted, String originSpec, String targetTable) {
        CopyInputGuard.require(!persisted.contains(alias) && ("LIST:" + targetTable).equals(originSpec));
    }

    public enum Operation {
        ASSET_CHECK, REALM_INIT, OPEN_COPY, SCHEMA_TABLES, SCHEMA_COLUMNS, SCHEMA_BACKLINKS,
        SCHEMA_PRIMARY_KEY, SCHEMA_TYPES, BIND_SCOPE, READ_ROWS, READ_VALUE, COMPARE_VALUES,
        BIND_ROOT, BIND_PARENT, BIND_FILE, PROJECT
    }

    public static Map<String, Object> location(String role, String table, Operation operation, Set<String> tables) {
        CopyInputGuard.require(role.equals("NONE") || role.equals("APP") || role.equals("DRIVES") || role.equals("FILES"));
        CopyInputGuard.require(table.equals("NONE") || tables.contains(table));
        CopyInputGuard.require(!role.equals("NONE") || table.equals("NONE"));
        CopyInputGuard.require(operation != null);
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("role", role);
        out.put("table", table);
        out.put("operation", operation.name());
        return out;
    }

    public static String stringState(String value, String expected) {
        return value == null ? "NULL" : value.isEmpty() ? "EMPTY" : value.equals(expected) ? "MATCH" : "OTHER";
    }

    public static boolean rootLifetimeEqual(List<Object> before, List<Object> after) {
        CopyInputGuard.require(before.size() == 5 && after.size() == 5);
        return before.equals(after);
    }

    private static Map<Object, Integer> bag(List<?> values) {
        Map<Object, Integer> out = new HashMap<>();
        for (Object value : values) out.put(value, out.getOrDefault(value, 0) + 1);
        return out;
    }

    private static int unmatched(List<?> left, List<?> right) {
        Map<Object, Integer> other = bag(right);
        int count = 0;
        for (Map.Entry<Object, Integer> entry : bag(left).entrySet()) {
            count += Math.max(0, entry.getValue() - other.getOrDefault(entry.getKey(), 0));
        }
        return count;
    }

    public static Map<String, Object> delta(List<Map<String, String>> before, List<Map<String, String>> after,
                                           String primary, Set<String> expected) {
        CopyInputGuard.require(before.size() <= 5000 && after.size() <= 5000);
        for (Map<String, String> row : before) fields(row.keySet(), expected);
        for (Map<String, String> row : after) fields(row.keySet(), expected);
        String mode = primary != null ? "PRIMARY_KEY" : before.size() <= 1 && after.size() <= 1 ? "SINGLETON" : "BAG";
        Map<String, Integer> changes = new LinkedHashMap<>();
        int added, removed, changed;
        if (mode.equals("BAG")) {
            added = unmatched(after, before);
            removed = unmatched(before, after);
            changed = -1; // No invented identity for non-primary-key rows.
            for (String field : expected) {
                List<String> a = new ArrayList<>(), b = new ArrayList<>();
                for (Map<String, String> row : before) a.add(row.get(field));
                for (Map<String, String> row : after) b.add(row.get(field));
                int count = Math.max(unmatched(a, b), unmatched(b, a));
                if (count > 0) changes.put(field, count);
            }
        } else {
            Map<String, Map<String, String>> a = keyed(before, primary), b = keyed(after, primary);
            added = (int) b.keySet().stream().filter(k -> !a.containsKey(k)).count();
            removed = (int) a.keySet().stream().filter(k -> !b.containsKey(k)).count();
            changed = 0;
            for (String key : a.keySet()) {
                if (!b.containsKey(key)) continue;
                boolean different = false;
                for (String field : expected) {
                    if (!Objects.equals(a.get(key).get(field), b.get(key).get(field))) {
                        changes.put(field, changes.getOrDefault(field, 0) + 1);
                        different = true;
                    }
                }
                if (different) changed++;
            }
        }
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("mode", mode);
        out.put("beforeRows", before.size());
        out.put("afterRows", after.size());
        out.put("addedOrUnmatched", added);
        out.put("removedOrUnmatched", removed);
        out.put("matchedChangedRows", changed);
        out.put("changedFields", changes);
        out.put("allLogicalValuesEqual", added == 0 && removed == 0 && (changed == 0 || mode.equals("BAG")));
        return out;
    }

    private static Map<String, Map<String, String>> keyed(List<Map<String, String>> rows, String primary) {
        Map<String, Map<String, String>> out = new LinkedHashMap<>();
        for (Map<String, String> row : rows) {
            String key = primary == null ? "singleton" : row.get(primary);
            CopyInputGuard.require(key != null && !key.equals("null") && out.put(key, row) == null);
        }
        return out;
    }
}
