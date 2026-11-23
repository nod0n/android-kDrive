package com.infomaniak.drive.data.cache.copyreader;

import java.util.*;

/** Test-only scalar contract. Expected identities remain private and never enter output. */
public final class SyncScopeGuard {
    private SyncScopeGuard() {}
    public static final String ENTRY = "readSyncScopeProjection";
    public static final String UNKNOWN = "UNKNOWN";
    public static void input(String role, String file, String original, long schema) {
        CopyInputGuard.require("SYNC".equals(role) && "sync.realm".equals(file)
                && "Sync.realm".equals(original) && schema == 13);
    }
    public static void field(String actual, String expected, boolean nullable, boolean expectedNullable) {
        CopyInputGuard.require(expected.equals(actual) && nullable == expectedNullable);
    }
    public static long pending(Iterable<boolean[]> rows) {
        long count = 0;
        for (boolean[] row : rows) {
            CopyInputGuard.require(row.length == 2);
            if (AggregateCopyGuard.pending(row[0], row[1])) count++;
        }
        return count;
    }
    public static final class Scope {
        final long user, drive, destination;
        public Scope(long user, long drive, long destination) {
            CopyInputGuard.require(user > 0 && user <= Integer.MAX_VALUE && drive > 0
                    && drive <= Integer.MAX_VALUE && destination > 0 && destination <= Integer.MAX_VALUE);
            this.user = user; this.drive = drive; this.destination = destination;
        }
        boolean same(Scope other) {
            return user == other.user && drive == other.drive && destination == other.destination;
        }
    }
    public static final class Setting {
        final Scope scope;
        final boolean immediately, video, deleteAfter, trash;
        public Setting(Scope scope, boolean immediately, boolean video, boolean deleteAfter, boolean trash) {
            this.scope = scope; this.immediately = immediately; this.video = video;
            this.deleteAfter = deleteAfter; this.trash = trash;
        }
    }
    public static final class Folder {
        final long id; final String path;
        public Folder(long id, String path) {
            CopyInputGuard.require(path != null && !path.isEmpty() && path.length() <= 4096 && path.indexOf('\0') < 0);
            this.id = id; this.path = path;
        }
        @Override public boolean equals(Object value) {
            if (!(value instanceof Folder)) return false;
            Folder other = (Folder) value;
            return id == other.id && path.equals(other.path);
        }
        @Override public int hashCode() { return Objects.hash(id, path); }
    }
    public static Map<String, Object> project(long pending, List<Setting> settings, Scope expected,
                                             List<Folder> selected, List<Folder> expectedFolders) {
        CopyInputGuard.require(pending >= 0);
        Setting row = settings.size() == 1 ? settings.get(0) : null;
        Map<String, Object> out = new LinkedHashMap<>();
        out.put("pendingGlobal", pending);
        out.put("settingsRows", (long) settings.size());
        out.put("expectedSyncScopeMatches", expected == null ? UNKNOWN : row != null && row.scope != null && expected.same(row.scope));
        out.put("syncImmediately", row == null ? UNKNOWN : row.immediately);
        out.put("syncVideo", row == null ? UNKNOWN : row.video);
        out.put("deleteAfterSync", row == null ? UNKNOWN : row.deleteAfter);
        out.put("photoTrashSyncEnabled", row == null ? UNKNOWN : row.trash);
        out.put("syncedMediaFolderCount", (long) selected.size());
        if (expectedFolders != null) {
            CopyInputGuard.require(!expectedFolders.isEmpty() && new HashSet<>(expectedFolders).size() == expectedFolders.size());
        }
        out.put("exactExpectedSelectedFolderSetMatches", expectedFolders == null ? UNKNOWN :
                selected.size() == expectedFolders.size() && new HashSet<>(selected).equals(new HashSet<>(expectedFolders)));
        output(out);
        return out;
    }
    public static void output(Map<String, Object> values) {
        Set<String> counts = new HashSet<>(Arrays.asList("pendingGlobal", "settingsRows", "syncedMediaFolderCount"));
        Set<String> flags = new HashSet<>(Arrays.asList("expectedSyncScopeMatches", "syncImmediately", "syncVideo",
                "deleteAfterSync", "photoTrashSyncEnabled", "exactExpectedSelectedFolderSetMatches"));
        Set<String> all = new HashSet<>(counts); all.addAll(flags);
        CopyInputGuard.require(values.keySet().equals(all));
        for (Map.Entry<String, Object> entry : values.entrySet()) {
            Object v = entry.getValue();
            CopyInputGuard.require(counts.contains(entry.getKey()) ? v instanceof Long && (Long) v >= 0
                    : v instanceof Boolean || UNKNOWN.equals(v));
        }
    }
}
