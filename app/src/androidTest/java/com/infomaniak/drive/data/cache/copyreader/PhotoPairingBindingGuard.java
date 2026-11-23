package com.infomaniak.drive.data.cache.copyreader;

import java.util.List;

/**
 * Pure sidecar and binding decisions shared by the reader and its host behavior
 * controls (compiled from this exact source by the host control test). No Android
 * or Realm types; never holds or returns raw identities.
 */
public final class PhotoPairingBindingGuard {
    private PhotoPairingBindingGuard() {}

    /**
     * Parsed MediaStore alias grammar: content://media/{external_primary|external}/images/media/<digits>.
     * Returns the parsed positive item id, or -1 for anything outside the exact recorded shape
     * (wrong scheme/authority, unrecorded volume, wrong collection/kind, non-numeric, zero,
     * leading zeros, query/fragment, missing or extra segments).
     */
    public static long aliasId(String value) {
        if (value == null || !value.startsWith("content://")) return -1L;
        String rest = value.substring("content://".length());
        if (rest.isEmpty() || rest.indexOf('?') >= 0 || rest.indexOf('#') >= 0) return -1L;
        String[] parts = rest.split("/", -1);
        if (parts.length != 5) return -1L;
        if (!parts[0].equals("media")) return -1L;
        if (!parts[1].equals("external_primary") && !parts[1].equals("external")) return -1L;
        if (!parts[2].equals("images") || !parts[3].equals("media")) return -1L;
        String id = parts[4];
        if (id.isEmpty() || id.length() > 18 || (id.length() > 1 && id.charAt(0) == '0')) return -1L;
        for (int index = 0; index < id.length(); index++) {
            char digit = id.charAt(index);
            if (digit < '0' || digit > '9') return -1L;
        }
        long parsed = Long.parseLong(id);
        return parsed > 0 ? parsed : -1L;
    }

    /**
     * Exact staged-role structure: exactly one SYNC, zero or one FILES, nothing else
     * (APP/DRIVES/any foreign role rejected), total 1..2. Anything else returns a
     * rejection token; the reader must reject before opening any Realm.
     */
    public static String roles(int syncCount, int filesCount, int foreignCount) {
        if (foreignCount != 0) return "FOREIGN_ROLE";
        if (syncCount != 1) return "SYNC_REQUIRED";
        if (filesCount > 1) return "FILES_DUPLICATE";
        return "OK";
    }

    /**
     * A stored row is an ESTABLISHED pairing only with scope+digest+size+identity
     * AND a captured pairedAt AND a known state. A stored row alone is never BOUND.
     */
    public static String bindingStatus(boolean scopeMatch, boolean digestEqual, boolean sizeEqual,
                                       boolean remoteIdValid, boolean pairedPresent, boolean stateKnown) {
        if (!(scopeMatch && digestEqual && sizeEqual && remoteIdValid)) return "MISMATCH";
        if (!(pairedPresent && stateKnown)) return "UNESTABLISHED";
        return "BOUND";
    }

    /** The singleton settings row must carry exactly the expected user/drive identity. */
    public static boolean settingsScope(long rowUser, long rowDrive, long expectedUser, long expectedDrive) {
        return rowUser == expectedUser && rowDrive == expectedDrive;
    }

    /** Wrong settings scope, ambiguous or missing rows, unestablished bindings or any issue: never COMPLETE. */
    public static String certification(String bindingStatus, String crossbindStatus, int issueTotal) {
        boolean crossbindOk = crossbindStatus.equals("NOT_STAGED") || crossbindStatus.equals("BOUND");
        return issueTotal == 0 && "BOUND".equals(bindingStatus) && crossbindOk ? "COMPLETE" : "UNKNOWN";
    }

    /** Read-only convenience for reader-side counting of exactly the two supported roles. */
    public static boolean supportedRole(String role) {
        return "SYNC".equals(role) || "FILES".equals(role);
    }

    /** Host/reader shared structural check over the raw role list (null elements are foreign). */
    public static String roles(List<String> roleList) {
        int sync = 0, files = 0, foreign = 0;
        for (String role : roleList) {
            if (!supportedRole(role)) foreign++;
            else if ("SYNC".equals(role)) sync++;
            else files++;
        }
        return roles(sync, files, foreign);
    }
}
