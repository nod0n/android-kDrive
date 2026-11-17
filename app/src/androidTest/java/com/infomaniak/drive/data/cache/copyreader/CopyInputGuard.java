package com.infomaniak.drive.data.cache.copyreader;

import java.io.File;
import java.io.FileInputStream;
import java.nio.file.Files;
import java.security.MessageDigest;

/** Pure input checks shared by the isolated diagnostic and its host guard checks. */
public final class CopyInputGuard {
    private CopyInputGuard() {}

    public static void require(boolean condition) {
        if (!condition) throw new IllegalStateException("COPY_INPUT_REJECTED");
    }

    public static void stamp(String value) {
        require(value != null && value.matches("[A-Za-z0-9_-]{1,64}"));
    }

    public static void hash(String value) {
        require(value != null && value.matches("[0-9a-f]{64}"));
    }

    public static void scope(long user, long drive, String originalName) {
        require(user > 0 && drive > 0 && user <= Integer.MAX_VALUE && drive <= Integer.MAX_VALUE);
        require(("kDrive-" + user + "-" + drive + ".realm").equals(originalName));
    }

    public static void stat(int mode, long links, int uid, int expectedUid, boolean directory) {
        require(uid == expectedUid);
        require((mode & 0170000) == (directory ? 0040000 : 0100000));
        require((mode & 07777) == (directory ? 0700 : 0600));
        require(directory || links == 1);
    }

    public static File child(File root, String relative) throws Exception {
        require(relative != null && relative.matches("[A-Za-z0-9_.-]+(/[A-Za-z0-9_.-]+)*"));
        File current = root;
        require(root.isDirectory() && !Files.isSymbolicLink(root.toPath()));
        for (String part : relative.split("/")) {
            require(!part.equals(".") && !part.equals(".."));
            current = new File(current, part);
            require(current.exists() && !Files.isSymbolicLink(current.toPath()));
        }
        require(current.getCanonicalPath().startsWith(root.getCanonicalPath() + File.separator));
        require(current.isFile());
        return current;
    }

    public static String sha256(File file) throws Exception {
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (FileInputStream stream = new FileInputStream(file)) {
            byte[] buffer = new byte[65536];
            int count;
            while ((count = stream.read(buffer)) != -1) digest.update(buffer, 0, count);
        }
        StringBuilder text = new StringBuilder();
        for (byte value : digest.digest()) text.append(String.format(java.util.Locale.ROOT, "%02x", value & 255));
        return text.toString();
    }

    public static void matches(File file, long size, String expected) throws Exception {
        hash(expected);
        require(size > 0 && size <= 67108864 && file.length() == size);
        require(sha256(file).equals(expected));
    }
}
