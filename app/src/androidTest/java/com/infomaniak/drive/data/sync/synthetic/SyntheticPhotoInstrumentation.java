/* Copyright (C) 2026 Infomaniak Network SA. SPDX-License-Identifier: GPL-3.0-or-later */
package com.infomaniak.drive.data.sync.synthetic;

import android.app.Activity;
import android.app.Application;
import android.app.Instrumentation;
import android.content.Context;
import android.content.pm.ApplicationInfo;
import android.os.Bundle;
import android.os.Build;
import android.os.Process;
import android.system.Os;
import dalvik.system.PathClassLoader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.Map;
import java.util.zip.ZipFile;
import org.json.JSONArray;
import org.json.JSONObject;

/** Self-targeted test entry. It does not instantiate the production Application/providers. */
public final class SyntheticPhotoInstrumentation extends Instrumentation {
    private Bundle input;
    private Context fixtureContext;

    @Override public void onCreate(Bundle args) {
        super.onCreate(args);
        input = args == null ? new Bundle() : new Bundle(args);
        start();
    }
    @Override public Context getContext() { return fixtureContext == null ? super.getContext() : fixtureContext; }
    @Override public Context getTargetContext() { return fixtureContext == null ? super.getTargetContext() : fixtureContext; }

    @Override public void onStart() {
        Bundle result = new Bundle();
        String phase = "INPUT";
        File root = null;
        ClassLoader previousLoader = Thread.currentThread().getContextClassLoader();
        String previousTmp = System.getProperty("java.io.tmpdir");
        String previousDexCache = System.getProperty("dexmaker.dexcache");
        int code = Activity.RESULT_CANCELED;
        try {
            Map<String, String> args = new HashMap<>();
            for (String key : input.keySet()) {
                SyntheticPhotoContract.require(input.get(key) instanceof String);
                args.put(key, input.getString(key));
            }
            SyntheticPhotoContract.input(args);
            phase = "IDENTITY";
            Context isolated = super.getTargetContext();
            ApplicationInfo own = isolated.getApplicationInfo();
            ApplicationInfo main = isolated.getPackageManager().getApplicationInfo("com.infomaniak.drive", 0);
            SyntheticPhotoContract.require((main.splitSourceDirs == null || main.splitSourceDirs.length == 0) &&
                (own.splitSourceDirs == null || own.splitSourceDirs.length == 0));
            File ownData = isolated.getDataDir().getCanonicalFile();
            SyntheticPhotoContract.identity(isolated.getPackageName(), isolated.getApplicationContext().getClass().getName(),
                Process.myUid(), own.uid, main.uid,
                ownData.equals(new File(own.dataDir).getCanonicalFile()) && !ownData.equals(new File(main.dataDir).getCanonicalFile()),
                sha(new File(main.sourceDir)), sha(new File(own.sourceDir)), args.get("testSha"));
            SyntheticPhotoContract.require(isolated.getApplicationContext().getClass() == Application.class);
            phase = "SANDBOX";
            File files = isolated.getFilesDir().getCanonicalFile();
            SyntheticPhotoContract.require(files.getParentFile().equals(ownData) && !Files.isSymbolicLink(isolated.getFilesDir().toPath()));
            root = SyntheticPhotoContract.runDirectory(ownData, isolated.getFilesDir(), args.get("syntheticStamp"));
            Os.chmod(root.getPath(), 0700);
            String paths = main.sourceDir + File.pathSeparator + own.sourceDir;
            String libraries = libraries(main, own);
            PathClassLoader loader = new PathClassLoader(paths, libraries, ClassLoader.getSystemClassLoader());
            phase = "SANDBOX_CONTEXT_LOADER";
            Class<?> contextType = Class.forName("com.infomaniak.drive.data.sync.synthetic.SyntheticPhotoContext", true, loader);
            SyntheticPhotoContract.require(contextType.getClassLoader() == loader && Context.class.isAssignableFrom(contextType));
            phase = "SANDBOX_CONTEXT_CONSTRUCTOR";
            fixtureContext = (Context) contextType.getConstructor(Context.class, File.class, ApplicationInfo.class, ClassLoader.class)
                .newInstance(isolated, root, main, loader);
            SyntheticPhotoContract.require(fixtureContext.getClass().getClassLoader() == loader &&
                fixtureContext.getClassLoader() == loader && fixtureContext.getApplicationContext() == fixtureContext);
            // MockK's pinned JvmtiAgent uses File.createTempFile for agent/bootstrap assets.
            // This changes only this isolated process's Java property, never shell/OS TMPDIR.
            System.setProperty("java.io.tmpdir", fixtureContext.getCacheDir().getPath());
            System.setProperty("dexmaker.dexcache", fixtureContext.getCodeCacheDir().getPath());
            Thread.currentThread().setContextClassLoader(loader);
            phase = "BOOTSTRAP_AND_JUNIT";
            Class<?> bridge = Class.forName("com.infomaniak.drive.data.sync.synthetic.SyntheticPhotoSuite", true, loader);
            result = (Bundle) bridge.getMethod("run", Instrumentation.class, Context.class, Bundle.class)
                .invoke(null, this, fixtureContext, new Bundle(input));
            SyntheticPhotoContract.require(result != null && result.containsKey("syntheticResult"));
            code = result.getBoolean("syntheticCompleted") ? Activity.RESULT_OK : Activity.RESULT_CANCELED;
            phase = "RESULT";
        } catch (Throwable failure) {
            // Bootstrap errors have a closed location only. Real JUnit failures are emitted
            // by AndroidX's existing result printer, not replaced with this generic failure.
            result = new Bundle();
            result.putString("syntheticFailure", "ISOLATED_SYNTHETIC_REJECTED");
            result.putString("syntheticPhase", phase);
            result.putString("syntheticResult", failureSummary(phase, failure));
        } finally {
            Thread.currentThread().setContextClassLoader(previousLoader);
            if (previousTmp == null) System.clearProperty("java.io.tmpdir"); else System.setProperty("java.io.tmpdir", previousTmp);
            if (previousDexCache == null) System.clearProperty("dexmaker.dexcache"); else System.setProperty("dexmaker.dexcache", previousDexCache);
        }
        try {
            if (root != null) {
                File saved = new File(root, "result.json");
                SyntheticPhotoContract.require(saved.createNewFile());
                Os.chmod(saved.getPath(), 0600);
                try (FileOutputStream out = new FileOutputStream(saved)) {
                    out.write(result.getString("syntheticResult").getBytes(StandardCharsets.UTF_8));
                    out.getFD().sync();
                }
            }
        } catch (Throwable failure) {
            code = Activity.RESULT_CANCELED;
            result.putString("syntheticFailure", "RESULT_PERSISTENCE_REJECTED");
        }
        finish(code, result);
    }

    private static String failureSummary(String phase, Throwable failure) {
        try {
            JSONArray causes = new JSONArray();
            for (int i = 0; failure != null && i < 8; i++) {
                JSONArray frames = new JSONArray();
                for (StackTraceElement frame : failure.getStackTrace()) {
                    if (frame.getClassName().startsWith("com.infomaniak.drive.data.sync.synthetic.") && frames.length() < 24) {
                        frames.put(new JSONObject().put("class", frame.getClassName()).put("method", frame.getMethodName())
                            .put("line", frame.getLineNumber()));
                    }
                }
                causes.put(new JSONObject().put("class", failure.getClass().getName()).put("frames", frames));
                Throwable next = failure.getCause();
                failure = next == failure ? null : next;
            }
            return new JSONObject().put("completed", false).put("phase", phase).put("diagnostic", causes).toString();
        } catch (org.json.JSONException invalidDiagnostic) {
            return "{\"completed\":false,\"phase\":\"DIAGNOSTIC_SERIALIZATION_REJECTED\"}";
        }
    }

    private static String libraries(ApplicationInfo main, ApplicationInfo own) throws Exception {
        // extractNativeLibs=false: include verified APK ZIP directories, not only the
        // possibly empty extracted lib directories. Select this process's actual bitness.
        String[] abis = Process.is64Bit() ? Build.SUPPORTED_64_BIT_ABIS : Build.SUPPORTED_32_BIT_ABIS;
        try (ZipFile code = new ZipFile(main.sourceDir); ZipFile tests = new ZipFile(own.sourceDir)) {
            for (String abi : abis) {
                if (code.getEntry("lib/" + abi + "/librealm-jni.so") != null &&
                    tests.getEntry("lib/" + abi + "/libmockkjvmtiagent.so") != null) {
                    return main.sourceDir + "!/lib/" + abi + File.pathSeparator +
                        own.sourceDir + "!/lib/" + abi + File.pathSeparator +
                        main.nativeLibraryDir + File.pathSeparator + own.nativeLibraryDir;
                }
            }
        }
        throw new IllegalArgumentException("SYNTHETIC_NATIVE_ABI_REJECTED");
    }

    private static String sha(File file) throws Exception {
        SyntheticPhotoContract.require(file.isFile() && !Files.isSymbolicLink(file.toPath()));
        MessageDigest digest = MessageDigest.getInstance("SHA-256");
        try (FileInputStream in = new FileInputStream(file)) {
            byte[] buffer = new byte[65536];
            for (int n; (n = in.read(buffer)) != -1;) digest.update(buffer, 0, n);
        }
        StringBuilder result = new StringBuilder();
        for (byte value : digest.digest()) result.append(String.format(java.util.Locale.ROOT, "%02x", value & 255));
        return result.toString();
    }
}
