/* Copyright (C) 2026 Infomaniak Network SA. SPDX-License-Identifier: GPL-3.0-or-later */
package com.infomaniak.drive.data.sync;

import android.app.Activity;
import android.app.Application;
import android.app.Instrumentation;
import android.content.Context;
import android.content.pm.ApplicationInfo;
import android.os.Build;
import android.os.Bundle;
import android.os.Process;
import dalvik.system.PathClassLoader;
import java.io.File;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashSet;
import java.util.zip.ZipFile;
import com.infomaniak.drive.data.cache.copyreader.CopyInputGuard;

/** Self-targeted fixed synthetic migration only. Never starts the main Application/providers. */
public final class UploadMigrationInstrumentation extends Instrumentation {
    private Bundle input;
    private Context fixtureContext;
    @Override public void onCreate(Bundle args) { super.onCreate(args); input = args; start(); }
    @Override public Context getTargetContext() { return fixtureContext == null ? super.getTargetContext() : fixtureContext; }
    @Override public Context getContext() { return fixtureContext == null ? super.getContext() : fixtureContext; }
    @Override public void onStart() {
        Bundle result = new Bundle();
        ClassLoader previous = Thread.currentThread().getContextClassLoader();
        try {
            CopyInputGuard.require(input != null && input.keySet().equals(new HashSet<>(Arrays.asList("migrationStamp", "mainSha", "testSha"))));
            CopyInputGuard.stamp(input.getString("migrationStamp"));
            CopyInputGuard.hash(input.getString("mainSha")); CopyInputGuard.hash(input.getString("testSha"));
            Context ownContext = super.getTargetContext();
            ApplicationInfo own = ownContext.getApplicationInfo();
            ApplicationInfo main = ownContext.getPackageManager().getApplicationInfo("com.infomaniak.drive", 0);
            CopyInputGuard.require(ownContext.getPackageName().equals("com.infomaniak.drive.test") &&
                ownContext.getApplicationContext().getClass() == Application.class && Process.myUid() == own.uid && own.uid != main.uid);
            CopyInputGuard.require((main.splitSourceDirs == null || main.splitSourceDirs.length == 0) &&
                (own.splitSourceDirs == null || own.splitSourceDirs.length == 0));
            CopyInputGuard.require(CopyInputGuard.sha256(new File(main.sourceDir)).equals(input.getString("mainSha")) &&
                CopyInputGuard.sha256(new File(own.sourceDir)).equals(input.getString("testSha")));
            File files = ownContext.getFilesDir().getCanonicalFile();
            CopyInputGuard.require(!Files.isSymbolicLink(ownContext.getFilesDir().toPath()) &&
                files.getParentFile().equals(new File(own.dataDir).getCanonicalFile()));
            File root = new File(files, "migration7-" + input.getString("migrationStamp"));
            CopyInputGuard.require(root.mkdir()); // fresh-only; never reuse an outcome
            String libraries = null;
            try (ZipFile apk = new ZipFile(main.sourceDir)) {
                for (String abi : Process.is64Bit() ? Build.SUPPORTED_64_BIT_ABIS : Build.SUPPORTED_32_BIT_ABIS) {
                    if (apk.getEntry("lib/" + abi + "/librealm-jni.so") != null) {
                        libraries = main.sourceDir + "!/lib/" + abi + File.pathSeparator + main.nativeLibraryDir;
                        break;
                    }
                }
            }
            CopyInputGuard.require(libraries != null);
            PathClassLoader loader = new PathClassLoader(main.sourceDir + File.pathSeparator + own.sourceDir,
                libraries, ClassLoader.getSystemClassLoader());
            fixtureContext = (Context) Class.forName("com.infomaniak.drive.data.sync.synthetic.SyntheticPhotoContext", true, loader)
                .getConstructor(Context.class, File.class, ApplicationInfo.class, ClassLoader.class)
                .newInstance(ownContext, root, main, loader);
            CopyInputGuard.require(fixtureContext.getApplicationContext() == fixtureContext && fixtureContext.getClassLoader() == loader);
            Thread.currentThread().setContextClassLoader(loader);
            result = (Bundle) Class.forName("com.infomaniak.drive.data.sync.UploadMigrationSuite", true, loader)
                .getMethod("run", Instrumentation.class, Context.class).invoke(null, this, fixtureContext);
            finish(result.getBoolean("migrationPassed") ? Activity.RESULT_OK : Activity.RESULT_CANCELED, result);
        } catch (Throwable rejected) {
            result.putString("migrationFailure", "ISOLATED_MIGRATION_REJECTED");
            finish(Activity.RESULT_CANCELED, result);
        } finally { Thread.currentThread().setContextClassLoader(previous); }
    }
}
