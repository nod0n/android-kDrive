package com.infomaniak.drive.data.cache.copyreader;

import android.app.Activity;
import android.app.Application;
import android.app.Instrumentation;
import android.content.Context;
import android.content.ContextWrapper;
import android.content.pm.ApplicationInfo;
import android.os.Bundle;
import dalvik.system.PathClassLoader;
import java.io.File;

/** Test-APK-only, self-targeted entry: never starts the production Application or providers. */
public final class OfflineCopyInstrumentation extends Instrumentation {
    private Bundle input;

    @Override public void onCreate(Bundle arguments) {
        super.onCreate(arguments);
        input = arguments;
        start();
    }

    @Override public void onStart() {
        Bundle outcome = new Bundle();
        try {
            final Context isolated = getTargetContext();
            CopyInputGuard.require(isolated.getPackageName().equals("com.infomaniak.drive.test"));
            CopyInputGuard.require(isolated.getApplicationContext().getClass() == Application.class);
            CopyInputGuard.stamp(input.getString("copyStamp"));
            CopyInputGuard.hash(input.getString("sidecarSha"));
            CopyInputGuard.hash(input.getString("mainSha"));
            final ApplicationInfo code = isolated.getPackageManager().getApplicationInfo("com.infomaniak.drive", 0);
            CopyInputGuard.require(CopyInputGuard.sha256(new File(code.sourceDir)).equals(input.getString("mainSha")));
            // The app APK supplies code/native libraries only. File/dir access remains under the test UID.
            final ApplicationInfo isolatedCode = new ApplicationInfo(isolated.getApplicationInfo());
            isolatedCode.sourceDir = code.sourceDir;
            isolatedCode.publicSourceDir = code.publicSourceDir;
            isolatedCode.splitSourceDirs = code.splitSourceDirs;
            isolatedCode.nativeLibraryDir = code.nativeLibraryDir;
            Context context = new ContextWrapper(isolated) {
                @Override public ApplicationInfo getApplicationInfo() { return new ApplicationInfo(isolatedCode); }
            };
            String apkPaths = isolated.getApplicationInfo().sourceDir + File.pathSeparator + code.sourceDir;
            PathClassLoader loader = new PathClassLoader(apkPaths, code.nativeLibraryDir, ClassLoader.getSystemClassLoader());
            Class<?> reader = Class.forName("com.infomaniak.drive.data.cache.copyreader.OfflineAttentionCopyReader", true, loader);
            String result = (String) reader.getMethod("readFiveArchivedRows", Context.class, Bundle.class).invoke(null, context, input);
            outcome.putString("copyReaderResult", result);
            finish(Activity.RESULT_OK, outcome);
        } catch (Throwable error) {
            // Exception messages/stack traces can contain identifying stored values or paths.
            outcome.putString("copyReaderFailure", "ISOLATED_ENTRY_REJECTED");
            finish(Activity.RESULT_CANCELED, outcome);
        }
    }
}
