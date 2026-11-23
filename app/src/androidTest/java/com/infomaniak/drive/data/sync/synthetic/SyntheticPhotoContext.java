/* Copyright (C) 2026 Infomaniak Network SA. SPDX-License-Identifier: GPL-3.0-or-later */
package com.infomaniak.drive.data.sync.synthetic;

import android.content.ContentResolver;
import android.content.Context;
import android.content.ContextWrapper;
import android.content.Intent;
import android.content.ComponentName;
import android.content.ServiceConnection;
import android.content.SharedPreferences;
import android.content.pm.ApplicationInfo;
import android.content.res.AssetManager;
import android.content.res.Resources;
import android.database.DatabaseErrorHandler;
import android.database.sqlite.SQLiteDatabase;
import android.system.Os;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileNotFoundException;
import java.nio.file.Files;

/** Context/storage only for these fixed synthetic fixtures; no production package context. */
public class SyntheticPhotoContext extends ContextWrapper {
    private final File root, files, cache, codeCache, noBackup, databases;
    private final ApplicationInfo info;
    private final Resources resources;
    private final ClassLoader loader;
    private ContentResolver fixtureResolver;

    public SyntheticPhotoContext(Context base, File root, ApplicationInfo main, ClassLoader loader) throws Exception {
        super(base);
        this.root = root.getCanonicalFile(); this.loader = loader;
        files = directory("files"); cache = directory("cache"); codeCache = directory("code_cache"); noBackup = directory("no_backup");
        databases = directory("databases");
        info = new ApplicationInfo(base.getApplicationInfo());
        info.dataDir = this.root.getPath();
        info.sourceDir = main.sourceDir; info.publicSourceDir = main.publicSourceDir;
        info.splitSourceDirs = main.splitSourceDirs; info.nativeLibraryDir = main.nativeLibraryDir;
        resources = base.getPackageManager().getResourcesForApplication(main);
    }
    private File directory(String name) throws Exception {
        File file = new File(root, name);
        SyntheticPhotoContract.require(file.mkdir()); Os.chmod(file.getPath(), 0700); return file;
    }
    private File named(File parent, String name) {
        SyntheticPhotoContract.require(name != null && name.matches("[A-Za-z0-9_.-]+") && !name.equals(".") && !name.equals(".."));
        File file = new File(parent, name);
        SyntheticPhotoContract.require(!Files.isSymbolicLink(file.toPath()));
        return file;
    }
    @Override public Context getApplicationContext() { return this; }
    @Override public ApplicationInfo getApplicationInfo() { return new ApplicationInfo(info); }
    @Override public ClassLoader getClassLoader() { return loader; }
    @Override public Resources getResources() { return resources; }
    @Override public AssetManager getAssets() { return resources.getAssets(); }
    @Override public File getDataDir() { return root; }
    @Override public File getFilesDir() { return files; }
    @Override public File getCacheDir() { return cache; }
    @Override public File getCodeCacheDir() { return codeCache; }
    @Override public File getNoBackupFilesDir() { return noBackup; }
    @Override public File getDir(String name, int mode) {
        SyntheticPhotoContract.require(mode == MODE_PRIVATE);
        File dir = named(root, "app_" + name);
        try { if (!dir.exists()) { SyntheticPhotoContract.require(dir.mkdir()); Os.chmod(dir.getPath(), 0700); } }
        catch (Exception e) { throw rejected(); }
        SyntheticPhotoContract.require(dir.isDirectory()); return dir;
    }
    @Override public File getFileStreamPath(String name) { return named(files, name); }
    @Override public FileInputStream openFileInput(String name) throws FileNotFoundException { return new FileInputStream(named(files, name)); }
    @Override public FileOutputStream openFileOutput(String name, int mode) throws FileNotFoundException {
        SyntheticPhotoContract.require(mode == MODE_PRIVATE || mode == MODE_APPEND);
        File file = named(files, name); FileOutputStream out = new FileOutputStream(file, mode == MODE_APPEND);
        try { Os.chmod(file.getPath(), 0600); } catch (Exception e) { try { out.close(); } catch (Exception ignored) {} throw rejected(); }
        return out;
    }
    @Override public String[] fileList() { String[] list = files.list(); return list == null ? new String[0] : list; }
    @Override public boolean deleteFile(String name) { return named(files, name).delete(); }
    // MockK invokes the spy's original getter while recording the fixture's stub.
    // Bootstrap supplies only a strict, non-routing mock; never use the base resolver.
    public void installFixtureResolver(ContentResolver resolver) {
        SyntheticPhotoContract.require(fixtureResolver == null && resolver != null);
        fixtureResolver = resolver;
    }
    @Override public ContentResolver getContentResolver() {
        if (fixtureResolver == null) throw rejected();
        return fixtureResolver;
    }
    @Override public SharedPreferences getSharedPreferences(String name, int mode) { throw rejected(); }
    @Override public File getDatabasePath(String name) {
        // CredentialManager's superclass eagerly constructs Core's EMPTY Room singleton.
        // Only that fixed database is allowed, in this new run; never a live account path.
        File file = new File(databases, "user_database");
        SyntheticPhotoContract.require("user_database".equals(name) || file.getPath().equals(name));
        SyntheticPhotoContract.require(!Files.isSymbolicLink(file.toPath()));
        return file;
    }
    @Override public SQLiteDatabase openOrCreateDatabase(String name, int mode, SQLiteDatabase.CursorFactory factory) {
        return openOrCreateDatabase(name, mode, factory, null);
    }
    @Override public SQLiteDatabase openOrCreateDatabase(String name, int mode, SQLiteDatabase.CursorFactory factory, DatabaseErrorHandler handler) {
        SyntheticPhotoContract.require((mode & (MODE_WORLD_READABLE | MODE_WORLD_WRITEABLE)) == 0);
        return SQLiteDatabase.openOrCreateDatabase(getDatabasePath(name).getPath(), factory, handler);
    }
    @Override public String[] databaseList() { String[] list = databases.list(); return list == null ? new String[0] : list; }
    @Override public boolean deleteDatabase(String name) { throw rejected(); }
    @Override public Context createPackageContext(String name, int flags) {
        SyntheticPhotoContract.require("com.infomaniak.drive.test".equals(name)); return this;
    }
    @Override public Context createDeviceProtectedStorageContext() { throw rejected(); }
    @Override public File getExternalFilesDir(String type) { throw rejected(); }
    @Override public File[] getExternalFilesDirs(String type) { throw rejected(); }
    @Override public File getExternalCacheDir() { throw rejected(); }
    @Override public File[] getExternalCacheDirs() { throw rejected(); }
    @Override public void startActivity(Intent intent) { throw rejected(); }
    @Override public ComponentName startService(Intent service) { throw rejected(); }
    @Override public ComponentName startForegroundService(Intent service) { throw rejected(); }
    @Override public boolean bindService(Intent service, ServiceConnection connection, int flags) {
        ComponentName component = service.getComponent();
        if (component != null && "com.infomaniak.drive.test".equals(component.getPackageName()) &&
            "androidx.room.MultiInstanceInvalidationService".equals(component.getClassName())) return false;
        throw rejected(); // No service is actually bound, including the synthetic Room request.
    }
    @Override public void sendBroadcast(Intent intent) { throw rejected(); }
    private static SecurityException rejected() { return new SecurityException("SYNTHETIC_CONTEXT_REJECTED"); }
}
