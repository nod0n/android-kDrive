package com.infomaniak.drive.data.cache.copyreader

import android.app.Application
import android.content.Context
import android.os.Bundle
import android.os.Process
import android.system.Os
import com.infomaniak.drive.data.sync.UploadMigration
import com.infomaniak.drive.utils.RealmModules
import io.realm.DynamicRealm
import io.realm.Realm
import io.realm.RealmFieldType
import org.json.JSONObject
import java.io.File
import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.StandardOpenOption

/** SYNC13 only. No account, preferences, upload payload or production database access. */
object SyncScopeProjectionReader {
    private const val asset = "sync-scope-projection-no-fallback.realm"
    private fun guard(value: Boolean) = CopyInputGuard.require(value)
    private fun keys(value: JSONObject, vararg names: String) = AggregateCopyGuard.keys(value.keys().asSequence().toSet(), *names)
    private fun text(value: JSONObject, name: String): String = value.get(name).let { guard(it is String); it as String }
    private fun number(value: JSONObject, name: String): Long = value.get(name).let {
        guard(it is Int || it is Long); (it as Number).toLong()
    }
    private fun stat(file: File, directory: Boolean = false) = Os.lstat(file.path).let {
        CopyInputGuard.stat(it.st_mode, it.st_nlink, it.st_uid, Process.myUid(), directory)
    }
    private fun scope(value: JSONObject): SyncScopeGuard.Scope {
        keys(value, "userId", "driveId", "syncFolder")
        return SyncScopeGuard.Scope(number(value, "userId"), number(value, "driveId"), number(value, "syncFolder"))
    }
    private fun field(realm: DynamicRealm, table: String, name: String, type: RealmFieldType, nullable: Boolean = false) {
        val schema = requireNotNull(realm.schema.get(table))
        guard(schema.hasField(name))
        SyncScopeGuard.field(schema.getFieldType(name).name, type.name, schema.isNullable(name), nullable)
    }

    @JvmStatic
    fun readSyncScopeProjection(context: Context, arguments: Bundle): String {
        guard(context.packageName == "com.infomaniak.drive.test" && context.applicationContext.javaClass == Application::class.java)
        guard(arguments.keySet() == setOf("copyEntry", "copyStamp", "sidecarSha", "mainSha"))
        guard(arguments.getString("copyEntry") == SyncScopeGuard.ENTRY && UploadMigration.DB_VERSION == 13L)
        val stamp = requireNotNull(arguments.getString("copyStamp")).also(CopyInputGuard::stamp)
        val root = File(context.filesDir.canonicalFile, "sync-scope-projection-$stamp")
        guard(root.isDirectory && root.canonicalFile == root.absoluteFile); stat(root, true)
        val sidecar = CopyInputGuard.child(root, "inputs.json"); stat(sidecar)
        val sidecarSha = requireNotNull(arguments.getString("sidecarSha")).also(CopyInputGuard::hash)
        guard(sidecar.length() in 1..65536 && CopyInputGuard.sha256(sidecar) == sidecarSha)
        val json = JSONObject(sidecar.readText())
        keys(json, "protocol", "expectedScope", "expectedSelectedFolders", "file")
        guard(number(json, "protocol") == 1L)
        // Explicit null preserves UNKNOWN; an absent/malformed key is rejected, never guessed.
        val expected = if (json.isNull("expectedScope")) null else scope(json.getJSONObject("expectedScope"))
        val expectedFolders = if (json.isNull("expectedSelectedFolders")) null else {
            val array = json.getJSONArray("expectedSelectedFolders"); guard(array.length() in 1..32)
            (0 until array.length()).map { index ->
                val folder = array.getJSONObject(index); keys(folder, "id", "path")
                SyncScopeGuard.Folder(number(folder, "id"), text(folder, "path"))
            }.also { guard(it.toSet().size == it.size) }
        }
        val spec = json.getJSONObject("file")
        keys(spec, "role", "file", "originalName", "schema", "sha256", "bytes")
        SyncScopeGuard.input(text(spec, "role"), text(spec, "file"), text(spec, "originalName"), number(spec, "schema"))
        val file = CopyInputGuard.child(root, text(spec, "file")); stat(file)
        val sha = text(spec, "sha256")
        CopyInputGuard.matches(file, number(spec, "bytes"), sha)
        guard(root.list()?.toSet() == setOf("inputs.json", "sync.realm"))
        try { context.assets.open(asset).use { guard(false) } } catch (_: FileNotFoundException) { /* Required missing asset. */ }
        Realm.init(context)
        val config = io.realm.RealmConfiguration.Builder().directory(root).name(file.name)
            .schemaVersion(13L).modules(RealmModules.SyncFilesModule()).assetFile(asset).readOnly().build()
        val projection = try {
            DynamicRealm.getInstance(config).use { realm ->
                guard(realm.version == 13L)
                listOf("uploadAt", "deletedAt").forEach { field(realm, "UploadFile", it, RealmFieldType.DATE, true) }
                listOf("userId", "driveId", "syncFolder").forEach { field(realm, "SyncSettings", it, RealmFieldType.INTEGER) }
                listOf("syncImmediately", "syncVideo", "deleteAfterSync", "photoTrashSyncEnabled").forEach {
                    field(realm, "SyncSettings", it, RealmFieldType.BOOLEAN)
                }
                field(realm, "MediaFolder", "id", RealmFieldType.INTEGER)
                field(realm, "MediaFolder", "path", RealmFieldType.STRING)
                field(realm, "MediaFolder", "isSynced", RealmFieldType.BOOLEAN)
                // Whole table: no user, drive or destination predicate, and no payload reads.
                val pending = SyncScopeGuard.pending(realm.where("UploadFile").findAll().map {
                    booleanArrayOf(it.isNull("uploadAt"), it.isNull("deletedAt"))
                })
                val settings = realm.where("SyncSettings").findAll().map {
                    // Invalid stored scope is a mismatch, not a rejection hiding the settings count.
                    val user = it.getLong("userId"); val drive = it.getLong("driveId"); val destination = it.getLong("syncFolder")
                    val stored = if (user in 1..Int.MAX_VALUE && drive in 1..Int.MAX_VALUE && destination in 1..Int.MAX_VALUE) {
                        SyncScopeGuard.Scope(user, drive, destination)
                    } else null
                    SyncScopeGuard.Setting(stored, it.getBoolean("syncImmediately"), it.getBoolean("syncVideo"),
                        it.getBoolean("deleteAfterSync"), it.getBoolean("photoTrashSyncEnabled"))
                }
                val selected = realm.where("MediaFolder").equalTo("isSynced", true).findAll().map {
                    SyncScopeGuard.Folder(it.getLong("id"), requireNotNull(it.getString("path")))
                }
                SyncScopeGuard.project(pending, settings, expected, selected, expectedFolders)
            }
        } finally {
            guard(CopyInputGuard.sha256(file) == sha && CopyInputGuard.sha256(sidecar) == sidecarSha)
        }
        SyncScopeGuard.output(projection)
        val output = JSONObject(projection).toString()
        guard(output.toByteArray(Charsets.UTF_8).size <= 2048)
        Files.newOutputStream(File(root, "result.json").toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE).use {
            Os.chmod(File(root, "result.json").path, 0b110000000); it.write(output.toByteArray(Charsets.UTF_8))
        }
        return output
    }
}
