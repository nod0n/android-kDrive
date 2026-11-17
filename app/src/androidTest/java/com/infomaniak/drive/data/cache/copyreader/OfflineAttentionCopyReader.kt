package com.infomaniak.drive.data.cache.copyreader

import android.app.Application
import android.content.Context
import android.os.Bundle
import android.os.Process
import android.system.Os
import com.infomaniak.drive.data.cache.FileMigration
import com.infomaniak.drive.utils.RealmModules
import io.realm.DynamicRealm
import io.realm.DynamicRealmObject
import io.realm.Realm
import io.realm.RealmConfiguration
import io.realm.RealmFieldType
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.StandardOpenOption

/** One copy-only diagnostic, loaded in the test package. No production data paths are opened. */
object OfflineAttentionCopyReader {
    private val aliases = listOf("PNG", "large_text", "PDF", "root_text", "deep_text")
    private const val absentAsset = "offline-copy-reader-no-fallback.realm"

    private fun guard(value: Boolean) = CopyInputGuard.require(value)
    private fun text(json: JSONObject, key: String): String {
        val value = json.get(key)
        guard(value is String)
        return value as String
    }

    private fun number(json: JSONObject, key: String): Long {
        val value = json.get(key)
        guard(value is Int || value is Long)
        return (value as Number).toLong()
    }

    private fun keys(json: JSONObject, expected: Set<String>) = guard(json.keys().asSequence().toSet() == expected)

    private fun parts(value: String): List<String> {
        val result = value.split('/')
        guard(result.size in 1..8 && result.all { it.matches(Regex("[A-Za-z0-9_. -]{1,160}")) && it != "." && it != ".." })
        return result
    }

    private fun stat(file: File, directory: Boolean) {
        val stat = Os.lstat(file.path)
        CopyInputGuard.stat(stat.st_mode, stat.st_nlink, stat.st_uid, Process.myUid(), directory)
    }

    private data class Input(
        val alias: String,
        val name: String,
        val parents: List<String>,
        val size: Long,
        val sha: String,
        val mtime: Long,
        val remotePath: String,
        val publishedPaths: List<String>,
    )

    private fun inputs(json: JSONObject, user: Long, drive: Long): List<Input> {
        val rootName = text(json, "rootName")
        guard(rootName.matches(Regex("T0079-offline-[0-9]{8}")))
        val rootPath = text(json, "rootRelativePath")
        guard(parts(rootPath).last() == rootName)
        val records = json.getJSONArray("files")
        guard(records.length() == aliases.size)
        return aliases.mapIndexed { index, alias ->
            val row = records.getJSONObject(index)
            keys(row, setOf("alias", "name", "parents", "size", "sha256", "mtimeSec", "relativeRemotePath", "publishedPaths"))
            guard(text(row, "alias") == alias)
            val name = text(row, "name")
            guard(parts(name).size == 1 && name.startsWith("T0079-") && name.length <= 160)
            val parentsJson = row.getJSONArray("parents")
            val parents = (0 until parentsJson.length()).map { parentsJson.getString(it).also { name -> guard(parts(name).size == 1) } }
            guard(parents.size == if (alias == "deep_text") 3 else 1)
            guard(parents.first() == rootName && parents.toSet().size == parents.size)
            val relative = text(row, "relativeRemotePath")
            guard(relative == (listOf(rootPath) + parents.drop(1) + name).joinToString("/"))
            val publishedJson = row.getJSONArray("publishedPaths")
            guard(publishedJson.length() in 1..2)
            val published = (0 until publishedJson.length()).map { publishedJson.getString(it) }
            published.forEach { path ->
                guard(path.startsWith("/storage/emulated/0/Android/media/com.infomaniak.drive/") || path.startsWith("/sdcard/Android/media/com.infomaniak.drive/"))
                guard(path.endsWith("/$user/$drive/$relative") && path.split('/').none { it == ".." || it == "." })
            }
            val size = number(row, "size")
            val mtime = number(row, "mtimeSec")
            guard(size in 1..262144 && mtime > 0)
            val sha = text(row, "sha256").also(CopyInputGuard::hash)
            Input(alias, name, parents, size, sha, mtime, relative, published)
        }.also { guard(it.map(Input::name).toSet().size == aliases.size) }
    }

    @JvmStatic
    fun readFiveArchivedRows(context: Context, arguments: Bundle): String {
        guard(context.packageName == "com.infomaniak.drive.test")
        guard(context.applicationContext.javaClass == Application::class.java)
        val stamp = arguments.getString("copyStamp").also(CopyInputGuard::stamp)
        val sidecarSha = arguments.getString("sidecarSha").also(CopyInputGuard::hash)
        val root = File(context.filesDir.canonicalFile, "offline-copy-reader-$stamp")
        guard(root.isDirectory && root.canonicalFile == root.absoluteFile)
        stat(root, directory = true)
        guard(root.list()?.toSet() == setOf("inputs.json", "before", "after"))
        val sidecar = CopyInputGuard.child(root, "inputs.json")
        stat(sidecar, directory = false)
        guard(sidecar.length() in 1..16384 && CopyInputGuard.sha256(sidecar) == sidecarSha)
        val json = JSONObject(sidecar.readText())
        keys(json, setOf("schemaVersion", "userId", "driveId", "realmOriginalName", "rootName", "rootRelativePath", "beforeRealmSha", "beforeRealmBytes", "afterRealmSha", "afterRealmBytes", "files"))
        guard(number(json, "schemaVersion") == FileMigration.DB_VERSION && FileMigration.DB_VERSION == 20L)
        val user = number(json, "userId")
        val drive = number(json, "driveId")
        CopyInputGuard.scope(user, drive, text(json, "realmOriginalName"))
        val records = inputs(json, user, drive)
        val checked = mutableListOf(sidecar to requireNotNull(sidecarSha))
        val copies = listOf("before", "after").map { stage ->
            val directory = File(root, stage)
            guard(!Files.isSymbolicLink(directory.toPath()))
            stat(directory, directory = true)
            guard(directory.list()?.toSet() == aliases.map { "$it.bytes" }.toSet() + "known-scope.realm")
            val realm = CopyInputGuard.child(root, "$stage/known-scope.realm")
            val hash = text(json, "${stage}RealmSha")
            stat(realm, directory = false)
            CopyInputGuard.matches(realm, number(json, "${stage}RealmBytes"), hash)
            checked += realm to hash
            records.forEach { record ->
                val bytes = CopyInputGuard.child(root, "$stage/${record.alias}.bytes")
                stat(bytes, directory = false)
                CopyInputGuard.matches(bytes, record.size, record.sha)
                checked += bytes to record.sha
            }
            realm
        }
        // All paths, scopes, hashes and declared schema are checked before initializing Realm.
        var phase = "realm_init"
        val result = JSONObject().put("protocol", 1).put("entry", "readFiveArchivedRows")
        var failure: Throwable? = null
        try {
            try {
                context.assets.open(absentAsset).use { guard(false) }
            } catch (_: FileNotFoundException) {
                // Required existing readOnly target; missing input can never fall back to an asset.
            }
            Realm.init(context)
            fun config(file: File) = RealmConfiguration.Builder()
                .directory(file.parentFile!!).name(file.name)
                .schemaVersion(FileMigration.DB_VERSION).modules(RealmModules.LocalFilesModule())
                .assetFile(absentAsset).readOnly().build()
            phase = "copy_schema_and_scope"
            DynamicRealm.getInstance(config(copies[0])).use { before ->
                DynamicRealm.getInstance(config(copies[1])).use { after ->
                    val bound = listOf(before, after).map { bind(it, records, text(json, "rootName"), user, drive) }
                    phase = "five_fields_only"
                    val observations = bound.map { cohort -> records.zip(cohort).map { (input, selected) -> observe(input, selected) } }
                    guard(bound[0].zip(bound[1]).all { (a, b) -> a.identity == b.identity })
                    result.put("before", JSONArray(observations[0].map { it.first }))
                    result.put("after", JSONArray(observations[1].map { it.first }))
                    result.put("five_recorded_values_equal", observations[0].map { it.second } == observations[1].map { it.second })
                }
            }
            result.put("status", "READ_COMPLETE").put("rows_per_copy", 5)
        } catch (error: Throwable) {
            failure = error
            result.put("status", "REJECTED").put("phase", phase).put("errorClass", error.javaClass.simpleName)
        } finally {
            val unchanged = checked.all { (file, expected) -> CopyInputGuard.sha256(file) == expected }
            result.put("all_input_sha_unchanged", unchanged)
            guard(unchanged)
            val output = File(root, "result.json")
            Files.newOutputStream(output.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE).use {
                Os.chmod(output.path, 0b110000000)
                it.write(result.toString().toByteArray(Charsets.UTF_8))
            }
        }
        guard(failure == null)
        return result.toString()
    }

    private data class Selected(val row: DynamicRealmObject, val identity: List<Any>, val covered: Boolean)

    private fun bind(realm: DynamicRealm, inputs: List<Input>, rootName: String, user: Long, drive: Long): List<Selected> {
        guard(realm.version == 20L)
        val fileSchema = realm.schema.get("File")!!
        val fields = mapOf(
            "id" to RealmFieldType.INTEGER, "driveId" to RealmFieldType.INTEGER, "parentId" to RealmFieldType.INTEGER,
            "name" to RealmFieldType.STRING, "type" to RealmFieldType.STRING, "path" to RealmFieldType.STRING,
            "offlineBasisModifiedAt" to RealmFieldType.INTEGER, "offlineBasisSize" to RealmFieldType.INTEGER,
            "offlineContentDigest" to RealmFieldType.STRING, "offlinePublishedPath" to RealmFieldType.STRING,
            "serverContentHash" to RealmFieldType.STRING,
        ) + listOf("offlineOwnedByRoot", "isDirectlyPinned", "isOffline", "isMarkedAsOffline", "offlineNeedsAttention").associateWith { RealmFieldType.BOOLEAN }
        fields.forEach { (name, type) -> guard(fileSchema.hasField(name) && fileSchema.getFieldType(name) == type) }
        val rootSchema = realm.schema.get("OfflineFolderRoot")!!
        mapOf("uid" to RealmFieldType.STRING, "folderId" to RealmFieldType.INTEGER, "driveId" to RealmFieldType.INTEGER, "userId" to RealmFieldType.INTEGER, "markedAt" to RealmFieldType.DATE).forEach { (name, type) ->
            guard(rootSchema.hasField(name) && rootSchema.getFieldType(name) == type)
        }
        fun single(name: String, type: String, parent: Long? = null): DynamicRealmObject {
            val query = realm.where("File").equalTo("driveId", drive).equalTo("name", name).equalTo("type", type)
            if (parent != null) query.equalTo("parentId", parent)
            val rows = query.findAll()
            guard(rows.size == 1)
            return rows.first()!!
        }
        val root = single(rootName, "dir")
        return inputs.map { input ->
            var parent = root
            val identity = mutableListOf<Any>()
            var covered = false
            input.parents.forEachIndexed { index, name ->
                if (index > 0) parent = single(name, "dir", parent.getLong("id"))
                val id = parent.getLong("id")
                guard(id > 0)
                val registrations = realm.where("OfflineFolderRoot").equalTo("uid", "${id}_${drive}_$user").findAll()
                guard(registrations.size <= 1)
                identity += id
                registrations.firstOrNull()?.let { registration ->
                    guard(registration.getLong("userId") == user && registration.getLong("driveId") == drive && registration.getLong("folderId") == id)
                    identity += registration.getDate("markedAt").time
                    covered = true
                }
                if (index == 0) guard(covered)
            }
            val row = single(input.name, "file", parent.getLong("id"))
            identity += row.getLong("id")
            Selected(row, identity, covered)
        }
    }

    private fun observe(input: Input, selected: Selected): Pair<JSONObject, List<Any?>> {
        val row = selected.row
        val modified = row.getLong("offlineBasisModifiedAt")
        val size = if (row.isNull("offlineBasisSize")) null else row.getLong("offlineBasisSize")
        val digest = row.getString("offlineContentDigest")
        val published = row.getString("offlinePublishedPath")
        val path = row.getString("path")
        val serverHash = row.getString("serverContentHash")
        val state = when {
            modified <= 0 -> "UNKNOWN"
            modified != input.mtime || size != input.size -> "LOCALLY_EDITED"
            else -> "INTACT"
        }
        fun stringState(value: String?, matches: Boolean) = when {
            value == null -> "NULL"
            value.isEmpty() -> "EMPTY"
            matches -> "MATCH"
            else -> "OTHER"
        }
        val flags = listOf("offlineOwnedByRoot", "isDirectlyPinned", "isOffline", "isMarkedAsOffline", "offlineNeedsAttention").associateWith(row::getBoolean)
        val result = JSONObject().put("alias", input.alias).put("currentLocalState", state)
            .put("basisModifiedAtState", if (modified == 0L) "ZERO" else if (modified > 0) "POSITIVE" else "NEGATIVE")
            .put("basisSizeState", when { size == null -> "NULL"; size == 0L -> "ZERO"; size > 0 -> "POSITIVE"; else -> "NEGATIVE" })
            .put("basisSizeMatchesArchive", size == input.size).put("basisMtimeMatchesArchive", modified == input.mtime)
            .put("digestState", stringState(digest, digest == input.sha))
            .put("publishedPathState", stringState(published, published in input.publishedPaths))
            .put("rowPathState", stringState(path, path?.trim('/') == input.remotePath))
            .put("serverHashState", stringState(serverHash, serverHash == input.sha))
            .put("knownRootCoverage", selected.covered)
        flags.forEach { (name, value) -> result.put(name, value) }
        // Identifying values remain in memory; only enums/booleans per alias are serialized.
        return result to (listOf(modified, size, digest, published, path, serverHash) + flags.values)
    }
}
