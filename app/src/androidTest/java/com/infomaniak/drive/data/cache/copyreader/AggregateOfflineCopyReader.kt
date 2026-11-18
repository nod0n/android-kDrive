package com.infomaniak.drive.data.cache.copyreader

import android.app.Application
import android.content.Context
import android.os.Bundle
import android.os.Process
import android.system.Os
import com.infomaniak.drive.data.cache.DriveMigration
import com.infomaniak.drive.data.cache.FileMigration
import com.infomaniak.drive.data.models.AppSettings.AppSettingsMigration
import com.infomaniak.drive.data.sync.UploadMigration
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

/** Fixed aggregate queries on explicitly staged copies. Never opens application databases. */
object AggregateOfflineCopyReader {
    private const val asset = "offline-aggregate-reader-no-fallback.realm"
    private const val limit = 100_000
    private fun guard(value: Boolean) = CopyInputGuard.require(value)
    private fun keys(value: JSONObject, vararg names: String) = AggregateCopyGuard.keys(value.keys().asSequence().toSet(), *names)
    private fun text(value: JSONObject, key: String): String = value.get(key).let { guard(it is String); it as String }
    private fun number(value: JSONObject, key: String): Long = value.get(key).let {
        guard(it is Int || it is Long); (it as Number).toLong()
    }
    private fun stat(file: File, directory: Boolean = false) = Os.lstat(file.path).let {
        CopyInputGuard.stat(it.st_mode, it.st_nlink, it.st_uid, Process.myUid(), directory)
    }
    private data class Input(val role: String, val file: File, val schema: Long, val sha: String, val user: Long, val drive: Long)
    private enum class Issue {
        SELECTION, DRIVE_REGISTRY, MISSING_DRIVE_COPY, FOREIGN_ROOT, MISSING_ROOT_ROW, DUPLICATE_ROOT,
        FOREIGN_FILE, DUPLICATE_FILE, MISSING_PARENT, CYCLE, ANCESTRY_LIMIT, UNKNOWN_TYPE, UPLOAD_SCOPE,
        SETTINGS_SCOPE, AMBIGUOUS_SETTINGS, PAIRING_SCOPE, LEGACY_SCOPE, LEGACY_UNCLASSIFIED,
    }
    private class Queries(private val accounts: Set<Long>, private val inputs: List<Input>, private val sidecar: JSONObject) {
        val issues = Issue.entries.associateWith { 0 }.toMutableMap()
        private fun issue(value: Issue, count: Int = 1) { issues[value] = issues.getValue(value) + count }
        var user = -1L
        private var drive = -1L
        private data class Drive(val user: Long, val id: Long, val shared: Boolean)
        private var registry = emptyList<Drive>()
        private var epoch: Long? = null
        private fun userClass(id: Long) = when (id) { user -> "CURRENT"; in accounts -> "OTHER_ALLOWED"; else -> "UNKNOWN" }
        private fun driveIndex(u: Long, d: Long) = inputs.filter { it.role == "FILES" }.indexOfFirst { it.user == u && it.drive == d }
        private fun registryIndex(u: Long, d: Long) = registry.indexOfFirst { it.user == u && it.id == d }
        private fun known(u: Long, d: Long) = u in accounts && registry.any { it.user == u && it.id == d }
        private fun rows(realm: DynamicRealm, table: String, fields: Map<String, RealmFieldType>): List<DynamicRealmObject> {
            val schema = requireNotNull(realm.schema.get(table))
            fields.forEach { (name, type) -> guard(schema.hasField(name) && schema.getFieldType(name) == type) }
            val rows = realm.where(table).findAll()
            guard(rows.size <= limit)
            return rows.toList()
        }
        private fun fields(vararg types: Pair<RealmFieldType, List<String>>) = types.flatMap { (type, names) -> names.map { it to type } }.toMap()
        private val integer = RealmFieldType.INTEGER
        private val boolean = RealmFieldType.BOOLEAN
        private val string = RealmFieldType.STRING
        private val date = RealmFieldType.DATE

        fun selection(realm: DynamicRealm): JSONObject {
            val rows = rows(realm, "AppSettings", fields(integer to listOf("_currentUserId", "_currentDriveId"), boolean to listOf("_onlyWifiSyncOffline")))
            if (rows.size != 1) { issue(Issue.SELECTION); return JSONObject().put("rows", rows.size).put("knownAccount", false) }
            user = rows.single().getLong("_currentUserId"); drive = rows.single().getLong("_currentDriveId")
            if (user !in accounts || drive <= 0) issue(Issue.SELECTION)
            return JSONObject().put("rows", 1).put("knownAccount", user in accounts).put("onlyWifiOffline", rows.single().getBoolean("_onlyWifiSyncOffline"))
        }

        fun drives(realm: DynamicRealm): JSONArray {
            val rows = rows(realm, "Drive", fields(integer to listOf("userId", "id"), boolean to listOf("sharedWithMe", "maintenance")))
            registry = rows.map { Drive(it.getLong("userId"), it.getLong("id"), it.getBoolean("sharedWithMe")) }
            if (registry.map { it.user to it.id }.distinct().size != registry.size || registry.any { it.user !in accounts || it.id <= 0 }) issue(Issue.DRIVE_REGISTRY)
            val eligible = registry.filter { it.user == user && !it.shared }
            if (eligible.none { it.id == drive }) issue(Issue.SELECTION)
            val copies = inputs.filter { it.role == "FILES" }
            if (copies.map { it.user to it.drive }.toSet() != eligible.map { it.user to it.id }.toSet()) issue(Issue.MISSING_DRIVE_COPY)
            return JSONArray(rows.map { row ->
                val u = row.getLong("userId"); val d = row.getLong("id")
                JSONObject().put("account", userClass(u)).put("registryIndex", registryIndex(u, d)).put("inputIndex", driveIndex(u, d))
                    .put("shared", row.getBoolean("sharedWithMe")).put("maintenance", row.getBoolean("maintenance")).put("selected", u == user && d == drive)
            })
        }

        fun files(realm: DynamicRealm, input: Input): Pair<JSONObject, JSONObject?> {
            val roots = rows(realm, "OfflineFolderRoot", fields(integer to listOf("userId", "driveId", "folderId"), string to listOf("uid"), date to listOf("markedAt")))
            val rootIds = roots.map { it.getLong("folderId") }.toSet()
            val validRoots = roots.filter {
                val id = it.getLong("folderId")
                val valid = it.getLong("userId") == input.user && it.getLong("driveId") == input.drive && id > 0 &&
                    it.getString("uid") == "${id}_${input.drive}_${input.user}" && !it.isNull("markedAt")
                if (!valid) issue(Issue.FOREIGN_ROOT)
                valid
            }.map { it.getLong("folderId") }.toSet()
            if (rootIds.size != roots.size) issue(Issue.DUPLICATE_ROOT)
            val rows = rows(realm, "File", fields(integer to listOf("id", "driveId", "parentId"), string to listOf("type", "path"),
                boolean to listOf("isOffline", "isMarkedAsOffline", "isDirectlyPinned", "offlineOwnedByRoot", "offlineNeedsAttention")))
            val matching = rows.filter { it.getLong("driveId") == input.drive }
            issue(Issue.FOREIGN_FILE, rows.size - matching.size)
            val grouped = matching.groupBy { it.getLong("id") }
            val duplicate = grouped.values.any { it.size != 1 }
            if (duplicate) issue(Issue.DUPLICATE_FILE)
            val parents = grouped.filterValues { it.size == 1 }.mapValues { (_, group) -> group.single().let { if (it.isNull("parentId")) null else it.getLong("parentId") } }
            validRoots.forEach { id -> if (grouped[id]?.singleOrNull()?.getString("type") != "dir") issue(Issue.MISSING_ROOT_ROW) }
            validRoots.forEach { id ->
                when (AggregateCopyGuard.coverage(parents[id], parents, emptySet())) {
                    AggregateCopyGuard.Coverage.CYCLE -> issue(Issue.CYCLE)
                    AggregateCopyGuard.Coverage.DEPTH_LIMIT -> issue(Issue.ANCESTRY_LIMIT)
                    else -> Unit
                }
            }
            val coverage = matching.associateWith { row ->
                val parent = if (row.isNull("parentId")) null else row.getLong("parentId")
                if (duplicate) AggregateCopyGuard.Coverage.MISSING_PARENT else AggregateCopyGuard.coverage(parent, parents, validRoots)
            }
            // Only relevant intent rows require complete negative ancestry. Cached unrelated rows are not an export target.
            coverage.forEach { (row, state) ->
                if (row.getBoolean("isOffline") || row.getBoolean("isMarkedAsOffline") || row.getBoolean("isDirectlyPinned") || row.getBoolean("offlineOwnedByRoot")) {
                    if (state == AggregateCopyGuard.Coverage.MISSING_PARENT) issue(Issue.MISSING_PARENT)
                    if (state == AggregateCopyGuard.Coverage.CYCLE) issue(Issue.CYCLE)
                    if (state == AggregateCopyGuard.Coverage.DEPTH_LIMIT) issue(Issue.ANCESTRY_LIMIT)
                }
                if (row.getString("type") !in setOf("file", "dir")) issue(Issue.UNKNOWN_TYPE)
            }
            fun count(test: (DynamicRealmObject) -> Boolean) = rows.count(test)
            val counts = JSONObject().put("inputIndex", driveIndex(input.user, input.drive)).put("rootCount", roots.size)
                .put("baselineOffline", count { it.getBoolean("isOffline") && it.getString("type") != "dir" })
                .put("marked", count { it.getBoolean("isMarkedAsOffline") }).put("direct", count { it.getBoolean("isDirectlyPinned") })
                .put("claimed", count { it.getBoolean("offlineOwnedByRoot") }).put("attention", count { it.getBoolean("offlineNeedsAttention") })
                .put("covered", coverage.count { (row, state) -> row.getString("type") != "dir" && state == AggregateCopyGuard.Coverage.COVERED })
                .put("orphanCandidates", coverage.count { (row, state) -> row.getString("type") != "dir" && row.getBoolean("offlineOwnedByRoot") && !row.getBoolean("isDirectlyPinned") && state == AggregateCopyGuard.Coverage.OUTSIDE })
            // Detail is observational only: the original issue counters and certification stay unchanged.
            fun primitive(row: DynamicRealmObject) = AggregateCopyGuard.FileState(
                row.getLong("id"), row.getLong("driveId"), if (row.isNull("parentId")) null else row.getLong("parentId"), row.getString("type"),
                row.getBoolean("isOffline"), row.getBoolean("isMarkedAsOffline"), row.getBoolean("isDirectlyPinned"),
                row.getBoolean("offlineOwnedByRoot"), row.getBoolean("offlineNeedsAttention"),
            )
            val states = rows.associateWith(::primitive)
            val unique = grouped.filterValues { it.size == 1 }.mapValues { states.getValue(it.value.single()) }
            val duplicates = grouped.filterValues { it.size != 1 }.keys
            val foreign = rows.filter { it.getLong("driveId") != input.drive }
            val foreignIds = foreign.map { it.getLong("id") }.toSet()
            fun details(observations: List<Map<String, Any>>): JSONArray {
                val groups = observations.groupingBy { JSONObject(it).toString() }.eachCount()
                guard(groups.size <= 256)
                return JSONArray(groups.map { (row, count) -> JSONObject(row).put("count", count) })
            }
            counts.put("foreignDetails", details(foreign.map { row ->
                AggregateCopyGuard.foreignDetail(states.getValue(row), input.drive, known(input.user, row.getLong("driveId")))
            }))
            val ancestry = validRoots.map { id -> AggregateCopyGuard.ancestryDetail("ROOT_ANCESTOR", id, input.drive, unique, duplicates, validRoots, foreignIds) } +
                matching.filter { it.getBoolean("isOffline") || it.getBoolean("isMarkedAsOffline") || it.getBoolean("isDirectlyPinned") || it.getBoolean("offlineOwnedByRoot") }.map { row ->
                    AggregateCopyGuard.ancestryDetail("ACTIVE_INTENT", row.getLong("id"), input.drive, unique, duplicates, validRoots, foreignIds)
                }
            counts.put("ancestryDetails", details(ancestry))
            if (input.user != user || input.drive != drive) return counts to null
            val scopes = sidecar.getJSONArray("legacyScopes")
            val scope = (0 until scopes.length()).map { scopes.getJSONObject(it) }.filter { number(it, "userId") == user && number(it, "driveId") == drive }
            if (scope.size != 1) { issue(Issue.LEGACY_SCOPE); return counts to null }
            val prefixes = scope.single().getJSONArray("prefixes")
            // Match coveredSubtreePaths' folder-only BFS, including raw (not normalized) paths.
            val coveredPaths = mutableSetOf<String>()
            val queue = ArrayDeque(validRoots)
            val visited = mutableSetOf<Long>()
            val children = matching.filter { !it.isNull("parentId") }.groupBy { it.getLong("parentId") }
            while (queue.isNotEmpty()) {
                val folder = queue.removeFirst()
                if (!visited.add(folder)) continue
                children[folder].orEmpty().forEach { child ->
                    if (child.getString("type") == "dir") queue.add(child.getLong("id"))
                    else child.getString("path")?.takeIf { it.isNotBlank() }?.let(coveredPaths::add)
                }
            }
            var allowed = 0; var covered = 0; var unknown = 0
            for (i in 0 until prefixes.length()) {
                val prefix = prefixes.getString(i)
                when {
                    prefix in setOf("Private", "Common documents") -> allowed++
                    coveredPaths.any { it == prefix || it.startsWith("$prefix/") } -> covered++
                    else -> unknown++
                }
            }
            issue(Issue.LEGACY_UNCLASSIFIED, unknown)
            return counts to JSONObject().put("archivedPrefixes", prefixes.length()).put("legacyAllowed", allowed).put("rootCovered", covered).put("unclassifiedDeletionCandidates", unknown)
        }

        private fun histogram(rows: List<DynamicRealmObject>, observe: (DynamicRealmObject) -> JSONObject): JSONArray {
            val groups = rows.map(observe).groupingBy { it.toString() }.eachCount()
            guard(groups.size <= 256)
            return JSONArray(groups.map { (row, count) -> JSONObject(row).put("count", count) })
        }

        fun sync(realm: DynamicRealm): JSONObject {
            val uploads = rows(realm, "UploadFile", fields(integer to listOf("userId", "driveId"), string to listOf("type"), date to listOf("uploadAt", "deletedAt")))
            val uploadTypes = setOf("SYNC", "UPLOAD", "SHARED_FILE", "SYNC_OFFLINE", "CLOUD_STORAGE")
            val uploadSummary = histogram(uploads) { row ->
                val u = row.getLong("userId"); val d = row.getLong("driveId"); val type = row.getString("type")
                if (!known(u, d)) issue(Issue.UPLOAD_SCOPE)
                if (type !in uploadTypes) issue(Issue.UNKNOWN_TYPE)
                JSONObject().put("account", userClass(u)).put("registryIndex", registryIndex(u, d)).put("inputIndex", driveIndex(u, d)).put("knownDrive", known(u, d))
                    .put("type", if (type in uploadTypes) type else "UNKNOWN").put("uploaded", !row.isNull("uploadAt")).put("deleted", !row.isNull("deletedAt"))
            }
            val settings = rows(realm, "SyncSettings", fields(integer to listOf("userId", "driveId", "syncInterval", "photoTrashActivationEpoch"),
                boolean to listOf("syncImmediately", "syncVideo", "deleteAfterSync", "onlyWifiSyncMedia", "photoTrashSyncEnabled")))
            if (settings.size > 1) issue(Issue.AMBIGUOUS_SETTINGS)
            val settingsSummary = JSONArray(settings.map { row ->
                val u = row.getLong("userId"); val d = row.getLong("driveId")
                if (!known(u, d) || row.getLong("syncInterval") < 0 || row.getLong("photoTrashActivationEpoch") < 0) issue(Issue.SETTINGS_SCOPE)
                JSONObject().put("account", userClass(u)).put("registryIndex", registryIndex(u, d)).put("inputIndex", driveIndex(u, d)).put("knownDrive", known(u, d))
                    .put("interval", row.getLong("syncInterval")).put("activationEpoch", row.getLong("photoTrashActivationEpoch")).also { result ->
                        listOf("syncImmediately", "syncVideo", "deleteAfterSync", "onlyWifiSyncMedia", "photoTrashSyncEnabled").forEach { result.put(it, row.getBoolean(it)) }
                    }
            })
            epoch = settings.singleOrNull()?.getLong("photoTrashActivationEpoch")
            val media = rows(realm, "MediaFolder", mapOf("isSynced" to boolean))
            val pairs = rows(realm, "PhotoPairing", fields(integer to listOf("userId", "driveId", "remoteTrashPendingSince", "activationEpoch"),
                string to listOf("state", "localContentDigest", "remoteContentHash"), date to listOf("pairedAt"), boolean to listOf("baselineTrashed")))
            val states = setOf("ACTIVE", "CONFLICT_LOCAL_CHANGED", "CONFLICT_REMOTE_CHANGED", "INCONCLUSIVE", "DENIED")
            val pairSummary = histogram(pairs) { row ->
                val u = row.getLong("userId"); val d = row.getLong("driveId"); val state = row.getString("state")
                if (!known(u, d)) issue(Issue.PAIRING_SCOPE)
                if (state !in states) issue(Issue.UNKNOWN_TYPE)
                JSONObject().put("account", userClass(u)).put("registryIndex", registryIndex(u, d)).put("inputIndex", driveIndex(u, d)).put("knownDrive", known(u, d))
                    .put("state", if (state in states) state else "UNKNOWN").put("pendingConsent", row.getLong("remoteTrashPendingSince") > 0 && state != "DENIED")
                    .put("epochMatches", epoch != null && row.getLong("activationEpoch") == epoch).put("paired", !row.isNull("pairedAt"))
                    .put("baselineUnknown", row.isNull("baselineTrashed")).put("localDigestPresent", !row.isNull("localContentDigest"))
                    .put("remoteHashPresent", !row.isNull("remoteContentHash"))
            }
            return JSONObject().put("uploads", uploadSummary).put("pendingGlobal", uploads.count { AggregateCopyGuard.pending(it.isNull("uploadAt"), it.isNull("deletedAt")) })
                .put("uploadedSyncDeletionCandidates", uploads.count { it.getString("type") == "SYNC" && !it.isNull("uploadAt") && it.isNull("deletedAt") })
                .put("settings", settingsSummary).put("appSyncRowExists", settings.isNotEmpty()).put("mediaFolders", media.size)
                .put("syncedMediaFolders", media.count { it.getBoolean("isSynced") }).put("pairings", pairSummary)
        }
    }

    private fun validateOutput(result: JSONObject) {
        fun check(section: String, value: JSONObject) = AggregateCopyGuard.outputKeys(section, value.keys().asSequence().toSet())
        fun array(section: String, value: JSONArray) {
            guard(value.length() <= 256)
            for (i in 0 until value.length()) check(section, value.getJSONObject(i))
        }
        check("result", result)
        listOf("selection", "legacy").forEach { name -> result.optJSONObject(name)?.let { check(name, it) } }
        listOf("drives", "files").forEach { name -> result.optJSONArray(name)?.let { array(name, it) } }
        result.optJSONArray("files")?.let { files ->
            for (i in 0 until files.length()) {
                val file = files.getJSONObject(i)
                array("foreignDetails", file.getJSONArray("foreignDetails"))
                array("ancestryDetails", file.getJSONArray("ancestryDetails"))
            }
        }
        result.optJSONObject("sync")?.let { sync ->
            check("sync", sync)
            listOf("uploads", "settings", "pairings").forEach { name -> array(name, sync.getJSONArray(name)) }
        }
        result.optJSONObject("issues")?.let { keys(it, *Issue.entries.map(Issue::name).toTypedArray()) }
        guard(result.toString().toByteArray(Charsets.UTF_8).size <= 65536)
    }

    @JvmStatic
    fun readAggregateArchivedScope(context: Context, arguments: Bundle): String {
        guard(context.packageName == "com.infomaniak.drive.test" && context.applicationContext.javaClass == Application::class.java)
        guard(AggregateCopyGuard.entry(arguments.getString("copyEntry")) == "readAggregateArchivedScope")
        guard(AppSettingsMigration.DB_VERSION == 3L && DriveMigration.DB_VERSION == 4L && UploadMigration.DB_VERSION == 13L && FileMigration.DB_VERSION == 20L)
        val stamp = arguments.getString("copyStamp").also(CopyInputGuard::stamp)
        val sidecarSha = requireNotNull(arguments.getString("sidecarSha")).also(CopyInputGuard::hash)
        val root = File(context.filesDir.canonicalFile, "offline-aggregate-reader-$stamp")
        guard(root.isDirectory && root.canonicalFile == root.absoluteFile); stat(root, true)
        val sidecar = CopyInputGuard.child(root, "inputs.json"); stat(sidecar)
        guard(sidecar.length() in 1..65536 && CopyInputGuard.sha256(sidecar) == sidecarSha)
        val json = JSONObject(sidecar.readText())
        keys(json, "protocol", "accountIds", "files", "legacyScopes")
        guard(number(json, "protocol") == 1L)
        val ids = json.getJSONArray("accountIds")
        guard(ids.length() in 1..16)
        val accounts = (0 until ids.length()).map { index ->
            val id = ids.get(index); guard(id is Int || id is Long)
            (id as Number).toLong().also { guard(it in 1..Int.MAX_VALUE) }
        }.toSet()
        guard(accounts.size == ids.length())
        val specs = json.getJSONArray("files"); guard(specs.length() in 4..19)
        val checked = mutableListOf(sidecar to sidecarSha)
        val inputs = (0 until specs.length()).map { index ->
            val spec = specs.getJSONObject(index)
            keys(spec, "role", "file", "originalName", "schema", "sha256", "bytes", "userId", "driveId")
            val role = text(spec, "role"); val name = text(spec, "file"); val schema = number(spec, "schema")
            val user = number(spec, "userId"); val drive = number(spec, "driveId")
            AggregateCopyGuard.input(role, name, text(spec, "originalName"), schema, user, drive)
            guard(role != "FILES" || user in accounts)
            val file = CopyInputGuard.child(root, name); stat(file)
            val sha = text(spec, "sha256"); CopyInputGuard.matches(file, number(spec, "bytes"), sha)
            checked += file to sha
            Input(role, file, schema, sha, user, drive)
        }
        guard(inputs.map { it.file.name }.toSet().size == inputs.size)
        listOf("APP", "DRIVES", "SYNC").forEach { role -> guard(inputs.count { it.role == role } == 1) }
        guard(inputs.filter { it.role == "FILES" }.map { it.user to it.drive }.distinct().size == inputs.size - 3)
        guard(root.list()?.toSet() == inputs.map { it.file.name }.toSet() + "inputs.json")
        val scopes = json.getJSONArray("legacyScopes"); guard(scopes.length() in 1..16)
        guard((0 until scopes.length()).map { scopes.getJSONObject(it).let { scope -> number(scope, "userId") to number(scope, "driveId") } }.distinct().size == scopes.length())
        for (i in 0 until scopes.length()) {
            val scope = scopes.getJSONObject(i); keys(scope, "userId", "driveId", "prefixes")
            guard(inputs.any { it.role == "FILES" && it.user == number(scope, "userId") && it.drive == number(scope, "driveId") })
            val prefixes = scope.getJSONArray("prefixes"); guard(prefixes.length() <= 256)
            val names = (0 until prefixes.length()).map { index ->
                val value = prefixes.get(index); guard(value is String)
                (value as String).also { guard(AggregateCopyGuard.prefix(it) == it) }
            }
            guard(names.distinct().size == names.size)
        }
        var phase = "SCHEMA"
        val result = JSONObject().put("protocol", 1).put("entry", "readAggregateArchivedScope").put("certification", "UNKNOWN")
        var failed = false
        try {
            try { context.assets.open(asset).use { guard(false) } } catch (_: FileNotFoundException) { /* No asset fallback. */ }
            Realm.init(context)
            val queries = Queries(accounts, inputs, json)
            fun <T> read(input: Input, block: (DynamicRealm) -> T): T {
                val module = when (input.role) {
                    "APP" -> RealmModules.AppSettingsModule()
                    "DRIVES" -> RealmModules.DriveFilesModule()
                    "SYNC" -> RealmModules.SyncFilesModule()
                    else -> RealmModules.LocalFilesModule()
                }
                val config = RealmConfiguration.Builder().directory(root).name(input.file.name).schemaVersion(input.schema)
                    .modules(module).assetFile(asset).readOnly().build()
                return DynamicRealm.getInstance(config).use { realm -> guard(realm.version == input.schema); block(realm) }
            }
            phase = "QUERIES"
            result.put("selection", read(inputs.single { it.role == "APP" }, queries::selection))
            result.put("drives", read(inputs.single { it.role == "DRIVES" }, queries::drives))
            val fileResults = inputs.filter { it.role == "FILES" }.map { input -> read(input) { queries.files(it, input) } }
            result.put("files", JSONArray(fileResults.map { it.first }))
            val legacy = fileResults.mapNotNull { it.second }
            result.put("legacy", if (legacy.size == 1) legacy.single() else JSONObject())
            result.put("sync", read(inputs.single { it.role == "SYNC" }, queries::sync))
            result.put("issues", JSONObject().also { out -> queries.issues.forEach { (issue, count) -> out.put(issue.name, count) } })
            result.put("certification", if (queries.issues.values.all { it == 0 } && legacy.size == 1) "COMPLETE" else "UNKNOWN")
            result.put("status", "READ_COMPLETE").put("phase", "COMPLETE")
        } catch (_: Throwable) {
            failed = true; result.put("status", "REJECTED").put("phase", phase)
        } finally {
            val unchanged = checked.all { (file, sha) -> CopyInputGuard.sha256(file) == sha }
            result.put("allInputShaUnchanged", unchanged); guard(unchanged)
            validateOutput(result)
            val output = File(root, "result.json")
            Files.newOutputStream(output.toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE).use {
                Os.chmod(output.path, 0b110000000); it.write(result.toString().toByteArray(Charsets.UTF_8))
            }
        }
        guard(!failed)
        return result.toString()
    }
}
