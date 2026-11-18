package com.infomaniak.drive.data.cache.copyreader

import android.app.Application
import android.content.Context
import android.os.Bundle
import android.os.Process
import android.system.Os
import com.infomaniak.drive.data.cache.copyreader.TransitionCopyGuard.Operation
import com.infomaniak.drive.data.cache.copyreader.TransitionCopyGuard.Scenario
import com.infomaniak.drive.utils.RealmModules
import io.realm.DynamicRealm
import io.realm.DynamicRealmObject
import io.realm.Realm
import io.realm.RealmConfiguration
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.StandardOpenOption

/** One fixed before/after diagnostic on six authenticated copies. No live path or account database. */
object ArchivedTransitionCopyReader {
    private const val asset = "offline-transition-reader-no-fallback.realm"
    private val aliases = listOf("PNG", "large_text", "PDF", "root_text", "deep_text", "fresh_child")
    private val roles = mapOf("APP" to 3L, "DRIVES" to 4L, "FILES" to 20L)
    private val protective = listOf(
        "offlineBasisModifiedAt", "offlineBasisSize", "offlineContentDigest", "offlinePublishedPath", "path",
        "serverContentHash", "isOffline", "isMarkedAsOffline", "isDirectlyPinned", "offlineOwnedByRoot", "offlineNeedsAttention",
    )
    private fun guard(value: Boolean) = CopyInputGuard.require(value)
    private fun text(j: JSONObject, k: String): String = j.get(k).let { guard(it is String); it as String }
    private fun number(j: JSONObject, k: String): Long = j.get(k).let {
        guard(it is Int || it is Long); (it as Number).toLong()
    }
    private fun keys(j: JSONObject, vararg names: String) = AggregateCopyGuard.keys(j.keys().asSequence().toSet(), *names)
    private fun stat(file: File, directory: Boolean = false) = Os.lstat(file.path).let {
        CopyInputGuard.stat(it.st_mode, it.st_nlink, it.st_uid, Process.myUid(), directory)
    }
    private data class Copy(val role: String, val stage: String, val file: File, val sha: String)
    private data class Body(val present: Boolean, val size: Long?, val mtime: Long?, val sha: String?)
    private data class KnownFile(
        val alias: String, val name: String, val parents: List<String>, val relative: String,
        val published: List<String>, val bodies: Map<String, Body>,
    )
    private data class Cohort(
        val root: List<Any>, val observations: List<JSONObject>, val values: List<List<String>?>,
        val identities: List<List<Any>?>,
        val hashes: List<String?>,
    )

    private class Location {
        var current = TransitionCopyGuard.location("NONE", "NONE", Operation.ASSET_CHECK, emptySet())
            private set

        fun at(role: String, table: String, operation: Operation) {
            current = TransitionCopyGuard.location(role, table, operation, TransitionSchema.modules[role]?.keys.orEmpty())
        }
    }

    private fun body(json: JSONObject, present: Boolean): Body {
        keys(json, "present", "size", "mtimeSec", "sha256")
        guard(json.get("present") is Boolean && json.getBoolean("present") == present)
        if (!present) {
            guard(listOf("size", "mtimeSec", "sha256").all(json::isNull))
            return Body(false, null, null, null)
        }
        val size = number(json, "size"); val time = number(json, "mtimeSec"); val sha = text(json, "sha256")
        guard(size in 1..262144 && time > 0); CopyInputGuard.hash(sha)
        return Body(true, size, time, sha)
    }

    private fun knownFiles(json: JSONObject, user: Long, drive: Long, scenario: Scenario): List<KnownFile> {
        val root = text(json, "rootName"); guard(root.matches(Regex("T0079-offline-[0-9]{8}")))
        val relativeRoot = text(json, "rootRelativePath")
        fun parts(value: String): List<String> = value.split('/').also { names ->
            guard(names.size in 1..8 && names.all { it.matches(Regex("[A-Za-z0-9_. -]{1,160}")) && it !in setOf(".", "..") })
        }
        guard(parts(relativeRoot).last() == root)
        val files = json.getJSONArray("files"); guard(files.length() == 6)
        return aliases.mapIndexed { i, alias ->
            val row = files.getJSONObject(i)
            keys(row, "alias", "name", "parents", "relativePath", "publishedPaths", "before", "after")
            guard(text(row, "alias") == alias)
            val name = text(row, "name"); guard(parts(name).size == 1)
            guard(if (alias == "fresh_child") name.matches(Regex("T0006-fresh-[0-9]{8}-[0-9a-f]{12}\\.txt")) else name.startsWith("T0079-"))
            val parentArray = row.getJSONArray("parents")
            val parents = (0 until parentArray.length()).map { parentArray.getString(it).also { guard(parts(it).size == 1) } }
            guard(parents.size == if (alias in setOf("deep_text", "fresh_child")) 3 else 1)
            guard(parents.first() == root && parents.distinct().size == parents.size)
            val relative = text(row, "relativePath")
            guard(relative == (listOf(relativeRoot) + parents.drop(1) + name).joinToString("/"))
            val paths = row.getJSONArray("publishedPaths"); guard(paths.length() in 1..2)
            val published = (0 until paths.length()).map { paths.getString(it) }
            published.forEach { path ->
                guard(path.startsWith("/storage/emulated/0/Android/media/com.infomaniak.drive/") || path.startsWith("/sdcard/Android/media/com.infomaniak.drive/"))
                guard(path.endsWith("/$user/$drive/$relative") && path.split('/').none { it in setOf(".", "..") })
            }
            KnownFile(alias, name, parents, relative, published, mapOf(
                "before" to body(row.getJSONObject("before"), TransitionCopyGuard.beforePresent(scenario, i)),
                "after" to body(row.getJSONObject("after"), true),
            ))
        }.also { files ->
            guard(files.map(KnownFile::name).distinct().size == 6)
            files.forEachIndexed { i, file ->
                val before = file.bodies.getValue("before"); val after = file.bodies.getValue("after")
                TransitionCopyGuard.bodyPair(scenario, i, before.present, after.present, before == after)
            }
            guard(files.last().bodies.getValue("after").size == 61L)
        }
    }

    @JvmStatic
    fun readArchivedTransition(context: Context, arguments: Bundle): String {
        guard(context.packageName == "com.infomaniak.drive.test" && context.applicationContext.javaClass == Application::class.java)
        val stamp = arguments.getString("copyStamp").also(CopyInputGuard::stamp)
        val sidecarSha = arguments.getString("sidecarSha").also(CopyInputGuard::hash)
        val root = File(context.filesDir.canonicalFile, "offline-transition-reader-$stamp")
        guard(root.isDirectory && root.canonicalFile == root.absoluteFile); stat(root, true)
        val sidecar = CopyInputGuard.child(root, "inputs.json"); stat(sidecar)
        guard(sidecar.length() in 1..32768 && CopyInputGuard.sha256(sidecar) == sidecarSha)
        val json = JSONObject(sidecar.readText())
        TransitionCopyGuard.inputFields(json.keys().asSequence().toSet(), json.has("scenario"))
        val scenario = TransitionCopyGuard.scenario(if (json.has("scenario")) text(json, "scenario") else null)
        guard(number(json, "protocol") == 1L)
        val user = number(json, "userId"); val drive = number(json, "driveId")
        CopyInputGuard.scope(user, drive, text(json, "originalRealmName")); guard(number(json, "childId") > 0)
        val known = knownFiles(json, user, drive, scenario)
        val specs = json.getJSONArray("copies"); guard(specs.length() == 6)
        val checked = mutableListOf(sidecar to requireNotNull(sidecarSha))
        val copies = (0 until 6).map { i ->
            val spec = specs.getJSONObject(i); keys(spec, "role", "stage", "file", "schema", "bytes", "sha256")
            val role = text(spec, "role"); val stage = text(spec, "stage")
            guard(stage in setOf("before", "after") && number(spec, "schema") == roles[role])
            val suffix = when (role) { "APP" -> "app.realm"; "DRIVES" -> "drives.realm"; "FILES" -> "drive-0.realm"; else -> error("COPY_INPUT_REJECTED") }
            guard(text(spec, "file") == "$stage-$suffix")
            val file = CopyInputGuard.child(root, text(spec, "file")); stat(file)
            val sha = text(spec, "sha256"); CopyInputGuard.matches(file, number(spec, "bytes"), sha); checked += file to sha
            Copy(role, stage, file, sha)
        }
        guard(copies.map { it.role to it.stage }.toSet() == roles.keys.flatMap { listOf(it to "before", it to "after") }.toSet())
        guard(root.list()?.toSet() == copies.map { it.file.name }.toSet() + "inputs.json")
        var phase = "SCHEMA"; var failed = false
        val location = Location()
        val result = JSONObject().put("protocol", 1).put("entry", "readArchivedTransition")
        if (scenario == Scenario.UNCHANGED_CHILD_CADENCE) result.put("scenario", scenario.name)
        try {
            try { context.assets.open(asset).use { guard(false) } } catch (_: FileNotFoundException) { /* No asset fallback. */ }
            location.at("NONE", "NONE", Operation.REALM_INIT)
            Realm.init(context)
            fun open(copy: Copy): DynamicRealm {
                location.at(copy.role, "NONE", Operation.OPEN_COPY)
                val module = when (copy.role) {
                    "APP" -> RealmModules.AppSettingsModule()
                    "DRIVES" -> RealmModules.DriveFilesModule()
                    else -> RealmModules.LocalFilesModule()
                }
                val configuration = RealmConfiguration.Builder().directory(root).name(copy.file.name)
                    .schemaVersion(roles.getValue(copy.role)).modules(module).assetFile(asset).readOnly().build()
                val realm = DynamicRealm.getInstance(configuration)
                if (realm.version != roles[copy.role]) { realm.close(); guard(false) }
                return realm
            }
            val moduleResults = JSONArray()
            roles.keys.forEach { role ->
                open(copies.single { it.role == role && it.stage == "before" }).use { before ->
                    open(copies.single { it.role == role && it.stage == "after" }).use { after ->
                        validateSchema(before, role, location); validateSchema(after, role, location)
                        phase = "QUERIES"
                        if (role == "APP") listOf(before, after).forEach { realm ->
                            location.at(role, "AppSettings", Operation.BIND_SCOPE)
                            val rows = realm.where("AppSettings").findAll(); guard(rows.size == 1)
                            guard(rows.single()!!.getLong("_currentUserId") == user && rows.single()!!.getLong("_currentDriveId") == drive)
                        }
                        if (role == "DRIVES") listOf(before, after).forEach { realm ->
                            location.at(role, "Drive", Operation.BIND_SCOPE)
                            val rows = realm.where("Drive").findAll(); guard(rows.size == 1)
                            guard(rows.single()!!.getLong("userId") == user && rows.single()!!.getLong("id") == drive)
                        }
                        val tables = JSONArray()
                        TransitionSchema.modules.getValue(role).forEach { (table, rule) ->
                            val a = tableRows(before, role, table, location); val b = tableRows(after, role, table, location)
                            location.at(role, table, Operation.COMPARE_VALUES)
                            tables.put(JSONObject(TransitionCopyGuard.delta(a, b, rule.primaryKey, rule.fields.keys)).put("table", table))
                        }
                        moduleResults.put(JSONObject().put("role", role).put("tables", tables))
                        if (role == "FILES") {
                            val a = cohort(before, known, json, "before", user, drive, scenario, location)
                            val b = cohort(after, known, json, "after", user, drive, scenario, location)
                            location.at(role, "NONE", Operation.PROJECT)
                            result.put("root", JSONObject().put("identityEqual", a.root.take(4) == b.root.take(4))
                                .put("markedAtEqual", a.root.last() == b.root.last())
                                .put("lifetimeEqual", TransitionCopyGuard.rootLifetimeEqual(a.root, b.root)))
                            result.put("files", JSONArray(aliases.mapIndexed { i, alias ->
                                val comparable = TransitionCopyGuard.beforePresent(scenario, i)
                                JSONObject().put("alias", alias).put("before", a.observations[i]).put("after", b.observations[i])
                                    .put("rowIdentityComparable", comparable).put("rowIdentityEqual", comparable && a.identities[i] == b.identities[i])
                                    .put("protectiveValuesComparable", comparable).put("protectiveValuesEqual", comparable && a.values[i] == b.values[i])
                                    .also { out ->
                                        if (scenario == Scenario.UNCHANGED_CHILD_CADENCE) {
                                            out.put("serverContentHashEqual", a.hashes[i] == b.hashes[i])
                                                .put("serverContentHashTransition", TransitionCopyGuard.hashTransition(a.hashes[i], b.hashes[i]))
                                                .put("bodyPublicationValuesEqual", TransitionCopyGuard.bodyPublicationValuesEqual(
                                                    requireNotNull(a.values[i]), requireNotNull(b.values[i]),
                                                ))
                                        }
                                    }
                            }))
                        }
                    }
                }
            }
            result.put("modules", moduleResults).put("status", "READ_COMPLETE").put("phase", "COMPLETE")
        } catch (_: Throwable) {
            failed = true; result.put("status", "REJECTED").put("phase", phase)
                .put("failureLocation", JSONObject(location.current))
        } finally {
            val unchanged = checked.all { (file, sha) -> CopyInputGuard.sha256(file) == sha }
            result.put("allInputShaUnchanged", unchanged); guard(unchanged)
            validateOutput(result)
            Files.newOutputStream(File(root, "result.json").toPath(), StandardOpenOption.CREATE_NEW, StandardOpenOption.WRITE).use {
                Os.chmod(File(root, "result.json").path, 0b110000000); it.write(result.toString().toByteArray(Charsets.UTF_8))
            }
        }
        guard(!failed)
        return result.toString()
    }

    private fun validateSchema(realm: DynamicRealm, role: String, location: Location) {
        val tables = TransitionSchema.modules.getValue(role)
        location.at(role, "NONE", Operation.SCHEMA_TABLES)
        guard(realm.schema.all.map { it.className }.toSet() == tables.keys)
        tables.forEach { (name, rule) ->
            location.at(role, name, Operation.SCHEMA_COLUMNS)
            val schema = requireNotNull(realm.schema.get(name))
            // getFieldNames enumerates native persisted columns, not computed backlink aliases.
            TransitionCopyGuard.persistedFields(schema.fieldNames, rule.fields.keys, rule.computed.keys)
            location.at(role, name, Operation.SCHEMA_BACKLINKS)
            rule.computed.forEach { (alias, origin) ->
                TransitionCopyGuard.backlink(alias, rule.fields.keys, tables.getValue(origin.table).fields[origin.field], name)
            }
            location.at(role, name, Operation.SCHEMA_PRIMARY_KEY)
            guard((if (schema.hasPrimaryKey()) schema.primaryKey else null) == rule.primaryKey)
            location.at(role, name, Operation.SCHEMA_TYPES)
            rule.fields.forEach { (field, type) -> guard(schema.getFieldType(field).name == type.substringBefore(':')) }
        }
    }

    private fun tableRows(realm: DynamicRealm, role: String, table: String, location: Location): List<Map<String, String>> {
        location.at(role, table, Operation.READ_ROWS)
        val rows = realm.where(table).findAll(); guard(rows.size <= 5000)
        val rule = TransitionSchema.modules.getValue(role).getValue(table)
        return rows.map { row -> rule.fields.mapValues { (field, type) -> value(row!!, role, field, type, 0, location) } }
    }

    private fun value(row: DynamicRealmObject, role: String, field: String, type: String, depth: Int, location: Location): String {
        location.at(role, row.type, Operation.READ_VALUE)
        guard(depth <= 8)
        val kind = type.substringBefore(':')
        if (kind !in setOf("LIST", "INTEGER_LIST", "STRING_LIST") && row.isNull(field)) return "null"
        fun link(child: DynamicRealmObject?): String {
            if (child == null) return "null"
            val target = type.substringAfter(':'); guard(child.type == target)
            val rule = TransitionSchema.modules.getValue(role).getValue(target)
            return rule.primaryKey?.let { key -> target + ":" + value(child, role, key, rule.fields.getValue(key), depth + 1, location) }
                ?: JSONArray(rule.fields.map { (key, spec) -> value(child, role, key, spec, depth + 1, location) }).toString()
        }
        return when (kind) {
            "INTEGER" -> "integer:" + row.getLong(field)
            "BOOLEAN" -> "boolean:" + row.getBoolean(field)
            "STRING" -> "string:" + JSONObject.quote(row.getString(field))
            "DATE" -> "date:" + row.getDate(field).time
            "OBJECT" -> link(row.getObject(field))
            "LIST" -> row.getList(field).also { guard(it.size <= 5000) }.let { JSONArray(it.map(::link)).toString() }
            "INTEGER_LIST" -> row.getList(field, Long::class.javaObjectType).also { guard(it.size <= 5000) }
                .let { JSONArray(it.map { item -> item?.let { "integer:$it" } ?: "null" }).toString() }
            "STRING_LIST" -> row.getList(field, String::class.java).also { guard(it.size <= 5000) }
                .let { JSONArray(it.map { item -> item?.let { "string:" + JSONObject.quote(it) } ?: "null" }).toString() }
            else -> error("COPY_SCHEMA_REJECTED")
        }
    }

    private fun cohort(realm: DynamicRealm, inputs: List<KnownFile>, json: JSONObject, stage: String, user: Long, drive: Long, scenario: Scenario, location: Location): Cohort {
        fun singleFolder(name: String, parent: Long? = null): DynamicRealmObject {
            location.at("FILES", "File", if (parent == null) Operation.BIND_ROOT else Operation.BIND_PARENT)
            val query = realm.where("File").equalTo("driveId", drive).equalTo("name", name).equalTo("type", "dir")
            parent?.let { query.equalTo("parentId", it) }
            return query.findAll().also { guard(it.size == 1) }.single()!!
        }
        val root = singleFolder(text(json, "rootName")); val id = root.getLong("id")
        location.at("FILES", "OfflineFolderRoot", Operation.BIND_ROOT)
        val registrations = realm.where("OfflineFolderRoot").findAll(); guard(registrations.size == 1)
        val registration = registrations.single()!!
        guard(registration.getString("uid") == "${id}_${drive}_$user" && registration.getLong("folderId") == id)
        guard(registration.getLong("driveId") == drive && registration.getLong("userId") == user && !registration.isNull("markedAt"))
        val identity = listOf(registration.getString("uid")!!, user, drive, id, registration.getDate("markedAt").time)
        val observations = mutableListOf<JSONObject>(); val values = mutableListOf<List<String>?>()
        val identities = mutableListOf<List<Any>?>()
        val hashes = mutableListOf<String?>()
        inputs.forEach { input ->
            var parent = root
            input.parents.drop(1).forEach { parent = singleFolder(it, parent.getLong("id")) }
            location.at("FILES", "File", Operation.BIND_FILE)
            val selected = realm.where("File").equalTo("driveId", drive).equalTo("parentId", parent.getLong("id"))
                .equalTo("name", input.name).equalTo("type", "file").findAll()
            val body = input.bodies.getValue(stage); guard(selected.size == if (body.present) 1 else 0)
            if (!body.present) { observations += JSONObject().put("presence", "ABSENT"); values += null; identities += null; hashes += null }
            else {
                val row = selected.single()!!
                identities += listOf(row.getLong("id"), drive, parent.getLong("id"), requireNotNull(row.getString("uid")))
                if (input.alias == "fresh_child") guard(row.getLong("id") == number(json, "childId"))
                val basis = row.getLong("offlineBasisModifiedAt")
                val size = if (row.isNull("offlineBasisSize")) null else row.getLong("offlineBasisSize")
                val digest = row.getString("offlineContentDigest"); val published = row.getString("offlinePublishedPath")
                val serverHash = row.getString("serverContentHash"); hashes += serverHash
                val out = JSONObject().put("presence", "PRESENT")
                    .put("basisModifiedState", if (basis == 0L) "ZERO" else if (basis > 0) "POSITIVE" else "NEGATIVE")
                    .put("basisSizeState", if (size == null) "NULL" else if (size == 0L) "ZERO" else if (size > 0) "POSITIVE" else "NEGATIVE")
                    .put("basisMtimeMatchesArchive", basis == body.mtime).put("basisSizeMatchesArchive", size == body.size)
                    .put("digestState", TransitionCopyGuard.stringState(digest, body.sha))
                    .put("publishedPathState", if (published in input.published) "MATCH" else TransitionCopyGuard.stringState(published, ""))
                    .put("rowPathMatchesArchive", row.getString("path")?.trim('/') == input.relative)
                if (scenario == Scenario.UNCHANGED_CHILD_CADENCE) out.put("serverContentHashState", TransitionCopyGuard.hashState(serverHash))
                protective.takeLast(5).forEach { out.put(it, row.getBoolean(it)) }
                observations += out
                val rule = TransitionSchema.modules.getValue("FILES").getValue("File")
                values += protective.map { key -> value(row, "FILES", key, rule.fields.getValue(key), 0, location) }
            }
        }
        return Cohort(identity, observations, values, identities, hashes)
    }

    private fun validateOutput(result: JSONObject) {
        val literalKeys = ("protocol entry status phase failureLocation operation allInputShaUnchanged root identityEqual markedAtEqual lifetimeEqual files alias before after " +
            "rowIdentityComparable rowIdentityEqual protectiveValuesComparable protectiveValuesEqual presence basisModifiedState basisSizeState basisMtimeMatchesArchive " +
            "basisSizeMatchesArchive digestState publishedPathState rowPathMatchesArchive isOffline isMarkedAsOffline isDirectlyPinned " +
            "offlineOwnedByRoot offlineNeedsAttention modules role tables table mode beforeRows afterRows addedOrUnmatched " +
            "removedOrUnmatched matchedChangedRows changedFields allLogicalValuesEqual scenario serverContentHashState " +
            "serverContentHashEqual serverContentHashTransition bodyPublicationValuesEqual").split(' ').toSet()
        val names = TransitionSchema.modules.values.flatMap { tables -> tables.keys + tables.values.flatMap { it.fields.keys } }.toSet()
        val literals = ("readArchivedTransition READ_COMPLETE REJECTED SCHEMA QUERIES COMPLETE APP DRIVES FILES ABSENT PRESENT " +
            "NULL EMPTY MATCH OTHER ZERO POSITIVE NEGATIVE PRIMARY_KEY SINGLETON BAG NONE NONEMPTY UNCHANGED_CHILD_CADENCE " +
            "SAME_NONEMPTY DIFFERENT_NONEMPTY NONEMPTY_TO_ABSENT ABSENT_TO_NONEMPTY SAME_ABSENT ABSENT_STATE_CHANGED").split(' ').toSet() +
            aliases + names + Operation.values().map { it.name }
        fun check(value: Any?) {
            when (value) {
                is JSONObject -> value.keys().asSequence().forEach { key -> guard(key in literalKeys || key in names); check(value.get(key)) }
                is JSONArray -> { guard(value.length() <= 256); for (i in 0 until value.length()) check(value.get(i)) }
                is String -> guard(value in literals)
                is Boolean, is Int, is Long -> Unit
                else -> guard(false)
            }
        }
        check(result)
    }
}
