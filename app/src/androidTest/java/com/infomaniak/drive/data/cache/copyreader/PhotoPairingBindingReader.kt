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
import io.realm.RealmFieldType
import org.json.JSONArray
import org.json.JSONObject
import java.io.File
import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.StandardOpenOption

/** Selected-photo binding projection over explicitly staged copies. Never opens application databases. */
object PhotoPairingBindingReader {
    private const val asset = "photo-pairing-binding-reader-no-fallback.realm"
    private const val limit = 100_000
    private val states = setOf("ACTIVE", "CONFLICT_LOCAL_CHANGED", "CONFLICT_REMOTE_CHANGED", "INCONCLUSIVE", "DENIED")

    private fun guard(value: Boolean) = CopyInputGuard.require(value)
    private fun keys(value: JSONObject, vararg names: String) = AggregateCopyGuard.keys(value.keys().asSequence().toSet(), *names)
    private fun text(value: JSONObject, key: String): String = value.get(key).let { guard(it is String); it as String }
    private fun number(value: JSONObject, key: String): Long = value.get(key).let {
        guard(it is Int || it is Long); (it as Number).toLong()
    }
    private fun stat(file: File, directory: Boolean = false) = Os.lstat(file.path).let {
        CopyInputGuard.stat(it.st_mode, it.st_nlink, it.st_uid, Process.myUid(), directory)
    }

    private enum class Issue {
        NOT_FOUND, AMBIGUOUS, SCOPE_MISMATCH, DIGEST_MISSING, DIGEST_MISMATCH, SIZE_MISMATCH,
        REMOTE_ID_INVALID, PAIRED_AT_MISSING, STATE_UNKNOWN, SETTINGS_SCOPE,
        CROSSBIND_NOT_FOUND, CROSSBIND_AMBIGUOUS, CROSSBIND_DRIVE_MISMATCH,
    }

    private val pairingFields = mapOf(
        "userId" to RealmFieldType.INTEGER, "driveId" to RealmFieldType.INTEGER,
        "remoteFileId" to RealmFieldType.INTEGER, "localSizeAtPairing" to RealmFieldType.INTEGER,
        "localModifiedAtPairing" to RealmFieldType.INTEGER, "activationEpoch" to RealmFieldType.INTEGER,
        "lastLocalTrashReconciledAt" to RealmFieldType.INTEGER, "remoteTrashPendingSince" to RealmFieldType.INTEGER,
        "lastRemoteTrashAppliedAt" to RealmFieldType.INTEGER, "uri" to RealmFieldType.STRING,
        "fileName" to RealmFieldType.STRING, "state" to RealmFieldType.STRING,
        "localContentDigest" to RealmFieldType.STRING, "remoteContentHash" to RealmFieldType.STRING,
        "baselineTrashed" to RealmFieldType.BOOLEAN, "lastObservedTrashed" to RealmFieldType.BOOLEAN,
        "pairedAt" to RealmFieldType.DATE,
    )
    private val settingsFields = mapOf(
        "userId" to RealmFieldType.INTEGER, "driveId" to RealmFieldType.INTEGER,
        "photoTrashActivationEpoch" to RealmFieldType.INTEGER, "photoTrashSyncEnabled" to RealmFieldType.BOOLEAN,
    )
    private val fileFields = mapOf(
        "id" to RealmFieldType.INTEGER, "driveId" to RealmFieldType.INTEGER, "name" to RealmFieldType.STRING,
    )

    private fun rows(realm: DynamicRealm, table: String, fields: Map<String, RealmFieldType>): List<DynamicRealmObject> {
        val schema = requireNotNull(realm.schema.get(table))
        fields.forEach { (name, type) -> guard(schema.hasField(name) && schema.getFieldType(name) == type) }
        val rows = realm.where(table).findAll()
        guard(rows.size <= limit)
        return rows.toList()
    }

    private fun binding(
        realm: DynamicRealm, issues: MutableMap<Issue, Int>, canonical: String, worker: String,
        expectedDigest: String, expectedSize: Long, expectedUser: Long, expectedDrive: Long,
        fileNameOut: Array<String?>,
    ): JSONObject {
        val pairs = rows(realm, "PhotoPairing", pairingFields)
        val matched = pairs.filter { it.getString("uri") == canonical || it.getString("uri") == worker }
        val out = JSONObject()
            .put("matchCount", matched.size).put("totalPairingRows", pairs.size)
            .put("matchedAlias", "NONE").put("bindingStatus", "URI_NOT_FOUND")
        if (matched.isEmpty()) { issues.merge(Issue.NOT_FOUND, 1, Int::plus); return out }
        if (matched.size > 1) {
            issues.merge(Issue.AMBIGUOUS, 1, Int::plus)
            return out.put("matchedAlias", "AMBIGUOUS").put("bindingStatus", "AMBIGUOUS")
        }
        val row = matched.single()
        val storedUri = row.getString("uri")
        guard(storedUri == canonical || storedUri == worker)
        val scopeMatch = row.getLong("userId") == expectedUser && row.getLong("driveId") == expectedDrive
        if (!scopeMatch) issues.merge(Issue.SCOPE_MISMATCH, 1, Int::plus)
        val digestStoredPresent = !row.isNull("localContentDigest")
        val digestEqual = digestStoredPresent && row.getString("localContentDigest") == expectedDigest
        if (!digestStoredPresent) issues.merge(Issue.DIGEST_MISSING, 1, Int::plus)
        if (digestStoredPresent && !digestEqual) issues.merge(Issue.DIGEST_MISMATCH, 1, Int::plus)
        val sizeEqual = row.getLong("localSizeAtPairing") == expectedSize
        if (!sizeEqual) issues.merge(Issue.SIZE_MISMATCH, 1, Int::plus)
        val remoteIdValid = row.getLong("remoteFileId") in 1..Int.MAX_VALUE
        if (!remoteIdValid) issues.merge(Issue.REMOTE_ID_INVALID, 1, Int::plus)
        val pairedPresent = !row.isNull("pairedAt")
        if (!pairedPresent) issues.merge(Issue.PAIRED_AT_MISSING, 1, Int::plus)
        val stateKnown = row.getString("state") in states
        if (!stateKnown) issues.merge(Issue.STATE_UNKNOWN, 1, Int::plus)
        fileNameOut[0] = row.getString("fileName")
        return out
            .put("matchedAlias", if (storedUri == canonical) "CANONICAL" else "WORKER")
            .put("bindingStatus", PhotoPairingBindingGuard.bindingStatus(
                scopeMatch, digestEqual, sizeEqual, remoteIdValid, pairedPresent, stateKnown))
            .put("scopeMatch", scopeMatch).put("digestStoredPresent", digestStoredPresent).put("digestEqual", digestEqual)
            .put("sizeEqual", sizeEqual).put("remoteIdValid", remoteIdValid).put("storedRemoteId", row.getLong("remoteFileId"))
            .put("state", if (stateKnown) row.getString("state") else "UNKNOWN")
            .put("stateKnown", stateKnown)
            .put("pairedPresent", pairedPresent)
            .put("baselineKnown", !row.isNull("baselineTrashed"))
            .put("baselineTrashed", !row.isNull("baselineTrashed") && row.getBoolean("baselineTrashed"))
            .put("lastObservedTrashed", row.getBoolean("lastObservedTrashed"))
            .put("activationEpoch", row.getLong("activationEpoch")).put("epochPresent", true)
            .put("pendingSinceZero", row.getLong("remoteTrashPendingSince") == 0L)
            .put("lastLocalReconciledZero", row.getLong("lastLocalTrashReconciledAt") == 0L)
            .put("lastRemoteAppliedZero", row.getLong("lastRemoteTrashAppliedAt") == 0L)
            .put("remoteHashPresent", !row.isNull("remoteContentHash"))
            .put("remoteHashEqualsExpected", !row.isNull("remoteContentHash") && row.getString("remoteContentHash") == expectedDigest)
            .put("fileNamePresent", !row.getString("fileName").isNullOrEmpty())
    }

    private fun crossbind(
        realm: DynamicRealm, issues: MutableMap<Issue, Int>, storedRemoteId: Long?, pairingFileName: String?, expectedDrive: Long,
    ): JSONObject {
        val out = JSONObject().put("staged", true).put("status", "NOT_APPLICABLE")
            .put("fileRowCount", 0).put("driveMatch", false).put("nameEqualsPairing", false)
        val id = storedRemoteId ?: return out
        val matched = rows(realm, "File", fileFields).filter { it.getLong("id") == id }
        out.put("fileRowCount", matched.size)
        if (matched.isEmpty()) { issues.merge(Issue.CROSSBIND_NOT_FOUND, 1, Int::plus); return out.put("status", "NOT_FOUND") }
        if (matched.size > 1) { issues.merge(Issue.CROSSBIND_AMBIGUOUS, 1, Int::plus); return out.put("status", "AMBIGUOUS") }
        val row = matched.single()
        val driveMatch = row.getLong("driveId") == expectedDrive
        if (!driveMatch) issues.merge(Issue.CROSSBIND_DRIVE_MISMATCH, 1, Int::plus)
        val nameEqualsPairing = pairingFileName != null && row.getString("name") == pairingFileName
        return out.put("driveMatch", driveMatch).put("nameEqualsPairing", nameEqualsPairing)
            .put("status", if (driveMatch && nameEqualsPairing) "BOUND" else "MISMATCH")
    }

    /** The singleton settings row must exist with exactly the expected identity; otherwise no scope evidence. */
    private fun settings(
        realm: DynamicRealm, issues: MutableMap<Issue, Int>, expectedUser: Long, expectedDrive: Long,
    ): JSONObject {
        val rows = rows(realm, "SyncSettings", settingsFields)
        val row = rows.singleOrNull()
        val scopeMatch = row != null && PhotoPairingBindingGuard.settingsScope(
            row.getLong("userId"), row.getLong("driveId"), expectedUser, expectedDrive)
        if (rows.size != 1 || !scopeMatch) issues.merge(Issue.SETTINGS_SCOPE, 1, Int::plus)
        return JSONObject()
            .put("rows", rows.size).put("settingsScopeMatch", scopeMatch)
            .put("photoTrashSyncEnabled", scopeMatch && row.getBoolean("photoTrashSyncEnabled"))
            .put("epochKnown", scopeMatch && !row.isNull("photoTrashActivationEpoch"))
            .put("activationEpoch", if (scopeMatch && !row.isNull("photoTrashActivationEpoch")) row.getLong("photoTrashActivationEpoch") else -1L)
    }

    /** Output keys are a bounded allowlist per section; sections may omit keys on non-bound outcomes. */
    private fun allowed(section: String, actual: Set<String>) {
        val allowlist = when (section) {
            "result" -> "protocol entry certification status phase binding settings crossbind issues allInputShaUnchanged"
            "binding" -> "matchCount totalPairingRows matchedAlias bindingStatus scopeMatch digestStoredPresent digestEqual " +
                "sizeEqual remoteIdValid storedRemoteId state stateKnown pairedPresent baselineKnown baselineTrashed " +
                "lastObservedTrashed activationEpoch epochPresent epochMatchesSettings pendingSinceZero " +
                "lastLocalReconciledZero lastRemoteAppliedZero remoteHashPresent remoteHashEqualsExpected fileNamePresent"
            "settings" -> "rows settingsScopeMatch photoTrashSyncEnabled epochKnown activationEpoch"
            "crossbind" -> "staged status fileRowCount driveMatch nameEqualsPairing"
            "issues" -> Issue.entries.joinToString(" ") { it.name }
            else -> throw IllegalStateException("COPY_OUTPUT_REJECTED")
        }
        guard(allowlist.split(' ').toSet().containsAll(actual))
    }

    private fun validateOutput(result: JSONObject) {
        fun check(section: String, value: JSONObject) = allowed(section, value.keys().asSequence().toSet())
        check("result", result)
        listOf("binding", "settings", "crossbind", "issues").forEach { name -> result.optJSONObject(name)?.let { check(name, it) } }
        guard(result.toString().toByteArray(Charsets.UTF_8).size <= 65536)
    }

    @JvmStatic
    fun readPhotoPairingBinding(context: Context, arguments: Bundle): String {
        guard(context.packageName == "com.infomaniak.drive.test" && context.applicationContext.javaClass == Application::class.java)
        guard(AggregateCopyGuard.entry(arguments.getString("copyEntry")) == "readPhotoPairingBinding")
        guard(AppSettingsMigration.DB_VERSION == 3L && DriveMigration.DB_VERSION == 4L &&
            UploadMigration.DB_VERSION == 13L && FileMigration.DB_VERSION == 20L)
        val stamp = arguments.getString("copyStamp").also(CopyInputGuard::stamp)
        val sidecarSha = requireNotNull(arguments.getString("sidecarSha")).also(CopyInputGuard::hash)
        val root = File(context.filesDir.canonicalFile, "photo-pairing-binding-reader-$stamp")
        guard(root.isDirectory && root.canonicalFile == root.absoluteFile); stat(root, true)
        val sidecar = CopyInputGuard.child(root, "inputs.json"); stat(sidecar)
        guard(sidecar.length() in 1..65536 && CopyInputGuard.sha256(sidecar) == sidecarSha)
        val json = JSONObject(sidecar.readText())
        keys(json, "protocol", "canonicalUri", "workerUri", "expectedDigest", "expectedSize",
            "expectedUserId", "expectedDriveId", "files")
        guard(number(json, "protocol") == 1L)
        val canonical = text(json, "canonicalUri")
        val worker = text(json, "workerUri")
        guard(canonical != worker)
        // Parsed-grammar alias validation: both aliases must be well-formed MediaStore
        // content URIs of the two recorded shapes and must denote the SAME item id.
        val canonicalId = PhotoPairingBindingGuard.aliasId(canonical)
        val workerId = PhotoPairingBindingGuard.aliasId(worker)
        guard(canonicalId > 0 && workerId > 0 && canonicalId == workerId)
        val expectedDigest = text(json, "expectedDigest").also(CopyInputGuard::hash)
        val expectedSize = number(json, "expectedSize"); guard(expectedSize in 1..Int.MAX_VALUE)
        val expectedUser = number(json, "expectedUserId"); val expectedDrive = number(json, "expectedDriveId")
        guard(expectedUser in 1..Int.MAX_VALUE && expectedDrive in 1..Int.MAX_VALUE)
        // Exact role structure BEFORE any input verification or Realm open: exactly one
        // SYNC, zero or one FILES, everything else (APP/DRIVES/foreign) rejected here.
        val specs = json.getJSONArray("files"); guard(specs.length() in 1..2)
        val roleList = (0 until specs.length()).map { text(specs.getJSONObject(it), "role") }
        guard(PhotoPairingBindingGuard.roles(roleList) == "OK")
        val checked = mutableListOf(sidecar to sidecarSha)
        data class Input(val role: String, val file: File, val schema: Long)
        val inputs = (0 until specs.length()).map { index ->
            val spec = specs.getJSONObject(index)
            keys(spec, "role", "file", "originalName", "schema", "sha256", "bytes", "userId", "driveId")
            val role = text(spec, "role"); val name = text(spec, "file"); val schema = number(spec, "schema")
            AggregateCopyGuard.input(role, name, text(spec, "originalName"), schema, number(spec, "userId"), number(spec, "driveId"))
            if (role == "FILES") {
                guard(number(spec, "userId") == expectedUser && number(spec, "driveId") == expectedDrive)
            }
            val file = CopyInputGuard.child(root, name); stat(file)
            val sha = text(spec, "sha256"); CopyInputGuard.matches(file, number(spec, "bytes"), sha)
            checked += file to sha
            Input(role, file, schema)
        }
        guard(inputs.count { it.role == "SYNC" } == 1)
        guard(inputs.map { it.file.name }.toSet().size == inputs.size)
        guard(root.list()?.toSet() == inputs.map { it.file.name }.toSet() + "inputs.json")
        val issues = Issue.entries.associateWith { 0 }.toMutableMap()
        var phase = "SCHEMA"
        val result = JSONObject().put("protocol", 1).put("entry", "readPhotoPairingBinding").put("certification", "UNKNOWN")
        var failed = false
        try {
            try { context.assets.open(asset).use { guard(false) } } catch (_: FileNotFoundException) { /* No asset fallback. */ }
            // Realm.init is required before any RealmConfiguration.Builder (cached SDK
            // Builder reads BaseRealm.applicationContext). Same isolated context the
            // dispatcher supplies to the proven aggregate/transition readers.
            Realm.init(context)
            fun <T> read(input: Input, module: Any, block: (DynamicRealm) -> T): T {
                val config = io.realm.RealmConfiguration.Builder().directory(root).name(input.file.name)
                    .schemaVersion(input.schema).modules(module).assetFile(asset).readOnly().build()
                return DynamicRealm.getInstance(config).use { realm -> guard(realm.version == input.schema); block(realm) }
            }
            phase = "QUERIES"
            val fileNameHolder = arrayOfNulls<String?>(1)
            val bindingResult = read(inputs.single { it.role == "SYNC" }, RealmModules.SyncFilesModule()) { realm ->
                val settingsResult = settings(realm, issues, expectedUser, expectedDrive)
                val bound = binding(realm, issues, canonical, worker, expectedDigest, expectedSize, expectedUser, expectedDrive, fileNameHolder)
                bound.put("epochMatchesSettings", settingsResult.optLong("activationEpoch", -1L) >= 0 &&
                    bound.optLong("activationEpoch", Long.MIN_VALUE) == settingsResult.getLong("activationEpoch"))
                result.put("settings", settingsResult); bound
            }
            val filesInput = inputs.singleOrNull { it.role == "FILES" }
            val crossbindResult = if (filesInput == null) {
                JSONObject().put("staged", false).put("status", "NOT_STAGED")
                    .put("fileRowCount", 0).put("driveMatch", false).put("nameEqualsPairing", false)
            } else {
                read(filesInput, RealmModules.LocalFilesModule()) { realm ->
                    val single = bindingResult.optInt("matchCount", 0) == 1
                    crossbind(
                        realm, issues, bindingResult.optLong("storedRemoteId", -1L).takeIf { single },
                        fileNameHolder[0].takeIf { single }, expectedDrive,
                    )
                }
            }
            result.put("binding", bindingResult).put("crossbind", crossbindResult)
            result.put("issues", JSONObject().also { out -> issues.forEach { (issue, count) -> out.put(issue.name, count) } })
            result.put("certification", PhotoPairingBindingGuard.certification(
                bindingResult.optString("bindingStatus", ""), crossbindResult.optString("status", ""), issues.values.sum()))
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
