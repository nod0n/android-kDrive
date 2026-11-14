package com.infomaniak.drive.data.cache

import androidx.test.platform.app.InstrumentationRegistry
import com.infomaniak.drive.utils.RealmModules
import io.realm.DynamicRealm
import io.realm.DynamicRealmObject
import io.realm.Realm
import io.realm.RealmConfiguration
import io.realm.RealmFieldType
import io.realm.RealmResults
import io.realm.annotations.RealmModule
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotEquals
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import java.io.File
import java.io.FileNotFoundException
import java.nio.file.Files
import java.nio.file.LinkOption
import java.nio.file.attribute.PosixFileAttributes
import java.security.MessageDigest

/**
 * androidTest-only, READ-ONLY proof helper: asserts that the BEFORE and AFTER
 * captured copies of DrivesInfos.realm from one run are row-for-row and
 * field-for-field identical, using the app's own pinned Realm library.
 *
 * Inputs (instrumentation args, nothing captured is hardcoded): copyStamp
 * (strict ASCII grammar, canonical containment under the dedicated
 * files/realm-copy-proof-<stamp>/ fixture namespace) and beforeSha/afterSha -
 * the pinned SHA-256 digests of the two capture copies, checked against the
 * measured digests BEFORE any Realm is opened. Copies are re-hashed after the
 * run on every path; any byte change fails. Copies must be regular
 * non-symlink files under real (non-symlink) fixture directories.
 *
 * Realm access: readOnly configuration (name/directory/schemaVersion/
 * DriveFilesModule), no migration, no compaction, no write transaction, both
 * Realms closed via nested Closeable.use. Library exceptions are sanitized:
 * only the exception class and a fixed phase/class label are emitted, never its message.
 * The pinned library requires assetFile even for existing read-only files.
 * Its cache skips asset copying when the target exists; an explicitly absent
 * asset prevents fallback from supplying bytes if an input disappears.
 *
 * Comparison: schema-driven over each copy's own schema (class, field, type,
 * primary-key, required, nullable - see schema_covers in the output for the
 * honest limit: index status and schema-level link targets are not
 * introspectable in the pinned fork; link targets ARE verified at row level
 * via class-qualified tokens). PK classes match rows by primary key; PK-less
 * classes match as sorted multisets of full-row tokens (no row-order
 * assumption). Every token is produced by an INJECTIVE typed length-prefixed
 * encoder (see the encoder block and its collision regression test), so
 * distinct rows/lists/links can never collapse into one token.
 *
 * Supported field types are exactly the seven the Drive-module schema uses;
 * anything else fails CLOSED. Output: one JSON verdict to the instrumentation
 * stream (field names, counts, digests and booleans only - never persisted
 * values). See PREPARE-REALM-COPY-PROOF-20261113-RESULT.txt.
 *
 * v2 config diagnostics: the config-era assertions run ONE PER PHASE under
 * stable RC-CONFIG-* reason codes, each with its own sanitized catch, so one
 * early failure can never mask the remaining evidence and reachability is
 * explicit per phase (a phase gated on a failed prerequisite is recorded as
 * "skipped"). Two presence booleans are computed BEFORE any builder runs by
 * pure reflection on already-loaded classes - the runtime @RealmModule
 * annotation on RealmModules.DriveFilesModule, and the generated
 * io.realm.DriveFilesModuleMediator as seen from the module's own classloader
 * (Class.forName with initialize=false; no build(), no asset IO, no writes).
 * The negative-contract probe records the OBSERVED library exception class on
 * a dedicated field ("none" when no exception, null when never reached):
 * the assertion itself can never carry it. These markers make a config-build
 * failure identifiable in one run; they are diagnostic only and are never a
 * substitute for the row-identity comparison below.
 */
class RealmCopyProofTest {

    private val verdicts = mutableListOf<String>()
    private val classReports = mutableListOf<Map<String, Any?>>()
    private val unsupported = mutableListOf<String>()
    private lateinit var infos: Map<String, ClassInfo>
    private var phase = "inputs"
    private var rowClass: String? = null
    private var assetFallbackDisabled = false

    // v2 config diagnostics: one PhaseRecord per config phase, pure-reflection
    // presence booleans computed before any build, and the negative probe's
    // observed library exception class on its own dedicated field.
    private val configPhases = mutableListOf<PhaseRecord>()
    private var moduleAnnotationPresent = false
    private var moduleMediatorPresent = false
    private var negativeProbeObservedClass: String? = null // null: probe never reached
    private var realmPhasesAttempted = false
    private var beforeConfig: RealmConfiguration? = null
    private var afterConfig: RealmConfiguration? = null

    @Test
    fun beforeAfterRealmCopiesAreRowIdentical() {
        val args = InstrumentationRegistry.getArguments()
        val stamp = args.getString("copyStamp") ?: ""
        if (!STAMP_REGEX.matches(stamp)) {
            throw AssertionError("FAIL_CLOSED: copyStamp missing/malformed (expected [A-Za-z0-9_-]{1,64})")
        }
        val beforeShaArg = args.getString("beforeSha") ?: ""
        val afterShaArg = args.getString("afterSha") ?: ""
        if (!HEX64_REGEX.matches(beforeShaArg) || !HEX64_REGEX.matches(afterShaArg)) {
            throw AssertionError("FAIL_CLOSED: beforeSha/afterSha missing/malformed (expected 64-hex)")
        }

        val context = InstrumentationRegistry.getInstrumentation().targetContext
        val filesDir = context.filesDir
        val fixtureRoot = File(filesDir, "realm-copy-proof-$stamp")
        val expectedCanonical = File(filesDir.canonicalFile, "realm-copy-proof-$stamp").canonicalFile
        if (fixtureRoot.canonicalFile.path != expectedCanonical.path) {
            throw AssertionError("FAIL_CLOSED: fixture namespace failed canonical containment under filesDir")
        }

        val before = requireCopy(fixtureRoot, "before")
        val after = requireCopy(fixtureRoot, "after")

        val shaBefore = mapOf("before" to sha256(before), "after" to sha256(after))
        if (shaBefore["before"] != beforeShaArg || shaBefore["after"] != afterShaArg) {
            emitJson(VERDICT_FAIL_CLOSED, schemaEqual = false, copiesUnchanged = false, errorClass = null, shaBefore = shaBefore, shaAfter = null)
            throw AssertionError("FAIL_CLOSED: measured copy digests do not match the pinned beforeSha/afterSha args (refusing to open)")
        }

        var verdict = VERDICT_FAIL_CLOSED
        var schemaEqual = false
        var errorClass: String? = null
        var errorPhase: String? = null
        var errorRowClass: String? = null
        var shaAfter: Map<String, String> = emptyMap()
        try {
            if (step("realm-init", RC_REALM_INIT) {
                    Realm.init(context) // Idempotent library initialization; no production Realm is opened here.
                }
            ) {
                val assetGuardOk = step("asset-guard", RC_ASSET_GUARD) { requireAbsentFallbackAsset() }
                step("module-annotation-presence", RC_MODULE_ANNOTATION_PRESENCE) {
                    // Pure reflection on the runtime class checkModule inspects; no build(), no writes.
                    moduleAnnotationPresent = RealmModules.DriveFilesModule::class.java
                        .isAnnotationPresent(RealmModule::class.java)
                    check(moduleAnnotationPresent)
                }
                step("module-mediator-presence", RC_MODULE_MEDIATOR_PRESENCE) {
                    // Presence only: initialize=false loads the generated mediator without
                    // running any code in it. getModuleMediator resolves this class via
                    // single-arg Class.forName from RealmConfiguration's DEFINING loader,
                    // so the inspection mirrors that exact lookup (no build(), no writes).
                    moduleMediatorPresent = try {
                        Class.forName(
                            DRIVE_MODULE_MEDIATOR,
                            false,
                            RealmConfiguration::class.java.classLoader,
                        )
                        true
                    } catch (_: ClassNotFoundException) {
                        false
                    }
                    check(moduleMediatorPresent)
                }
                if (assetGuardOk) {
                    step("before-config-build", RC_BEFORE_CONFIG_BUILD) { beforeConfig = readOnlyConfiguration(before) }
                    step("after-config-build", RC_AFTER_CONFIG_BUILD) { afterConfig = readOnlyConfiguration(after) }
                } else {
                    configPhases.add(PhaseRecord("before-config-build", RC_BEFORE_CONFIG_BUILD, "skipped", null))
                    configPhases.add(PhaseRecord("after-config-build", RC_AFTER_CONFIG_BUILD, "skipped", null))
                }
                // Regression for the pinned builder's assetFile precondition.
                // Building configurations never opens or writes either input.
                // Pinned-bytecode reachability (verified host-side, javap of the
                // pinned AAR): checkModule runs at the modules() SETTER (inside
                // copyConfiguration), so annotation loss (C1a) surfaces here as
                // IAE; the mediator lookup runs in build()'s createSchemaMediator
                // AFTER the readOnly branch, so mediator absence (C1b) is NOT
                // visible to this probe (which then sees exactly the readOnly
                // ISE) - C1b is covered by the build-step classes plus the
                // presence boolean. The OBSERVED library exception class is
                // recorded on its own field: the assertion below can never carry it.
                step("negative-contract-probe", RC_NEGATIVE_CONTRACT_PROBE) {
                    val observed = runCatching { copyConfiguration(before).readOnly().build() }.exceptionOrNull()
                    negativeProbeObservedClass = observed?.javaClass?.simpleName ?: "none"
                    check(observed is IllegalStateException)
                }
                val cfgBefore = beforeConfig
                val cfgAfter = afterConfig
                if (cfgBefore != null && cfgAfter != null) {
                    step("readonly-flag-check", RC_READONLY_FLAG_CHECK) {
                        check(cfgBefore.isReadOnly && cfgAfter.isReadOnly)
                    }
                    step("path-identity-check", RC_PATH_IDENTITY_CHECK) {
                        check(
                            before.canonicalFile == File(cfgBefore.path).canonicalFile &&
                                after.canonicalFile == File(cfgAfter.path).canonicalFile
                        )
                    }
                } else {
                    configPhases.add(PhaseRecord("readonly-flag-check", RC_READONLY_FLAG_CHECK, "skipped", null))
                    configPhases.add(PhaseRecord("path-identity-check", RC_PATH_IDENTITY_CHECK, "skipped", null))
                }
                if (configPhases.all { it.status == "ok" }) {
                    realmPhasesAttempted = true
                    phase = "open-before"
                    DynamicRealm.getInstance(requireNotNull(beforeConfig)).use { beforeRealm ->
                        phase = "open-after"
                        DynamicRealm.getInstance(requireNotNull(afterConfig)).use { afterRealm ->
                            phase = "schema"
                            infos = buildClassInfos(beforeRealm)
                            schemaEqual = compareSchemas(beforeRealm, afterRealm)
                            verdict = if (!schemaEqual) VERDICT_DIFFER else compareClasses(beforeRealm, afterRealm)
                            phase = "close"
                        }
                    }
                }
            }
        } catch (e: Exception) {
            // Sanitized: class name only. Library exception messages can embed
            // paths or schema text and are never emitted.
            errorClass = e.javaClass.simpleName
            errorPhase = phase
            errorRowClass = rowClass.takeIf { phase == "rowclass" }
            verdict = VERDICT_FAIL_CLOSED
        } finally {
            shaAfter = mapOf("before" to sha256(before), "after" to sha256(after))
        }
        val copiesUnchanged = shaBefore == shaAfter

        emitJson(verdict, schemaEqual, copiesUnchanged, errorClass, shaBefore, shaAfter, errorPhase, errorRowClass)

        val failure = StringBuilder()
        if (verdict != VERDICT_EQUAL) failure.append("verdict=$verdict; ")
        if (errorClass != null) failure.append("errorClass=$errorClass; phase=$errorPhase; rowClass=$errorRowClass; ")
        if (configPhases.any { it.status != "ok" }) {
            failure.append(
                "config=" + configPhases.filter { it.status != "ok" }
                    .joinToString(",") { "${it.rc}:${it.phase}:${it.status}" } + "; "
            )
        }
        if (!copiesUnchanged) failure.append("a copy was modified by the read; ")
        if (verdicts.isNotEmpty()) failure.append("diffs=${verdicts.take(20)}; ")
        if (unsupported.isNotEmpty()) failure.append("unsupported=${unsupported.take(20)}; ")
        if (failure.isNotEmpty()) throw AssertionError("RealmCopyProof FAIL_CLOSED/EVIDENCE summary: $failure")
    }

    /**
     * P1 regression: the formerly-ambiguous raw-concatenation pairs must now
     * differ, identical structures must stay equal, and the encoder must be
     * exactly equality-reflecting on adversarial content. Exercises the same
     * pure encoder functions the comparison uses - not a substitute.
     */
    @Test
    fun tokenEncodingIsInjectiveForAdversarialPairs() {
        // Old bug: STRING row fields could absorb the "|field=" delimiters.
        val rowA = rowTokenFromParts("C", listOf("f" to str('s', "x"), "g" to str('s', "y|f=sz")))
        val rowB = rowTokenFromParts("C", listOf("f" to str('s', "x|g=sy"), "g" to str('s', "z")))
        assertNotEquals(rowA, rowB, "distinct rows must not collapse")
        // Old bug: scalar-list element boundaries could shift inside a joined token.
        val listA = listTokenFromParts(listOf(str('s', "a,vb"), str('s', "c")))
        val listB = listTokenFromParts(listOf(str('s', "a"), str('s', "b,vc")))
        assertNotEquals(listA, listB, "distinct lists must not collapse")
        // Identical structures stay equal:
        assertEquals(rowA, rowTokenFromParts("C", listOf("f" to str('s', "x"), "g" to str('s', "y|f=sz"))))
        assertEquals(listA, listTokenFromParts(listOf(str('s', "a,vb"), str('s', "c"))))
        // Class-qualified links: the same PK value under different target classes differs.
        assertNotEquals(linkToken("Drive", str('i', "7")), linkToken("Team", str('i', "7")))
        // The encoder is exactly equality-reflecting, including length-prefix
        // lookalikes and delimiter-bearing content:
        val adversarial = listOf("", "5:abc", "R2[c1:s]", "a,vb", "|f=", ":", "0", "L0[")
        for (a in adversarial) {
            for (b in adversarial) {
                assertEquals(a == b, str('s', a) == str('s', b), "encoding must reflect equality exactly")
            }
        }
    }

    /**
     * Runs ONE diagnostic phase. Records completion and the sanitized observed
     * exception class at every catch; never rethrows - fail-closed is applied
     * by the config gate, not by masking the remaining diagnostics. The class
     * name is the only exception content that ever leaves this method.
     */
    private fun step(phaseName: String, rc: String, block: () -> Unit): Boolean {
        phase = phaseName
        return try {
            block()
            configPhases.add(PhaseRecord(phaseName, rc, "ok", null))
            true
        } catch (e: Throwable) {
            configPhases.add(PhaseRecord(phaseName, rc, "failed", e.javaClass.simpleName))
            false
        }
    }

    private fun requireCopy(fixtureRoot: File, side: String): File {
        checkNonLinkDirectory(fixtureRoot, "fixture root")
        val sideDir = File(fixtureRoot, side)
        checkNonLinkDirectory(sideDir, "$side dir")
        val copy = File(sideDir, "DrivesInfos.realm")
        val attr = try {
            Files.readAttributes(copy.toPath(), PosixFileAttributes::class.java, LinkOption.NOFOLLOW_LINKS)
        } catch (e: Exception) {
            throw AssertionError("FAIL_CLOSED: copy not stat-able without following links: $side")
        }
        if (attr.isSymbolicLink) throw AssertionError("FAIL_CLOSED: copy is a symlink: $side")
        if (!attr.isRegularFile) throw AssertionError("FAIL_CLOSED: copy is not a regular file: $side")
        if (attr.size() == 0L) throw AssertionError("FAIL_CLOSED: empty copy: $side")
        if (!copy.canRead()) throw AssertionError("FAIL_CLOSED: unreadable copy: $side")
        return copy
    }

    private fun checkNonLinkDirectory(dir: File, what: String) {
        val attr = try {
            Files.readAttributes(dir.toPath(), PosixFileAttributes::class.java, LinkOption.NOFOLLOW_LINKS)
        } catch (e: Exception) {
            throw AssertionError("FAIL_CLOSED: directory not stat-able without following links: $what")
        }
        if (attr.isSymbolicLink || !attr.isDirectory) {
            throw AssertionError("FAIL_CLOSED: not a real directory: $what")
        }
    }

    private fun copyConfiguration(copy: File): RealmConfiguration.Builder =
        RealmConfiguration.Builder()
            .name(copy.name)
            .directory(requireNotNull(copy.parentFile).canonicalFile)
            .schemaVersion(DriveMigration.DB_VERSION)
            .modules(RealmModules.DriveFilesModule())

    private fun requireAbsentFallbackAsset() {
        try {
            InstrumentationRegistry.getInstrumentation().targetContext.assets.open(NO_FALLBACK_ASSET).use {
                error("COPY proof fallback asset must not exist")
            }
        } catch (_: FileNotFoundException) {
            assetFallbackDisabled = true
        }
    }

    private fun readOnlyConfiguration(copy: File): RealmConfiguration {
        check(assetFallbackDisabled && copy.isFile)
        // Realm 11.0.1: RealmCache.copyFileIfNeeded returns before asset IO
        // for an existing target. For a missing target it opens the asset
        // BEFORE FileOutputStream; our absent asset makes that path fail.
        // Public readOnly propagates SCHEMA_MODE_READONLY to native config.
        return copyConfiguration(copy)
            .assetFile(NO_FALLBACK_ASSET)
            .readOnly()
            .build()
    }

    // ------------------------------------------------------------------
    // Schema comparison: class names, field names, types, pk/required/
    // nullable flags. Index status and schema-level link-target classes are
    // NOT introspectable in the pinned fork (reported in schema_covers);
    // link targets are verified at row level via class-qualified tokens.
    // ------------------------------------------------------------------

    private fun compareSchemas(before: DynamicRealm, after: DynamicRealm): Boolean {
        val b = schemaDigest(before)
        val a = schemaDigest(after)
        val diffs = mutableListOf<String>()
        (b.keys - a.keys).forEach { diffs.add("class only in BEFORE: $it") }
        (a.keys - b.keys).forEach { diffs.add("class only in AFTER: $it") }
        (b.keys intersect a.keys).forEach { name ->
            val bf = b.getValue(name)
            val af = a.getValue(name)
            (bf.keys - af.keys).forEach { diffs.add("$name: field only in BEFORE: $it") }
            (af.keys - bf.keys).forEach { diffs.add("$name: field only in AFTER: $it") }
            (bf.keys intersect af.keys).forEach { field ->
                if (bf.getValue(field) != af.getValue(field)) {
                    diffs.add("$name.$field: schema differs")
                }
            }
        }
        verdicts.addAll(diffs)
        return diffs.isEmpty()
    }

    /** class name -> field name -> "TYPE|pk|req|nullable" (names and flags only, never values). */
    private fun schemaDigest(realm: DynamicRealm): Map<String, Map<String, String>> {
        val out = mutableMapOf<String, Map<String, String>>()
        for (objSchema in realm.getSchema().getAll()) {
            val fields = mutableMapOf<String, String>()
            for (field in objSchema.getFieldNames().sorted()) {
                val type = objSchema.getFieldType(field)
                checkSupported(type, "${objSchema.getClassName()}.$field")
                fields[field] = "${type.name}|pk=${objSchema.isPrimaryKey(field)}" +
                    "|req=${objSchema.isRequired(field)}|nullable=${objSchema.isNullable(field)}"
            }
            out[objSchema.getClassName()] = fields
        }
        return out
    }

    private fun buildClassInfos(realm: DynamicRealm): Map<String, ClassInfo> =
        realm.getSchema().getAll().associate { schema ->
            val fields = schema.getFieldNames().sorted()
            schema.getClassName() to ClassInfo(
                name = schema.getClassName(),
                fields = fields,
                types = fields.associateWith { schema.getFieldType(it) },
                pkField = fields.firstOrNull { schema.isPrimaryKey(it) },
            )
        }

    // ------------------------------------------------------------------
    // Row comparison.
    // ------------------------------------------------------------------

    private fun compareClasses(beforeRealm: DynamicRealm, afterRealm: DynamicRealm): String {
        val classNames = beforeRealm.getSchema().getAll().map { it.getClassName() }.sorted()
        var allEqual = true
        for (name in classNames) {
            phase = "rowclass"
            rowClass = name
            val info = infos.getValue(name)
            val before = beforeRealm.where(name).findAll()
            val after = afterRealm.where(name).findAll()
            val (equal, detail) = if (info.pkField != null) {
                compareByPrimaryKey(info, before, after)
            } else {
                compareAsTupleMultiset(name, before, after)
            }
            if (!equal) allEqual = false
            classReports.add(
                mapOf(
                    "class" to name,
                    "before_count" to before.size,
                    "after_count" to after.size,
                    "pk" to (info.pkField != null),
                    "equal" to equal,
                    "detail" to detail,
                )
            )
        }
        return if (allEqual && unsupported.isEmpty()) VERDICT_EQUAL else VERDICT_DIFFER
    }

    private fun compareByPrimaryKey(
        info: ClassInfo,
        beforeRows: RealmResults<DynamicRealmObject>,
        afterRows: RealmResults<DynamicRealmObject>,
    ): Pair<Boolean, String> {
        val pkField = info.pkField!!
        val pkType = info.types.getValue(pkField)
        // Realm enforces PK uniqueness (null String PKs included); if a copy
        // ever violates that, keyed matching would silently collapse rows -
        // fail closed.
        val beforeGrouped = beforeRows.groupBy { fieldToken(it, pkField, pkType, 0) }
        val afterGrouped = afterRows.groupBy { fieldToken(it, pkField, pkType, 0) }
        if (beforeGrouped.any { it.value.size > 1 } || afterGrouped.any { it.value.size > 1 }) {
            verdicts.add("${info.name}: duplicate PK tokens (invalid copy) - failing closed")
            unsupported.add("${info.name}: duplicate PK tokens")
            return false to "duplicate PK tokens (invalid copy)"
        }
        val beforeByKey = beforeGrouped.mapValues { it.value.single() }
        val afterByKey = afterGrouped.mapValues { it.value.single() }
        val missingInAfter = beforeByKey.keys - afterByKey.keys
        val extraInAfter = afterByKey.keys - beforeByKey.keys
        val fieldDiffs = sortedSetOf<String>()
        for (key in beforeByKey.keys intersect afterByKey.keys) {
            compareRow(info, beforeByKey.getValue(key), afterByKey.getValue(key)).forEach { fieldDiffs.add(it) }
        }
        val lines = mutableListOf<String>()
        if (missingInAfter.isNotEmpty()) lines.add("rows missing in AFTER: ${missingInAfter.size}")
        if (extraInAfter.isNotEmpty()) lines.add("rows extra in AFTER: ${extraInAfter.size}")
        if (fieldDiffs.isNotEmpty()) lines.add("field diffs: $fieldDiffs")
        val equal = lines.isEmpty()
        if (!equal) verdicts.add("${info.name}: ${lines.joinToString("; ")}")
        return equal to lines.joinToString("; ").ifEmpty { "pk-matched, all fields equal" }
    }

    /** PK-less classes: sorted multiset of full-row tokens; no row-order assumption; every field contributes. */
    private fun compareAsTupleMultiset(
        name: String,
        beforeRows: RealmResults<DynamicRealmObject>,
        afterRows: RealmResults<DynamicRealmObject>,
    ): Pair<Boolean, String> {
        val beforeTuples = beforeRows.map { token(it, 0) }.sorted()
        val afterTuples = afterRows.map { token(it, 0) }.sorted()
        var firstMismatch = -1
        for (i in 0 until maxOf(beforeTuples.size, afterTuples.size)) {
            if (beforeTuples.getOrNull(i) != afterTuples.getOrNull(i)) {
                firstMismatch = i
                break
            }
        }
        val equal = firstMismatch == -1 && beforeTuples.size == afterTuples.size
        val detail = if (equal) "multiset of full-row tokens equal" else "first tuple mismatch at sorted index $firstMismatch"
        if (!equal) verdicts.add("$name: tuple-multiset differs ($detail)")
        return equal to detail
    }

    /** Field-name-level comparison of two PK-matched rows; returns differing field NAMES only. */
    private fun compareRow(info: ClassInfo, beforeRow: DynamicRealmObject, afterRow: DynamicRealmObject): List<String> {
        val diffs = mutableListOf<String>()
        for (field in info.fields) {
            val type = info.types.getValue(field)
            if (fieldToken(beforeRow, field, type, 1) != fieldToken(afterRow, field, type, 1)) diffs.add(field)
        }
        return diffs
    }

    // ------------------------------------------------------------------
    // P1: injective typed length-prefixed token encoder.
    //
    // Atom:  <kind><len>:<bytes>   for variable-length content;
    //        "N", "B0", "B1"       fixed atoms.
    // Seq:   <kind><count>[items]  items are atoms, concatenated.
    // Every atom parses deterministically (kind, decimal length, ':', exact
    // bytes), so distinct structures can never produce equal token strings.
    // These pure companion functions ARE the encoder the comparison uses; the
    // collision regression above exercises this implementation directly.
    // Token content never leaves the process: only names/counts/digests/
    // booleans reach the stream.
    // ------------------------------------------------------------------

    /** Full-row token: class + (field name, value atom) pairs. THE row encoder. */
    private fun rowTokenFromParts(className: String, fieldParts: List<Pair<String, String>>): String =
        seq('R', listOf(str('c', className)) + fieldParts.map { (name, valueAtom) -> str('f', name) + valueAtom })

    /** List token (link lists and scalar lists): elements are value atoms. THE list encoder. */
    private fun listTokenFromParts(elementAtoms: List<String>): String = seq('L', elementAtoms)

    /** Class-qualified to-one link token: target class + identity atom. THE link encoder. */
    private fun linkToken(targetClass: String, identityAtom: String): String =
        seq('O', listOf(str('c', targetClass), identityAtom))

    /** Full-row token of one row: every persisted field contributes, typed. */
    private fun token(obj: DynamicRealmObject, depth: Int): String {
        val info = infos[obj.getType()]
        if (info == null) {
            unsupported.add("row of class ${obj.getType()} absent from the schema digest")
            return UNSUPPORTED_TOKEN
        }
        if (depth > MAX_LINK_DEPTH) {
            unsupported.add("link depth exceeded at ${info.name}")
            return UNSUPPORTED_TOKEN
        }
        return rowTokenFromParts(info.name, info.fields.map { field ->
            field to fieldToken(obj, field, info.types.getValue(field), depth)
        })
    }

    /** Identity of one linked row: its PK value atom; PK-less rows use their full-row token (class included). */
    private fun identityToken(target: DynamicRealmObject, depth: Int): String {
        val info = infos[target.getType()]
        if (info == null) {
            unsupported.add("linked row of class ${target.getType()} absent from the schema digest")
            return UNSUPPORTED_TOKEN
        }
        val pk = info.pkField ?: return token(target, depth)
        return fieldToken(target, pk, info.types.getValue(pk), depth)
    }

    private fun fieldToken(obj: DynamicRealmObject, field: String, type: RealmFieldType, depth: Int): String {
        if (type !in SUPPORTED_FIELD_TYPES) {
            unsupported.add("${obj.getType()}.$field: unsupported type ${type.name}")
            return UNSUPPORTED_TOKEN
        }
        return when (type) {
            RealmFieldType.OBJECT -> {
                val target = if (obj.isNull(field)) null else obj.getObject(field)
                if (target == null) NULL_ATOM else linkToken(target.type, identityToken(target, depth + 1))
            }
            RealmFieldType.LIST -> listTokenFromParts(
                obj.getList(field).map { linkToken(it.type, identityToken(it, depth + 1)) }
            )
            RealmFieldType.INTEGER_LIST -> listTokenFromParts(
                obj.getList(field, Int::class.javaObjectType).map { str('i', it.toString()) }
            )
            RealmFieldType.INTEGER -> if (obj.isNull(field)) NULL_ATOM else str('i', obj.getLong(field).toString())
            RealmFieldType.BOOLEAN -> if (obj.isNull(field)) NULL_ATOM else if (obj.getBoolean(field)) "B1" else "B0"
            RealmFieldType.STRING -> if (obj.isNull(field)) NULL_ATOM else str('s', obj.getString(field))
            RealmFieldType.DATE -> if (obj.isNull(field)) NULL_ATOM else str('d', obj.getDate(field).time.toString())
            else -> {
                unsupported.add("${obj.getType()}.$field: unreadable type ${type.name}")
                UNSUPPORTED_TOKEN
            }
        }
    }

    // ------------------------------------------------------------------
    // Output + utilities.
    // ------------------------------------------------------------------

    private fun emitJson(
        verdict: String,
        schemaEqual: Boolean,
        copiesUnchanged: Boolean,
        errorClass: String?,
        shaBefore: Map<String, String>,
        shaAfter: Map<String, String>?,
        errorPhase: String? = null,
        errorRowClass: String? = null,
    ) {
        val json = buildString {
            append("{\"verdict\":\"").append(verdict).append('"')
            append(",\"schema_equal\":").append(schemaEqual)
            append(",\"copies_unchanged\":").append(copiesUnchanged)
            append(",\"sha_args_pinned\":true")
            append(",\"error_class\":").append(errorClass?.let { "\"$it\"" } ?: "null")
            append(",\"error_phase\":").append(errorPhase?.let { "\"$it\"" } ?: "null")
            append(",\"error_row_class\":").append(errorRowClass?.let { "\"${jsonSafe(it)}\"" } ?: "null")
            append(",\"module_annotation_present\":").append(moduleAnnotationPresent)
            append(",\"module_mediator_present\":").append(moduleMediatorPresent)
            append(",\"negative_probe_observed_class\":")
                .append(negativeProbeObservedClass?.let { "\"" + jsonSafe(it) + "\"" } ?: "null")
            append(",\"realm_phases_attempted\":").append(realmPhasesAttempted)
            append(",\"config_phases\":[")
            append(
                configPhases.joinToString(",") { rec ->
                    "{\"phase\":\"" + rec.phase + "\",\"rc\":\"" + rec.rc + "\",\"status\":\"" + rec.status + "\"" +
                        (rec.observedClass?.let { ",\"observed_class\":\"" + jsonSafe(it) + "\"" } ?: "") + "}"
                }
            )
            append("]")
            append(",\"access_mode\":\"read-only-existing-input\"")
            append(",\"asset_fallback_disabled\":").append(assetFallbackDisabled)
            append(",\"schema_covers\":\"class,field,type,pk,required,nullable; index-status and schema-level")
            append(" link-targets not introspectable in pinned fork; link targets verified in row tokens\"")
            append(",\"diff_count\":").append(verdicts.size)
            append(",\"diffs\":[")
            append(verdicts.take(50).joinToString(",") { "\"" + jsonSafe(it) + "\"" })
            append("],\"unsupported\":[")
            append(unsupported.take(50).joinToString(",") { "\"" + jsonSafe(it) + "\"" })
            append("],\"sha_before_read\":{\"before\":\"").append(shaBefore["before"])
            append("\",\"after\":\"").append(shaBefore["after"]).append("\"}")
            if (shaAfter != null) {
                append(",\"sha_after_close\":{\"before\":\"").append(shaAfter["before"])
                append("\",\"after\":\"").append(shaAfter["after"]).append("\"}")
            }
            append(",\"classes\":[")
            append(
                classReports.joinToString(",") { report ->
                    "{" + report.entries.joinToString(",") { (key, value) ->
                        "\"" + key + "\":" + when (value) {
                            is String -> "\"" + jsonSafe(value) + "\""
                            else -> value.toString()
                        }
                    } + "}"
                }
            )
            append("]}")
        }
        println("[RealmCopyProof] $json")
    }

    private fun jsonSafe(text: String): String =
        text.replace("\\", "\\\\").replace("\"", "'").replace("\n", " ")

    private fun sha256(file: File): String {
        val digest = MessageDigest.getInstance("SHA-256")
        file.inputStream().use { input ->
            val buffer = ByteArray(64 * 1024)
            while (true) {
                val read = input.read(buffer)
                if (read < 0) break
                digest.update(buffer, 0, read)
            }
        }
        return digest.digest().joinToString("") { "%02x".format(it) }
    }

    private fun checkSupported(type: RealmFieldType, where: String) {
        if (type !in SUPPORTED_FIELD_TYPES) unsupported.add("$where: unsupported type ${type.name}")
    }

    private class ClassInfo(
        val name: String,
        val fields: List<String>,
        val types: Map<String, RealmFieldType>,
        val pkField: String?,
    )

    /** One config-diagnostic phase outcome; observedClass is a sanitized simple name or null. */
    private class PhaseRecord(val phase: String, val rc: String, val status: String, val observedClass: String?)

    companion object {
        private const val VERDICT_EQUAL = "EQUAL"
        private const val VERDICT_DIFFER = "DIFFER"
        private const val VERDICT_FAIL_CLOSED = "FAIL_CLOSED"
        private const val MAX_LINK_DEPTH = 8
        private const val UNSUPPORTED_TOKEN = "UNSUPPORTED"
        private const val NULL_ATOM = "N"
        private const val NO_FALLBACK_ASSET = "__realm_copy_proof_no_fallback__.realm"

        // v2 config-diagnostics reason codes: stable ASCII identifiers, no
        // semantics beyond identifying one phase each (execution order 001..009).
        private const val RC_REALM_INIT = "RC-CONFIG-001"
        private const val RC_ASSET_GUARD = "RC-CONFIG-002"
        private const val RC_MODULE_ANNOTATION_PRESENCE = "RC-CONFIG-003"
        private const val RC_MODULE_MEDIATOR_PRESENCE = "RC-CONFIG-004"
        private const val RC_BEFORE_CONFIG_BUILD = "RC-CONFIG-005"
        private const val RC_AFTER_CONFIG_BUILD = "RC-CONFIG-006"
        private const val RC_NEGATIVE_CONTRACT_PROBE = "RC-CONFIG-007"
        private const val RC_READONLY_FLAG_CHECK = "RC-CONFIG-008"
        private const val RC_PATH_IDENTITY_CHECK = "RC-CONFIG-009"

        // Generated by kapt for the app module (verified host-side in this tree:
        // app/build/generated/source/kapt/*/debug/io/realm/DriveFilesModuleMediator.java,
        // package-private in io.realm, extends RealmProxyMediator). The pinned
        // getModuleMediator derives exactly "io.realm.<last-segment>Mediator" and
        // resolves it with single-arg Class.forName from RealmConfiguration's
        // defining loader; absence/uninstantiability is the C1b RealmException site.
        private const val DRIVE_MODULE_MEDIATOR = "io.realm.DriveFilesModuleMediator"

        private val STAMP_REGEX = Regex("^[A-Za-z0-9_-]{1,64}$")
        private val HEX64_REGEX = Regex("^[0-9a-f]{64}$")

        // P6: exactly the seven field types the Drive-module schema uses
        // (grep-verified across all 17 persisted classes). Anything else fails
        // closed; no generic type architecture.
        private val SUPPORTED_FIELD_TYPES: Set<RealmFieldType> = setOf(
            RealmFieldType.INTEGER, RealmFieldType.BOOLEAN, RealmFieldType.STRING,
            RealmFieldType.DATE, RealmFieldType.OBJECT, RealmFieldType.LIST,
            RealmFieldType.INTEGER_LIST,
        )

        // --- P1 encoder primitives (used by the comparison AND the regression test) ---

        fun str(kind: Char, raw: String): String = "$kind${raw.length}:$raw"

        fun seq(kind: Char, items: List<String>): String =
            "$kind${items.size}[" + items.joinToString("") + "]"
    }
}
