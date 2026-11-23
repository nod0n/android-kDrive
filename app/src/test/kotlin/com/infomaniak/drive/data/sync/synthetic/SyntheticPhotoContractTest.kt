/* Copyright (C) 2026 Infomaniak Network SA. SPDX-License-Identifier: GPL-3.0-or-later */
package com.infomaniak.drive.data.sync.synthetic

import com.google.gson.Gson
import org.junit.jupiter.api.AfterAll
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertFalse
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import java.lang.reflect.InvocationTargetException
import java.net.URLClassLoader
import java.nio.file.Files
import javax.tools.ToolProvider

/** Compiles/executes the actual AndroidTest pure-Java gate and event audit, never a mirror.
 * Android context, classloader/native libraries and engine execution remain unrun here. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SyntheticPhotoContractTest {
    private lateinit var contract: Class<*>
    private lateinit var events: Class<*>
    private lateinit var loader: URLClassLoader
    private lateinit var classes: File
    private val photo = "PHOTO_TRASH_22"
    private val upload = "UPLOAD_DIGEST_5"
    private val testSha = "a".repeat(64)
    private val sourceRoot: File get() = sequenceOf(File("src/androidTest/java"), File("app/src/androidTest/java"))
        .first { it.isDirectory }
    private fun invoke(name: String, vararg args: Any?): Any? = contract.methods.single { it.name == name }.invoke(null, *args)
    private fun call(target: Any, name: String, vararg args: Any?): Any? = events.methods.single { it.name == name }.invoke(target, *args)
    private fun methods(entry: String): List<String> = (invoke("methods", entry) as Array<*>).map { it as String }
    private fun cls(entry: String) = invoke("className", entry) as String
    private fun audit(entry: String): Any = events.getConstructor(String::class.java).newInstance(entry)
    private fun reject(block: () -> Unit) {
        val error = assertThrows(InvocationTargetException::class.java, block)
        assertTrue(error.cause is IllegalArgumentException)
    }

    @BeforeAll
    fun compileRealGuard() {
        classes = Files.createTempDirectory("q12-synthetic-guard-").toFile()
        val compiler = ToolProvider.getSystemJavaCompiler()
        assertNotNull(compiler, "real host JDK required; missing compiler is not a skipped PASS")
        val source = File(sourceRoot, "com/infomaniak/drive/data/sync/synthetic/SyntheticPhotoContract.java")
        assertTrue(source.isFile)
        assertEquals(0, compiler.run(null, null, null, "-d", classes.path, source.path))
        loader = URLClassLoader(arrayOf(classes.toURI().toURL()), javaClass.classLoader)
        contract = loader.loadClass("com.infomaniak.drive.data.sync.synthetic.SyntheticPhotoContract")
        events = loader.loadClass("com.infomaniak.drive.data.sync.synthetic.SyntheticPhotoContract\$Events")
    }
    private fun args() = mapOf("syntheticEntry" to photo, "syntheticStamp" to "q12-synthetic-20261120-0123456789ab",
        "mainSha" to contract.getField("MAIN_SHA").get(null), "testSha" to testSha)

    @Test
    fun acceptsOnlyTheTwoFixedEntriesAndFourArgumentKeys() {
        invoke("input", args())
        invoke("input", args() + ("syntheticEntry" to upload))
        assertEquals(22, methods(photo).size)
        assertEquals(5, methods(upload).size)
    }
    @Test
    fun rejectsMissingExtraForeignClassAndCopiedReaderArguments() {
        for (key in args().keys) reject { invoke("input", args() - key) }
        for (key in listOf("class", "copyEntry", "sidecarSha", "runnerBuilder", "configurationParameters"))
            reject { invoke("input", args() + (key to "foreign")) }
        for (value in listOf("", "ALL", cls(photo), "$photo#one", "readArchivedTransition"))
            reject { invoke("input", args() + ("syntheticEntry" to value)) }
    }
    @Test
    fun rejectsMalformedOrReusedLookingPathStampAndWrongPinnedMain() {
        for (stamp in listOf("", "../owned", "q12-synthetic-20261120-abc", "q12-synthetic-20261120-ABCDEF012345", "q12-synthetic-20261120-0123456789ab/extra"))
            reject { invoke("input", args() + ("syntheticStamp" to stamp)) }
        reject { invoke("input", args() + ("mainSha" to testSha)) }
        reject { invoke("input", args() + ("testSha" to "short")) }
    }
    private fun identity(pkg: String = "com.infomaniak.drive.test", app: String = "android.app.Application", running: Int = 10118,
                         own: Int = 10118, main: Int = 10119, canonical: Boolean = true,
                         mainSha: String = contract.getField("MAIN_SHA").get(null) as String, actualTest: String = testSha) {
        invoke("identity", pkg, app, running, own, main, canonical, mainSha, actualTest, testSha)
    }
    @Test
    fun requiresOwnUidPlainApplicationAndExactApkIdentity() { identity() }
    @Test
    fun wrongUidSharedUidProductionContextOrApplicationIsRejected() {
        reject { identity(pkg = "com.infomaniak.drive") }
        reject { identity(app = "com.infomaniak.drive.MainApplication") }
        reject { identity(running = 0) }
        reject { identity(running = 10117) }
        reject { identity(main = 10118) }
        reject { identity(canonical = false) }
        reject { identity(mainSha = testSha) }
        reject { identity(actualTest = "b".repeat(64)) }
    }
    @Test
    fun fixedInventoryMatchesEveryActualTestBodyIncludingSixNewMethods() {
        for (entry in listOf(photo, upload)) {
            val path = File(sourceRoot, cls(entry).replace('.', '/') + ".kt")
            val declarations = Regex("@Test\\s*(?:@DisplayName\\((\"(?:\\\\.|[^\"\\\\])*\")\\)\\s*)?fun\\s+(\\w+)")
                .findAll(path.readText()).toList()
            val actual = declarations.map { it.groupValues[2] }
            assertEquals(methods(entry).toSet(), actual.toSet())
            assertEquals(methods(entry).size, actual.size)
            invoke("inventory", entry, actual.toTypedArray())
            for (declaration in declarations) if (declaration.groupValues[1].isNotEmpty()) {
                val display = Gson().fromJson(declaration.groupValues[1], String::class.java)
                assertEquals(declaration.groupValues[2], invoke("bare", entry, cls(entry), display), "actual source display alias")
            }
        }
    }
    @Test
    fun rejectsDroppedDuplicatedAndForeignDiscoveredCases() {
        val names = methods(photo)
        reject { invoke("inventory", photo, names.drop(1).toTypedArray()) }
        reject { invoke("inventory", photo, (names.dropLast(1) + names.first()).toTypedArray()) }
        reject { invoke("inventory", photo, (names.dropLast(1) + "foreignCase").toTypedArray()) }
        reject { invoke("inventory", upload, names.toTypedArray()) }
    }
    @Test
    fun auditCountsRealStartedFinishedCasesAndNeverInventsZeroTestPass() {
        val audit = audit(upload)
        assertFalse(call(audit, "complete") as Boolean)
        for (name in methods(upload)) { call(audit, "start", cls(upload), name); call(audit, "finish", cls(upload), name) }
        assertEquals(5, call(audit, "started")); assertEquals(5, call(audit, "finished")); assertEquals(5, call(audit, "passed"))
        assertTrue(call(audit, "complete") as Boolean)
    }
    @Test
    fun assertionFailureRemainsFailureAndOtherCasesRemainRealPasses() {
        val audit = audit(upload)
        for ((i, name) in methods(upload).withIndex()) {
            call(audit, "start", cls(upload), name)
            if (i == 0) call(audit, "fail", cls(upload), name)
            call(audit, "finish", cls(upload), name)
        }
        assertEquals(1, call(audit, "failed")); assertEquals(4, call(audit, "passed")); assertTrue(call(audit, "complete") as Boolean)
    }
    @Test
    fun ignoredAndAssumptionCasesAreSkippedNotPassed() {
        val audit = audit(upload)
        for ((i, name) in methods(upload).withIndex()) {
            if (i == 0) call(audit, "skip", cls(upload), name, true)
            else {
                call(audit, "start", cls(upload), name)
                if (i == 1) call(audit, "skip", cls(upload), name, false)
                call(audit, "finish", cls(upload), name)
            }
        }
        assertEquals(4, call(audit, "started")); assertEquals(2, call(audit, "skipped")); assertEquals(3, call(audit, "passed"))
        assertTrue(call(audit, "complete") as Boolean)
    }
    @Test
    fun duplicateOrForeignEventsCannotBecomeACompleteRun() {
        for (bad in listOf("duplicateStart", "duplicateFinish", "foreign", "finishWithoutStart", "wrongClass")) {
            val audit = audit(upload); val name = methods(upload).first()
            when (bad) {
                "duplicateStart" -> { call(audit, "start", cls(upload), name); call(audit, "start", cls(upload), name) }
                "duplicateFinish" -> { call(audit, "start", cls(upload), name); call(audit, "finish", cls(upload), name); call(audit, "finish", cls(upload), name) }
                "foreign" -> call(audit, "start", cls(upload), "foreign")
                "finishWithoutStart" -> call(audit, "finish", cls(upload), name)
                "wrongClass" -> call(audit, "start", cls(photo), name)
            }
            assertTrue((call(audit, "invalid") as Int) > 0)
            assertFalse(call(audit, "complete") as Boolean)
        }
    }
    @Test
    fun eventAliasAcceptsOnlySourceMethodsAndKnownJupiterDefaultForm() {
        for (entry in listOf(photo, upload)) for (name in methods(entry)) {
            assertEquals(name, invoke("bare", entry, cls(entry), name))
            assertEquals(name, invoke("bare", entry, cls(entry), "$name()"))
            assertEquals(null, invoke("bare", entry, cls(entry), "${name}Unknown"))
        }
    }
    @Test
    fun exclusiveRunDirectoryNeverAdoptsAPreexistingPartialStamp() {
        val data = Files.createTempDirectory("q12-stamp-control-").toFile()
        try {
            val files = File(data, "files").apply { assertTrue(mkdir()) }
            val stamp = "q12-synthetic-20261120-0123456789ab"
            val root = invoke("runDirectory", data, files, stamp) as File
            assertEquals(File(files, stamp).canonicalFile, root)
            val retained = File(root, "retained.txt").apply { writeText("owned-control") }
            reject { invoke("runDirectory", data, files, stamp) }
            assertEquals("owned-control", retained.readText())
            assertEquals(listOf("retained.txt"), root.list()!!.toList())
        } finally { data.deleteRecursively() }
    }
    @Test
    fun linkedOrWrongAncestryFilesDirectoryIsRejected() {
        val data = Files.createTempDirectory("q12-path-control-").toFile()
        try {
            val files = File(data, "files").apply { assertTrue(mkdir()) }
            val other = File(data, "other").apply { assertTrue(mkdir()) }
            val linked = File(data, "linked")
            Files.createSymbolicLink(linked.toPath(), files.toPath())
            reject { invoke("runDirectory", data, linked, "q12-synthetic-20261120-0123456789ab") }
            reject { invoke("runDirectory", other, files, "q12-synthetic-20261120-0123456789ab") }
            assertTrue(files.list()!!.isEmpty())
        } finally { data.deleteRecursively() }
    }
    @Test
    fun duplicateCompletionOrLateFailureRemainsInvalidEvenAfterAllCasesFinished() {
        for (lateFailure in listOf(false, true)) {
            val audit = audit(upload)
            for (name in methods(upload)) { call(audit, "start", cls(upload), name); call(audit, "finish", cls(upload), name) }
            if (lateFailure) call(audit, "fail", cls(upload), methods(upload).first())
            else call(audit, "finish", cls(upload), methods(upload).first())
            assertFalse(call(audit, "complete") as Boolean)
            assertTrue((call(audit, "invalid") as Int) > 0)
        }
    }
    @AfterAll
    fun removeOnlyHostCompiledGuard() { if (::loader.isInitialized) loader.close(); if (::classes.isInitialized) classes.deleteRecursively() }
}
