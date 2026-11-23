package com.infomaniak.drive.data.cache

import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.io.TempDir
import java.io.File
import java.lang.reflect.InvocationTargetException
import java.net.URLClassLoader
import java.nio.file.Path
import java.nio.file.Paths
import java.security.MessageDigest
import javax.tools.ToolProvider

/** Host controls compile the actual AndroidTest guard used by the reader; no mock Realm execution. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RemoteChildStateGuardControlTest {
    @TempDir lateinit var temporary: Path
    private lateinit var guard: Class<*>
    private val name = "T0006-G1-20260101-0123456789ab-renamed.txt"

    @BeforeAll
    fun compileActualGuard(@TempDir compilerDirectory: Path) {
        val roots = generateSequence(Paths.get("").toAbsolutePath()) { it.parent }.take(4).toList()
        val relative = "src/androidTest/java/com/infomaniak/drive/data/cache/copyreader"
        val source = roots.flatMap { listOf(it.resolve(relative), it.resolve("app/$relative")) }
            .first { it.resolve("RemoteChildReleaseGuard.java").toFile().isFile }
        val compiler = requireNotNull(ToolProvider.getSystemJavaCompiler())
        val output = compilerDirectory.resolve("classes").toFile().also { assertTrue(it.mkdir()) }
        compiler.getStandardFileManager(null, null, null).use { manager ->
            val files = listOf("CopyInputGuard", "AggregateCopyGuard", "RemoteChildReleaseGuard")
                .map { source.resolve("$it.java").toFile() }
            val diagnostic = java.io.StringWriter()
            val task = compiler.getTask(diagnostic, manager, null, listOf("-d", output.path), null,
                manager.getJavaFileObjectsFromFiles(files))
            assertTrue(task.call(), diagnostic.toString())
        }
        guard = URLClassLoader(arrayOf(output.toURI().toURL()), javaClass.classLoader)
            .loadClass("com.infomaniak.drive.data.cache.copyreader.RemoteChildReleaseGuard")
    }

    private fun call(method: String, vararg arguments: Any?): Any? =
        guard.methods.single { it.name == method }.invoke(null, *arguments)

    private fun rejected(block: () -> Unit) {
        val error = assertThrows(InvocationTargetException::class.java, block)
        assertTrue(error.cause is IllegalStateException)
    }

    private fun hash(file: File) = MessageDigest.getInstance("SHA-256").digest(file.readBytes())
        .joinToString("") { "%02x".format(it) }

    private fun values() = mutableMapOf<String, Any>(
        "isOffline" to false, "isMarkedAsOffline" to false, "isDirectlyPinned" to false,
        "offlineOwnedByRoot" to false, "offlineNeedsAttention" to true, "rowPathMatchesArchive" to true,
        "basisModifiedState" to "ZERO", "basisSizeState" to "NULL", "digestState" to "NULL",
        "serverContentHashState" to "NULL", "publishedPathState" to "MATCH",
    )

    @Test
    fun `two pinned cohort files are read without changing either input`() {
        val before = temporary.resolve("before.realm").toFile().apply { writeBytes(byteArrayOf(1, 2, 3)) }
        val after = temporary.resolve("after.realm").toFile().apply { writeBytes(byteArrayOf(4, 5, 6)) }
        val a = hash(before); val b = hash(after)
        call("copy", "before", 0, "FILES", 20L, "before-drive-0.realm", before, 3L, a)
        call("copy", "after", 1, "FILES", 20L, "after-drive-0.realm", after, 3L, b)
        assertEquals(a, hash(before)); assertEquals(b, hash(after))
    }

    @Test
    fun `swapped stale wrong module schema and malformed cohort inputs are rejected`() {
        val file = temporary.resolve("cohort.realm").toFile().apply { writeBytes(byteArrayOf(7, 8, 9)) }
        val sha = hash(file)
        rejected { call("copy", "after", 0, "FILES", 20L, "after-drive-0.realm", file, 3L, sha) }
        rejected { call("copy", "before", 0, "APP", 20L, "before-drive-0.realm", file, 3L, sha) }
        rejected { call("copy", "before", 0, "FILES", 17L, "before-drive-0.realm", file, 3L, sha) }
        rejected { call("copy", "before", 0, "FILES", 20L, "other.realm", file, 3L, sha) }
        rejected { call("copy", "before", 0, "FILES", 20L, "before-drive-0.realm", file, 2L, sha) }
        rejected { call("copy", "before", 0, "FILES", 20L, "before-drive-0.realm", file, 3L, "0".repeat(64)) }
        call("baselineCount", 0)
        rejected { call("baselineCount", 1) }
    }

    @Test
    fun `fixed target and renamed synthetic identity cannot be repurposed`() {
        call("target", 19L, 14L, name)
        rejected { call("target", 18L, 14L, name) }
        rejected { call("target", 19L, 15L, name) }
        rejected { call("target", 19L, 14L, "T0079-private.txt") }
        rejected { call("target", 19L, 14L, "../$name") }
        rejected { call("target", 19L, 14L, name.replace("-renamed", "")) }
    }

    @Test
    fun `stored target identity binds drive parent name and persisted uid`() {
        assertEquals(true, call("identity", 19L, 9L, 14L, name, "19_9", 9L, name))
        assertEquals(false, call("identity", 18L, 9L, 14L, name, "19_9", 9L, name))
        assertEquals(false, call("identity", 19L, 8L, 14L, name, "19_9", 9L, name))
        assertEquals(false, call("identity", 19L, 9L, 15L, name, "19_9", 9L, name))
        assertEquals(false, call("identity", 19L, 9L, null, name, "19_9", 9L, name))
        assertEquals(false, call("identity", 19L, 9L, 14L, "other.txt", "19_9", 9L, name))
        assertEquals(false, call("identity", 19L, 9L, 14L, name, "19_8", 9L, name))
    }

    @Test
    fun `restricted sidecar shape rejects missing or extra private input channels`() {
        val keys = setOf("protocol", "scenario", "userId", "driveId", "originalRealmName", "rootName",
            "rootRelativePath", "childId", "files", "copies", "targetId", "targetParentId", "targetName")
        call("inputFields", keys)
        rejected { call("inputFields", keys - "targetId") }
        listOf("account", "token", "liveRealmPath", "APP", "credentials").forEach {
            rejected { call("inputFields", keys + it) }
        }
    }

    @Test
    fun `absent target has no invented field values and duplicates fail closed`() {
        assertEquals(mapOf("presence" to "ABSENT"), call("observation", 0, false, emptyMap<String, Any>()))
        rejected { call("observation", 0, false, values()) }
        rejected { call("observation", 0, true, emptyMap<String, Any>()) }
        rejected { call("observation", 2, true, values()) }
        rejected { call("observation", 1, false, values()) }
    }

    @Test
    fun `present state observes attention and retained path without pretending those must clear`() {
        val result = call("observation", 1, true, values()) as Map<*, *>
        assertEquals("PRESENT", result["presence"]); assertEquals(true, result["identityMatches"])
        assertEquals(false, result["offlineOwnedByRoot"])
        assertEquals(true, result["offlineNeedsAttention"]); assertEquals("MATCH", result["publishedPathState"])
        val active = values().apply { this["isOffline"] = true; this["offlineOwnedByRoot"] = true }
        assertEquals(true, (call("observation", 1, true, active) as Map<*, *>)["offlineOwnedByRoot"])
        assertEquals("ZERO", call("numberState", 0L)); assertEquals("NEGATIVE", call("numberState", -1L))
        assertEquals("POSITIVE", call("numberState", 2L))
    }

    @Test
    fun `closed output rejects raw identifiers paths hashes missing fields and wrong types`() {
        rejected { call("observation", 1, true, values() + ("id" to 19L)) }
        rejected { call("observation", 1, true, values() - "isOffline") }
        rejected { call("observation", 1, true, values() + ("isOffline" to "false")) }
        rejected { call("observation", 1, true, values() + ("digestState" to "a".repeat(64))) }
        rejected { call("observation", 1, true, values() + ("publishedPathState" to "/private/path")) }
        rejected { call("observation", 1, true, values() + ("basisSizeState" to 0L)) }
    }
}
