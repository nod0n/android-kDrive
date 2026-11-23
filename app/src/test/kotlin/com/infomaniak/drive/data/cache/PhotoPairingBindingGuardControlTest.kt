package com.infomaniak.drive.data.cache

import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assumptions.assumeTrue
import org.junit.jupiter.api.BeforeAll
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance
import java.io.File
import java.net.URLClassLoader
import java.nio.file.Path
import java.nio.file.Paths
import javax.tools.ToolProvider
import kotlin.io.path.exists

/**
 * Host behavior controls over the ACTUAL shared validator source (compiled from
 * the androidTest copyreader package, never mirrored logic). Synthetic safe
 * values only; no archived or private identity is present here.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PhotoPairingBindingGuardControlTest {

    private lateinit var guard: Class<*>

    @BeforeAll
    fun compileActualGuardSource() {
        val source = locate(
            "src/androidTest/java/com/infomaniak/drive/data/cache/copyreader/PhotoPairingBindingGuard.java",
        )
        assumeTrue(source != null, "guard source not found from working dir ${Paths.get("").toAbsolutePath()}")
        val compiler = ToolProvider.getSystemJavaCompiler()
        assumeTrue(compiler != null, "host JDK compiler unavailable")
        val output = File.createTempFile("ppbg-control", "").let { it.delete(); File(it.parentFile, "ppbg-control-${System.nanoTime()}") }
        assumeTrue(output.mkdirs())
        val fileManager = compiler.getStandardFileManager(null, null, null)
        val diagnostics = java.io.StringWriter()
        try {
            val task = compiler.getTask(
                java.io.PrintWriter(diagnostics), fileManager, null,
                listOf("-d", output.absolutePath), null, fileManager.getJavaFileObjects(source),
            )
            assertEquals(true, task.call(), "actual guard source must compile; diagnostics: $diagnostics")
        } finally {
            fileManager.close()
        }
        guard = URLClassLoader(arrayOf(output.toURI().toURL()), javaClass.classLoader)
            .loadClass("com.infomaniak.drive.data.cache.copyreader.PhotoPairingBindingGuard")
    }

    private fun locate(relative: String): Path? {
        var current: Path? = Paths.get("").toAbsolutePath()
        repeat(4) {
            current?.let { dir ->
                listOf(dir.resolve(relative), dir.resolve("app").resolve(relative)).forEach { candidate ->
                    if (candidate.exists()) return candidate
                }
            }
            current = current?.parent
        }
        return null
    }

    private fun call(name: String, vararg args: Any?): Any? =
        guard.methods.filter { it.name == name }.single { it.parameterCount == args.size }.invoke(null, *args)

    @Test
    fun `both recorded alias shapes parse to the same synthetic item id`() {
        assertEquals(123456L, call("aliasId", "content://media/external_primary/images/media/123456"))
        assertEquals(123456L, call("aliasId", "content://media/external/images/media/123456"))
    }

    @Test
    fun `malformed wrong-authority and out-of-grammar aliases are rejected`() {
        val invalid = listOf(
            null, "", "content://", "http://media/external/images/media/1",
            "content://other/external/images/media/1", "content://media/internal/images/media/1",
            "content://media/external/videos/media/1", "content://media/external/images/picture/1",
            "content://media/external/images/media/1x", "content://media/external/images/media/0",
            "content://media/external/images/media/01", "content://media/external/images/media/-1",
            "content://media/external/images/media/1?x=2", "content://media/external/images/media/1#f",
            "content://media/external/images/media", "content://media/external/images/media/1/2",
            "content://media/external/images/media/", "content://media/external_primary/images/media/1/extra",
        )
        invalid.forEach { value -> assertEquals(-1L, call("aliasId", value), "must reject: $value") }
    }

    @Test
    fun `role structure accepts exactly one sync and zero or one files`() {
        assertEquals("OK", call("roles", listOf("SYNC")))
        assertEquals("OK", call("roles", listOf("SYNC", "FILES")))
        assertEquals("SYNC_REQUIRED", call("roles", listOf("FILES")))
        assertEquals("SYNC_REQUIRED", call("roles", listOf("SYNC", "SYNC")))
        assertEquals("FILES_DUPLICATE", call("roles", listOf("SYNC", "FILES", "FILES")))
        assertEquals("FOREIGN_ROLE", call("roles", listOf("SYNC", "APP")))
        assertEquals("FOREIGN_ROLE", call("roles", listOf("SYNC", "DRIVES")))
        assertEquals("SYNC_REQUIRED", call("roles", listOf<String>()))
    }

    @Test
    fun `binding decision distinguishes bound unestablished and mismatched rows`() {
        fun status(vararg flags: Boolean) = call(
            "bindingStatus", flags[0], flags[1], flags[2], flags[3], flags[4], flags[5],
        ) as String
        // scope, digestEqual, sizeEqual, remoteIdValid, pairedPresent, stateKnown
        assertEquals("BOUND", status(true, true, true, true, true, true))
        assertEquals("MISMATCH", status(false, true, true, true, true, true))
        assertEquals("MISMATCH", status(true, false, true, true, true, true))
        assertEquals("MISMATCH", status(true, true, false, true, true, true))
        assertEquals("MISMATCH", status(true, true, true, false, true, true))
        assertEquals("UNESTABLISHED", status(true, true, true, true, false, true))
        assertEquals("UNESTABLISHED", status(true, true, true, true, true, false))
        assertEquals("UNESTABLISHED", status(true, true, true, true, false, false))
    }

    @Test
    fun `settings scope compares identity to the expected account`() {
        assertEquals(true, call("settingsScope", 7L, 9L, 7L, 9L))
        assertEquals(false, call("settingsScope", 7L, 9L, 8L, 9L))
        assertEquals(false, call("settingsScope", 7L, 9L, 7L, 8L))
    }

    @Test
    fun `certification requires bound established crossbind and zero issues`() {
        assertEquals("COMPLETE", call("certification", "BOUND", "NOT_STAGED", 0))
        assertEquals("COMPLETE", call("certification", "BOUND", "BOUND", 0))
        assertEquals("UNKNOWN", call("certification", "UNESTABLISHED", "BOUND", 0))
        assertEquals("UNKNOWN", call("certification", "MISMATCH", "BOUND", 0))
        assertEquals("UNKNOWN", call("certification", "BOUND", "NOT_FOUND", 0))
        assertEquals("UNKNOWN", call("certification", "BOUND", "AMBIGUOUS", 0))
        assertEquals("UNKNOWN", call("certification", "BOUND", "MISMATCH", 0))
        assertEquals("UNKNOWN", call("certification", "BOUND", "NOT_APPLICABLE", 0))
        assertEquals("UNKNOWN", call("certification", "BOUND", "NOT_STAGED", 1))
    }
}
