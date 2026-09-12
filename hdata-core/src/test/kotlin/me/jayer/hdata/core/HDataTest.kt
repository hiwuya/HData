package me.jayer.hdata.core

import me.jayer.hdata.core.config.HDataOptions
import me.jayer.hdata.core.spec.PipelineSpecLoader
import me.jayer.hdata.core.spec.SpecMappers
import org.apache.beam.sdk.options.PipelineOptionsFactory
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Behavior boundaries of the [HData] entry point: missing `--pipeline`, `--dryRun`, and rendering
 * array values from `options:`.
 *
 * `execute` is private, but it only returns an exit code and never calls `exitProcess`, so it can be
 * invoked directly via reflection.
 */
class HDataTest {

    private fun callExecute(args: Array<String>): Int {
        PipelineOptionsFactory.register(HDataOptions::class.java)
        val companionClass = Class.forName("me.jayer.hdata.core.HData\$Companion")
        val method = companionClass.getDeclaredMethod("execute", Array<String>::class.java)
        method.isAccessible = true
        return method.invoke(HData.Companion, args) as Int
    }

    @Test
    fun `execute returns 1 and prints usage when --pipeline is missing`() {
        assertEquals(1, callExecute(emptyArray()))
    }

    @Test
    fun `execute --dryRun only builds the graph without running it, returns 0`() {
        val file = File.createTempFile("pipeline", ".yaml").apply {
            writeText(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config:
                        elements:
                          - { id: 1 }
                """.trimIndent()
            )
            deleteOnExit()
        }
        assertEquals(0, callExecute(arrayOf("--pipeline=${file.absolutePath}", "--dryRun")))
    }

    @Test
    fun `dry run writes a redacted validation manifest`() {
        val file = File.createTempFile("pipeline", ".yaml").apply {
            writeText("""
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config:
                        elements: [{ id: 1 }]
            """.trimIndent())
            deleteOnExit()
        }
        val manifest = File.createTempFile("hdata-run", ".json").apply { delete() }
        assertEquals(0, callExecute(arrayOf("--pipeline=${file.absolutePath}", "--dryRun", "--runManifest=${manifest.absolutePath}")))
        val document = SpecMappers.CONFIG.readTree(manifest)
        assertEquals("VALIDATED", document.get("phase").asString())
        assertEquals(64, document.get("pipeline_fingerprint_sha256").asString().length)
        assertEquals("Create", document.get("nodes").get(0).get("type").asString())
        assertTrue(!manifest.readText().contains("elements"), "the manifest must not include connector configuration")
    }

    @Test
    fun `an array value in options renders as one comma-separated argument`() {
        val spec = PipelineSpecLoader.parse(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1 }
            options:
              tags: [a, b, c]
            """.trimIndent(),
            SpecMappers.YAML,
            "test",
        )
        val merged = HData.mergeOptionArgs(spec, arrayOf("--pipeline=job.yaml"))
        assertTrue(merged.contains("--tags=a,b,c"), merged.contentToString())
    }
}
