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
 * [HData] 入口的行为边界：`--pipeline` 缺失 / `--dryRun` / `options:` 里数组值的渲染。
 *
 * `execute` 是 private，但只返回退出码、不调用 `exitProcess`，可用反射直接调用。
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
    fun `execute 缺 --pipeline 时返回 1 并打印用法`() {
        assertEquals(1, callExecute(emptyArray()))
    }

    @Test
    fun `execute --dryRun 只构图不执行，返回 0`() {
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
    fun `options 里的数组值渲染成逗号分隔的单个参数`() {
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
