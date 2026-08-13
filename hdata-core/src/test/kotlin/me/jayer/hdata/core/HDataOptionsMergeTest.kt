package me.jayer.hdata.core

import me.jayer.hdata.core.spec.PipelineSpecLoader
import me.jayer.hdata.core.spec.SpecMappers
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * @author wuya
 * @date 2022-08-30
 */
class HDataOptionsMergeTest {

    private val spec = PipelineSpecLoader.parse(
        """
        pipeline:
          type: chain
          transforms:
            - type: Create
              config:
                elements:
                  - { id: 1 }
        options:
          runner: DirectRunner
          jobName: from-file
        """.trimIndent(),
        SpecMappers.YAML,
        "test",
    )

    @Test
    fun `文件里的选项作为默认值传下去`() {
        val merged = HData.mergeOptionArgs(spec, arrayOf("--pipeline=job.yaml"))
        assertEquals(
            listOf("--runner=DirectRunner", "--jobName=from-file", "--pipeline=job.yaml"),
            merged.toList(),
        )
    }

    @Test
    fun `命令行上出现的同名选项不会重复下发`() {
        // Beam 见到重复的 --runner 会直接报错，所以必须是"剔除"而不是"追加覆盖"
        val merged = HData.mergeOptionArgs(spec, arrayOf("--runner=FlinkRunner", "--pipeline=job.yaml"))
        assertEquals(
            listOf("--jobName=from-file", "--runner=FlinkRunner", "--pipeline=job.yaml"),
            merged.toList(),
        )
    }
}
