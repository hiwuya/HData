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
    fun `options from the file pass through as defaults`() {
        val merged = HData.mergeOptionArgs(spec, arrayOf("--pipeline=job.yaml"))
        assertEquals(
            listOf("--runner=DirectRunner", "--jobName=from-file", "--pipeline=job.yaml"),
            merged.toList(),
        )
    }

    @Test
    fun `an option also present on the command line is not sent twice`() {
        // Beam errors out on a duplicate --runner, so the file's copy must be dropped, not appended after it
        val merged = HData.mergeOptionArgs(spec, arrayOf("--runner=FlinkRunner", "--pipeline=job.yaml"))
        assertEquals(
            listOf("--jobName=from-file", "--runner=FlinkRunner", "--pipeline=job.yaml"),
            merged.toList(),
        )
    }
}
