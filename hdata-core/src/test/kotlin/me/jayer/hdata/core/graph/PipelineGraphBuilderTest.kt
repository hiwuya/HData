package me.jayer.hdata.core.graph

import me.jayer.hdata.core.HData
import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.PipelineSpecLoader
import me.jayer.hdata.core.spec.SpecMappers
import org.apache.beam.sdk.options.PipelineOptionsFactory
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 端到端跑在 DirectRunner 上：pipeline 文件 -> DAG -> 真的执行，断言写在 pipeline 文件里。
 *
 * @author wuya
 * @date 2022-08-30
 */
class PipelineGraphBuilderTest {

    @Test
    fun `chain 按书写顺序串联`() {
        run(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1, name: "a" }
                      - { id: 2, name: "b" }
                - type: MapToFields
                  config:
                    fields:
                      order_id: id
                - type: AssertEqual
                  config:
                    elements:
                      - { order_id: 1 }
                      - { order_id: 2 }
            """
        )
    }

    @Test
    fun `composite 支持分支与合流且与书写顺序无关`() {
        val graph = run(
            """
            pipeline:
              transforms:
                # 故意把下游写在上游前面，验证拓扑排序生效
                - type: AssertEqual
                  input: Merged
                  config:
                    elements:
                      - { id: 1 }
                      - { id: 2 }
                      - { id: 3 }
                - type: Flatten
                  name: Merged
                  input: [Left, Right]
                - type: Create
                  name: Left
                  config:
                    elements:
                      - { id: 1 }
                - type: Create
                  name: Right
                  config:
                    elements:
                      - { id: 2 }
                      - { id: 3 }
            """
        )

        assertEquals(listOf("Left", "Right", "Merged", "AssertEqual"), graph.nodes.map { it.name })
    }

    @Test
    fun `死信流可以被 extra_transforms 消费`() {
        run(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1, bad: "x" }
                      - { id: 2, bad: null }
                - type: TestSink
                  name: Sink
                  config:
                    fail_field: bad
                    error_handling:
                      output: rejected
              extra_transforms:
                - type: StripErrorMetadata
                  name: Recovered
                  input: Sink.rejected
                - type: AssertEqual
                  input: Recovered
                  config:
                    elements:
                      - { id: 1, bad: "x" }
            """
        )
    }

    @Test
    fun `声明了 error_handling 却没人消费会报错`() {
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config:
                        elements:
                          - { id: 1 }
                    - type: TestSink
                      name: Sink
                      config:
                        error_handling:
                          output: rejected
                """
            )
        }
        assertTrue("Sink.rejected" in error.message!!)
    }

    @Test
    fun `不支持 error_handling 的 transform 会报错`() {
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config:
                        elements:
                          - { id: 1 }
                    - type: LogForTesting
                      name: Log
                      config:
                        error_handling:
                          output: rejected
                  extra_transforms:
                    - type: LogForTesting
                      name: Errors
                      input: Log.rejected
                """
            )
        }
        assertTrue("不支持 error_handling" in error.message!!)
    }

    @Test
    fun `error_handling 的 threshold 暂未实现，声明即报错以免静默失效`() {
        // 文档明说 threshold 还没实现：要是只收下不报错，用户以为自己配了错误率阈值，
        // 实际完全不生效。必须在构图阶段直接拒绝
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config:
                        elements:
                          - { id: 1 }
                    - type: TestSink
                      name: Sink
                      config:
                        fail_field: bad
                        error_handling:
                          output: rejected
                          threshold: 0.1
                  extra_transforms:
                    - type: LogForTesting
                      input: Sink.rejected
                """
            )
        }
        assertTrue("threshold" in error.message!!)
    }

    @Test
    fun `error_handling 的 output 为空时报错`() {
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config:
                        elements:
                          - { id: 1 }
                    - type: TestSink
                      name: Sink
                      config:
                        fail_field: bad
                        error_handling:
                          output: ""
                  extra_transforms:
                    - type: LogForTesting
                      input: Sink.rejected
                """
            )
        }
        assertTrue("output" in error.message!!)
    }

    @Test
    fun `嵌套 chain 会拿到外层节点的输出`() {
        run(
            """
            pipeline:
              transforms:
                - type: Create
                  name: Source
                  config:
                    elements:
                      - { id: 1, name: "a" }
                - type: chain
                  name: Rename
                  input: Source
                  transforms:
                    - type: MapToFields
                      config:
                        fields:
                          order_id: id
                    - type: LogForTesting
                - type: AssertEqual
                  input: Rename
                  config:
                    elements:
                      - { order_id: 1 }
            """
        )
    }

    @Test
    fun `引用不存在的节点时报错并列出可用节点`() {
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  transforms:
                    - type: Create
                      name: Source
                      config:
                        elements:
                          - { id: 1 }
                    - type: LogForTesting
                      input: Typo
                """
            )
        }
        assertTrue("Typo" in error.message!! && "Source" in error.message!!)
    }

    @Test
    fun `环会被检测出来`() {
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  transforms:
                    - type: LogForTesting
                      name: A
                      input: B
                    - type: LogForTesting
                      name: B
                      input: A
                """
            )
        }
        assertTrue("环" in error.message!!)
    }

    @Test
    fun `未知的 transform 类型会给出已注册列表`() {
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: ReadFromNowhere
                """
            )
        }
        assertTrue("ReadFromNowhere" in error.message!! && "Create" in error.message!!)
    }

    private fun build(yaml: String): PipelineGraph {
        val spec = PipelineSpecLoader.parse(yaml.trimIndent(), SpecMappers.YAML, "test")
        val options = PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create()
        return HData(spec).build(options).second
    }

    private fun run(yaml: String): PipelineGraph {
        val spec = PipelineSpecLoader.parse(yaml.trimIndent(), SpecMappers.YAML, "test")
        val options = PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create()
        val (pipeline, graph) = HData(spec).build(options)
        pipeline.run().waitUntilFinish()
        return graph
    }
}
