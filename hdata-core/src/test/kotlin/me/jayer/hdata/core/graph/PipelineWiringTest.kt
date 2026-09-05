package me.jayer.hdata.core.graph

import me.jayer.hdata.core.HData
import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.PipelineSpecLoader
import me.jayer.hdata.core.spec.SpecMappers
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.GlobalWindows
import org.apache.beam.sdk.transforms.windowing.SlidingWindows
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * 构图时的接线细节：端口校验、复合节点输出、窗口下发。
 *
 * @author wuya
 * @date 2022-08-30
 */
class PipelineWiringTest {

    // ---------- 端口校验 ----------

    @Test
    fun `读取端被显式接上输入时报错`() {
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
                    - type: Create
                      name: Bogus
                      input: Source
                      config:
                        elements:
                          - { id: 2 }
                """
            )
        }
        assertTrue("读取端" in error.message!!)
    }

    @Test
    fun `处理端没有输入时报错`() {
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  transforms:
                    - type: LogForTesting
                      name: Orphan
                """
            )
        }
        assertTrue("需要输入" in error.message!!)
    }

    @Test
    fun `引用不存在的输出端口时列出可用端口`() {
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
                      input: Source.nope
                """
            )
        }
        assertTrue("Source.nope" in error.message!! && "output" in error.message!!)
    }

    @Test
    fun `同名节点会被拒绝并提示用 name 区分`() {
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
                    - type: LogForTesting
                """
            )
        }
        assertTrue("LogForTesting" in error.message!! && "name" in error.message!!)
    }

    @Test
    fun `节点名拒绝引用分隔符与空字符串`() {
        listOf("A.B", " ").forEach { name ->
            val error = assertFailsWith<HDataException> {
                build(
                    """
                    pipeline:
                      transforms:
                        - type: Create
                          name: "$name"
                          config:
                            elements: [{id: 1}]
                    """
                )
            }
            assertTrue("节点名" in error.message!!, error.message)
        }
    }

    @Test
    fun `extra_transforms 与主子节点同名会在构图前被拒绝`() {
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  transforms:
                    - type: Create
                      name: Same
                      config:
                        elements: [{id: 1}]
                  extra_transforms:
                    - type: LogForTesting
                      name: Same
                      input: Same
                """
            )
        }
        assertTrue("Same" in error.message!! && "同名" in error.message!!, error.message)
    }

    @Test
    fun `input 与 inputs 不能同时声明`() {
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
                      input: Source
                      inputs: Source
                """
            )
        }
        assertTrue("不能同时声明" in error.message!!)
    }

    // ---------- extraTransforms（side input）也走同样的端口校验 ----------

    @Test
    fun `extraTransform 引用不存在的输出端口时报错`() {
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
                    - type: chain
                      name: Stage
                      input: Source
                      transforms:
                        - type: MapToFields
                          name: M
                          input: input
                          config:
                            fields:
                              id: id
                      extra_transforms:
                        - type: LogForTesting
                          input: M.nope
                """
            )
        }
        assertTrue("M.nope" in error.message!! && "不存在" in error.message!!, error.message)
    }

    @Test
    fun `extraTransform 引用不存在的节点时报错`() {
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
                    - type: chain
                      name: Stage
                      input: Source
                      transforms:
                        - type: MapToFields
                          name: M
                          input: input
                          config:
                            fields:
                              id: id
                      extra_transforms:
                        - type: LogForTesting
                          input: Ghost
                """
            )
        }
        assertTrue("Ghost" in error.message!! && "不存在的节点" in error.message!!, error.message)
    }

    @Test
    fun `extraTransform 需要输入但没声明时报错`() {
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
                    - type: chain
                      name: Stage
                      input: Source
                      transforms:
                        - type: MapToFields
                          name: M
                          input: input
                          config:
                            fields:
                              id: id
                      extra_transforms:
                        - type: LogForTesting
                """
            )
        }
        assertTrue("需要输入" in error.message!!, error.message)
    }

    @Test
    fun `extraTransform 声明了 error_handling 但类型不支持时报错`() {
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
                    - type: chain
                      name: Stage
                      input: Source
                      transforms:
                        - type: MapToFields
                          name: M
                          input: input
                          config:
                            fields:
                              id: id
                      extra_transforms:
                        - type: Create
                          config:
                            error_handling:
                              output: dead
                            elements:
                              - { id: 1 }
                """
            )
        }
        assertTrue("error_handling" in error.message!! && "errors" in error.message!!, error.message)
    }

    // ---------- source / sink 简写与复合输出 ----------

    @Test
    fun `source 与 sink 简写参与 chain 串联`() {
        val graph = run(
            """
            pipeline:
              type: chain
              source:
                type: Create
                config:
                  elements:
                    - { id: 1 }
              transforms:
                - type: LogForTesting
              sink:
                type: AssertEqual
                config:
                  elements:
                    - { id: 1 }
            """
        )

        assertEquals(listOf("Create", "LogForTesting", "AssertEqual"), graph.nodes.map { it.name })
    }

    @Test
    fun `composite 用 output 指定对外输出`() {
        run(
            """
            pipeline:
              transforms:
                - type: Create
                  name: Source
                  config:
                    elements:
                      - { id: 1, name: "a" }
                - type: composite
                  name: Pick
                  input: Source
                  output: Renamed
                  transforms:
                    - type: MapToFields
                      name: Renamed
                      input: input
                      config:
                        fields:
                          order_id: id
                - type: AssertEqual
                  input: Pick
                  config:
                    elements:
                      - { order_id: 1 }
            """
        )
    }

    @Test
    fun `嵌套 composite 内部用 input 引用复合节点自身的输入`() {
        run(
            """
            pipeline:
              transforms:
                - type: Create
                  name: Source
                  config:
                    elements:
                      - { id: 1 }
                - type: composite
                  name: PassThrough
                  input: Source
                  output: Log
                  transforms:
                    - type: LogForTesting
                      name: Log
                      input: input
                - type: AssertEqual
                  input: PassThrough
                  config:
                    elements:
                      - { id: 1 }
            """
        )
    }

    @Test
    fun `嵌套作用域各自独立，同名节点互不冲突`() {
        val graph = run(
            """
            pipeline:
              transforms:
                - type: Create
                  name: Source
                  config:
                    elements:
                      - { id: 1 }
                - type: chain
                  name: First
                  input: Source
                  transforms:
                    - type: LogForTesting
                      name: Step
                - type: chain
                  name: Second
                  input: First
                  transforms:
                    - type: LogForTesting
                      name: Step
                - type: AssertEqual
                  input: Second
                  config:
                    elements:
                      - { id: 1 }
            """
        )

        // 两个同名 Step 分处不同作用域，Beam 侧的全名带上了作用域前缀
        assertEquals(2, graph.nodes.count { it.name == "Step" })
    }

    // ---------- 窗口 ----------

    @Test
    fun `pipeline 级窗口作用于根节点的输出`() {
        val graph = build(
            """
            pipeline:
              type: chain
              windowing:
                type: fixed
                size: 60s
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1 }
                - type: LogForTesting
                  name: Log
            """
        )

        assertIs<FixedWindows>(windowFnOf(graph, "Create"))
        // 下游继承上游的窗口策略
        assertIs<FixedWindows>(windowFnOf(graph, "Log"))
    }

    @Test
    fun `transform 级窗口覆盖 pipeline 级`() {
        val graph = build(
            """
            pipeline:
              type: chain
              windowing:
                type: fixed
                size: 60s
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1 }
                  windowing:
                    type: sliding
                    size: 60s
                    period: 10s
                - type: LogForTesting
                  name: Log
            """
        )

        assertIs<SlidingWindows>(windowFnOf(graph, "Create"))
    }

    @Test
    fun `没有窗口声明时保持全局窗口`() {
        val graph = build(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1 }
            """
        )

        assertIs<GlobalWindows>(windowFnOf(graph, "Create"))
    }

    @Test
    fun `窗口作用于输入后 schema 不丢`() {
        val graph = build(
            """
            pipeline:
              transforms:
                - type: Create
                  name: Source
                  config:
                    elements:
                      - { id: 1, name: "a" }
                - type: LogForTesting
                  name: Log
                  input: Source
                  windowing:
                    type: fixed
                    size: 60s
            """
        )

        val output = graph.nodes.single { it.name == "Log" }.outputs.getValue("output")
        assertTrue(output.hasSchema())
        assertEquals(listOf("id", "name"), output.schema.fieldNames)
        assertIs<FixedWindows>(output.windowingStrategy.windowFn)
    }

    private fun windowFnOf(graph: PipelineGraph, node: String) =
        graph.nodes.single { it.name == node }.outputs.getValue("output").windowingStrategy.windowFn

    private fun build(yaml: String) = HData(PipelineSpecLoader.parse(yaml.trimIndent(), SpecMappers.YAML, "test"))
        .build(PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create())
        .second

    private fun run(yaml: String) = HData(PipelineSpecLoader.parse(yaml.trimIndent(), SpecMappers.YAML, "test"))
        .build(PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create())
        .also { it.first.run().waitUntilFinish() }
        .second
}
