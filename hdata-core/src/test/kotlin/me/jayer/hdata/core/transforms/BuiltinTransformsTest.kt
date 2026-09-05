package me.jayer.hdata.core.transforms

import me.jayer.hdata.core.HData
import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.PipelineSpecLoader
import me.jayer.hdata.core.spec.SpecMappers
import org.apache.beam.sdk.options.PipelineOptionsFactory
import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 内置 transform 的行为与错误路径。
 *
 * 行为断言写在 pipeline 文件里（`AssertEqual`），跑在 DirectRunner 上；
 * 错误路径只构图不运行。
 *
 * @author wuya
 * @date 2022-08-30
 */
class BuiltinTransformsTest {

    // ---------- MapToFields ----------

    @Test
    fun `MapToFields 改名并只保留列出的字段`() {
        val graph = run(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1, name: "a", junk: "x" }
                - type: MapToFields
                  name: Renamed
                  config:
                    fields:
                      order_id: id
                      title: name
                - type: AssertEqual
                  config:
                    elements:
                      - { order_id: 1, title: "a" }
            """
        )

        val schema = graph.nodes.single { it.name == "Renamed" }.outputs.getValue("output").schema
        assertEquals(listOf("order_id", "title"), schema.fieldNames)
    }

    @Test
    fun `MapToFields 保留源字段的类型与可空性`() {
        val graph = build(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1, name: "a" }
                      - { id: 2, name: null }
                - type: MapToFields
                  name: Renamed
                  config:
                    fields:
                      order_id: id
                      title: name
            """
        )

        val schema = graph.nodes.single { it.name == "Renamed" }.outputs.getValue("output").schema
        assertEquals(Schema.FieldType.INT64, schema.getField("order_id").type)
        assertEquals(Schema.FieldType.STRING.withNullable(true), schema.getField("title").type)
    }

    @Test
    fun `MapToFields 的 append 保留全部输入字段再叠加`() {
        run(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1, name: "a" }
                - type: MapToFields
                  config:
                    append: true
                    fields:
                      alias: name
                - type: AssertEqual
                  config:
                    elements:
                      - { id: 1, name: "a", alias: "a" }
            """
        )
    }

    @Test
    fun `MapToFields 的 drop 配合 append 做减法`() {
        run(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1, name: "a", secret: "s" }
                - type: MapToFields
                  config:
                    append: true
                    drop: [secret]
                - type: AssertEqual
                  config:
                    elements:
                      - { id: 1, name: "a" }
            """
        )
    }

    @Test
    fun `MapToFields 的 fields 可以覆盖 append 带进来的同名字段`() {
        run(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1, name: "a" }
                - type: MapToFields
                  config:
                    append: true
                    fields:
                      name: id
                - type: AssertEqual
                  config:
                    elements:
                      - { id: 1, name: 1 }
            """
        )
    }

    @Test
    fun `MapToFields 引用不存在的字段时报错并列出可用字段`() {
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
                    - type: MapToFields
                      config:
                        fields:
                          x: nope
                """
            )
        }
        assertTrue("nope" in error.message!! && "id" in error.message!!)
    }

    @Test
    fun `MapToFields 的 drop 引用不存在的字段时报错`() {
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
                    - type: MapToFields
                      config:
                        append: true
                        drop: [nope]
                """
            )
        }
        assertTrue("nope" in error.message!!)
    }

    @Test
    fun `MapToFields 的 drop 离开 append 无意义，直接报错`() {
        val error = assertFailsWith<IllegalArgumentException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config:
                        elements:
                          - { id: 1 }
                    - type: MapToFields
                      config:
                        fields:
                          a: id
                        drop: [id]
                """
            )
        }
        assertTrue("append" in error.message!!)
    }

    @Test
    fun `MapToFields 既没有 fields 也没有 drop 时报错`() {
        assertFailsWith<IllegalArgumentException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config:
                        elements:
                          - { id: 1 }
                    - type: MapToFields
                """
            )
        }
    }

    @Test
    fun `MapToFields 拒绝空字段名与重复 drop`() {
        assertFailsWith<IllegalArgumentException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config: { elements: [{ id: 1 }] }
                    - type: MapToFields
                      config:
                        fields: { x: "" }
                """
            )
        }
        assertFailsWith<IllegalArgumentException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config: { elements: [{ id: 1 }] }
                    - type: MapToFields
                      config:
                        append: true
                        drop: [id, id]
                """
            )
        }
    }

    // ---------- Create ----------

    @Test
    fun `Create 的 elements 为空时报错`() {
        val error = assertFailsWith<IllegalArgumentException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config:
                        elements: []
                """
            )
        }
        assertTrue("elements" in error.message!!)
    }

    @Test
    fun `Create 的记录类型不一致时报错`() {
        assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config:
                        elements:
                          - { id: 1 }
                          - { id: "x" }
                """
            )
        }
    }

    // ---------- Flatten ----------

    @Test
    fun `Flatten 合并多路并保留 schema`() {
        run(
            """
            pipeline:
              transforms:
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
                - type: Flatten
                  name: Merged
                  input: {a: Left, b: Right}
                - type: AssertEqual
                  input: Merged
                  config:
                    elements:
                      - { id: 1 }
                      - { id: 2 }
            """
        )
    }

    @Test
    fun `Flatten 各路 schema 不一致时报错`() {
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  transforms:
                    - type: Create
                      name: Left
                      config:
                        elements:
                          - { id: 1 }
                    - type: Create
                      name: Right
                      config:
                        elements:
                          - { name: "a" }
                    - type: Flatten
                      input: [Left, Right]
                """
            )
        }
        assertTrue("schema 必须一致" in error.message!!)
    }

    @Test
    fun `Flatten 不接受 config`() {
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  transforms:
                    - type: Create
                      name: Left
                      config:
                        elements:
                          - { id: 1 }
                    - type: Flatten
                      input: [Left]
                      config:
                        whatever: 1
                """
            )
        }
        assertTrue("不接受任何 config" in error.message!!)
    }

    // ---------- StripErrorMetadata ----------

    @Test
    fun `StripErrorMetadata 的输入不是死信流时报错`() {
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
                    - type: StripErrorMetadata
                """
            )
        }
        assertTrue("不是死信流" in error.message!!)
    }

    // ---------- LogForTesting ----------

    @Test
    fun `LogForTesting 原样透传且 schema 不变`() {
        run(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1, name: "a" }
                - type: LogForTesting
                  config:
                    level: debug
                    prefix: "seen: "
                - type: AssertEqual
                  config:
                    elements:
                      - { id: 1, name: "a" }
            """
        )
    }

    @Test
    fun `LogForTesting 的 level 非法时报错`() {
        val error = assertFailsWith<IllegalArgumentException> {
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
                      config:
                        level: VERBOSE
                """
            )
        }
        assertTrue("VERBOSE" in error.message!!)
    }

    // ---------- AssertEqual ----------

    @Test
    fun `AssertEqual 不计顺序`() {
        run(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1 }
                      - { id: 2 }
                - type: AssertEqual
                  config:
                    elements:
                      - { id: 2 }
                      - { id: 1 }
            """
        )
    }

    @Test
    fun `AssertEqual 对不上时作业失败`() {
        assertFailsWith<Throwable> {
            run(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config:
                        elements:
                          - { id: 1 }
                    - type: AssertEqual
                      config:
                        elements:
                          - { id: 999 }
                """
            )
        }
    }

    @Test
    fun `AssertEqual 的期望值字段与输入 schema 对不上时报错`() {
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
                    - type: AssertEqual
                      config:
                        elements:
                          - { nope: 1 }
                """
            )
        }
        assertTrue("nope" in error.message!!)
    }

    private fun build(yaml: String) = HData(PipelineSpecLoader.parse(yaml.trimIndent(), SpecMappers.YAML, "test"))
        .build(PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create())
        .second

    private fun run(yaml: String) = HData(PipelineSpecLoader.parse(yaml.trimIndent(), SpecMappers.YAML, "test"))
        .build(PipelineOptionsFactory.fromArgs("--runner=DirectRunner").create())
        .also { it.first.run().waitUntilFinish() }
        .second
}
