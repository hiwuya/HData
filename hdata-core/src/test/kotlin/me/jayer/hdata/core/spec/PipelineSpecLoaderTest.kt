package me.jayer.hdata.core.spec

import me.jayer.hdata.core.exception.HDataException
import java.io.File
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * @author wuya
 * @date 2022-08-30
 */
class PipelineSpecLoaderTest {

    @Test
    fun `chain 与 composite 的输入语义`() {
        val spec = PipelineSpecLoader.parse(
            """
            pipeline:
              transforms:
                - type: Create
                  name: Left
                - type: Create
                  name: Right
                - type: Flatten
                  input: [Left, Right]
                - type: LogForTesting
                  input: Flatten
            """.trimIndent(),
            SpecMappers.YAML,
            "test",
        )

        // 顶层省略 type 时按含子节点推断为 composite
        assertEquals(TransformSpec.COMPOSITE, spec.pipeline.kind)
        val flatten = spec.pipeline.transforms[2]
        assertEquals(mapOf("0" to "Left", "1" to "Right"), flatten.inputRefs())
        assertEquals(mapOf("" to "Flatten"), spec.pipeline.transforms[3].inputRefs())
    }

    @Test
    fun `只接受 yaml 文件`() {
        assertEquals(SpecMappers.YAML, SpecMappers.forFile(File("job.yaml")))
        assertEquals(SpecMappers.YAML, SpecMappers.forFile(File("job.YML")))

        val error = assertFailsWith<HDataException> { SpecMappers.forFile(File("job.toml")) }
        assertTrue(".toml" in error.message!! && ".yaml" in error.message!!)
    }

    @Test
    fun `变量替换支持默认值并优先取显式变量`() {
        val spec = PipelineSpecLoader.parse(
            """
            pipeline:
              type: chain
              transforms:
                - type: WriteToJdbc
                  config:
                    password: ${'$'}{DB_PASSWORD}
                    table: ${'$'}{DB_TABLE:-t_default}
            """.trimIndent(),
            SpecMappers.YAML,
            "test",
            mapOf("DB_PASSWORD" to "s3cret"),
        )

        val config = spec.pipeline.transforms.single().configNode()
        assertEquals("s3cret", config.get("password").stringValue())
        assertEquals("t_default", config.get("table").stringValue())
    }

    @Test
    fun `未定义的变量直接报错`() {
        val error = assertFailsWith<HDataException> {
            PipelineSpecLoader.parse(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: WriteToJdbc
                      config:
                        password: ${'$'}{NOT_DEFINED_ANYWHERE}
                """.trimIndent(),
                SpecMappers.YAML,
                "test",
            )
        }
        assertTrue("NOT_DEFINED_ANYWHERE" in error.message!!)
    }

    @Test
    fun `表名区间语法不会被当成变量`() {
        val spec = PipelineSpecLoader.parse(
            """
            pipeline:
              type: chain
              transforms:
                - type: ReadFromJdbc
                  config:
                    tables: ["t_order_${'$'}{00-15}"]
            """.trimIndent(),
            SpecMappers.YAML,
            "test",
        )

        val tables = spec.pipeline.transforms.single().configNode().get("tables")
        assertEquals("t_order_\${00-15}", tables.get(0).stringValue())
    }

    @Test
    fun `chain 内声明 input 会被拒绝`() {
        val error = assertFailsWith<HDataException> {
            PipelineSpecLoader.parse(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      name: Source
                    - type: LogForTesting
                      input: Source
                """.trimIndent(),
                SpecMappers.YAML,
                "test",
            )
        }
        assertTrue("chain" in error.message!!)
    }

    @Test
    fun `拼错的字段会被拒绝而不是静默忽略`() {
        val error = assertFailsWith<HDataException> {
            PipelineSpecLoader.parse(
                """
                pipeline:
                  type: chain
                  transfroms:
                    - type: Create
                """.trimIndent(),
                SpecMappers.YAML,
                "test",
            )
        }
        assertTrue("transfroms" in error.message!!)
    }

    @Test
    fun `source 与 sink 简写会展开成子节点`() {
        val spec = PipelineSpecLoader.parse(
            """
            pipeline:
              type: chain
              source:
                type: Create
              transforms:
                - type: LogForTesting
              sink:
                type: WriteToJdbc
            """.trimIndent(),
            SpecMappers.YAML,
            "test",
        )

        assertEquals(listOf("Create", "LogForTesting", "WriteToJdbc"), spec.pipeline.children().map { it.displayName })
        assertNull(spec.pipeline.output)
    }

    @Test
    fun `顶层 pipeline 必须是 chain 或 composite`() {
        val error = assertFailsWith<HDataException> {
            PipelineSpecLoader.parse(
                """
                pipeline:
                  type: bogus
                """.trimIndent(),
                SpecMappers.YAML,
                "test",
            )
        }
        assertTrue("bogus" in error.message!! && "composite" in error.message!!)
    }

    @Test
    fun `空 transforms 报错`() {
        val error = assertFailsWith<HDataException> {
            PipelineSpecLoader.parse(
                """
                pipeline:
                  type: chain
                  transforms: []
                """.trimIndent(),
                SpecMappers.YAML,
                "test",
            )
        }
        assertTrue("declares no transforms" in error.message!!)
    }

    @Test
    fun `transform 缺少 type 字段报错`() {
        val error = assertFailsWith<HDataException> {
            PipelineSpecLoader.parse(
                """
                pipeline:
                  type: chain
                  transforms:
                    - config:
                        elements: [{ id: 1 }]
                """.trimIndent(),
                SpecMappers.YAML,
                "test",
            )
        }
        assertTrue("type" in error.message!!)
    }

    @Test
    fun `非复合节点声明 output 报错`() {
        // 只有 composite 才能用 output 对外暴露子节点；普通节点要引用其输出得写 Node.<tag>
        val error = assertFailsWith<HDataException> {
            PipelineSpecLoader.parse(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      output: Foo
                """.trimIndent(),
                SpecMappers.YAML,
                "test",
            )
        }
        assertTrue("is not a composite node" in error.message!! && "output" in error.message!!)
    }

    @Test
    fun `文件不存在直接报错`() {
        val error = assertFailsWith<HDataException> {
            PipelineSpecLoader.load(java.io.File("does-not-exist.yaml"))
        }
        assertTrue("does not exist" in error.message!!)
    }

    @Test
    fun `非法 YAML 解析失败报错`() {
        val error = assertFailsWith<HDataException> {
            PipelineSpecLoader.parse("pipeline: [unclosed", SpecMappers.YAML, "test")
        }
        assertTrue("failed to parse" in error.message!!)
    }
}
