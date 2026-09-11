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
    fun `input semantics of chain vs composite`() {
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

        // Omitting type at the top level is inferred as composite because it has child nodes
        assertEquals(TransformSpec.COMPOSITE, spec.pipeline.kind)
        val flatten = spec.pipeline.transforms[2]
        assertEquals(mapOf("0" to "Left", "1" to "Right"), flatten.inputRefs())
        assertEquals(mapOf("" to "Flatten"), spec.pipeline.transforms[3].inputRefs())
    }

    @Test
    fun `only accepts a yaml file`() {
        assertEquals(SpecMappers.YAML, SpecMappers.forFile(File("job.yaml")))
        assertEquals(SpecMappers.YAML, SpecMappers.forFile(File("job.YML")))

        val error = assertFailsWith<HDataException> { SpecMappers.forFile(File("job.toml")) }
        assertTrue(".toml" in error.message!! && ".yaml" in error.message!!)
    }

    @Test
    fun `variable substitution supports a default and prefers an explicit variable`() {
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
    fun `an undefined variable errors out directly`() {
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
    fun `a table-name range expression is not mistaken for a variable`() {
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
    fun `declaring input inside a chain is rejected`() {
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
    fun `a misspelled field is rejected instead of silently ignored`() {
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
    fun `source and sink shorthand expand into child nodes`() {
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
    fun `the top-level pipeline must be chain or composite`() {
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
    fun `an empty transforms errors out`() {
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
    fun `a transform missing the type field errors out`() {
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
    fun `a non-composite node declaring output errors out`() {
        // Only composite can expose a child node externally via output; a plain node's output must be referenced as Node.<tag>
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
    fun `a missing file errors out directly`() {
        val error = assertFailsWith<HDataException> {
            PipelineSpecLoader.load(java.io.File("does-not-exist.yaml"))
        }
        assertTrue("does not exist" in error.message!!)
    }

    @Test
    fun `invalid YAML fails to parse and errors out`() {
        val error = assertFailsWith<HDataException> {
            PipelineSpecLoader.parse("pipeline: [unclosed", SpecMappers.YAML, "test")
        }
        assertTrue("failed to parse" in error.message!!)
    }
}
