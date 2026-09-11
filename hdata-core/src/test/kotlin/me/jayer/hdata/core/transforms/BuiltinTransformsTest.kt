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
 * Behavior and error paths of the built-in transforms.
 *
 * Behavior assertions are written into the pipeline file itself (`AssertEqual`) and run on
 * DirectRunner; error paths only build the graph without running it.
 *
 * @author wuya
 * @date 2022-08-30
 */
class BuiltinTransformsTest {

    // ---------- MapToFields ----------

    @Test
    fun `MapToFields renames and keeps only the listed fields`() {
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
    fun `MapToFields keeps the source field's type and nullability`() {
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
    fun `MapToFields' append keeps every input field and adds on top`() {
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
    fun `MapToFields' drop subtracts fields together with append`() {
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
    fun `MapToFields' fields can override a same-named field brought in by append`() {
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
    fun `MapToFields referencing a nonexistent field errors out and lists the available fields`() {
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
    fun `MapToFields' drop referencing a nonexistent field errors out`() {
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
    fun `MapToFields' drop is meaningless without append and errors out directly`() {
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
    fun `MapToFields with neither fields nor drop errors out`() {
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
    fun `MapToFields rejects a blank field name and a duplicate drop`() {
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
    fun `Create with an empty elements errors out`() {
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
    fun `Create with inconsistent record types errors out`() {
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
    fun `Flatten merges several inputs and keeps the schema`() {
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
    fun `Flatten with inconsistent schemas across inputs errors out`() {
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
        assertTrue("must all have the same schema" in error.message!!)
    }

    @Test
    fun `Flatten does not accept config`() {
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
        assertTrue("does not accept any config" in error.message!!)
    }

    // ---------- StripErrorMetadata ----------

    @Test
    fun `StripErrorMetadata errors out when the input is not a dead-letter stream`() {
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
        assertTrue("is not a dead-letter stream" in error.message!!)
    }

    // ---------- LogForTesting ----------

    @Test
    fun `LogForTesting passes rows through unchanged with the same schema`() {
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
    fun `LogForTesting with an invalid level errors out`() {
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
    fun `AssertEqual disregards order`() {
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
    fun `the job fails when AssertEqual does not match`() {
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
    fun `AssertEqual errors out when an expected field does not match the input schema`() {
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
