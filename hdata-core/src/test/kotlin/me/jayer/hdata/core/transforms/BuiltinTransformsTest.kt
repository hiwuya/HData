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

    // ---------- AddFields ----------

    @Test
    fun `AddFields appends typed constants without changing the input fields`() {
        run(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config: { elements: [{ id: 1, name: "a" }] }
                - type: AddFields
                  config:
                    fields: { source_system: orders, replay: false, priority: 3 }
                - type: AssertEqual
                  config:
                    elements:
                      - { id: 1, name: "a", source_system: orders, replay: false, priority: 3 }
            """
        )
    }

    @Test
    fun `AddFields rejects an existing field instead of silently overwriting it`() {
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config: { elements: [{ id: 1 }] }
                    - type: AddFields
                      config: { fields: { id: 2 } }
                """
            )
        }
        assertTrue("id" in error.message!!)
    }

    // ---------- Filter ----------

    @Test
    fun `Filter supports typed equality and membership predicates`() {
        run(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1, op: c }
                      - { id: 2, op: u }
                      - { id: 3, op: d }
                - type: Filter
                  name: KeepIds
                  config: { field: id, operator: in, values: [1, 3] }
                - type: Filter
                  name: DropDeletes
                  config: { field: op, operator: not_equals, value: d }
                - type: AssertEqual
                  config: { elements: [{ id: 1, op: c }] }
            """
        )
    }

    @Test
    fun `Filter supports null predicates and rejects invalid field references`() {
        run(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config: { elements: [{ id: 1, deleted_at: null }, { id: 2, deleted_at: "now" }] }
                - type: Filter
                  config: { field: deleted_at, operator: is_null }
                - type: AssertEqual
                  config: { elements: [{ id: 1, deleted_at: null }] }
            """
        )
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config: { elements: [{ id: 1 }] }
                    - type: Filter
                      config: { field: missing, value: 1 }
                """
            )
        }
        assertTrue("missing" in error.message!!)
    }

    // ---------- Explode ----------

    @Test
    fun `Explode expands each collection element and preserves the original collection when named separately`() {
        run(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config:
                    elements:
                      - { id: 1, tags: [a, b] }
                      - { id: 2, tags: [] }
                - type: Explode
                  config: { field: tags, output_field: tag }
                - type: AssertEqual
                  config:
                    elements:
                      - { id: 1, tags: [a, b], tag: a }
                      - { id: 1, tags: [a, b], tag: b }
            """
        )
    }

    @Test
    fun `Explode replaces the collection field by default and rejects scalar fields`() {
        run(
            """
            pipeline:
              type: chain
              transforms:
                - type: Create
                  config: { elements: [{ id: 1, tags: [a, b] }] }
                - type: Explode
                  config: { field: tags }
                - type: AssertEqual
                  config: { elements: [{ id: 1, tags: a }, { id: 1, tags: b }] }
            """
        )
        val error = assertFailsWith<HDataException> {
            build(
                """
                pipeline:
                  type: chain
                  transforms:
                    - type: Create
                      config: { elements: [{ id: 1 }] }
                    - type: Explode
                      config: { field: id }
                """
            )
        }
        assertTrue("ARRAY" in error.message!!)
    }

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
