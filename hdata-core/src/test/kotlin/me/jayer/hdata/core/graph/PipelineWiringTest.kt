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
 * Wiring details during graph construction: port validation, composite node output, window propagation.
 *
 * @author wuya
 * @date 2022-08-30
 */
class PipelineWiringTest {

    // ---------- port validation ----------

    @Test
    fun `a source explicitly wired to an input errors out`() {
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
        assertTrue("is a source and accepts no input" in error.message!!)
    }

    @Test
    fun `a processing node with no input errors out`() {
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
        assertTrue("requires an input" in error.message!!)
    }

    @Test
    fun `referencing a nonexistent output port lists the available ports`() {
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
    fun `duplicate node names are rejected with a hint to use name`() {
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
    fun `a node name rejects the reference separator and a blank string`() {
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
            assertTrue(
                "node name" in error.message!! || "must have a non-blank name" in error.message!!,
                error.message,
            )
        }
    }

    @Test
    fun `an extra_transforms node sharing a name with a main node is rejected before graph construction`() {
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
        assertTrue("Same" in error.message!! && "multiple nodes share the name" in error.message!!, error.message)
    }

    @Test
    fun `input and inputs cannot both be declared`() {
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
        assertTrue("cannot declare both input and inputs" in error.message!!)
    }

    // ---------- extraTransforms (a side input) go through the same port validation ----------

    @Test
    fun `an extraTransform referencing a nonexistent output port errors out`() {
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
        assertTrue("M.nope" in error.message!! && "does not exist" in error.message!!, error.message)
    }

    @Test
    fun `an extraTransform referencing a nonexistent node errors out`() {
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
        assertTrue("Ghost" in error.message!! && "non-existent node" in error.message!!, error.message)
    }

    @Test
    fun `an extraTransform that needs an input but declares none errors out`() {
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
        assertTrue("requires an input" in error.message!!, error.message)
    }

    @Test
    fun `an extraTransform declaring error_handling on an unsupported type errors out`() {
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

    // ---------- source / sink shorthand and composite output ----------

    @Test
    fun `source and sink shorthand participate in a chain`() {
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
    fun `composite uses output to designate its external output`() {
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
    fun `inside a nested composite, input refers to the composite node's own input`() {
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
    fun `nested scopes are independent, same-named nodes do not conflict`() {
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

        // The two same-named Step nodes live in different scopes; Beam's own full name carries a scope prefix
        assertEquals(2, graph.nodes.count { it.name == "Step" })
    }

    // ---------- windowing ----------

    @Test
    fun `a pipeline-level window applies to the root node's output`() {
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
        // The downstream node inherits the upstream node's windowing strategy
        assertIs<FixedWindows>(windowFnOf(graph, "Log"))
    }

    @Test
    fun `a transform-level window overrides the pipeline-level one`() {
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
    fun `stays in the global window when nothing is declared`() {
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
    fun `applying a window to an input does not lose the schema`() {
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
