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
 * End-to-end on DirectRunner: pipeline file -> DAG -> actually run, with the assertion written into
 * the pipeline file itself.
 *
 * @author wuya
 * @date 2022-08-30
 */
class PipelineGraphBuilderTest {

    @Test
    fun `chain wires nodes in the order they are written`() {
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
    fun `composite supports branching and merging regardless of write order`() {
        val graph = run(
            """
            pipeline:
              transforms:
                # Deliberately writes the downstream node before its upstream, to verify topological sort kicks in
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
    fun `the dead-letter stream can be consumed by extra_transforms`() {
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
    fun `declaring error_handling with no consumer errors out`() {
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
    fun `a transform that does not support error_handling errors out`() {
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
        assertTrue("does not support error_handling" in error.message!!)
    }

    @Test
    fun `error_handling's threshold is not implemented yet, so declaring it errors out instead of silently doing nothing`() {
        // The docs say plainly that threshold is not implemented: if it were just accepted without erroring, a user
        // would believe they had configured an error-rate threshold when it actually does nothing at all. It must
        // be rejected outright at graph-construction time.
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
    fun `an empty error_handling output errors out`() {
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
    fun `a nested chain receives the outer node's output`() {
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
    fun `referencing a nonexistent node errors out and lists the available nodes`() {
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
    fun `a cycle is detected`() {
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
        assertTrue("contains a cycle" in error.message!!)
    }

    @Test
    fun `an unknown transform type lists what is registered`() {
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
