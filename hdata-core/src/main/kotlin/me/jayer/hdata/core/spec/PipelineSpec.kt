package me.jayer.hdata.core.spec

import tools.jackson.databind.JsonNode

/**
 * Top-level structure of the pipeline file, aligned with Beam YAML:
 *
 * ```yaml
 * pipeline:
 *   type: chain
 *   transforms:
 *     - type: ReadFromJdbc
 *       config: { ... }
 * options:
 *   runner: DirectRunner
 * execution:
 *   mode: streaming
 * ```
 *
 * [pipeline] itself is a composite/chain-form [TransformSpec], so the top level and nested
 * composite transforms share the same semantics; there is no need for a separate three-part
 * sources/transforms/sinks model.
 *
 * @author wuya
 * @date 2022-08-30
 */
data class PipelineSpec(
    /** Version of the HData YAML pipeline contract. Missing means the stable version 1. */
    val formatVersion: Int = CURRENT_FORMAT_VERSION,
    val pipeline: TransformSpec,
    /** Execution intent. `auto` infers streaming when an unbounded source is present. */
    val execution: ExecutionSpec = ExecutionSpec(),
    /** Options passed through to Beam's [org.apache.beam.sdk.options.PipelineOptions]; command-line args take higher priority. */
    val options: Map<String, JsonNode> = emptyMap(),
) {
    companion object {
        const val CURRENT_FORMAT_VERSION: Int = 1
    }
}
