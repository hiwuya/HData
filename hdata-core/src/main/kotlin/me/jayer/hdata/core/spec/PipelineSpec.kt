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
    val pipeline: TransformSpec,
    /** Options passed through to Beam's [org.apache.beam.sdk.options.PipelineOptions]; command-line args take higher priority. */
    val options: Map<String, JsonNode> = emptyMap(),
)
