package me.jayer.hdata.core.spec

import tools.jackson.databind.JsonNode

/**
 * pipeline 文件的顶层结构，对齐 Beam YAML：
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
 * [pipeline] 本身就是一个 composite/chain 形态的 [TransformSpec]，因此顶层与嵌套
 * 复合 transform 共用同一套语义，不需要单独的 sources/transforms/sinks 三段式模型。
 *
 * @author wuya
 * @date 2022-08-30
 */
data class PipelineSpec(
    val pipeline: TransformSpec,
    /** 透传给 Beam [org.apache.beam.sdk.options.PipelineOptions] 的选项，命令行参数优先级更高。 */
    val options: Map<String, JsonNode> = emptyMap(),
)
