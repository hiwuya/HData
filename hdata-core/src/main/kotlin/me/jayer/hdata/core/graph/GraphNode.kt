package me.jayer.hdata.core.graph

import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.Row

/**
 * A node in the graph-construction result, mainly used for `--dryRun` printing and error localization.
 *
 * @author wuya
 * @date 2022-08-30
 */
class GraphNode(
    val name: String,
    val type: String,
    /** Input port -> reference string. */
    val inputs: Map<String, String>,
    val outputs: Map<String, PCollection<Row>>,
    /** The output port that a tag-less reference (`Foo`) points to; null for a write side. */
    val mainOutput: String?,
    /** The alias declared by `error_handling.output`. */
    val errorAlias: String?,
) {
    fun describe(): String {
        val from = if (inputs.isEmpty()) "-" else inputs.entries.joinToString(", ") { (port, ref) ->
            if (port.isBlank()) ref else "$port=$ref"
        }
        val to = if (outputs.isEmpty()) "-" else outputs.keys.joinToString(", ")
        return "$name [$type] inputs($from) outputs($to)"
    }
}

/**
 * The whole graph; [nodes] are ordered by actual construction order.
 */
class PipelineGraph(val nodes: List<GraphNode>) {
    fun describe(): String = nodes.joinToString("\n") { "  ${it.describe()}" }
}
