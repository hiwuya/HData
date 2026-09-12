package me.jayer.hdata.core.graph

import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryCapabilities
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
    /** This node's declared delivery contract ([DeliveryCapabilities]); null if not yet declared. */
    val deliveryCapabilities: DeliveryCapabilities? = null,
    /** This node's declared [ConnectorSupportTier]; null if not yet classified. */
    val supportTier: ConnectorSupportTier? = null,
) {
    fun describe(): String {
        val from = if (inputs.isEmpty()) "-" else inputs.entries.joinToString(", ") { (port, ref) ->
            if (port.isBlank()) ref else "$port=$ref"
        }
        val to = if (outputs.isEmpty()) "-" else outputs.keys.joinToString(", ")
        val base = "$name [$type] inputs($from) outputs($to)"
        val extras = buildList {
            if (supportTier != null) add("tier=$supportTier")
            if (deliveryCapabilities != null) add(deliveryCapabilities.describe())
        }
        return if (extras.isEmpty()) base else "$base [${extras.joinToString("; ")}]"
    }
}

/**
 * The whole graph; [nodes] are ordered by actual construction order.
 */
class PipelineGraph(val nodes: List<GraphNode>) {
    fun describe(): String = nodes.joinToString("\n") { "  ${it.describe()}" }

    /**
     * Nodes with no declared [DeliveryCapabilities] — a pipeline author cannot yet learn this node's crash
     * behavior from the graph alone. Surfaced separately from [describe] so `--dryRun` output can call this
     * out instead of silently omitting it.
     */
    fun undeclaredDeliveryCapabilities(): List<GraphNode> = nodes.filter { it.deliveryCapabilities == null }

    /** Nodes with no declared [ConnectorSupportTier] — see [undeclaredDeliveryCapabilities] for the rationale. */
    fun undeclaredSupportTier(): List<GraphNode> = nodes.filter { it.supportTier == null }
}
