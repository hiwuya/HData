package me.jayer.hdata.core.graph

import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.exception.HDataException
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

    /** Reject a full-replay source path feeding a sink that declares retries unsafe without deduplication. */
    fun validateDeliveryCompatibility() {
        val byName = nodes.associateBy { it.name }
        val upstream = nodes.associateWith { node ->
            node.inputs.values.map { it.substringBefore('.') }.mapNotNull(byName::get)
        }
        nodes.filter { it.deliveryCapabilities?.requiresIdempotencyKey == true }.forEach { sink ->
            val source = findFullReplaySource(sink, upstream, mutableSetOf())
            if (source != null) throw HDataException(
                "Unsafe delivery path: ${source.name} [${source.type}] can fully replay after a restart but " +
                    "${sink.name} [${sink.type}] requires an idempotency key. Add a replay-safe sink configuration " +
                    "or deduplicate before this sink.",
            )
        }
    }

    private fun findFullReplaySource(node: GraphNode, upstream: Map<GraphNode, List<GraphNode>>, visited: MutableSet<String>): GraphNode? {
        if (!visited.add(node.name)) return null
        if (node.inputs.isEmpty() && node.deliveryCapabilities?.replayBehavior == ReplayBehavior.FULL_REPLAY) return node
        return upstream[node].orEmpty().firstNotNullOfOrNull { findFullReplaySource(it, upstream, visited) }
    }
}
