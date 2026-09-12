package me.jayer.hdata.core.graph

import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * [GraphNode.describe] and [PipelineGraph.undeclaredDeliveryCapabilities] surface the delivery contract in
 * `--dryRun` output and normal run logs (critical gap #2 in docs/MATURITY_ASSESSMENT.md: "Emit the resolved
 * contract in `--dryRun` output"). Constructs [GraphNode]s directly rather than through a full pipeline
 * build, since this behavior does not depend on any specific connector.
 */
class GraphNodeDeliveryCapabilitiesTest {

    private fun node(name: String, caps: DeliveryCapabilities? = null) = GraphNode(
        name = name,
        type = "TestType",
        inputs = emptyMap(),
        outputs = emptyMap(),
        mainOutput = null,
        errorAlias = null,
        deliveryCapabilities = caps,
    )

    private val declared = DeliveryCapabilities(
        deliveryMode = DeliveryMode.AT_LEAST_ONCE,
        replayBehavior = ReplayBehavior.RESUMABLE,
        ordering = OrderingScope.PER_KEY,
        requiresIdempotencyKey = true,
        notes = "example",
    )

    @Test
    fun `describe omits the delivery suffix when no contract is declared`() {
        val description = node("a").describe()
        assertTrue(!description.contains("delivery="), "should not print an empty contract: $description")
    }

    @Test
    fun `describe appends the contract summary when one is declared`() {
        val description = node("a", declared).describe()
        assertTrue(description.contains("delivery=AT_LEAST_ONCE"), description)
        assertTrue(description.contains("replay=RESUMABLE"), description)
        assertTrue(description.contains("order=PER_KEY"), description)
        assertTrue(description.contains("requires_idempotency_key"), description)
        assertTrue(description.contains("(example)"), description)
    }

    @Test
    fun `undeclaredDeliveryCapabilities lists only nodes without a declared contract`() {
        val graph = PipelineGraph(listOf(node("has-contract", declared), node("no-contract")))
        val undeclared = graph.undeclaredDeliveryCapabilities()
        assertEquals(listOf("no-contract"), undeclared.map { it.name })
    }
}
