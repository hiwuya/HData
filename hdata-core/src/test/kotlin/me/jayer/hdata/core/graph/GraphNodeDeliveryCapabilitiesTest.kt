package me.jayer.hdata.core.graph

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryCapabilities
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.test.assertFailsWith

/**
 * [GraphNode.describe] and [PipelineGraph.undeclaredDeliveryCapabilities]/[PipelineGraph.undeclaredSupportTier]
 * surface the delivery contract and connector support tier in `--dryRun` output and normal run logs
 * (critical gap #2 in docs/MATURITY_ASSESSMENT.md: "Emit the resolved contract in `--dryRun` output"; Phase 0
 * item 3: connector support tiers). Constructs [GraphNode]s directly rather than through a full pipeline
 * build, since this behavior does not depend on any specific connector.
 */
class GraphNodeDeliveryCapabilitiesTest {

    private fun node(
        name: String,
        caps: DeliveryCapabilities? = null,
        tier: ConnectorSupportTier? = null,
    ) = GraphNode(
        name = name,
        type = "TestType",
        inputs = emptyMap(),
        outputs = emptyMap(),
        mainOutput = null,
        errorAlias = null,
        deliveryCapabilities = caps,
        supportTier = tier,
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

    @Test
    fun `describe appends the tier alongside the delivery contract`() {
        val description = node("a", declared, ConnectorSupportTier.EXPERIMENTAL).describe()
        assertTrue(description.contains("tier=EXPERIMENTAL"), description)
        assertTrue(description.contains("delivery=AT_LEAST_ONCE"), description)
    }

    @Test
    fun `describe omits the tier when none is declared`() {
        val description = node("a").describe()
        assertTrue(!description.contains("tier="), "should not print an undeclared tier: $description")
    }

    @Test
    fun `undeclaredSupportTier lists only nodes without a declared tier`() {
        val graph = PipelineGraph(
            listOf(node("classified", tier = ConnectorSupportTier.QUALIFIED), node("unclassified")),
        )
        val undeclared = graph.undeclaredSupportTier()
        assertEquals(listOf("unclassified"), undeclared.map { it.name })
    }

    @Test
    fun `full replay source to non-idempotent sink is rejected`() {
        val source = node("source", DeliveryCapabilities(DeliveryMode.AT_LEAST_ONCE, ReplayBehavior.FULL_REPLAY, OrderingScope.NONE))
        val sink = GraphNode("sink", "Write", mapOf("input" to "source"), emptyMap(), null, null,
            DeliveryCapabilities(DeliveryMode.AT_LEAST_ONCE, ReplayBehavior.NOT_APPLICABLE, OrderingScope.NONE, requiresIdempotencyKey = true))
        val error = assertFailsWith<HDataException> { PipelineGraph(listOf(source, sink)).validateDeliveryCompatibility() }
        assertTrue(error.message!!.contains("Unsafe delivery path"))
    }

    @Test
    fun `idempotent sink accepts a replaying source`() {
        val source = node("source", DeliveryCapabilities(DeliveryMode.AT_LEAST_ONCE, ReplayBehavior.FULL_REPLAY, OrderingScope.NONE))
        val sink = GraphNode("sink", "Write", mapOf("input" to "source"), emptyMap(), null, null,
            DeliveryCapabilities(DeliveryMode.AT_LEAST_ONCE, ReplayBehavior.NOT_APPLICABLE, OrderingScope.NONE))
        PipelineGraph(listOf(source, sink)).validateDeliveryCompatibility()
    }
}
