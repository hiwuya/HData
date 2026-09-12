package me.jayer.hdata.kafka

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals

/**
 * Pins down the declared [me.jayer.hdata.core.spi.DeliveryCapabilities] for Kafka read/write (see critical
 * gap #2 in docs/MATURITY_ASSESSMENT.md).
 */
class KafkaDeliveryCapabilitiesTest {

    private fun config(yaml: String) = TransformConfig("t", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    @Test
    fun `a bounded ReadFromKafka (default scan_bounded_mode) is full-replay`() {
        val caps = KafkaReadProvider().deliveryCapabilities(config("{}"))
        assertEquals(DeliveryMode.AT_LEAST_ONCE, caps.deliveryMode)
        assertEquals(ReplayBehavior.FULL_REPLAY, caps.replayBehavior)
        assertEquals(OrderingScope.PER_KEY, caps.ordering)
    }

    @Test
    fun `an unbounded ReadFromKafka is resumable via the runner's checkpoint`() {
        val caps = KafkaReadProvider().deliveryCapabilities(config("scan_bounded_mode: unbounded"))
        assertEquals(ReplayBehavior.RESUMABLE, caps.replayBehavior)
    }

    @Test
    fun `WriteToKafka at-least-once requires idempotency`() {
        val caps = KafkaWriteProvider().deliveryCapabilities(config("{}"))
        assertEquals(DeliveryMode.AT_LEAST_ONCE, caps.deliveryMode)
        assertEquals(true, caps.requiresIdempotencyKey)
    }

    @Test
    fun `WriteToKafka sink_delivery_guarantee none is at-most-once`() {
        val caps = KafkaWriteProvider().deliveryCapabilities(config("sink_delivery_guarantee: none"))
        assertEquals(DeliveryMode.AT_MOST_ONCE, caps.deliveryMode)
        assertEquals(false, caps.requiresIdempotencyKey)
    }

    @Test
    fun `both providers declare an EXPERIMENTAL support tier`() {
        assertEquals(ConnectorSupportTier.EXPERIMENTAL, KafkaReadProvider().supportTier())
        assertEquals(ConnectorSupportTier.EXPERIMENTAL, KafkaWriteProvider().supportTier())
    }
}
