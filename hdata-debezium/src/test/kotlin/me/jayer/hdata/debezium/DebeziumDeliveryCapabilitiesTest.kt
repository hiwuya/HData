package me.jayer.hdata.debezium

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals

/**
 * Pins down the declared [me.jayer.hdata.core.spi.DeliveryCapabilities] for `ReadFromDebezium` (see critical
 * gap #2 in docs/MATURITY_ASSESSMENT.md): unbounded (persisted-state) CDC is resumable, a bounded/testing
 * read is not.
 */
class DebeziumDeliveryCapabilitiesTest {

    private fun config(yaml: String) = TransformConfig("t", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    @Test
    fun `an unbounded job is resumable via the persisted offset`() {
        val caps = DebeziumReadProvider().deliveryCapabilities(config("{}"))
        assertEquals(DeliveryMode.AT_LEAST_ONCE, caps.deliveryMode)
        assertEquals(ReplayBehavior.RESUMABLE, caps.replayBehavior)
        assertEquals(OrderingScope.GLOBAL, caps.ordering)
    }

    @Test
    fun `a bounded job (max_records set) is a full replay`() {
        val caps = DebeziumReadProvider().deliveryCapabilities(config("max_records: 10"))
        assertEquals(ReplayBehavior.FULL_REPLAY, caps.replayBehavior)
    }
}
