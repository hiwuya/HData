package me.jayer.hdata.cassandra

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class CassandraDeliveryCapabilitiesTest {
    private fun config() = TransformConfig("t", SpecMappers.YAML.readTree("{}") as ObjectNode)

    @Test
    fun `read is an unordered full-replay snapshot`() {
        val caps = CassandraReadProvider().deliveryCapabilities(config())
        assertEquals(DeliveryMode.AT_LEAST_ONCE, caps.deliveryMode)
        assertEquals(ReplayBehavior.FULL_REPLAY, caps.replayBehavior)
        assertEquals(OrderingScope.NONE, caps.ordering)
    }

    @Test
    fun `write requires idempotent mutations on retry`() {
        val caps = CassandraWriteProvider().deliveryCapabilities(config())
        assertEquals(DeliveryMode.AT_LEAST_ONCE, caps.deliveryMode)
        assertTrue(caps.requiresIdempotencyKey)
        assertTrue(caps.notes!!.contains("counter"))
    }
}
