package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Elasticsearch6DeliveryCapabilitiesTest {
    private fun config() = TransformConfig("t", SpecMappers.YAML.readTree("{}") as ObjectNode)
    @Test fun `read replays a bounded snapshot`() = assertEquals(ReplayBehavior.FULL_REPLAY, ReadFromElasticsearch6().deliveryCapabilities(config()).replayBehavior)
    @Test fun `write needs stable IDs on retry`() {
        val caps = WriteToElasticsearch6().deliveryCapabilities(config())
        assertEquals(DeliveryMode.AT_LEAST_ONCE, caps.deliveryMode)
        assertTrue(caps.requiresIdempotencyKey)
    }
}
