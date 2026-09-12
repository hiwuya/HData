package me.jayer.hdata.elasticsearch8

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class EsDeliveryCapabilitiesTest {
    private fun config() = TransformConfig("t", SpecMappers.YAML.readTree("{}") as ObjectNode)
    @Test fun `read replays bounded PIT scans`() = assertEquals(ReplayBehavior.FULL_REPLAY, EsReadProvider().deliveryCapabilities(config()).replayBehavior)
    @Test fun `write requires deduplication on retry`() {
        val caps = EsWriteProvider().deliveryCapabilities(config())
        assertEquals(DeliveryMode.AT_LEAST_ONCE, caps.deliveryMode)
        assertTrue(caps.requiresIdempotencyKey)
    }
}
