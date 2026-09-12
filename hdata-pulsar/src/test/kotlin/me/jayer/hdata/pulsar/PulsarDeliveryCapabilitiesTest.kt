package me.jayer.hdata.pulsar

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class PulsarDeliveryCapabilitiesTest {
    private fun config() = TransformConfig("t", SpecMappers.YAML.readTree("{}") as ObjectNode)
    @Test fun `read restarts from configured position`() = assertEquals(ReplayBehavior.FULL_REPLAY, PulsarReadProvider().deliveryCapabilities(config()).replayBehavior)
    @Test fun `write requires deduplication`() = assertTrue(PulsarWriteProvider().deliveryCapabilities(config()).requiresIdempotencyKey)
}
