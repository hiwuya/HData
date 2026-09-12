package me.jayer.hdata.prometheus

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals

class PrometheusDeliveryCapabilitiesTest {
    @Test fun `instant queries are full-replay snapshots`() {
        val config = TransformConfig("t", SpecMappers.YAML.readTree("{}") as ObjectNode)
        assertEquals(ReplayBehavior.FULL_REPLAY, PrometheusReadProvider().deliveryCapabilities(config).replayBehavior)
    }
}
