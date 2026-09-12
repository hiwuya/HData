package me.jayer.hdata.hbase

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertFalse

class HBaseDeliveryCapabilitiesTest {
    private fun config() = TransformConfig("t", SpecMappers.YAML.readTree("{}") as ObjectNode)
    @Test fun `read replays bounded region scans`() = assertEquals(ReplayBehavior.FULL_REPLAY, HBaseReadProvider().deliveryCapabilities(config()).replayBehavior)
    @Test fun `deterministic Put retry is idempotent`() = assertFalse(HBaseWriteProvider().deliveryCapabilities(config()).requiresIdempotencyKey)
}
