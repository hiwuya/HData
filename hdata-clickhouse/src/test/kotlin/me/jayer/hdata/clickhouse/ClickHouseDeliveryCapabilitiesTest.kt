package me.jayer.hdata.clickhouse

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ClickHouseDeliveryCapabilitiesTest {
    private fun config() = TransformConfig("t", SpecMappers.YAML.readTree("{}") as ObjectNode)

    @Test fun `read replays a bounded snapshot`() = assertEquals(ReplayBehavior.FULL_REPLAY, ClickHouseReadProvider().deliveryCapabilities(config()).replayBehavior)

    @Test fun `write requires deduplication on retry`() {
        val caps = ClickHouseWriteProvider().deliveryCapabilities(config())
        assertEquals(DeliveryMode.AT_LEAST_ONCE, caps.deliveryMode)
        assertTrue(caps.requiresIdempotencyKey)
    }
}
