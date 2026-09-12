package me.jayer.hdata.dynamodb

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DynamoDBDeliveryCapabilitiesTest {
    private fun config() = TransformConfig("t", SpecMappers.YAML.readTree("{}") as ObjectNode)

    @Test fun `read replays its bounded scan`() = assertEquals(ReplayBehavior.FULL_REPLAY, DynamoDBReadProvider().deliveryCapabilities(config()).replayBehavior)

    @Test fun `write requires deterministic keys on retry`() {
        val caps = DynamoDBWriteProvider().deliveryCapabilities(config())
        assertEquals(DeliveryMode.AT_LEAST_ONCE, caps.deliveryMode)
        assertTrue(caps.requiresIdempotencyKey)
    }
}
