package me.jayer.hdata.rabbitmq

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class RabbitMQDeliveryCapabilitiesTest {
    private fun config() = TransformConfig("t", SpecMappers.YAML.readTree("{}") as ObjectNode)
    @Test fun `auto acknowledged read is at most once`() = assertEquals(DeliveryMode.AT_MOST_ONCE, RabbitMQReadProvider().deliveryCapabilities(config()).deliveryMode)
    @Test fun `confirmed write still needs deduplication after ambiguity`() = assertTrue(RabbitMQWriteProvider().deliveryCapabilities(config()).requiresIdempotencyKey)
}
