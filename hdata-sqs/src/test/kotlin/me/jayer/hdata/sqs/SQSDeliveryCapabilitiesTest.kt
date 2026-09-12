package me.jayer.hdata.sqs

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class SQSDeliveryCapabilitiesTest {
    private fun config(yaml: String) = TransformConfig("t", SpecMappers.YAML.readTree(yaml) as ObjectNode)
    @Test fun `delete after read is at most once but retained messages are at least once`() {
        assertEquals(DeliveryMode.AT_MOST_ONCE, SQSReadProvider().deliveryCapabilities(config("{}")).deliveryMode)
        assertEquals(DeliveryMode.AT_LEAST_ONCE, SQSReadProvider().deliveryCapabilities(config("delete_after_read: false")).deliveryMode)
    }
    @Test fun `FIFO deduplication ID changes write requirement`() {
        assertTrue(SQSWriteProvider().deliveryCapabilities(config("{}")).requiresIdempotencyKey)
        assertFalse(SQSWriteProvider().deliveryCapabilities(config("message_deduplication_id_field: id")).requiresIdempotencyKey)
    }
}
