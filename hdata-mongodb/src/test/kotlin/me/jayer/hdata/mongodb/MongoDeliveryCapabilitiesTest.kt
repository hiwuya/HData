package me.jayer.hdata.mongodb

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class MongoDeliveryCapabilitiesTest {
    private fun config(yaml: String) = TransformConfig("t", SpecMappers.YAML.readTree(yaml) as ObjectNode)
    @Test fun `read replays bounded snapshots`() = assertEquals(ReplayBehavior.FULL_REPLAY, MongoReadProvider().deliveryCapabilities(config("{}")).replayBehavior)
    @Test fun `insert needs deduplication but upsert is idempotent`() {
        assertTrue(MongoWriteProvider().deliveryCapabilities(config("{}")).requiresIdempotencyKey)
        assertFalse(MongoWriteProvider().deliveryCapabilities(config("upsert_keys: [id]")).requiresIdempotencyKey)
    }
}
