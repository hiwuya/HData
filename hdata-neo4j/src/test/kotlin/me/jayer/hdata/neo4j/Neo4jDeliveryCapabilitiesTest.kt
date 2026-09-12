package me.jayer.hdata.neo4j

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class Neo4jDeliveryCapabilitiesTest {
    private fun config() = TransformConfig("t", SpecMappers.YAML.readTree("{}") as ObjectNode)
    @Test fun `read replays Cypher snapshots`() = assertEquals(ReplayBehavior.FULL_REPLAY, Neo4jReadProvider().deliveryCapabilities(config()).replayBehavior)
    @Test fun `write requires idempotent Cypher`() = assertTrue(Neo4jWriteProvider().deliveryCapabilities(config()).requiresIdempotencyKey)
}
