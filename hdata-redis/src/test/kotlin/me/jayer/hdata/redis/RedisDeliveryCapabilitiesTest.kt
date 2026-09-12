package me.jayer.hdata.redis

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Pins down the declared [me.jayer.hdata.core.spi.DeliveryCapabilities] for Redis read/write (see critical
 * gap #2 in docs/MATURITY_ASSESSMENT.md): list pushes duplicate on retry, set/sadd/hset do not.
 */
class RedisDeliveryCapabilitiesTest {

    private fun config(yaml: String) = TransformConfig("t", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    @Test
    fun `ReadFromRedis scan mode has no ordering, stream mode is globally ordered`() {
        val scan = RedisReadProvider().deliveryCapabilities(config("{}"))
        assertEquals(ReplayBehavior.FULL_REPLAY, scan.replayBehavior)
        assertEquals(OrderingScope.NONE, scan.ordering)

        val stream = RedisReadProvider().deliveryCapabilities(config("mode: stream"))
        assertEquals(OrderingScope.GLOBAL, stream.ordering)
    }

    @Test
    fun `WriteToRedis lpush-rpush require idempotency, set-sadd-hset do not`() {
        assertTrue(RedisWriteProvider().deliveryCapabilities(config("mode: lpush")).requiresIdempotencyKey)
        assertTrue(RedisWriteProvider().deliveryCapabilities(config("mode: rpush")).requiresIdempotencyKey)
        assertFalse(RedisWriteProvider().deliveryCapabilities(config("mode: set")).requiresIdempotencyKey)
        assertFalse(RedisWriteProvider().deliveryCapabilities(config("mode: sadd")).requiresIdempotencyKey)
        assertFalse(
            RedisWriteProvider().deliveryCapabilities(config("mode: hset\nhash_field: field")).requiresIdempotencyKey,
        )
    }

    @Test
    fun `both providers declare an EXPERIMENTAL support tier`() {
        assertEquals(ConnectorSupportTier.EXPERIMENTAL, RedisReadProvider().supportTier())
        assertEquals(ConnectorSupportTier.EXPERIMENTAL, RedisWriteProvider().supportTier())
    }
}
