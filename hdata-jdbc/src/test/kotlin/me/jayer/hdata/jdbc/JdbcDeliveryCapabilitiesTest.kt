package me.jayer.hdata.jdbc

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.OrderingScope
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Pins down the declared [me.jayer.hdata.core.spi.DeliveryCapabilities] for JDBC read/write (see critical gap
 * #2 in docs/MATURITY_ASSESSMENT.md): a bounded batch read with no persisted position, and a plain-INSERT
 * write that is not idempotent under retry.
 */
class JdbcDeliveryCapabilitiesTest {

    private fun config(yaml: String) = TransformConfig("t", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    @Test
    fun `ReadFromJdbc is a full-replay bounded read`() {
        val caps = JdbcReadProvider().deliveryCapabilities(config("{}"))
        assertEquals(DeliveryMode.AT_LEAST_ONCE, caps.deliveryMode)
        assertEquals(ReplayBehavior.FULL_REPLAY, caps.replayBehavior)
        assertEquals(OrderingScope.NONE, caps.ordering)
    }

    @Test
    fun `WriteToJdbc requires idempotency because it is a plain INSERT`() {
        val caps = JdbcWriteProvider().deliveryCapabilities(config("{}"))
        assertEquals(DeliveryMode.AT_LEAST_ONCE, caps.deliveryMode)
        assertTrue(caps.requiresIdempotencyKey)
        assertTrue(caps.notes!!.contains("INSERT"))
    }

    @Test
    fun `both providers declare an EXPERIMENTAL support tier`() {
        assertEquals(ConnectorSupportTier.EXPERIMENTAL, JdbcReadProvider().supportTier())
        assertEquals(ConnectorSupportTier.EXPERIMENTAL, JdbcWriteProvider().supportTier())
    }
}
