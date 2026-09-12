package me.jayer.hdata.iceberg

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Pins down the declared [me.jayer.hdata.core.spi.DeliveryCapabilities] for Iceberg read/write (see critical
 * gap #2 in docs/MATURITY_ASSESSMENT.md): append duplicates on a full rerun, overwrite does not.
 */
class IcebergDeliveryCapabilitiesTest {

    private fun config(yaml: String) = TransformConfig("t", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    @Test
    fun `ReadFromIceberg is a full-replay bounded scan`() {
        val caps = IcebergReadProvider().deliveryCapabilities(config("{}"))
        assertEquals(DeliveryMode.AT_LEAST_ONCE, caps.deliveryMode)
        assertEquals(ReplayBehavior.FULL_REPLAY, caps.replayBehavior)
    }

    @Test
    fun `WriteToIceberg append requires idempotency, overwrite does not`() {
        val append = IcebergWriteProvider().deliveryCapabilities(config("write_mode: append"))
        assertTrue(append.requiresIdempotencyKey)

        val overwrite = IcebergWriteProvider().deliveryCapabilities(config("write_mode: overwrite"))
        assertFalse(overwrite.requiresIdempotencyKey)
    }

    @Test
    fun `both providers declare an EXPERIMENTAL support tier`() {
        assertEquals(ConnectorSupportTier.EXPERIMENTAL, IcebergReadProvider().supportTier())
        assertEquals(ConnectorSupportTier.EXPERIMENTAL, IcebergWriteProvider().supportTier())
    }
}
