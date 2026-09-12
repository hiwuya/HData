package me.jayer.hdata.filesystem

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.DeliveryMode
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals

/**
 * Pins down the declared [me.jayer.hdata.core.spi.DeliveryCapabilities] for Filesystem read/write (see
 * critical gap #2 in docs/MATURITY_ASSESSMENT.md).
 */
class FilesystemDeliveryCapabilitiesTest {

    private fun config(yaml: String) = TransformConfig("t", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    @Test
    fun `ReadFromFilesystem re-reads matched files in full on every run`() {
        val caps = FilesystemReadProvider().deliveryCapabilities(config("{}"))
        assertEquals(DeliveryMode.AT_LEAST_ONCE, caps.deliveryMode)
        assertEquals(ReplayBehavior.FULL_REPLAY, caps.replayBehavior)
    }

    @Test
    fun `WriteToFilesystem commits shards atomically per run`() {
        val caps = FilesystemWriteProvider().deliveryCapabilities(config("{}"))
        assertEquals(DeliveryMode.EXACTLY_ONCE, caps.deliveryMode)
        assertEquals(ReplayBehavior.NOT_APPLICABLE, caps.replayBehavior)
    }

    @Test
    fun `both providers declare an EXPERIMENTAL support tier`() {
        assertEquals(ConnectorSupportTier.EXPERIMENTAL, FilesystemReadProvider().supportTier())
        assertEquals(ConnectorSupportTier.EXPERIMENTAL, FilesystemWriteProvider().supportTier())
    }
}
