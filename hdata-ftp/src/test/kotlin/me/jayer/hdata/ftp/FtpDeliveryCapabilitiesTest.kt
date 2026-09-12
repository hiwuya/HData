package me.jayer.hdata.ftp

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.ReplayBehavior
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class FtpDeliveryCapabilitiesTest {
    private fun config() = TransformConfig("t", SpecMappers.YAML.readTree("{}") as ObjectNode)
    @Test fun `read is a full-replay snapshot`() = assertEquals(ReplayBehavior.FULL_REPLAY, FtpReadProvider().deliveryCapabilities(config()).replayBehavior)
    @Test fun `write uses retry-visible UUID shards`() = assertTrue(FtpWriteProvider().deliveryCapabilities(config()).requiresIdempotencyKey)
}
