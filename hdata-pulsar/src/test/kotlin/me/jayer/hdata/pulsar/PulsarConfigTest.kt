package me.jayer.hdata.pulsar

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class PulsarConfigTest {
    @Test
    fun `binds read configuration and validates it`() {
        val config = TransformConfig(
            "ReadFromPulsar",
            SpecMappers.CONFIG.readTree("""{"service_url":"pulsar://localhost:6650","topic":"orders","start_position":"latest","max_messages":10}""") as ObjectNode,
        ).bind(PulsarReadConfig::class.java)
        config.validate()
        assertEquals(PulsarReadConfig.LATEST, config.startPosition)
        assertEquals(10, config.maxMessages)
    }

    @Test
    fun `rejects invalid write configuration`() {
        assertFailsWith<IllegalArgumentException> { PulsarWriteConfig(topic = "topic").validate() }
        assertFailsWith<IllegalArgumentException> { PulsarWriteConfig(serviceUrl = "pulsar://x", topic = "topic", batchSize = 0).validate() }
    }

}
