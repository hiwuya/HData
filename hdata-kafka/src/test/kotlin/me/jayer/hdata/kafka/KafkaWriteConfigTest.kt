package me.jayer.hdata.kafka

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.util.SerializableUtils
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * snake_case binding and validation of [KafkaWriteConfig].
 *
 * @author wuya
 */
class KafkaWriteConfigTest {

    private val minimal = KafkaWriteConfig(bootstrapServers = "localhost:9092", topic = "orders")

    private fun bind(json: String): KafkaWriteConfig =
        TransformConfig("WriteToKafka", SpecMappers.CONFIG.readTree(json) as ObjectNode)
            .bind(KafkaWriteConfig::class.java)

    @Test
    fun `config binds in snake_case and produces a serializable transform`() {
        val cfg = TransformConfig(
            "WriteToKafka",
            SpecMappers.CONFIG.readTree(
                """{"bootstrap_servers": "localhost:9092", "topic": "orders", "batch_size": 500}"""
            ) as ObjectNode,
        )

        val transform = KafkaWriteProvider().from(cfg)

        assertNotNull(transform)
        // The sink ships along with the DoFn; capturing a non-serializable object blows up at submission time
        SerializableUtils.ensureSerializable(transform)
    }

    @Test
    fun `default values`() {
        val config = bind("""{"bootstrap_servers": "localhost:9092", "topic": "orders"}""")

        assertEquals(1000, config.batchSize)
        assertEquals("string", config.keyFormat)
        assertEquals(KafkaWriteConfig.AT_LEAST_ONCE, config.sinkDeliveryGuarantee)
        assertEquals("all", config.producerProperties()["acks"])
    }

    @Test
    fun `an empty topic is legal and means routing by the row's topic field`() {
        KafkaWriteConfig(bootstrapServers = "localhost:9092").validate()
    }

    @Test
    fun `an empty bootstrap_servers fails validation`() {
        assertFailsWith<IllegalArgumentException> { KafkaWriteConfig(topic = "orders").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(bootstrapServers = "localhost:9092,").validate() }
    }

    @Test
    fun `batch_size must be positive`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(batchSize = 0).validate() }
    }

    @Test
    fun `a delivery guarantee of none drops acks to 0`() {
        val config = minimal.copy(sinkDeliveryGuarantee = KafkaWriteConfig.NONE)

        config.validate()
        assertEquals("0", config.producerProperties()["acks"])
    }

    @Test
    fun `exactly-once states clearly that it is unsupported rather than pretending to work`() {
        val error = assertFailsWith<IllegalArgumentException> {
            minimal.copy(sinkDeliveryGuarantee = KafkaWriteConfig.EXACTLY_ONCE).validate()
        }

        assertTrue("exactly-once" in error.message!! && "at-least-once" in error.message!!)
    }

    @Test
    fun `the delivery guarantee cannot be silently overridden by acks in user properties`() {
        val config = minimal.copy(properties = mapOf("acks" to "1", "compression.type" to "zstd"))

        val error = assertFailsWith<IllegalArgumentException> { config.validate() }
        assertTrue("acks" in error.message!! && "at-least-once" in error.message!!)
        // Even if the caller forgets validate, the explicit delivery guarantee still wins when properties are generated.
        assertEquals("all", config.producerProperties()["acks"])
        assertEquals("zstd", config.producerProperties()["compression.type"])
    }

    @Test
    fun `connection and serializer properties cannot be configured again in properties`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(properties = mapOf("bootstrap.servers" to "other:9092")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(properties = mapOf("value.serializer" to "custom.Serializer")).validate()
        }
    }
}
