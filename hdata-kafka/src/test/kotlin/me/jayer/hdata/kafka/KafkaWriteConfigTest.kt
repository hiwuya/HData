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
 * [KafkaWriteConfig] 的 snake_case 绑定与校验。
 *
 * @author wuya
 */
class KafkaWriteConfigTest {

    private val minimal = KafkaWriteConfig(bootstrapServers = "localhost:9092", topic = "orders")

    private fun bind(json: String): KafkaWriteConfig =
        TransformConfig("WriteToKafka", SpecMappers.CONFIG.readTree(json) as ObjectNode)
            .bind(KafkaWriteConfig::class.java)

    @Test
    fun `配置按 snake_case 绑定并能生成可序列化的 transform`() {
        val cfg = TransformConfig(
            "WriteToKafka",
            SpecMappers.CONFIG.readTree(
                """{"bootstrap_servers": "localhost:9092", "topic": "orders", "batch_size": 500}"""
            ) as ObjectNode,
        )

        val transform = KafkaWriteProvider().from(cfg)

        assertNotNull(transform)
        // sink 会跟着 DoFn 一起下发，捕获了不可序列化的对象就会在提交时炸掉
        SerializableUtils.ensureSerializable(transform)
    }

    @Test
    fun `默认值`() {
        val config = bind("""{"bootstrap_servers": "localhost:9092", "topic": "orders"}""")

        assertEquals(1000, config.batchSize)
        assertEquals("string", config.keyFormat)
        assertEquals(KafkaWriteConfig.AT_LEAST_ONCE, config.sinkDeliveryGuarantee)
        assertEquals("all", config.producerProperties()["acks"])
    }

    @Test
    fun `topic 留空是合法的，表示按行里的 topic 字段路由`() {
        KafkaWriteConfig(bootstrapServers = "localhost:9092").validate()
    }

    @Test
    fun `bootstrap_servers 为空时报错`() {
        assertFailsWith<IllegalArgumentException> { KafkaWriteConfig(topic = "orders").validate() }
    }

    @Test
    fun `batch_size 必须为正`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(batchSize = 0).validate() }
    }

    @Test
    fun `delivery guarantee 为 none 时 acks 降为 0`() {
        val config = minimal.copy(sinkDeliveryGuarantee = KafkaWriteConfig.NONE)

        config.validate()
        assertEquals("0", config.producerProperties()["acks"])
    }

    @Test
    fun `exactly-once 明确告知暂不支持而不是装作支持`() {
        val error = assertFailsWith<IllegalArgumentException> {
            minimal.copy(sinkDeliveryGuarantee = KafkaWriteConfig.EXACTLY_ONCE).validate()
        }

        assertTrue("exactly-once" in error.message!! && "at-least-once" in error.message!!)
    }

    @Test
    fun `用户属性覆盖默认的 acks`() {
        val config = minimal.copy(properties = mapOf("acks" to "1", "compression.type" to "zstd"))

        assertEquals("1", config.producerProperties()["acks"])
        assertEquals("zstd", config.producerProperties()["compression.type"])
    }
}
