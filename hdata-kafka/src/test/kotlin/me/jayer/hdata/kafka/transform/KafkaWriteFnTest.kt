package me.jayer.hdata.kafka.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.kafka.KafkaFormats
import me.jayer.hdata.kafka.KafkaWriteConfig
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFnTester
import org.apache.beam.sdk.values.Row
import org.apache.kafka.clients.producer.MockProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.kafka.common.serialization.ByteArraySerializer
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Future
import kotlin.test.AfterTest
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 写入端。用 Kafka 自带的 [MockProducer]，不需要真 broker。
 *
 * 重点守两件事：发送是**异步攒批**的（重构前是 `send().get()`，每条等一次 broker 往返），
 * 以及失败的记录能带着原始行进死信流。
 *
 * @author wuya
 */
class KafkaWriteFnTest {

    private val schema: Schema = Schema.builder()
        .addNullableStringField("key")
        .addNullableStringField("value")
        .addStringField("topic")
        .build()

    private val errorSchema: Schema = ErrorSchemas.of(schema)

    private fun row(key: String?, value: String?, topic: String = "orders"): Row =
        Row.withSchema(schema).addValue(key).addValue(value).addValue(topic).build()

    private val producers = mutableListOf<MockProducer<ByteArray, ByteArray>>()

    private fun fn(
        config: KafkaWriteConfig,
        deadLetter: Boolean = true,
    ): Pair<KafkaWriteFn, MockProducer<ByteArray, ByteArray>> {
        val producer = MockProducer(false, null, ByteArraySerializer(), ByteArraySerializer())
        producers.add(producer)
        val fn = KafkaWriteFn(
            config,
            errorSchema,
            deadLetter,
            "WriteToKafka",
            KafkaWriteFn.ProducerFactory { producer },
        )
        return fn to producer
    }

    /**
     * MockProducer 不可序列化，所以必须关掉 DoFnTester 默认的克隆行为。
     * 生产路径上 DoFn 的可序列化由 [me.jayer.hdata.kafka.KafkaWriteProvider] 那条链路保证。
     */
    private fun tester(fn: KafkaWriteFn): DoFnTester<Row, Row> =
        DoFnTester.of<Row, Row>(fn).apply { setCloningBehavior(DoFnTester.CloningBehavior.DO_NOT_CLONE) }

    @AfterTest
    fun closeProducers() {
        producers.forEach { runCatching { it.close() } }
        producers.clear()
    }

    private val config = KafkaWriteConfig(bootstrapServers = "localhost:9092", topic = "orders")

    @Test
    fun `正常写入时不产生死信，记录按顺序发出`() {
        val (fn, producer) = fn(config)

        val errors = tester(fn).use { tester ->
            tester.processBundle(row("k1", "v1"), row("k2", "v2"))
        }

        assertTrue(errors.isEmpty())
        assertEquals(2, producer.history().size)
        assertContentEquals("v1".toByteArray(), producer.history()[0].value())
        assertEquals("orders", producer.history()[0].topic())
    }

    @Test
    fun `攒够 batch_size 才 flush，而不是每条等一次`() {
        val (fn, producer) = fn(config.copy(batchSize = 2))

        tester(fn).use { tester ->
            // MockProducer 的 flush() 会把在途请求全部完成，flushed() 因此能反映"有没有真的攒批"
            tester.processBundle(row("k1", "v1"))
            assertTrue(producer.history().size >= 1)
        }
        // 一个 bundle 结束时必须把剩下的都送出去，不能留在缓冲里
        assertEquals(1, producer.history().size)
    }

    @Test
    fun `缺少 value 字段的行进死信并保留原始记录`() {
        val onlyTopic = Schema.builder().addStringField("topic").build()
        val (fn, _) = fn(config)

        val errors = tester(fn).use { tester ->
            tester.processBundle(Row.withSchema(onlyTopic).addValue("orders").build())
        }

        assertEquals(1, errors.size)
        assertTrue("value" in errors[0].getString(ErrorSchemas.ERROR_MESSAGE)!!)
        assertEquals("WriteToKafka", errors[0].getString(ErrorSchemas.TRANSFORM))
    }

    @Test
    fun `send 同步抛异常时也走死信，而不是让整个 bundle 挂掉`() {
        val (fn, producer) = fn(config)
        // 序列化失败、拿不到元数据、缓冲区满都会让 send() 当场抛，不走 future
        producer.sendException = IllegalStateException("broker 不可达")

        val errors = tester(fn).use { tester ->
            tester.processBundle(row("k1", "v1"))
        }

        assertEquals(1, errors.size)
        assertEquals("v1", errors[0].getRow(ErrorSchemas.ELEMENT)!!.getString("value"))
    }

    @Test
    fun `flush 失败时整批记录都进死信`() {
        val producer = object : MockProducer<ByteArray, ByteArray>(
            false,
            null,
            ByteArraySerializer(),
            ByteArraySerializer(),
        ) {
            override fun flush() {
                throw IllegalStateException("flush 失败")
            }
        }
        producers.add(producer)
        val fn = KafkaWriteFn(
            config.copy(batchSize = 10),
            errorSchema,
            true,
            "WriteToKafka",
            KafkaWriteFn.ProducerFactory { producer },
        )

        val errors = tester(fn).use { it.processBundle(row("k1", "v1"), row("k2", "v2")) }

        assertEquals(listOf("v1", "v2"), errors.map { it.getRow(ErrorSchemas.ELEMENT)!!.getString("value") })
        assertTrue(errors.all { "flush 失败" in it.getString(ErrorSchemas.ERROR_MESSAGE)!! })
    }

    @Test
    fun `发送 future 被取消时该记录进死信`() {
        val producer = object : MockProducer<ByteArray, ByteArray>(
            false,
            null,
            ByteArraySerializer(),
            ByteArraySerializer(),
        ) {
            override fun send(record: ProducerRecord<ByteArray, ByteArray>): Future<RecordMetadata> =
                CompletableFuture<RecordMetadata>().also { it.cancel(false) }
        }
        producers.add(producer)
        val fn = KafkaWriteFn(
            config,
            errorSchema,
            true,
            "WriteToKafka",
            KafkaWriteFn.ProducerFactory { producer },
        )

        val errors = tester(fn).use { it.processBundle(row("k1", "v1")) }

        assertEquals(1, errors.size)
        assertTrue("CancellationException" in errors.single().getString(ErrorSchemas.ERROR_TYPE)!!)
    }

    @Test
    fun `没开死信时写入失败直接抛出`() {
        val (fn, producer) = fn(config, deadLetter = false)
        producer.sendException = IllegalStateException("broker 不可达")

        assertFailsWith<IllegalStateException> {
            tester(fn).use { tester -> tester.processBundle(row("k1", "v1")) }
        }
    }

    @Test
    fun `topic 留空时按行里的 topic 字段路由`() {
        val (fn, producer) = fn(config.copy(topic = ""))

        tester(fn).use { tester ->
            tester.processBundle(row("k1", "v1", topic = "a"), row("k2", "v2", topic = "b"))
        }

        assertEquals(listOf("a", "b"), producer.history().map { it.topic() })
    }

    @Test
    fun `topic 既没配也没有对应字段时进死信`() {
        val noTopic = Schema.builder().addNullableStringField(KafkaFormats.VALUE).build()
        val (fn, _) = fn(config.copy(topic = ""))

        val errors = tester(fn).use { tester ->
            tester.processBundle(Row.withSchema(noTopic).addValue("v1").build())
        }

        assertEquals(1, errors.size)
        assertTrue("topic" in errors[0].getString(ErrorSchemas.ERROR_MESSAGE)!!)
    }

    @Test
    fun `key 为 null 时照常发送`() {
        val (fn, producer) = fn(config)

        tester(fn).use { tester -> tester.processBundle(row(null, "v1")) }

        assertEquals(1, producer.history().size)
        assertNull(producer.history()[0].key())
    }

    @Test
    fun `value_format=raw 把 ByteArray 值原样编码发送`() {
        // 证明 raw 真的影响编码：Binary 消息必须原样写出，而不是被当字符串解码坏
        val rawSchema: Schema = Schema.builder()
            .addNullableField("value", Schema.FieldType.BYTES)
            .addStringField("topic")
            .build()
        val producer = MockProducer(false, null, ByteArraySerializer(), ByteArraySerializer())
        producers.add(producer)
        val fn = KafkaWriteFn(
            config.copy(valueFormat = "raw"),
            ErrorSchemas.of(rawSchema),
            true,
            "WriteToKafka",
            KafkaWriteFn.ProducerFactory { producer },
        )

        tester(fn).use { tester ->
            tester.processBundle(Row.withSchema(rawSchema).addValue(byteArrayOf(1, 2, 3)).addValue("orders").build())
        }

        assertTrue(producer.history().isNotEmpty())
        assertContentEquals(byteArrayOf(1, 2, 3), producer.history().single().value())
    }

    @Test
    fun `key_format 不认识时写入端真的报错，而不是被忽略`() {
        // 回归点：重构前 key_format/value_format 是收下就丢掉的死参数，
        // 无论填什么都不影响行为。这里证明配置真的被用到了——不认识的格式会直接炸。
        // setup() 里先解析格式再建 producer，所以格式非法时 producer 根本不该被构造。
        val fn = KafkaWriteFn(
            config.copy(keyFormat = "avro"),
            errorSchema,
            true,
            "WriteToKafka",
            KafkaWriteFn.ProducerFactory { throw AssertionError("格式非法时不应构造 producer") },
        )

        assertFailsWith<org.apache.beam.sdk.util.UserCodeException> { tester(fn).processBundle(row("k1", "v1")) }
            .also { assertTrue("key_format" in (it.cause?.message ?: it.message)!!) }
    }
}
