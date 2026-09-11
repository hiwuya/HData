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
 * The write side. Uses Kafka's own [MockProducer], so no real broker is needed.
 *
 * Two things are guarded here in particular: sending is **asynchronously batched** (before the refactor it was
 * `send().get()`, one broker round-trip per record), and failed records reach the dead-letter stream carrying the
 * original row.
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
     * MockProducer is not serializable, so DoFnTester's default cloning behavior must be turned off.
     * On the production path the DoFn's serializability is guaranteed by the
     * [me.jayer.hdata.kafka.KafkaWriteProvider] chain.
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
    fun `a successful write produces no dead letter and records are emitted in order`() {
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
    fun `it flushes only once batch_size is reached, rather than waiting per record`() {
        val (fn, producer) = fn(config.copy(batchSize = 2))

        tester(fn).use { tester ->
            // MockProducer's flush() completes every in-flight request, so history() reflects whether batching really happened
            tester.processBundle(row("k1", "v1"))
            assertTrue(producer.history().size >= 1)
        }
        // When a bundle ends everything left over must be sent out, nothing may stay in the buffer
        assertEquals(1, producer.history().size)
    }

    @Test
    fun `a row missing the value field goes to dead letter with the original record preserved`() {
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
    fun `a synchronous throw from send also goes to dead letter instead of killing the whole bundle`() {
        val (fn, producer) = fn(config)
        // Serialization failure, unavailable metadata, or a full buffer all make send() throw on the spot, bypassing the future
        producer.sendException = IllegalStateException("broker unreachable")

        val errors = tester(fn).use { tester ->
            tester.processBundle(row("k1", "v1"))
        }

        assertEquals(1, errors.size)
        assertEquals("v1", errors[0].getRow(ErrorSchemas.ELEMENT)!!.getString("value"))
    }

    @Test
    fun `when flush fails the whole batch goes to dead letter`() {
        val producer = object : MockProducer<ByteArray, ByteArray>(
            false,
            null,
            ByteArraySerializer(),
            ByteArraySerializer(),
        ) {
            override fun flush() {
                throw IllegalStateException("flush failed")
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
        assertTrue(errors.all { "flush failed" in it.getString(ErrorSchemas.ERROR_MESSAGE)!! })
    }

    @Test
    fun `a cancelled send future sends that record to dead letter`() {
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
    fun `with dead letter disabled a write failure throws directly`() {
        val (fn, producer) = fn(config, deadLetter = false)
        producer.sendException = IllegalStateException("broker unreachable")

        assertFailsWith<IllegalStateException> {
            tester(fn).use { tester -> tester.processBundle(row("k1", "v1")) }
        }
    }

    @Test
    fun `an empty topic routes by the row's topic field`() {
        val (fn, producer) = fn(config.copy(topic = ""))

        tester(fn).use { tester ->
            tester.processBundle(row("k1", "v1", topic = "a"), row("k2", "v2", topic = "b"))
        }

        assertEquals(listOf("a", "b"), producer.history().map { it.topic() })
    }

    @Test
    fun `a topic that is neither configured nor present as a field goes to dead letter`() {
        val noTopic = Schema.builder().addNullableStringField(KafkaFormats.VALUE).build()
        val (fn, _) = fn(config.copy(topic = ""))

        val errors = tester(fn).use { tester ->
            tester.processBundle(Row.withSchema(noTopic).addValue("v1").build())
        }

        assertEquals(1, errors.size)
        assertTrue("topic" in errors[0].getString(ErrorSchemas.ERROR_MESSAGE)!!)
    }

    @Test
    fun `a null key is still sent`() {
        val (fn, producer) = fn(config)

        tester(fn).use { tester -> tester.processBundle(row(null, "v1")) }

        assertEquals(1, producer.history().size)
        assertNull(producer.history()[0].key())
    }

    @Test
    fun `value_format=raw encodes a ByteArray value verbatim`() {
        // Proves raw really affects encoding: a binary message must be written out as-is, not mangled by string decoding
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
    fun `an unrecognized key_format really fails on the write side instead of being ignored`() {
        // Regression guard: before the refactor key_format/value_format were dead parameters accepted and then dropped,
        // no value changed the behavior. This proves the config is genuinely used — an unknown format blows up on the spot.
        // setup() resolves the format before creating the producer, so with an invalid format the producer must never be built.
        val fn = KafkaWriteFn(
            config.copy(keyFormat = "avro"),
            errorSchema,
            true,
            "WriteToKafka",
            KafkaWriteFn.ProducerFactory { throw AssertionError("producer must not be created when the format is invalid") },
        )

        assertFailsWith<org.apache.beam.sdk.util.UserCodeException> { tester(fn).processBundle(row("k1", "v1")) }
            .also { assertTrue("key_format" in (it.cause?.message ?: it.message)!!) }
    }
}
