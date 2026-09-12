package me.jayer.hdata.kafka

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spec.ErrorHandlingSpec
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.kafka.internal.KafkaOffsets
import me.jayer.hdata.kafka.transform.KafkaRecordToRowFn
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.io.kafka.KafkaSourceDescriptor
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.Row
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.MockConsumer
import org.apache.kafka.clients.consumer.OffsetResetStrategy
import org.apache.kafka.common.Node
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.header.internals.RecordHeader
import org.apache.kafka.common.header.internals.RecordHeaders
import tools.jackson.databind.node.ObjectNode
import java.nio.charset.StandardCharsets
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNotNull

/**
 * End-to-end test for `ReadFromKafka`: it **really runs Beam's `ReadFromKafkaDoFn`**, only swapping the underlying
 * KafkaConsumer for Kafka's own [MockConsumer], so no broker is needed.
 *
 * That point matters — the core of this refactor was exactly "hand reading over to Beam's SDF", so if the tests only
 * covered our own little bit of offset computation, we would not have verified that the wiring is correct.
 * The injection point offered by `KafkaIO.readSourceDescriptors().withConsumerFactoryFn(...)` makes the whole chain run.
 *
 * @author wuya
 */
class KafkaPipelineTest {

    private val topic = "orders"
    private val tp0 = TopicPartition(topic, 0)
    private val tp1 = TopicPartition(topic, 1)

    /**
     * Builds a MockConsumer factory.
     *
     * Note it only captures serializable information such as "how many records per partition"; the messages themselves
     * are created on the fly inside the factory — `ConsumerRecord` does not implement Serializable, so capturing them
     * directly would make the whole DoFn fail at submission.
     */
    private class MockConsumerFactory(
        private val topic: String,
        private val counts: Map<Int, Int>,
    ) : org.apache.beam.sdk.transforms.SerializableFunction<Map<String, Any>, org.apache.kafka.clients.consumer.Consumer<ByteArray, ByteArray>> {

        override fun apply(input: Map<String, Any>): org.apache.kafka.clients.consumer.Consumer<ByteArray, ByteArray> {
            val consumer = MockConsumer<ByteArray, ByteArray>(OffsetResetStrategy.EARLIEST)
            val node = Node(0, "localhost", 9092)
            consumer.updatePartitions(
                topic,
                counts.keys.map { PartitionInfo(topic, it, node, arrayOf(node), arrayOf(node)) },
            )
            val partitions = counts.keys.associate { TopicPartition(topic, it) to it }
            consumer.updateBeginningOffsets(partitions.keys.associateWith { 0L })
            consumer.updateEndOffsets(partitions.keys.associateWith { counts.getValue(it.partition()).toLong() })
            // ReadFromKafkaDoFn only polls after assign + seek, so on the first poll we fill in records at the current position
            consumer.schedulePollTask { feed(consumer) }
            return consumer
        }

        private fun feed(consumer: MockConsumer<ByteArray, ByteArray>) {
            consumer.assignment().forEach { tp ->
                val total = counts[tp.partition()] ?: return@forEach
                val position = consumer.position(tp)
                (position until total).forEach { offset -> consumer.addRecord(record(tp.partition(), offset)) }
            }
            consumer.schedulePollTask { feed(consumer) }
        }

        private fun record(partition: Int, offset: Long): ConsumerRecord<ByteArray, ByteArray> = ConsumerRecord(
            topic,
            partition,
            offset,
            offset,
            org.apache.kafka.common.record.TimestampType.CREATE_TIME,
            -1,
            -1,
            "k$partition-$offset".toByteArray(),
            "v$partition-$offset".toByteArray(),
            RecordHeaders(listOf(RecordHeader("trace", "t$offset".toByteArray()))),
            java.util.Optional.empty(),
        )
    }

    private fun readPipeline(
        config: KafkaReadConfig,
        descriptors: List<KafkaSourceDescriptor>,
        counts: Map<Int, Int>,
    ): Pair<Pipeline, PCollection<Row>> {
        val pipeline = Pipeline.create()
        val schema = KafkaFormats.readSchema(config)
        val read = KafkaIO.readSourceDescriptors<ByteArray, ByteArray>()
            .withBootstrapServers(config.bootstrapServers)
            .withKeyDeserializer(org.apache.kafka.common.serialization.ByteArrayDeserializer::class.java)
            .withValueDeserializer(org.apache.kafka.common.serialization.ByteArrayDeserializer::class.java)
            .withConsumerConfigUpdates(KafkaOffsets.consumerProperties(config))
            .withConsumerFactoryFn(MockConsumerFactory(topic, counts))

        val rows = pipeline
            .apply(
                "Descriptors",
                Create.of(descriptors).withCoder(pipeline.schemaRegistry.getSchemaCoder(KafkaSourceDescriptor::class.java)),
            )
            .apply("ReadFromKafka", read)
            .setIsBoundedInternal(PCollection.IsBounded.BOUNDED)
            .apply("ToRow", ParDo.of(KafkaRecordToRowFn(config.keyFormat, config.valueFormat)))
            .setRowSchema(schema)
        return pipeline to rows
    }

    private fun descriptor(tp: TopicPartition, start: Long, stop: Long) =
        KafkaSourceDescriptor.of(tp, start, null, stop, null, listOf("localhost:9092"))

    private val config = KafkaReadConfig(bootstrapServers = "localhost:9092", topics = listOf(topic))

    @Test
    fun `a bounded read runs the whole Beam SDF chain with neither missing nor extra records`() {
        val (pipeline, rows) = readPipeline(
            config,
            listOf(descriptor(tp0, 0, 5), descriptor(tp1, 0, 3)),
            mapOf(0 to 5, 1 to 3),
        )
        PAssert.thatSingleton(rows.apply(Count.globally())).isEqualTo(8L)
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `the start and end offsets are genuinely honored`() {
        // Getting the offsets wrong means missing or re-reading records while the job still succeeds — only a count assertion catches it
        val (pipeline, rows) = readPipeline(config, listOf(descriptor(tp0, 3, 7)), mapOf(0 to 10))
        PAssert.thatSingleton(rows.apply(Count.globally())).isEqualTo(4L)
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `a resumed descriptor emits only records after the checkpoint position`() {
        // The runner owns an unbounded source's checkpoint. This test supplies the descriptor it would restore
        // after a worker replacement and verifies Beam's actual ReadFromKafkaDoFn seeks to that exclusive next
        // offset: records 0..2 were already acknowledged by the prior worker and must not be emitted again.
        // A remote-runner checkpoint recovery test is still required before this path can be qualified; DirectRunner
        // does not persist worker state across independent Pipeline instances.
        val (pipeline, rows) = readPipeline(config, listOf(descriptor(tp0, 3, 5)), mapOf(0 to 5))
        PAssert.that(rows).satisfies { output ->
            assertEquals(listOf(3L, 4L), output.map { it.getInt64(KafkaFormats.OFFSET) }.sorted())
            null
        }
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `in string format key and value decode into text and the metadata columns are complete`() {
        val (pipeline, rows) = readPipeline(config, listOf(descriptor(tp0, 0, 1)), mapOf(0 to 1))
        // This lambda gets serialized and shipped, so it must not capture test-class fields (hence topic is written as a literal)
        PAssert.that(rows).satisfies { output ->
            val row = output.single()
            assertEquals("k0-0", row.getString(KafkaFormats.KEY))
            assertEquals("v0-0", row.getString(KafkaFormats.VALUE))
            assertEquals("orders", row.getString(KafkaFormats.TOPIC))
            assertEquals(0, row.getInt32(KafkaFormats.PARTITION))
            assertEquals(0L, row.getInt64(KafkaFormats.OFFSET))
            assertNotNull(row.getString(KafkaFormats.TIMESTAMP_TYPE))
            // headers is the metadata column of the same name as in the Flink Kafka connector
            val headers = row.getMap<String, ByteArray>(KafkaFormats.HEADERS)
            assertContentEquals("t0".toByteArray(), headers!!["trace"])
            null
        }
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `the raw format preserves the original bytes`() {
        val (pipeline, rows) = readPipeline(
            config.copy(keyFormat = "raw", valueFormat = "raw"),
            listOf(descriptor(tp0, 0, 1)),
            mapOf(0 to 1),
        )
        PAssert.that(rows).satisfies { output ->
            val row = output.single()
            assertContentEquals("v0-0".toByteArray(StandardCharsets.UTF_8), row.getBytes(KafkaFormats.VALUE))
            null
        }
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `the write side can be wired into a pipeline as a sink and produce a dead-letter stream`() {
        val schema = KafkaFormats.readSchema(config)
        val pipeline = Pipeline.create()
        // A row missing the value field is bound for dead letter; this only verifies the wiring, sending itself is covered by KafkaWriteFnTest with MockProducer
        val bad = org.apache.beam.sdk.schemas.Schema.builder().addStringField("topic").build()
        val input = pipeline.apply(Create.of(Row.withSchema(bad).addValue(topic).build()).withRowSchema(bad))

        val out = org.apache.beam.sdk.values.PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
            KafkaWriteProvider().from(
                TransformConfig(
                    "WriteToKafka",
                    SpecMappers.YAML.readTree(
                        """
                        bootstrap_servers: "localhost:9092"
                        topic: "$topic"
                        """.trimIndent()
                    ) as ObjectNode,
                    ErrorHandlingSpec(output = "errors"),
                )
            )
        )

        val errors = out.get(Tags.ERROR_OUTPUT)
        assertEquals(ErrorSchemas.of(bad), errors.schema)
        assertEquals(schema.fieldCount, KafkaFormats.readSchema(config).fieldCount)
    }
}
