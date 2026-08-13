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
 * `ReadFromKafka` 的端到端测试：**真的跑 Beam 的 `ReadFromKafkaDoFn`**，
 * 只是把底层的 KafkaConsumer 换成 Kafka 自带的 [MockConsumer]，因此不需要 broker。
 *
 * 这一点很关键——本次重构的核心正是"读取交给 Beam 的 SDF"，
 * 如果测试只覆盖我们自己那点偏移量计算，就等于没验证接线是否正确。
 * `KafkaIO.readSourceDescriptors().withConsumerFactoryFn(...)` 提供的注入点让整条链路都跑起来。
 *
 * @author wuya
 */
class KafkaPipelineTest {

    private val topic = "orders"
    private val tp0 = TopicPartition(topic, 0)
    private val tp1 = TopicPartition(topic, 1)

    /**
     * 造一个 MockConsumer 的工厂。
     *
     * 注意它只捕获"每个分区有多少条"这种可序列化的信息，消息本身在工厂里现造——
     * `ConsumerRecord` 不实现 Serializable，直接捕获会让整个 DoFn 提交失败。
     */
    private class MockConsumerFactory(
        private val topic: String,
        private val counts: Map<Int, Int>,
    ) : org.apache.beam.sdk.transforms.SerializableFunction<Map<String, Any>, org.apache.kafka.clients.consumer.Consumer<ByteArray, ByteArray>> {

        override fun apply(input: Map<String, Any>): org.apache.kafka.clients.consumer.Consumer<ByteArray, ByteArray> {
            val consumer = MockConsumer<ByteArray, ByteArray>("earliest")
            val node = Node(0, "localhost", 9092)
            consumer.updatePartitions(
                topic,
                counts.keys.map { PartitionInfo(topic, it, node, arrayOf(node), arrayOf(node)) },
            )
            val partitions = counts.keys.associate { TopicPartition(topic, it) to it }
            consumer.updateBeginningOffsets(partitions.keys.associateWith { 0L })
            consumer.updateEndOffsets(partitions.keys.associateWith { counts.getValue(it.partition()).toLong() })
            // ReadFromKafkaDoFn 会 assign + seek 之后才 poll，这里在第一次 poll 时按当前位置补数据
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
    fun `有界读取跑完整条 Beam SDF 链路，条数不多不少`() {
        val (pipeline, rows) = readPipeline(
            config,
            listOf(descriptor(tp0, 0, 5), descriptor(tp1, 0, 3)),
            mapOf(0 to 5, 1 to 3),
        )
        PAssert.thatSingleton(rows.apply(Count.globally())).isEqualTo(8L)
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `起止偏移量真的会被遵守`() {
        // 偏移量算错的后果是漏读或重读，而作业照常成功——只能靠断言条数发现
        val (pipeline, rows) = readPipeline(config, listOf(descriptor(tp0, 3, 7)), mapOf(0 to 10))
        PAssert.thatSingleton(rows.apply(Count.globally())).isEqualTo(4L)
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `string 格式下 key value 解码成文本，元数据列齐全`() {
        val (pipeline, rows) = readPipeline(config, listOf(descriptor(tp0, 0, 1)), mapOf(0 to 1))
        // 这个 lambda 会被序列化下发，不能捕获测试类的字段（topic 因此写成字面量）
        PAssert.that(rows).satisfies { output ->
            val row = output.single()
            assertEquals("k0-0", row.getString(KafkaFormats.KEY))
            assertEquals("v0-0", row.getString(KafkaFormats.VALUE))
            assertEquals("orders", row.getString(KafkaFormats.TOPIC))
            assertEquals(0, row.getInt32(KafkaFormats.PARTITION))
            assertEquals(0L, row.getInt64(KafkaFormats.OFFSET))
            assertNotNull(row.getString(KafkaFormats.TIMESTAMP_TYPE))
            // headers 是 Flink Kafka connector 同名的元数据列
            val headers = row.getMap<String, ByteArray>(KafkaFormats.HEADERS)
            assertContentEquals("t0".toByteArray(), headers!!["trace"])
            null
        }
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `raw 格式保留原始字节`() {
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
    fun `写入端能作为 sink 接进 pipeline 并产出死信流`() {
        val schema = KafkaFormats.readSchema(config)
        val pipeline = Pipeline.create()
        // 缺 value 字段的行必然进死信，这里只验证接线，发送本身由 KafkaWriteFnTest 用 MockProducer 覆盖
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
