package me.jayer.hdata.kafka

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.kafka.transform.KafkaReadFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.LoggerFactory
import java.util.Properties

/**
 * `ReadFromKafka`：按 topic/分区偏移量范围读，使用 Splittable DoFn 并行读。
 *
 * 分区列表在构图阶段通过 consumer 的 `partitionsFor` 拿到，所以提交作业的机器需要能连上 broker。
 */
class KafkaReadProvider : TypedTransformProvider<KafkaReadConfig>(KafkaReadConfig::class.java) {

    override fun identifier(): String = "ReadFromKafka"

    override fun description(): String = "按 topic/分区偏移量范围从 Kafka 读取，使用 Splittable DoFn 并行读"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: KafkaReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return KafkaSource(config)
    }
}

private class KafkaSource(private val config: KafkaReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        val partitions = listPartitions()
        LOGGER.info("ReadFromKafka 共 {} 个分区: {}", partitions.size, partitions)
        return begin.apply("Partitions", Create.of(partitions))
            .apply(
                "Read",
                ParDo.of(
                    KafkaReadFn(
                        config.bootstrapServers,
                        config.groupId,
                        config.consumerConfig,
                        config.scanStartupMode,
                        config.scanStartupSpecificOffsets,
                        config.scanStartupTimestampMillis,
                        config.offsetSplitSize,
                    )
                ),
            )
            .setRowSchema(KAFKA_READ_SCHEMA)
    }

    private fun listPartitions(): List<KafkaTopicPartition> {
        val props = Properties().apply {
            this["bootstrap.servers"] = config.bootstrapServers
            this["key.deserializer"] = StringDeserializer::class.java.name
            this["value.deserializer"] = StringDeserializer::class.java.name
            this["group.id"] = if (config.groupId.isNotBlank()) config.groupId else "hdata-kafka-metadata"
            this["enable.auto.commit"] = "false"
            config.consumerConfig.forEach { (k, v) -> this[k] = v }
        }
        KafkaConsumer<String, String>(props).use { consumer ->
            return config.topics.flatMap { topic ->
                consumer.partitionsFor(topic).map { KafkaTopicPartition(topic, it.partition()) }
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/** 读出的固定 schema：`key` / `value`(STRING 可空) + `topic` / `partition` / `offset` / `timestamp`。 */
val KAFKA_READ_SCHEMA: Schema = Schema.builder()
    .addNullableField("key", Schema.FieldType.STRING)
    .addNullableField("value", Schema.FieldType.STRING)
    .addStringField("topic")
    .addInt32Field("partition")
    .addInt64Field("offset")
    .addInt64Field("timestamp")
    .build()

fun kafkaRecordToRow(rec: ConsumerRecord<String, String>): Row =
    Row.withSchema(KAFKA_READ_SCHEMA)
        .addValue(rec.key())
        .addValue(rec.value())
        .addValue(rec.topic())
        .addValue(rec.partition())
        .addValue(rec.offset())
        .addValue(rec.timestamp())
        .build()

private val LOGGER = LoggerFactory.getLogger(KafkaReadProvider::class.java)
