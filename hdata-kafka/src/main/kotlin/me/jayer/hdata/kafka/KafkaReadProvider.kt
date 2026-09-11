package me.jayer.hdata.kafka

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.kafka.internal.KafkaOffsets
import me.jayer.hdata.kafka.transform.KafkaRecordToRowFn
import org.apache.beam.sdk.io.kafka.KafkaIO
import org.apache.beam.sdk.io.kafka.KafkaSourceDescriptor
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.apache.kafka.common.serialization.ByteArrayDeserializer
import org.slf4j.LoggerFactory

/**
 * `ReadFromKafka`: reads from Kafka; the reading itself directly reuses Beam's `ReadFromKafkaDoFn`.
 *
 * This module **no longer writes its own Splittable DoFn for reading**. Behind Beam's
 * `KafkaIO.readSourceDescriptors()` is a mature SDF (`ReadFromKafkaDoFn`) with watermark estimation,
 * dynamic re-splitting, and offset committing, which is far more correct than a hand-written version:
 * the pre-refactor version had several fatal problems —
 *
 *  - `poll()` returning an empty batch would `break`, so a slightly slow broker was **treated as done**
 *    and data was silently lost;
 *  - `tryClaim(range.to - 1)` claimed the whole range at once, meaning no splitting at all, and a slow
 *    partition dragged down the whole job;
 *  - it could only run bounded snapshots, no streaming consumption;
 *  - `key_format` / `value_format` were accepted and then dropped, so binary messages were destroyed by
 *    `StringDeserializer`.
 *
 * The only responsibilities kept here are: translate the Flink-style startup/bounded modes into
 * per-partition start/stop offsets (see [KafkaOffsets]), and convert `KafkaRecord` into a
 * schema-bearing [Row].
 *
 * @author wuya
 */
class KafkaReadProvider : TypedTransformProvider<KafkaReadConfig>(KafkaReadConfig::class.java) {

    override fun identifier(): String = "ReadFromKafka"

    override fun description(): String = "Read from Kafka, reusing Beam's ReadFromKafkaDoFn (Splittable DoFn)"

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
        val schema = KafkaFormats.readSchema(config)
        val descriptors = KafkaOffsets.resolve(config)
        LOGGER.info("ReadFromKafka resolved {} partition read units", descriptors.size)

        if (descriptors.isEmpty()) {
            // None of the partitions have readable data (e.g. a bounded snapshot where start == end); just emit an
            // empty collection. Handing it to KafkaIO would fail because Create.of(emptyList()) cannot infer a coder.
            return begin.apply("Empty", Create.empty(schema)).setRowSchema(schema)
        }

        val records = begin
            .apply("Descriptors", Create.of(descriptors).withCoder(descriptorCoder(begin)))
            .apply("ReadFromKafka", readTransform())
            // ReadSourceDescriptors' withBounded() is package-private, but as long as every descriptor carries a
            // stopReadOffset the read is guaranteed to finish; not marking it BOUNDED would make Flink/Spark run it
            // as a streaming job that never exits on its own.
            .let { if (config.bounded) it.setIsBoundedInternal(PCollection.IsBounded.BOUNDED) else it }

        return records
            .apply("ToRow", ParDo.of(KafkaRecordToRowFn(config.keyFormat, config.valueFormat)))
            .setRowSchema(schema)
    }

    private fun descriptorCoder(begin: PBegin) =
        begin.pipeline.schemaRegistry.getSchemaCoder(KafkaSourceDescriptor::class.java)

    private fun readTransform(): KafkaIO.ReadSourceDescriptors<ByteArray, ByteArray> {
        var read = KafkaIO.readSourceDescriptors<ByteArray, ByteArray>()
            .withBootstrapServers(config.bootstrapServers)
            .withKeyDeserializer(ByteArrayDeserializer::class.java)
            .withValueDeserializer(ByteArrayDeserializer::class.java)
            .withConsumerConfigUpdates(KafkaOffsets.consumerProperties(config))
        if (config.commitOffsetsOnCheckpoint) {
            read = read.commitOffsets()
        }
        return read
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

private val LOGGER = LoggerFactory.getLogger(KafkaReadProvider::class.java)
