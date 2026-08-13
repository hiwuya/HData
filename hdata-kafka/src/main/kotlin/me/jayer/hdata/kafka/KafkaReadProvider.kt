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
 * `ReadFromKafka`：读取 Kafka，读取本身直接复用 Beam 的 `ReadFromKafkaDoFn`。
 *
 * 本模块**不再自己写读取用的 Splittable DoFn**。Beam 的 `KafkaIO.readSourceDescriptors()` 背后
 * 就是一个成熟的 SDF（`ReadFromKafkaDoFn`），带 watermark 估计、动态再切分、偏移量提交，
 * 比手写版本正确得多：重构前那版有几个致命问题——
 *
 *  - `poll()` 返回空批次就 `break`，broker 稍慢一点就**当作读完**，静默丢数据；
 *  - `tryClaim(range.to - 1)` 一次性认领整段，等于没有切分，慢分区拖垮整个作业；
 *  - 只能跑有界快照，没法流式消费；
 *  - `key_format` / `value_format` 收下就丢掉，二进制消息被 `StringDeserializer` 毁掉。
 *
 * 这里保留的职责只有一件：把 Flink 风格的 startup/bounded 模式翻译成每分区的起止偏移量
 * （见 [KafkaOffsets]），以及把 `KafkaRecord` 转成带 schema 的 [Row]。
 *
 * @author wuya
 */
class KafkaReadProvider : TypedTransformProvider<KafkaReadConfig>(KafkaReadConfig::class.java) {

    override fun identifier(): String = "ReadFromKafka"

    override fun description(): String = "从 Kafka 读取，复用 Beam 的 ReadFromKafkaDoFn（Splittable DoFn）"

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
        LOGGER.info("ReadFromKafka 解析出 {} 个分区读取单元", descriptors.size)

        if (descriptors.isEmpty()) {
            // 全部分区都没有可读数据（例如 bounded 快照时 start == end），产出一个空集合即可，
            // 交给 KafkaIO 会因为 Create.of(emptyList()) 推断不出 coder 而报错
            return begin.apply("Empty", Create.empty(schema)).setRowSchema(schema)
        }

        val records = begin
            .apply("Descriptors", Create.of(descriptors).withCoder(descriptorCoder(begin)))
            .apply("ReadFromKafka", readTransform())
            // ReadSourceDescriptors 的 withBounded() 是包级私有的，但只要每个 descriptor 都带了
            // stopReadOffset，读取就一定会结束；不标成 BOUNDED 会让 Flink/Spark 按流作业跑而不自行退出
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
