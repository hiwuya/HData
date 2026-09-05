package me.jayer.hdata.kafka.transform

import me.jayer.hdata.kafka.KafkaFormat
import me.jayer.hdata.kafka.KafkaFormats
import org.apache.beam.sdk.io.kafka.KafkaRecord
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row

/**
 * 把 Beam 的 [KafkaRecord] 转成带 schema 的 [Row]。
 *
 * 格式名而不是 [KafkaFormat] 本身走构造函数，是因为枚举跟着 DoFn 一起序列化虽然没问题，
 * 但 schema 也要在同一处重建，索性统一在 `@Setup` 里解析一次。
 *
 * @author wuya
 */
class KafkaRecordToRowFn(
    private val keyFormat: String,
    private val valueFormat: String,
) : DoFn<KafkaRecord<ByteArray, ByteArray>, Row>() {

    @Transient
    private var schema: Schema? = null

    @Transient
    private var key: KafkaFormat? = null

    @Transient
    private var value: KafkaFormat? = null

    @Setup
    fun setup() {
        key = KafkaFormats.of(keyFormat, "key_format")
        value = KafkaFormats.of(valueFormat, "value_format")
        schema = KafkaFormats.readSchema(key!!, value!!)
    }

    @ProcessElement
    fun processElement(@Element record: KafkaRecord<ByteArray, ByteArray>, receiver: OutputReceiver<Row>) {
        val row = Row.withSchema(checkNotNull(schema) { "schema 未初始化" })
            .addValue(checkNotNull(key).decode(record.kv.key))
            .addValue(checkNotNull(value).decode(record.kv.value))
            .addValue(record.topic)
            .addValue(record.partition)
            .addValue(record.offset)
            .addValue(record.timestamp)
            .addValue(record.timestampType.name)
            .addValue(headersOf(record))
            .build()
        RECORDS_READ.inc()
        receiver.output(row)
    }

    /** Kafka 允许同名 header 出现多次，转成 map 时保留最后一个（与 Flink 的行为一致）。 */
    private fun headersOf(record: KafkaRecord<ByteArray, ByteArray>): Map<String, ByteArray?>? {
        val headers = record.headers ?: return null
        val map = LinkedHashMap<String, ByteArray?>()
        headers.forEach { header -> map[header.key()] = header.value() }
        return map
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val RECORDS_READ = Metrics.counter(KafkaRecordToRowFn::class.java, "records_read")
    }
}
