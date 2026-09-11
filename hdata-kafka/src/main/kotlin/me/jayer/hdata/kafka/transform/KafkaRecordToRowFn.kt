package me.jayer.hdata.kafka.transform

import me.jayer.hdata.kafka.KafkaFormat
import me.jayer.hdata.kafka.KafkaFormats
import org.apache.beam.sdk.io.kafka.KafkaRecord
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row

/**
 * Convert Beam's [KafkaRecord] into a schema-bearing [Row].
 *
 * The format name rather than the [KafkaFormat] itself goes through the constructor, because although the
 * enum serializes fine alongside the DoFn, the schema must be rebuilt in the same place — so it's simpler
 * to parse everything once in `@Setup`.
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
        val row = Row.withSchema(checkNotNull(schema) { "schema not initialized" })
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

    /** Kafka allows a header with the same name to appear multiple times; when converting to a map the last one is kept (matching Flink's behavior). */
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
