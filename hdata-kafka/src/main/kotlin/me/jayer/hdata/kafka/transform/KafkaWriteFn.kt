package me.jayer.hdata.kafka.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.kafka.KafkaWriteConfig
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.LoggerFactory
import java.util.Properties

/**
 * 逐条（攒批）写入 Kafka。写失败且开了死信时退回逐条写，真正写不进去的记录进死信流；
 * 没开死信时异常直接抛出，作业失败。
 *
 * @author wuya
 */
class KafkaWriteFn(
    private val config: KafkaWriteConfig,
    private val inputSchema: Schema,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var producer: KafkaProducer<String, String>? = null

    private val buffered = mutableListOf<ValueInSingleWindow<Row>>()
    private val failures = mutableListOf<ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        producer = newProducer()
    }

    @StartBundle
    fun startBundle() {
        buffered.clear()
        failures.clear()
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        buffered.add(ValueInSingleWindow.of(row, timestamp, window, pane))
        if (buffered.size >= config.batchSize) {
            flush()
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        flush()
        failures.forEach { context.output(it.value, it.timestamp, it.window) }
        failures.clear()
    }

    @Teardown
    fun tearDown() {
        runCatching { producer?.close() }
        producer = null
    }

    private fun flush() {
        if (buffered.isEmpty()) {
            return
        }
        val p = checkNotNull(producer) { "生产者未初始化" }
        buffered.forEach { record ->
            try {
                val key = record.value.getString("key")
                val value = record.value.getString("value")
                    ?: throw IllegalStateException("写 Kafka 的行缺少 value 字段")
                p.send(ProducerRecord(config.topic, key, value)).get()
                RECORDS_WRITTEN.inc()
            } catch (e: Exception) {
                if (!deadLetter) {
                    throw e
                }
                LOGGER.warn("写入 Kafka 失败，转入死信: {}", e.message)
                RECORDS_REJECTED.inc()
                failures.add(
                    ValueInSingleWindow.of(
                        ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                        record.timestamp,
                        record.window,
                        record.paneInfo,
                    )
                )
            }
        }
        buffered.clear()
    }

    private fun newProducer(): KafkaProducer<String, String> {
        val props = Properties().apply {
            this["bootstrap.servers"] = config.bootstrapServers
            this["key.serializer"] = StringSerializer::class.java.name
            this["value.serializer"] = StringSerializer::class.java.name
            config.producerConfig.forEach { (k, v) -> this[k] = v }
        }
        return KafkaProducer(props)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(KafkaWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(KafkaWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(KafkaWriteFn::class.java, "records_rejected")
    }
}
