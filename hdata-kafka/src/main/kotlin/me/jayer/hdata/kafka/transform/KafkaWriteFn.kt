package me.jayer.hdata.kafka.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.kafka.KafkaFormat
import me.jayer.hdata.kafka.KafkaFormats
import me.jayer.hdata.kafka.KafkaWriteConfig
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.apache.kafka.clients.producer.KafkaProducer
import org.apache.kafka.clients.producer.Producer
import org.apache.kafka.clients.producer.ProducerRecord
import org.apache.kafka.common.serialization.ByteArraySerializer
import org.slf4j.LoggerFactory
import java.util.Properties
import java.util.concurrent.ExecutionException
import java.util.concurrent.Future

/**
 * 写入 Kafka，支持死信输出。
 *
 * 发送是**异步**的：`send()` 拿到 future 先攒着，攒够 `batch_size` 或 bundle 结束时才 `flush()`
 * 并逐个取结果。重构前是 `send(record).get()`——每条消息都等一次 broker 往返，
 * producer 的批量与流水线能力完全失效，吞吐大约是现在的百分之一量级。
 *
 * 没有直接用 `KafkaIO.write()`，是因为它返回 `PDone`，拿不到逐条的发送结果，
 * 而 HData 的 `error_handling` 要求把写失败的记录原样送进死信流。
 *
 * @author wuya
 */
class KafkaWriteFn(
    private val config: KafkaWriteConfig,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
    /** 只为测试留的注入点，生产路径走 [defaultProducerFactory]。 */
    private val producerFactory: ProducerFactory = defaultProducerFactory(),
) : DoFn<Row, Row>() {

    /** 与 `KafkaIO.Write.withProducerFactoryFn` 同样的思路：把 producer 的构造抽出去以便替换。 */
    fun interface ProducerFactory : java.io.Serializable {
        fun create(properties: Map<String, String>): Producer<ByteArray, ByteArray>
    }

    @Transient
    private var producer: Producer<ByteArray, ByteArray>? = null

    @Transient
    private var keyFormat: KafkaFormat? = null

    @Transient
    private var valueFormat: KafkaFormat? = null

    @Transient
    private var pending: MutableList<Pending>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    private class Pending(val record: ValueInSingleWindow<Row>, val ack: Future<*>)

    @Setup
    fun setup() {
        keyFormat = KafkaFormats.of(config.keyFormat, "key_format")
        valueFormat = KafkaFormats.of(config.valueFormat, "value_format")
        pending = mutableListOf()
        failures = mutableListOf()
        producer = producerFactory.create(config.producerProperties())
    }

    @Teardown
    fun tearDown() {
        runCatching { producer?.close() }
        producer = null
    }

    @StartBundle
    fun startBundle() {
        pending?.clear()
        failures?.clear()
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        val record = ValueInSingleWindow.of(row, timestamp, window, pane)
        val queue = checkNotNull(pending) { "写入器未初始化" }
        // send() 也可能同步抛（序列化失败、拿不到元数据、缓冲区满），和"构造 ProducerRecord 失败"
        // 一样属于这条记录没发出去，都要走死信而不是让整个 bundle 挂掉
        val ack = try {
            checkNotNull(producer).send(toProducerRecord(row))
        } catch (e: Exception) {
            reject(record, e)
            return
        }
        queue.add(Pending(record, ack))
        if (queue.size >= config.batchSize) {
            awaitPending()
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        awaitPending()
        val rejected = checkNotNull(failures)
        rejected.forEach { context.output(it.value, it.timestamp, it.window) }
        rejected.clear()
    }

    /**
     * 等这一批全部落地并逐条核对结果。
     *
     * 先 `flush()` 把在途请求推出去，之后每个 `future.get()` 都是立即返回的，
     * 不会退化成"发一条等一条"。
     */
    private fun awaitPending() {
        val queue = checkNotNull(pending)
        if (queue.isEmpty()) {
            return
        }
        try {
            checkNotNull(producer).flush()
        } catch (e: Exception) {
            // flush 是整批操作，失败时无法证明其中任何一条已经可靠落地；整批逐条进死信。
            // 先清队列，避免 deadLetter=false 时 reject 抛出后在 bundle 生命周期里留下陈旧 future。
            val failed = queue.toList()
            queue.clear()
            failed.forEach { reject(it.record, e) }
            return
        }

        val completed = queue.toList()
        queue.clear()
        completed.forEach { item ->
            try {
                item.ack.get()
                RECORDS_WRITTEN.inc()
            } catch (e: ExecutionException) {
                reject(item.record, e.cause as? Exception ?: e)
            } catch (e: InterruptedException) {
                Thread.currentThread().interrupt()
                throw e
            } catch (e: Exception) {
                // Future.get() 还可能抛 CancellationException 等运行时异常，同样属于该记录未确认。
                reject(item.record, e)
            }
        }
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("写入 Kafka 失败，转入死信: {}", e.message)
        RECORDS_REJECTED.inc()
        checkNotNull(failures).add(
            ValueInSingleWindow.of(
                ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                record.timestamp,
                record.window,
                record.paneInfo,
            )
        )
    }

    private fun toProducerRecord(row: Row): ProducerRecord<ByteArray, ByteArray> {
        val topic = config.topic.ifBlank {
            row.schema.takeIf { it.hasField(KafkaFormats.TOPIC) }?.let { row.getString(KafkaFormats.TOPIC) }
                ?: throw IllegalStateException(
                    "config 里没有配 topic，输入行也没有 ${KafkaFormats.TOPIC}(STRING) 字段，不知道该写到哪个 topic"
                )
        }
        // key 是可选的：没有这个字段就发 null key，由 broker 轮询分区
        val key = if (row.schema.hasField(KafkaFormats.KEY)) {
            checkNotNull(keyFormat).encode(row.getValue<Any?>(KafkaFormats.KEY))
        } else {
            null
        }
        require(row.schema.hasField(KafkaFormats.VALUE)) {
            "写 Kafka 的行缺少 ${KafkaFormats.VALUE} 字段，现有字段: ${row.schema.fieldNames}"
        }
        val value = checkNotNull(valueFormat).encode(row.getValue<Any?>(KafkaFormats.VALUE))
        return ProducerRecord(topic, key, value)
    }

    companion object {
        private const val serialVersionUID: Long = 1

        fun defaultProducerFactory(): ProducerFactory = ProducerFactory { properties ->
            val props = Properties().apply {
                putAll(properties)
                this["key.serializer"] = ByteArraySerializer::class.java.name
                this["value.serializer"] = ByteArraySerializer::class.java.name
            }
            KafkaProducer(props)
        }

        private val LOGGER = LoggerFactory.getLogger(KafkaWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(KafkaWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(KafkaWriteFn::class.java, "records_rejected")
    }
}
