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
 * Write to Kafka, with dead-letter output.
 *
 * Sending is **asynchronous**: `send()` grabs a future and holds onto it; only when `batch_size` accumulates
 * or the bundle ends do we `flush()` and collect the results one by one. Before the refactor it was
 * `send(record).get()` — every message waited for one broker round trip, so the producer's batching and
 * pipelining were completely wasted, with throughput roughly one hundredth of what it is now.
 *
 * We don't use `KafkaIO.write()` directly because it returns `PDone` and gives no per-record send result,
 * whereas HData's `error_handling` requires failed records to be sent to the dead-letter stream as-is.
 *
 * @author wuya
 */
class KafkaWriteFn(
    private val config: KafkaWriteConfig,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
    /** An injection point reserved for tests; the production path uses [defaultProducerFactory]. */
    private val producerFactory: ProducerFactory = defaultProducerFactory(),
) : DoFn<Row, Row>() {

    /** Same idea as `KafkaIO.Write.withProducerFactoryFn`: extract producer construction so it can be replaced. */
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
        val queue = checkNotNull(pending) { "writer not initialized" }
        // send() can also throw synchronously (serialization failure, metadata unavailable, buffer full); like
        // "failed to construct ProducerRecord", this means the record was not sent and must go to the dead letter
        // rather than failing the whole bundle.
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
     * Wait for the whole batch to land and verify each result.
     *
     * First `flush()` pushes the in-flight requests out; afterwards every `future.get()` returns immediately,
     * so it does not degrade into "send one, wait one".
     */
    private fun awaitPending() {
        val queue = checkNotNull(pending)
        if (queue.isEmpty()) {
            return
        }
        try {
            checkNotNull(producer).flush()
        } catch (e: Exception) {
            // flush is a whole-batch operation; on failure we cannot prove any single record landed reliably, so the
            // entire batch goes to the dead letter one by one. Clear the queue first to avoid leaving stale futures in
            // the bundle lifecycle after reject throws when deadLetter=false.
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
                // Future.get() may also throw runtime exceptions like CancellationException; this also means the record was not acknowledged.
                reject(item.record, e)
            }
        }
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("Failed to write to Kafka, routing to dead letter: {}", e.message)
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
                    "no topic configured and the input row has no ${KafkaFormats.TOPIC}(STRING) field either; don't know which topic to write to"
                )
        }
        // key is optional: if the field is absent, send a null key and let the broker round-robin partitions
        val key = if (row.schema.hasField(KafkaFormats.KEY)) {
            checkNotNull(keyFormat).encode(row.getValue<Any?>(KafkaFormats.KEY))
        } else {
            null
        }
        require(row.schema.hasField(KafkaFormats.VALUE)) {
            "the row written to Kafka is missing the ${KafkaFormats.VALUE} field; existing fields: ${row.schema.fieldNames}"
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
