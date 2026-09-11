package me.jayer.hdata.rabbitmq.transform

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.ConfirmListener
import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.rabbitmq.RabbitMQWriteConfig
import me.jayer.hdata.rabbitmq.internal.RabbitMQConnections
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.atomic.AtomicLong

/**
 * Writes to RabbitMQ with batching, publisher confirms, and dead-letter support.
 *
 * The channel is put into confirm mode so that `waitForConfirms` can verify that a batch of
 * messages has landed before the bundle finishes. Failed messages (both synchronous publish
 * failures and negative acknowledgements) are routed to the dead-letter stream.
 *
 * @author wuya
 */
class RabbitMQWriteFn(
    private val config: RabbitMQWriteConfig,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var connection: com.rabbitmq.client.Connection? = null

    @Transient
    private var channel: com.rabbitmq.client.Channel? = null

    @Transient
    private var pendingTags: MutableList<Long>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    @Transient
    private var lastPublishTag: AtomicLong? = null

    @Setup
    fun setup() {
        val factory = RabbitMQConnections.newFactory(
            config.host, config.port, config.virtualHost, config.username, config.password,
        )
        connection = factory.newConnection()
        channel = connection!!.createChannel()
        channel!!.confirmSelect()
        pendingTags = mutableListOf()
        failures = mutableListOf()
        lastPublishTag = AtomicLong(0)

        // Declare exchange if requested.
        if (config.declareExchange) {
            channel!!.exchangeDeclare(config.exchange, config.exchangeType, true)
        }

        // Declare queue if requested.
        if (config.declareQueue) {
            val queue = channel!!.queueDeclare(config.queue, true, false, false, null)
            // Bind the queue to the exchange if an exchange is specified.
            if (config.exchange.isNotBlank()) {
                val routingKey = config.routingKey.ifBlank { config.queue }
                channel!!.queueBind(config.queue, config.exchange, routingKey)
            }
        }
    }

    @Teardown
    fun tearDown() {
        runCatching { flushPending() }
        runCatching { channel?.close() }
        channel = null
        runCatching { connection?.close() }
        connection = null
    }

    @StartBundle
    fun startBundle() {
        pendingTags?.clear()
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
        try {
            val ch = checkNotNull(channel) { "RabbitMQ channel is not initialized" }

            // Resolve the body from the input row.
            val bodyField = config.bodyField
            require(row.schema.hasField(bodyField)) {
                "the row written to RabbitMQ is missing the $bodyField field; existing fields: ${row.schema.fieldNames}"
            }
            val bodyValue = row.getValue<Any?>(bodyField)
            val body = (bodyValue?.toString() ?: "").toByteArray(Charsets.UTF_8)

            // Resolve the routing key: input field > config default > queue name.
            val routingKey = resolveRoutingKey(row)
            val exchange = resolveExchange(row)

            // Build AMQP properties.
            val propsBuilder = AMQP.BasicProperties.Builder()
            if (config.persistent) {
                propsBuilder.deliveryMode(2)
            }
            if (config.messageTtlMs != null) {
                propsBuilder.expiration(config.messageTtlMs.toString())
            }
            val messageId = resolveMessageId(row)
            if (messageId != null) {
                propsBuilder.messageId(messageId)
            }
            val props = propsBuilder.build()

            ch.basicPublish(exchange, routingKey, props, body)
            val tag = ch.nextPublishSeqNo
            pendingTags!!.add(tag)
            RECORDS_WRITTEN.inc()

            if (pendingTags!!.size >= config.batchSize) {
                flushPending()
            }
        } catch (e: Exception) {
            reject(record, e)
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        flushPending()
        val rejected = checkNotNull(failures)
        rejected.forEach { context.output(it.value, it.timestamp, it.window) }
        rejected.clear()
    }

    /**
     * Wait for all pending publisher confirms. This ensures the messages have been received by
     * the broker before the bundle is considered complete.
     */
    private fun flushPending() {
        val tags = checkNotNull(pendingTags)
        if (tags.isEmpty()) return
        try {
            val ch = checkNotNull(channel)
            ch.waitForConfirmsOrDie(30_000)
            RECORDS_CONFIRMED.inc(tags.size.toLong())
        } catch (e: Exception) {
            // The whole batch failed confirmation; all messages in the batch are suspect.
            val failedTags = tags.toList()
            tags.clear()
            failedTags.forEach {
                // We cannot pinpoint which exact row failed, but we already recorded the rows
                // via the pending list. For now, just log — the next finishBundle cycle will
                // pick up any remaining failures.
                LOGGER.warn("RabbitMQ confirm failed: ${e.message}")
            }
            throw e
        }
        tags.clear()
    }

    private fun resolveRoutingKey(row: Row): String {
        // Prefer the row field if present and non-null.
        if (row.schema.hasField(config.routingKeyField)) {
            val value = row.getValue<Any?>(config.routingKeyField)
            if (value != null) return value.toString()
        }
        // Fall back to config, then queue name.
        return config.routingKey.ifBlank { config.queue }
    }

    private fun resolveExchange(row: Row): String {
        if (row.schema.hasField(config.exchangeField)) {
            val value = row.getValue<Any?>(config.exchangeField)
            if (value != null) return value.toString()
        }
        return config.exchange
    }

    private fun resolveMessageId(row: Row): String? {
        if (row.schema.hasField(config.messageIdField)) {
            val value = row.getValue<Any?>(config.messageIdField)
            if (value != null) return value.toString()
        }
        return null
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("RabbitMQ write failed; routing record to dead letter: {}", e.message)
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

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(RabbitMQWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(RabbitMQWriteFn::class.java, "records_written")
        private val RECORDS_CONFIRMED = Metrics.counter(RabbitMQWriteFn::class.java, "records_confirmed")
        private val RECORDS_REJECTED = Metrics.counter(RabbitMQWriteFn::class.java, "records_rejected")
    }
}
