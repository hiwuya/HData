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
import java.io.Serializable

internal data class RabbitMQChannelConnection(
    val connection: com.rabbitmq.client.Connection,
    val channel: com.rabbitmq.client.Channel,
)

/** Kept serializable because the factory travels with [RabbitMQWriteFn] to remote Beam workers. */
internal fun interface RabbitMQChannelFactory : Serializable {
    fun open(config: RabbitMQWriteConfig): RabbitMQChannelConnection
}

private object DefaultRabbitMQChannelFactory : RabbitMQChannelFactory {
    override fun open(config: RabbitMQWriteConfig): RabbitMQChannelConnection {
        val factory = RabbitMQConnections.newFactory(
            config.host, config.port, config.virtualHost, config.username, config.password,
        )
        val connection = factory.newConnection()
        return RabbitMQChannelConnection(connection, connection.createChannel())
    }
}

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

    // Kept out of the public connector constructor; the alternate factory exists solely to make confirmation
    // failures testable without a broker and is still serializable for DirectRunner's DoFn lifecycle.
    private var channelFactory: RabbitMQChannelFactory = DefaultRabbitMQChannelFactory

    internal constructor(
        config: RabbitMQWriteConfig,
        errorSchema: Schema,
        deadLetter: Boolean,
        transformName: String,
        channelFactory: RabbitMQChannelFactory,
    ) : this(config, errorSchema, deadLetter, transformName) {
        this.channelFactory = channelFactory
    }

    @Transient
    private var connection: com.rabbitmq.client.Connection? = null

    @Transient
    private var channel: com.rabbitmq.client.Channel? = null

    @Transient
    private var pending: MutableList<ValueInSingleWindow<Row>>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    @Setup
    fun setup() {
        openConnection()
        pending = mutableListOf()
        failures = mutableListOf()
    }

    private fun openConnection() {
        val opened = channelFactory.open(config)
        connection = opened.connection
        channel = opened.channel
        channel!!.confirmSelect()

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
        closeConnection()
    }

    private fun closeConnection() {
        runCatching { channel?.close() }
        channel = null
        runCatching { connection?.close() }
        connection = null
    }

    @StartBundle
    fun startBundle() {
        if (channel?.isOpen != true) {
            closeConnection()
            openConnection()
        }
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
            checkNotNull(pending).add(record)
            RECORDS_WRITTEN.inc()

            if (checkNotNull(pending).size >= config.batchSize) {
                flushPending()
            }
        } catch (e: Exception) {
            closeConnection()
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
        val records = checkNotNull(pending)
        if (records.isEmpty()) return
        try {
            val ch = checkNotNull(channel)
            ch.waitForConfirmsOrDie(30_000)
            RECORDS_CONFIRMED.inc(records.size.toLong())
            records.clear()
        } catch (e: Exception) {
            // A confirm can be lost after the broker accepted all or part of this batch. Surface every original
            // row for idempotent replay rather than claiming success or silently dropping it.
            val suspect = records.toList()
            records.clear()
            closeConnection()
            suspect.forEach { reject(it, e) }
        }
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
