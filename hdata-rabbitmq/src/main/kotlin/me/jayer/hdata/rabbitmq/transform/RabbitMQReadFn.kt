package me.jayer.hdata.rabbitmq.transform

import com.rabbitmq.client.AMQP
import com.rabbitmq.client.GetResponse
import me.jayer.hdata.rabbitmq.RabbitMQReadConfig
import me.jayer.hdata.rabbitmq.internal.RabbitMQConnections
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.TimeoutException

/**
 * Reads messages from a RabbitMQ queue via synchronous `basicGet`.
 *
 * This is a plain DoFn (not an SDF): parallelism comes from the number of DoFn instances, which
 * equals the input PCollection's width. Since a bounded read consumes messages from a single
 * queue, only one DoFn instance actually reads (the other instances get an empty input). For
 * higher throughput on a single queue, split the queue's contents into multiple sub-queues or
 * use multiple routing keys.
 *
 * @author wuya
 */
class RabbitMQReadFn(
    private val config: RabbitMQReadConfig,
) : DoFn<Any, Row>() {

    @Transient
    private var connection: com.rabbitmq.client.Connection? = null

    @Transient
    private var channel: com.rabbitmq.client.Channel? = null

    @Setup
    fun setup() {
        val factory = RabbitMQConnections.newFactory(
            config.host, config.port, config.virtualHost, config.username, config.password,
        )
        connection = factory.newConnection()
        channel = connection!!.createChannel()
        // Declare the queue if it does not exist; this is idempotent.
        channel!!.queueDeclare(config.queue, true, false, false, null)
    }

    @Teardown
    fun tearDown() {
        runCatching { channel?.close() }
        channel = null
        runCatching { connection?.close() }
        connection = null
    }

    @ProcessElement
    fun processElement(context: ProcessContext) {
        val ch = checkNotNull(channel) { "RabbitMQ channel is not initialized" }
        val maxMessages = if (config.maxMessages <= 0) Long.MAX_VALUE else config.maxMessages.toLong()
        var consumed = 0L

        while (consumed < maxMessages) {
            val response: GetResponse? = try {
                ch.basicGet(config.queue, true) // autoAck = true for bounded snapshot
            } catch (e: IOException) {
                LOGGER.warn("Failed to basicGet from queue ${config.queue}: ${e.message}")
                break
            } catch (e: TimeoutException) {
                LOGGER.warn("Timeout while reading from queue ${config.queue}: ${e.message}")
                break
            }

            if (response == null) {
                // Queue is empty — wait a bit in case more messages are arriving.
                if (config.waitTimeoutMs > 0 && consumed == 0L) {
                    Thread.sleep(config.waitTimeoutMs)
                    // Retry once after waiting.
                    val retry = try {
                        ch.basicGet(config.queue, true)
                    } catch (e: Exception) {
                        null
                    }
                    if (retry == null) break
                    emitMessage(context, retry)
                    consumed++
                } else {
                    break
                }
            } else {
                emitMessage(context, response)
                consumed++
            }
        }
    }

    private fun emitMessage(context: ProcessContext, response: GetResponse) {
        val envelope = response.envelope
        val body = String(response.body, Charsets.UTF_8)
        val properties = response.props
        val row = Row.withSchema(RABBITMQ_READ_SCHEMA)
            .addValue(envelope.exchange)
            .addValue(envelope.routingKey)
            .addValue(body)
            .addValue(properties.messageId)
            .addValue(envelope.deliveryTag)
            .build()
        context.output(row)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(RabbitMQReadFn::class.java)
    }
}
