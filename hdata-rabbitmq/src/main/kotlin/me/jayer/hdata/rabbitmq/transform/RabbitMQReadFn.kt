package me.jayer.hdata.rabbitmq.transform

import com.rabbitmq.client.GetResponse
import me.jayer.hdata.rabbitmq.RabbitMQReadConfig
import me.jayer.hdata.rabbitmq.internal.RabbitMQConnections
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.ManualWatermarkEstimator
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.transforms.splittabledofn.WatermarkEstimators
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.values.Row
import org.joda.time.Duration
import org.joda.time.Instant
import org.slf4j.LoggerFactory
import java.io.IOException
import java.util.concurrent.TimeoutException

/**
 * Reads messages from a RabbitMQ queue via synchronous `basicGet`.
 *
 * This is an unbounded Splittable DoFn. A RabbitMQ queue cannot safely be range-split because
 * competing consumers change ownership, so it deliberately has one trigger element. Its synthetic
 * offset is the number of emitted messages: it bounds batch reads precisely and lets a streaming
 * read checkpoint and yield instead of monopolizing a worker during idle polling.
 *
 * @author wuya
 */
@DoFn.UnboundedPerElement
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

    @GetInitialRestriction
    fun getInitialRestriction(): OffsetRange =
        OffsetRange(0, if (config.maxMessages == 0) Long.MAX_VALUE else config.maxMessages.toLong())

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = OffsetRangeTracker(restriction)

    @GetInitialWatermarkEstimatorState
    fun getInitialWatermarkEstimatorState(): Instant = BoundedWindow.TIMESTAMP_MIN_VALUE

    @NewWatermarkEstimator
    fun newWatermarkEstimator(@WatermarkEstimatorState state: Instant): WatermarkEstimators.Manual =
        WatermarkEstimators.Manual(state)

    @ProcessElement
    fun processElement(
        tracker: RestrictionTracker<OffsetRange, Long>,
        watermarkEstimator: ManualWatermarkEstimator<Instant>,
        output: OutputReceiver<Row>,
    ): ProcessContinuation {
        val ch = checkNotNull(channel) { "RabbitMQ channel is not initialized" }
        var position = tracker.currentRestriction().from
        val deadline = System.currentTimeMillis() + PROCESS_TIME_BUDGET.millis

        while (System.currentTimeMillis() < deadline) {
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
                if (config.waitTimeoutMs > 0) {
                    Thread.sleep(config.waitTimeoutMs)
                    val retry = try {
                        ch.basicGet(config.queue, true)
                    } catch (e: Exception) {
                        null
                    }
                    if (retry != null) {
                        if (!tracker.tryClaim(position)) return ProcessContinuation.stop()
                        emitMessage(output, watermarkEstimator, retry)
                        position++
                        continue
                    }
                }
                if (config.streaming) return ProcessContinuation.resume().withResumeDelay(RESUME_DELAY)
                // Record the terminal attempt so OffsetRangeTracker.checkDone() accepts a
                // snapshot that ended before its configured maximum.
                tracker.tryClaim(tracker.currentRestriction().to)
                return ProcessContinuation.stop()
            } else {
                if (!tracker.tryClaim(position)) return ProcessContinuation.stop()
                emitMessage(output, watermarkEstimator, response)
                position++
            }
        }
        return ProcessContinuation.resume().withResumeDelay(RESUME_DELAY)
    }

    private fun emitMessage(
        output: OutputReceiver<Row>,
        watermarkEstimator: ManualWatermarkEstimator<Instant>,
        response: GetResponse,
    ) {
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
        val timestamp = properties.timestamp?.let { Instant(it.time) } ?: Instant.now()
        output.outputWithTimestamp(row, timestamp)
        watermarkEstimator.setWatermark(timestamp)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val PROCESS_TIME_BUDGET = Duration.millis(2000)
        private val RESUME_DELAY = Duration.millis(100)
        private val LOGGER = LoggerFactory.getLogger(RabbitMQReadFn::class.java)
    }
}
