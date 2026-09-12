package me.jayer.hdata.sqs.transform

import me.jayer.hdata.sqs.SQSReadConfig
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
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import java.net.URI

/**
 * Reads messages from an Amazon SQS queue as a bounded snapshot or continuous stream.
 *
 * SQS does not expose a stable range that can be split across competing consumers. The SDF
 * restriction is therefore the monotonically increasing number of successfully emitted messages:
 * it enforces the batch cap exactly and gives a continuous consumer a checkpoint/resume boundary.
 *
 * @author wuya
 */
@DoFn.UnboundedPerElement
class SQSReadFn(
    private val config: SQSReadConfig,
) : DoFn<Any, Row>() {

    @Transient
    private var client: SqsClient? = null

    @Setup
    fun setup() {
        client = buildClient()
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.close() }
        client = null
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
        @Element ignored: Any,
        tracker: RestrictionTracker<OffsetRange, Long>,
        watermarkEstimator: ManualWatermarkEstimator<Instant>,
        output: OutputReceiver<Row>,
    ): ProcessContinuation {
        val sqs = checkNotNull(client) { "SQS client is not initialized" }
        var position = tracker.currentRestriction().from
        val deadline = System.currentTimeMillis() + PROCESS_TIME_BUDGET.millis

        while (System.currentTimeMillis() < deadline) {
            val request = ReceiveMessageRequest.builder()
                .queueUrl(config.queueUrl)
                .maxNumberOfMessages(config.batchSize)
                .visibilityTimeout(config.visibilityTimeout)
                .waitTimeSeconds(config.waitTimeSeconds)
                .attributeNamesWithStrings(
                    MessageSystemAttributeName.SENT_TIMESTAMP.toString(),
                    MessageSystemAttributeName.APPROXIMATE_RECEIVE_COUNT.toString(),
                    MessageSystemAttributeName.MESSAGE_GROUP_ID.toString(),
                )
                .build()

            val response = sqs.receiveMessage(request)
            val messages = response.messages()

            if (messages.isEmpty()) {
                if (config.streaming) return ProcessContinuation.resume().withResumeDelay(RESUME_DELAY)
                // An early empty queue is valid for a bounded snapshot. Record the final
                // attempt so OffsetRangeTracker.checkDone() accepts the restriction.
                tracker.tryClaim(tracker.currentRestriction().to)
                return ProcessContinuation.stop()
            }

            for (msg in messages) {
                if (!tracker.tryClaim(position)) return ProcessContinuation.stop()
                val attributes = mutableMapOf<String, String>()
                msg.attributes().forEach { (k, v) -> attributes[k.toString()] = v }

                val row = Row.withSchema(SQS_READ_SCHEMA)
                    .addValue(msg.messageId())
                    .addValue(msg.body())
                    .addValue(msg.receiptHandle())
                    .addValue(attributes)
                    .build()
                val timestamp = attributes[MessageSystemAttributeName.SENT_TIMESTAMP.toString()]
                    ?.toLongOrNull()
                    ?.let(::Instant)
                    ?: Instant.now()
                output.outputWithTimestamp(row, timestamp)
                watermarkEstimator.setWatermark(timestamp)

                // Delete after read to prevent re-delivery in the bounded snapshot.
                if (config.deleteAfterRead) {
                    sqs.deleteMessage(
                        DeleteMessageRequest.builder()
                            .queueUrl(config.queueUrl)
                            .receiptHandle(msg.receiptHandle())
                            .build()
                    )
                }
                position++
            }
        }
        return ProcessContinuation.resume().withResumeDelay(RESUME_DELAY)
    }

    private fun buildClient(): SqsClient {
        val builder = SqsClient.builder()
            .region(Region.of(config.region))

        if (config.endpointOverride.isNotBlank()) {
            builder.endpointOverride(URI.create(config.endpointOverride))
        }

        if (config.accessKeyId.isNotBlank() && config.secretAccessKey.isNotBlank()) {
            builder.credentialsProvider(
                StaticCredentialsProvider.create(
                    AwsBasicCredentials.create(config.accessKeyId, config.secretAccessKey)
                )
            )
        }
        // When no explicit credentials are provided, the SDK uses its default chain
        // (env vars, instance profile, ECS task role, etc.).

        return builder.build()
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val PROCESS_TIME_BUDGET = Duration.millis(2000)
        private val RESUME_DELAY = Duration.millis(100)
    }
}
