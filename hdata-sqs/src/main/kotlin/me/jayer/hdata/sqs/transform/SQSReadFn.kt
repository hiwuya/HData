package me.jayer.hdata.sqs.transform

import me.jayer.hdata.sqs.SQSReadConfig
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest
import software.amazon.awssdk.services.sqs.model.MessageSystemAttributeName
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest
import java.net.URI

/**
 * Reads messages from an Amazon SQS queue as a bounded snapshot via long-polling.
 *
 * @author wuya
 */
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

    @ProcessElement
    fun processElement(context: ProcessContext) {
        val sqs = checkNotNull(client) { "SQS client is not initialized" }
        val maxPolls = if (config.maxMessages <= 0) Int.MAX_VALUE else config.maxMessages
        var pollCount = 0

        while (pollCount < maxPolls) {
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

            if (messages.isEmpty()) break

            for (msg in messages) {
                val attributes = mutableMapOf<String, String>()
                msg.attributes().forEach { (k, v) -> attributes[k.toString()] = v }

                val row = Row.withSchema(SQS_READ_SCHEMA)
                    .addValue(msg.messageId())
                    .addValue(msg.body())
                    .addValue(msg.receiptHandle())
                    .addValue(attributes)
                    .build()
                context.output(row)

                // Delete after read to prevent re-delivery in the bounded snapshot.
                if (config.deleteAfterRead) {
                    sqs.deleteMessage(
                        DeleteMessageRequest.builder()
                            .queueUrl(config.queueUrl)
                            .receiptHandle(msg.receiptHandle())
                            .build()
                    )
                }
            }

            pollCount++
            // If we got fewer messages than requested, the queue is likely empty.
            if (messages.size < config.batchSize) break
        }

        LOGGER.info("Read completed after {} polls", pollCount)
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
        private val LOGGER = LoggerFactory.getLogger(SQSReadFn::class.java)
    }
}
