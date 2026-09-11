package me.jayer.hdata.sqs.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.sqs.SQSWriteConfig
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.slf4j.LoggerFactory
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider
import software.amazon.awssdk.regions.Region
import software.amazon.awssdk.services.sqs.SqsClient
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequest
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry
import java.net.URI
import java.util.UUID

/**
 * Writes messages to an Amazon SQS queue in batches, with dead-letter support.
 *
 * @author wuya
 */
class SQSWriteFn(
    private val config: SQSWriteConfig,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var client: SqsClient? = null

    @Transient
    private var pending: MutableList<ValueInSingleWindow<Row>>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    @Setup
    fun setup() {
        client = buildClient()
        pending = mutableListOf()
        failures = mutableListOf()
    }

    @Teardown
    fun tearDown() {
        runCatching { flushPending() }
        runCatching { client?.close() }
        client = null
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
        pending!!.add(ValueInSingleWindow.of(row, timestamp, window, pane))
        if (pending!!.size >= config.batchSize) {
            flushPending()
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        flushPending()
        val rejected = checkNotNull(failures)
        rejected.forEach { context.output(it.value, it.timestamp, it.window) }
        rejected.clear()
    }

    private fun flushPending() {
        val queue = checkNotNull(pending)
        if (queue.isEmpty()) return
        val records = queue.toList()
        queue.clear()

        try {
            val sqs = checkNotNull(client)
            // SQS batch limit is 10.
            records.chunked(10).forEach { chunk ->
                val entries = chunk.mapIndexed { index, record ->
                    buildEntry(record.value, index)
                }
                val batchRequest = SendMessageBatchRequest.builder()
                    .queueUrl(config.queueUrl)
                    .entries(entries)
                    .build()
                sqs.sendMessageBatch(batchRequest)
                RECORDS_WRITTEN.inc(chunk.size.toLong())
            }
        } catch (e: Exception) {
            LOGGER.warn("SQS batch write failed: {}", e.message)
            records.forEach { reject(it, e) }
        }
    }

    private fun buildEntry(row: Row, index: Int): SendMessageBatchRequestEntry {
        require(row.schema.hasField(config.bodyField)) {
            "row is missing field ${config.bodyField}; available: ${row.schema.fieldNames}"
        }
        val body = row.getValue<Any?>(config.bodyField)?.toString() ?: ""

        val builder = SendMessageBatchRequestEntry.builder()
            .id(index.toString())
            .messageBody(body)

        // FIFO fields.
        if (config.messageGroupIdField.isNotBlank() && row.schema.hasField(config.messageGroupIdField)) {
            builder.messageGroupId(row.getValue<String>(config.messageGroupIdField))
        }
        if (config.messageDeduplicationIdField.isNotBlank() && row.schema.hasField(config.messageDeduplicationIdField)) {
            builder.messageDeduplicationId(row.getValue<String>(config.messageDeduplicationIdField))
        }

        return builder.build()
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
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
        // When no explicit credentials are provided, the SDK uses its default chain.
        return builder.build()
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(SQSWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(SQSWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(SQSWriteFn::class.java, "records_rejected")
    }
}
