package me.jayer.hdata.dynamodb.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.dynamodb.DynamoDBWriteConfig
import me.jayer.hdata.dynamodb.internal.DynamoDBClients
import me.jayer.hdata.dynamodb.internal.DynamoDBTypeMappings
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemRequest
import software.amazon.awssdk.services.dynamodb.model.PutRequest
import software.amazon.awssdk.services.dynamodb.model.WriteRequest

/**
 * Writes rows to a DynamoDB table in batches via BatchWriteItem, with retries and
 * dead-letter support.
 *
 * Each bundle accumulates up to [DynamoDBWriteConfig.batchSize] rows. When the batch is full or
 * the bundle ends, the rows are flushed via a BatchWriteItem call. On transient failures, the
 * batch is retried up to [DynamoDBWriteConfig.maxRetries] times with exponential back-off.
 *
 * DynamoDB's BatchWriteItem supports a maximum of 25 items per call.
 *
 * @author wuya
 */
class DynamoDBWriteFn(
    private val config: DynamoDBWriteConfig,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var client: DynamoDbClient? = null

    @Transient
    private var pending: MutableList<ValueInSingleWindow<Row>>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    @Setup
    fun setup() {
        client = DynamoDBClients.newClient(
            region = config.region,
            endpointOverride = config.endpointOverride,
            accessKeyId = config.accessKeyId,
            secretAccessKey = config.secretAccessKey,
        )
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

        // DynamoDB BatchWriteItem max is 25.
        val chunks = records.chunked(25)

        for (chunk in chunks) {
            var lastException: Exception? = null
            var succeeded = false
            val maxAttempts = config.maxRetries + 1

            for (attempt in 1..maxAttempts) {
                try {
                    executeBatchWrite(chunk.map { it.value })
                    RECORDS_WRITTEN.inc(chunk.size.toLong())
                    succeeded = true
                    break
                } catch (e: Exception) {
                    lastException = e
                    LOGGER.warn(
                        "DynamoDB batch write attempt {}/{} failed: {}",
                        attempt, maxAttempts, e.message,
                    )
                    if (attempt < maxAttempts) {
                        Thread.sleep(config.retryDelayMs * attempt)
                    }
                }
            }

            if (!succeeded) {
                LOGGER.error("DynamoDB batch write failed after {} attempts", maxAttempts)
                // Try writing individual items one more time; failures go to dead letter.
                for (record in chunk) {
                    try {
                        executeSingleWrite(record.value)
                        RECORDS_WRITTEN.inc(1)
                    } catch (e: Exception) {
                        reject(record, e)
                    }
                }
            }
        }
    }

    private fun executeBatchWrite(rows: List<Row>) {
        val dynamoDb = checkNotNull(client) { "DynamoDB client is not initialized" }

        val writeRequests = rows.map { row ->
            val item = DynamoDBTypeMappings.rowToItem(row)
            WriteRequest.builder()
                .putRequest(PutRequest.builder().item(item).build())
                .build()
        }

        val request = BatchWriteItemRequest.builder()
            .requestItems(mapOf(config.tableName to writeRequests))
            .build()

        val response = dynamoDb.batchWriteItem(request)

        // Handle unprocessed items by retrying them.
        var unprocessed = response.unprocessedItems()[config.tableName] ?: emptyList()
        var retryCount = 0
        while (unprocessed.isNotEmpty() && retryCount < 10) {
            retryCount++
            Thread.sleep(100L * retryCount)
            val retryRequest = BatchWriteItemRequest.builder()
                .requestItems(mapOf(config.tableName to unprocessed))
                .build()
            val retryResponse = dynamoDb.batchWriteItem(retryRequest)
            unprocessed = retryResponse.unprocessedItems()[config.tableName] ?: emptyList()
        }

        if (unprocessed.isNotEmpty()) {
            throw RuntimeException("DynamoDB batch write left ${unprocessed.size} unprocessed items after retries")
        }
    }

    private fun executeSingleWrite(row: Row) {
        val dynamoDb = checkNotNull(client) { "DynamoDB client is not initialized" }
        val item = DynamoDBTypeMappings.rowToItem(row)
        val request = software.amazon.awssdk.services.dynamodb.model.PutItemRequest.builder()
            .tableName(config.tableName)
            .item(item)
            .build()
        dynamoDb.putItem(request)
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        RECORDS_REJECTED.inc()
        LOGGER.warn("DynamoDB write failed; routing record to dead letter: {}", e.message)
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
        private val LOGGER = LoggerFactory.getLogger(DynamoDBWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(DynamoDBWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(DynamoDBWriteFn::class.java, "records_rejected")
    }
}
