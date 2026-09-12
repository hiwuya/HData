package me.jayer.hdata.dynamodb.transform

import me.jayer.hdata.dynamodb.DynamoDBReadConfig
import me.jayer.hdata.dynamodb.internal.DynamoDBClients
import me.jayer.hdata.dynamodb.internal.DynamoDBTypeMappings
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse

/**
 * Executes a Scan (or Query) against a DynamoDB table and converts each item to a Beam [Row].
 *
 * The input element is a Scan segment index (`0` when [DynamoDBReadConfig.parallelScanSegments] is
 * 1, i.e. sequential Scan/Query); [me.jayer.hdata.dynamodb.DynamoDBReadProvider] emits one element
 * per segment, so a value > 1 lets the runner schedule segments onto different workers for real
 * read parallelism. Pages within a segment are emitted immediately without buffering, keeping
 * memory usage proportional to a single page rather than the entire segment. The output schema is
 * fixed (see [schema]), not re-derived here. Each DynamoDB segment is one durable SDF work item;
 * the provider creates one input element per native segment.
 *
 * @author wuya
 */
@DoFn.BoundedPerElement
class DynamoDBReadFn(
    private val config: DynamoDBReadConfig,
    /**
     * Schema probed by [me.jayer.hdata.dynamodb.DynamoDBReadProvider] at graph-construction time
     * (from a small sample of real items) and fixed for the life of this DoFn. Re-deriving a
     * schema from just the first page at runtime — the previous behavior — could disagree with
     * whatever schema the provider already committed to the PCollection's coder, so all pages
     * (including the first) are converted against this one fixed schema instead. An attribute
     * absent from the probe sample is silently dropped from rows that do have it; see the
     * provider's kdoc.
     */
    private val schema: Schema,
) : DoFn<Int, Row>() {

    @Transient
    private var client: DynamoDbClient? = null

    @Setup
    fun setup() {
        client = DynamoDBClients.newClient(
            region = config.region,
            endpointOverride = config.endpointOverride,
            accessKeyId = config.accessKeyId,
            secretAccessKey = config.secretAccessKey,
        )
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.close() }
        client = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(): OffsetRange = OffsetRange(0, 1)

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = OffsetRangeTracker(restriction)

    @ProcessElement
    fun processElement(
        @Element segmentBoxed: Int?,
        tracker: RestrictionTracker<OffsetRange, Long>,
        context: ProcessContext,
    ) {
        if (!tracker.tryClaim(0)) return
        // Kotlin compiles a non-null `Int` parameter to the JVM primitive `int`, which Beam's DoFn
        // reflection rejects against the always-boxed `Integer` type of a generic DoFn<Int, _> —
        // "Type of @Element must match the DoFn type". A nullable `Int?` parameter is boxed, which
        // matches; it is never actually null since every input element comes from Create.of(segments).
        val segment = checkNotNull(segmentBoxed)
        val dynamoDb = checkNotNull(client) { "DynamoDB client is not initialized" }
        val maxItems = if (config.maxItems > 0) config.maxItems else Long.MAX_VALUE
        var totalCount = 0L

        if (config.keyConditionExpression.isNotBlank()) {
            totalCount = executeQuery(dynamoDb, maxItems, context)
        } else {
            totalCount = executeScan(dynamoDb, segment, maxItems, context)
        }

        LOGGER.info("Read {} items from DynamoDB table {} (segment {}/{})", totalCount, config.tableName, segment, config.parallelScanSegments)
    }

    private fun executeScan(dynamoDb: DynamoDbClient, segment: Int, maxItems: Long, context: ProcessContext): Long {
        val builder = ScanRequest.builder()
            .tableName(config.tableName)
            .consistentRead(config.consistentRead)

        if (config.parallelScanSegments > 1) {
            builder.segment(segment).totalSegments(config.parallelScanSegments)
        }

        if (config.filterExpression.isNotBlank()) {
            builder.filterExpression(config.filterExpression)
        }
        if (config.projectionExpression.isNotBlank()) {
            builder.projectionExpression(config.projectionExpression)
        }
        if (config.expressionAttributeValues.isNotEmpty()) {
            val exprAttrValues = config.expressionAttributeValues.mapValues { (_, v) ->
                AttributeValue.builder().s(v).build()
            }
            builder.expressionAttributeValues(exprAttrValues)
        }

        var response: ScanResponse = dynamoDb.scan(builder.build())
        var count = 0L

        for (item in response.items()) {
            if (count >= maxItems) break
            context.output(DynamoDBTypeMappings.itemToRow(item, schema))
            count++
        }

        // Process subsequent pages: emit rows immediately without buffering. A page that returns
        // zero items yet still carries a LastEvaluatedKey is a valid (if unusual) DynamoDB
        // response — but if it happens over and over with no forward progress at all, treat it as
        // a stuck pagination cursor rather than spinning forever (observed against DynamoDB Local).
        var emptyPagesInARow = 0
        while (response.lastEvaluatedKey() != null && count < maxItems) {
            builder.exclusiveStartKey(response.lastEvaluatedKey())
            response = dynamoDb.scan(builder.build())
            if (response.items().isEmpty()) {
                emptyPagesInARow++
                if (emptyPagesInARow >= MAX_EMPTY_PAGES_IN_A_ROW) {
                    LOGGER.warn(
                        "DynamoDB Scan pagination made no progress for {} consecutive pages on table {}; stopping instead of looping forever",
                        emptyPagesInARow, config.tableName,
                    )
                    break
                }
                continue
            }
            emptyPagesInARow = 0
            for (item in response.items()) {
                if (count >= maxItems) break
                context.output(DynamoDBTypeMappings.itemToRow(item, schema))
                count++
            }
        }

        return count
    }

    private fun executeQuery(dynamoDb: DynamoDbClient, maxItems: Long, context: ProcessContext): Long {
        val builder = software.amazon.awssdk.services.dynamodb.model.QueryRequest.builder()
            .tableName(config.tableName)
            .consistentRead(config.consistentRead)
            .keyConditionExpression(config.keyConditionExpression)

        if (config.filterExpression.isNotBlank()) {
            builder.filterExpression(config.filterExpression)
        }
        if (config.projectionExpression.isNotBlank()) {
            builder.projectionExpression(config.projectionExpression)
        }
        if (config.expressionAttributeValues.isNotEmpty()) {
            val exprAttrValues = config.expressionAttributeValues.mapValues { (_, v) ->
                AttributeValue.builder().s(v).build()
            }
            builder.expressionAttributeValues(exprAttrValues)
        }

        var response = dynamoDb.query(builder.build())
        var count = 0L

        for (item in response.items()) {
            if (count >= maxItems) break
            context.output(DynamoDBTypeMappings.itemToRow(item, schema))
            count++
        }

        var emptyPagesInARow = 0
        while (response.lastEvaluatedKey() != null && count < maxItems) {
            builder.exclusiveStartKey(response.lastEvaluatedKey())
            response = dynamoDb.query(builder.build())
            if (response.items().isEmpty()) {
                emptyPagesInARow++
                if (emptyPagesInARow >= MAX_EMPTY_PAGES_IN_A_ROW) {
                    LOGGER.warn(
                        "DynamoDB Query pagination made no progress for {} consecutive pages on table {}; stopping instead of looping forever",
                        emptyPagesInARow, config.tableName,
                    )
                    break
                }
                continue
            }
            emptyPagesInARow = 0
            for (item in response.items()) {
                if (count >= maxItems) break
                context.output(DynamoDBTypeMappings.itemToRow(item, schema))
                count++
            }
        }

        return count
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(DynamoDBReadFn::class.java)

        /** Safety valve against a pagination cursor that never advances (observed against DynamoDB Local). */
        private const val MAX_EMPTY_PAGES_IN_A_ROW = 3
    }
}
