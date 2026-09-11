package me.jayer.hdata.dynamodb.transform

import me.jayer.hdata.dynamodb.DynamoDBReadConfig
import me.jayer.hdata.dynamodb.internal.DynamoDBClients
import me.jayer.hdata.dynamodb.internal.DynamoDBTypeMappings
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import software.amazon.awssdk.services.dynamodb.DynamoDbClient
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.ScanRequest
import software.amazon.awssdk.services.dynamodb.model.ScanResponse

/**
 * Executes a Scan (or Query) against a DynamoDB table and converts each item to a Beam [Row].
 *
 * The output schema is derived from the scanned items at runtime. This is a plain DoFn
 * (not an SDF): parallelism comes from the number of input elements. The read provider
 * supplies a single trigger element, so the scan/query runs exactly once.
 *
 * DynamoDB Scan/Query responses are paginated; this DoFn follows the pagination token
 * until all items are retrieved.
 *
 * @author wuya
 */
class DynamoDBReadFn(
    private val config: DynamoDBReadConfig,
) : DoFn<Any, Row>() {

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

    @ProcessElement
    fun processElement(context: ProcessContext) {
        val dynamoDb = checkNotNull(client) { "DynamoDB client is not initialized" }
        val allItems = mutableListOf<Map<String, AttributeValue>>()
        val maxItems = if (config.maxItems > 0) config.maxItems else Long.MAX_VALUE

        if (config.keyConditionExpression.isNotBlank()) {
            // Use Query instead of Scan.
            executeQuery(dynamoDb, allItems, maxItems)
        } else {
            executeScan(dynamoDb, allItems, maxItems)
        }

        if (allItems.isEmpty()) {
            LOGGER.info("No items read from DynamoDB table {}", config.tableName)
            return
        }

        // Derive schema from all collected items.
        val schema = DynamoDBTypeMappings.deriveSchema(allItems)
        for (item in allItems) {
            val row = DynamoDBTypeMappings.itemToRow(item, schema)
            context.output(row)
        }

        LOGGER.info("Read {} items from DynamoDB table {}", allItems.size, config.tableName)
    }

    private fun executeScan(dynamoDb: DynamoDbClient, items: MutableList<Map<String, AttributeValue>>, maxItems: Long) {
        val builder = ScanRequest.builder()
            .tableName(config.tableName)
            .consistentRead(config.consistentRead)

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
        items.addAll(response.items())

        while (response.lastEvaluatedKey() != null && items.size < maxItems) {
            builder.exclusiveStartKey(response.lastEvaluatedKey())
            response = dynamoDb.scan(builder.build())
            items.addAll(response.items())
        }

        // Trim to maxItems.
        if (items.size > maxItems) {
            val trimmed = items.subList(0, maxItems.toInt())
            items.clear()
            items.addAll(trimmed)
        }
    }

    private fun executeQuery(dynamoDb: DynamoDbClient, items: MutableList<Map<String, AttributeValue>>, maxItems: Long) {
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
        items.addAll(response.items())

        while (response.lastEvaluatedKey() != null && items.size < maxItems) {
            builder.exclusiveStartKey(response.lastEvaluatedKey())
            response = dynamoDb.query(builder.build())
            items.addAll(response.items())
        }

        // Trim to maxItems.
        if (items.size > maxItems) {
            val trimmed = items.subList(0, maxItems.toInt())
            items.clear()
            items.addAll(trimmed)
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(DynamoDBReadFn::class.java)
    }
}
