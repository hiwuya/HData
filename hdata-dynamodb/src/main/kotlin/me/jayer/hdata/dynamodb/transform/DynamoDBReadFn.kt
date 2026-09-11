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
 * The output schema is derived from the first page of results at runtime. Subsequent pages are
 * emitted immediately without buffering, keeping memory usage proportional to a single page
 * rather than the entire table.
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
        val maxItems = if (config.maxItems > 0) config.maxItems else Long.MAX_VALUE
        var totalCount = 0L

        if (config.keyConditionExpression.isNotBlank()) {
            totalCount = executeQuery(dynamoDb, maxItems, context)
        } else {
            totalCount = executeScan(dynamoDb, maxItems, context)
        }

        LOGGER.info("Read {} items from DynamoDB table {}", totalCount, config.tableName)
    }

    private fun executeScan(dynamoDb: DynamoDbClient, maxItems: Long, context: ProcessContext): Long {
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
        var schema: Schema? = null
        var count = 0L

        // Process first page: derive schema, emit rows.
        val firstPage = response.items()
        if (firstPage.isEmpty()) return 0

        schema = DynamoDBTypeMappings.deriveSchema(firstPage)
        for (item in firstPage) {
            if (count >= maxItems) break
            context.output(DynamoDBTypeMappings.itemToRow(item, schema))
            count++
        }

        // Process subsequent pages: emit rows immediately without buffering.
        while (response.lastEvaluatedKey() != null && count < maxItems) {
            builder.exclusiveStartKey(response.lastEvaluatedKey())
            response = dynamoDb.scan(builder.build())
            for (item in response.items()) {
                if (count >= maxItems) break
                context.output(DynamoDBTypeMappings.itemToRow(item, schema!!))
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
        var schema: Schema? = null
        var count = 0L

        val firstPage = response.items()
        if (firstPage.isEmpty()) return 0

        schema = DynamoDBTypeMappings.deriveSchema(firstPage)
        for (item in firstPage) {
            if (count >= maxItems) break
            context.output(DynamoDBTypeMappings.itemToRow(item, schema))
            count++
        }

        while (response.lastEvaluatedKey() != null && count < maxItems) {
            builder.exclusiveStartKey(response.lastEvaluatedKey())
            response = dynamoDb.query(builder.build())
            for (item in response.items()) {
                if (count >= maxItems) break
                context.output(DynamoDBTypeMappings.itemToRow(item, schema!!))
                count++
            }
        }

        return count
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(DynamoDBReadFn::class.java)
    }
}
