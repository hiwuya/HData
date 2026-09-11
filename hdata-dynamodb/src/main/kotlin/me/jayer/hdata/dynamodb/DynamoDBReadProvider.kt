package me.jayer.hdata.dynamodb

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.dynamodb.internal.DynamoDBClients
import me.jayer.hdata.dynamodb.internal.DynamoDBTypeMappings
import me.jayer.hdata.dynamodb.transform.DynamoDBReadFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.QueryRequest
import software.amazon.awssdk.services.dynamodb.model.ScanRequest

/**
 * `ReadFromDynamoDB`: reads items from a DynamoDB table via Scan or Query as a bounded snapshot.
 *
 * DynamoDB is schemaless, so the output schema is not known statically. `create()` connects to
 * DynamoDB once at graph-construction time and samples up to [PROBE_SAMPLE_SIZE] real items
 * (Scan, or Query when [DynamoDBReadConfig.keyConditionExpression] is set — the same request the
 * DoFn would issue, just with a small `limit`) to derive a schema and give the output
 * `PCollection` a coder. Without this, Beam cannot serialize the output and every downstream
 * transform fails with "Unable to return a default Coder for a Beam Row" at graph-finalization
 * time. This means `ReadFromDynamoDB` needs the real table reachable at graph-construction time
 * (`--dryRun` included), unlike most other connectors.
 *
 * A consequence of sampling: an attribute that exists on some items but not on any item in the
 * sample is silently dropped from the output, even for rows where it is present — there is no
 * static schema to fall back on. Prefer [DynamoDBReadConfig.projectionExpression] to pin down
 * exactly which attributes matter when a table's items vary in shape.
 *
 * @author wuya
 */
class DynamoDBReadProvider : TypedTransformProvider<DynamoDBReadConfig>(DynamoDBReadConfig::class.java) {

    override fun identifier(): String = "ReadFromDynamoDB"

    override fun description(): String = "Read items from Amazon DynamoDB (bounded snapshot)"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: DynamoDBReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        val schema = probeSchema(config)
        return DynamoDBSource(config, schema)
    }

    private fun probeSchema(config: DynamoDBReadConfig): Schema {
        val client = DynamoDBClients.newClient(
            region = config.region,
            endpointOverride = config.endpointOverride,
            accessKeyId = config.accessKeyId,
            secretAccessKey = config.secretAccessKey,
        )
        try {
            val exprAttrValues = config.expressionAttributeValues.mapValues { (_, v) ->
                AttributeValue.builder().s(v).build()
            }
            val items = if (config.keyConditionExpression.isNotBlank()) {
                val builder = QueryRequest.builder()
                    .tableName(config.tableName)
                    .consistentRead(config.consistentRead)
                    .keyConditionExpression(config.keyConditionExpression)
                    .limit(PROBE_SAMPLE_SIZE)
                if (config.filterExpression.isNotBlank()) builder.filterExpression(config.filterExpression)
                if (config.projectionExpression.isNotBlank()) builder.projectionExpression(config.projectionExpression)
                if (exprAttrValues.isNotEmpty()) builder.expressionAttributeValues(exprAttrValues)
                client.query(builder.build()).items()
            } else {
                val builder = ScanRequest.builder()
                    .tableName(config.tableName)
                    .consistentRead(config.consistentRead)
                    .limit(PROBE_SAMPLE_SIZE)
                if (config.filterExpression.isNotBlank()) builder.filterExpression(config.filterExpression)
                if (config.projectionExpression.isNotBlank()) builder.projectionExpression(config.projectionExpression)
                if (exprAttrValues.isNotEmpty()) builder.expressionAttributeValues(exprAttrValues)
                client.scan(builder.build()).items()
            }
            return DynamoDBTypeMappings.deriveSchema(items)
        } catch (e: Exception) {
            throw HDataException("ReadFromDynamoDB could not determine the output schema: ${e.message}", e)
        } finally {
            runCatching { client.close() }
        }
    }

    private companion object {
        const val PROBE_SAMPLE_SIZE = 100
    }
}

private class DynamoDBSource(
    private val config: DynamoDBReadConfig,
    private val schema: Schema,
) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        // One trigger element per Scan segment (just [0] when parallel_scan_segments is 1, the
        // default) so the runner can schedule segments onto different workers.
        val segments = (0 until config.parallelScanSegments).toList()
        return begin
            .apply("Trigger", Create.of(segments))
            .apply("ReadFromDynamoDB", ParDo.of(DynamoDBReadFn(config, schema)))
            .setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
