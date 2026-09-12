package me.jayer.hdata.dynamodb

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.ConnectorSupportTier
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.dynamodb.transform.DynamoDBWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToDynamoDB`: writes rows to a DynamoDB table in batches via BatchWriteItem,
 * with retries and dead-letter support.
 *
 * @author wuya
 */
class DynamoDBWriteProvider : TypedTransformProvider<DynamoDBWriteConfig>(DynamoDBWriteConfig::class.java) {

    override fun identifier(): String = "WriteToDynamoDB"

    override fun description(): String = "Write to Amazon DynamoDB in batches with dead-letter output"

    override fun supportTier(): ConnectorSupportTier = ConnectorSupportTier.EXPERIMENTAL

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(
        config: DynamoDBWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return DynamoDBSink(config, context.errorHandling != null, context.transformName)
    }
}

private class DynamoDBSink(
    private val config: DynamoDBWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        val errors = input
            .apply("Write", ParDo.of(DynamoDBWriteFn(config, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
