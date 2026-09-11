package me.jayer.hdata.dynamodb

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.dynamodb.transform.DynamoDBReadFn
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromDynamoDB`: reads items from a DynamoDB table via Scan or Query as a bounded snapshot.
 * The output schema is derived from item attributes at runtime.
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
        return DynamoDBSource(config)
    }
}

private class DynamoDBSource(private val config: DynamoDBReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        // Schema is derived at runtime from DynamoDB item attributes.
        return begin
            .apply("Trigger", Create.of(listOf(1)))
            .apply("ReadFromDynamoDB", ParDo.of(DynamoDBReadFn(config)))
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
