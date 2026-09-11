package me.jayer.hdata.clickhouse

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.clickhouse.transform.ClickHouseWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToClickHouse`: writes rows to a ClickHouse table in batches, with retries and
 * dead-letter support.
 *
 * @author wuya
 */
class ClickHouseWriteProvider : TypedTransformProvider<ClickHouseWriteConfig>(ClickHouseWriteConfig::class.java) {

    override fun identifier(): String = "WriteToClickHouse"

    override fun description(): String = "Write to ClickHouse in batches with dead-letter output"

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(
        config: ClickHouseWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return ClickHouseSink(config, context.errorHandling != null, context.transformName)
    }
}

private class ClickHouseSink(
    private val config: ClickHouseWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        val errors = input
            .apply("Write", ParDo.of(ClickHouseWriteFn(config, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
