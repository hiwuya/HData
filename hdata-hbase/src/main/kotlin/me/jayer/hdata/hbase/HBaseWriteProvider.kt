package me.jayer.hdata.hbase

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.hbase.transform.HBaseWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToHBase`：批量写入 HBase，支持死信输出。
 */
class HBaseWriteProvider : TypedTransformProvider<HBaseWriteConfig>(HBaseWriteConfig::class.java) {

    override fun identifier(): String = "WriteToHBase"

    override fun description(): String = "批量写入 HBase，支持死信输出"

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(
        config: HBaseWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return HBaseSink(config, context.errorHandling != null, context.transformName)
    }
}

private class HBaseSink(
    private val config: HBaseWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val inputSchema = input.schema
        val errorSchema = ErrorSchemas.of(inputSchema)
        val errors = input
            .apply(
                "Write",
                ParDo.of(
                    HBaseWriteFn(
                        config.zookeeperQuorum,
                        config.table,
                        config.rowkeyField,
                        config.family,
                        config.schemaFields,
                        config.batchSize,
                        inputSchema,
                        errorSchema,
                        deadLetter,
                        transformName,
                    )
                ),
            )
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
