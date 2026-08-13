package me.jayer.hdata.mongodb

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.mongodb.transform.MongoWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToMongoDb`：批量写入 MongoDB，支持死信输出。
 */
class MongoWriteProvider : TypedTransformProvider<MongoWriteConfig>(MongoWriteConfig::class.java) {

    override fun identifier(): String = "WriteToMongoDb"

    override fun description(): String = "批量写入 MongoDB，支持死信输出"

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(
        config: MongoWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return MongoSink(config, context.errorHandling != null, context.transformName)
    }
}

private class MongoSink(
    private val config: MongoWriteConfig,
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
                    MongoWriteFn(
                        config.connectionUri,
                        config.database,
                        config.collection,
                        config.schemaFields,
                        config.batchSize,
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
