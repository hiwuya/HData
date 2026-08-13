package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToElasticsearch6`：批量写入 ES 6.x，支持死信输出。
 */
class WriteToElasticsearch6 : TypedTransformProvider<Elasticsearch6WriteConfig>(Elasticsearch6WriteConfig::class.java) {

    override fun identifier(): String = "WriteToElasticsearch6"

    override fun description(): String = "批量写入 Elasticsearch 6.x，支持死信输出"

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(
        config: Elasticsearch6WriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        val fields = parseSchemaFields(config.schemaFields)
        return Elasticsearch6Sink(config, fields, context.errorHandling != null, context.transformName)
    }
}

private class Elasticsearch6Sink(
    private val config: Elasticsearch6WriteConfig,
    private val fields: List<EsField>,
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
                    Elasticsearch6WriteFn(
                        config.nodes(),
                        config.index,
                        config.username,
                        config.password,
                        fields,
                        config.batchSize,
                        inputSchema,
                        errorSchema,
                        deadLetter,
                        transformName,
                    ),
                ),
            )
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
