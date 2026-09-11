package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.elasticsearch6.Elasticsearch6AggregateFn
import me.jayer.hdata.elasticsearch6.buildAggregateSchema
import me.jayer.hdata.elasticsearch6.parseEs6Aggregations
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromElasticsearch6`: reads from ES 6.x by scroll pagination, parallel by index.
 */
class ReadFromElasticsearch6 : TypedTransformProvider<Elasticsearch6ReadConfig>(Elasticsearch6ReadConfig::class.java) {

    override fun identifier(): String = "ReadFromElasticsearch6"

    override fun description(): String = "Read from Elasticsearch 6.x by scroll pagination, parallel by index and slice"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: Elasticsearch6ReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        val fields = parseSchemaFields(config.schemaFields)
        val schema = if (fields.isEmpty()) DOCUMENT_SCHEMA else buildSchema(fields)
        return Elasticsearch6Source(config, fields, schema)
    }
}

private class Elasticsearch6Source(
    private val config: Elasticsearch6ReadConfig,
    private val fields: List<EsField>,
    private val schema: org.apache.beam.sdk.schemas.Schema,
) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        val indices = config.indexList()
        // Push-down aggregation: global semantics; all indices are combined into a single query that outputs one row —
        // we cannot emit an element per index, which would become "a per-index local aggregation of one row each"
        // rather than a global result.
        if (config.aggregations.isNotEmpty()) {
            val aggSchema = buildAggregateSchema(parseEs6Aggregations(config.aggregations))
            val indexExpression = indices.joinToString(",")
            return begin.apply("Indices", Create.of(listOf(indexExpression)))
                .apply(
                    "Aggregate",
                    ParDo.of(
                        Elasticsearch6AggregateFn(
                            config.nodes(),
                            config.username,
                            config.password,
                            config.aggregations,
                            config.scanQuery,
                        ),
                    ),
                )
                .setRowSchema(aggSchema)
        }
        return begin.apply("Indices", Create.of(indices))
            .apply(
                "Read",
                ParDo.of(
                    Elasticsearch6ReadFn(
                        config.nodes(),
                        config.username,
                        config.password,
                        schema,
                        fields,
                        config.scanQuery,
                        config.scrollSize,
                        config.scrollTimeoutMinutes,
                        config.scanSlices,
                        config.limit,
                    ),
                ),
            )
            .setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
