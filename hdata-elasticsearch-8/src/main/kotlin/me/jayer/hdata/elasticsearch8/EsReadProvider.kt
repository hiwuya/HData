package me.jayer.hdata.elasticsearch8

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.elasticsearch8.buildAggregateSchema
import me.jayer.hdata.elasticsearch8.parseEsAggregations
import me.jayer.hdata.elasticsearch8.transform.EsAggregateFn
import me.jayer.hdata.elasticsearch8.transform.EsReadFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `ReadFromElasticsearch8` reads indices in parallel with a Splittable DoFn and optional slices.
 */
class EsReadProvider : TypedTransformProvider<EsReadConfig>(EsReadConfig::class.java) {

    override fun identifier(): String = "ReadFromElasticsearch8"

    override fun description(): String = "Read Elasticsearch 8.x with a slice-parallel Splittable DoFn and PIT/search_after"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: EsReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        val elements = if (config.indices.isNotEmpty()) config.indices else listOf(config.index)
        return EsSource(config, buildSchema(config.schemaFields), elements)
    }
}

private class EsSource(
    private val config: EsReadConfig,
    private val schema: Schema,
    private val elements: List<String>,
) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        // Aggregation has global semantics: combine all indices in one query and emit one result row.
        if (config.aggregations.isNotEmpty()) {
            val aggSchema = buildAggregateSchema(parseEsAggregations(config.aggregations))
            val indexExpression = elements.joinToString(",")
            return begin.apply("Indices", Create.of(listOf(indexExpression)))
                .apply("Aggregate", ParDo.of(EsAggregateFn(config)))
                .setRowSchema(aggSchema)
        }
        return begin.apply("Indices", Create.of(elements))
            .apply("Read", ParDo.of(EsReadFn(config, config.schemaFields)))
            .setRowSchema(schema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
