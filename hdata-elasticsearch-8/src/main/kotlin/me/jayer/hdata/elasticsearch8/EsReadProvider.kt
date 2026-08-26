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
 * `ReadFromElasticsearch8`：按 index 用 Splittable DoFn 并行读，单个 index 还能按 `scan_slices` 再切分。
 */
class EsReadProvider : TypedTransformProvider<EsReadConfig>(EsReadConfig::class.java) {

    override fun identifier(): String = "ReadFromElasticsearch8"

    override fun description(): String = "从 Elasticsearch 8.x 读取，按 slice 并行的 Splittable DoFn + PIT/search_after"

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
        // 聚合下推：全局语义，所有索引合成一次查询只输出一行——
        // 不能按索引发元素，那会变成"每个索引一行的局部聚合"而不是全局结果
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
