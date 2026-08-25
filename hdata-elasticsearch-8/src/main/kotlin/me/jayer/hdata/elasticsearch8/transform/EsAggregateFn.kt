package me.jayer.hdata.elasticsearch8.transform

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.search.TrackHits
import co.elastic.clients.elasticsearch._types.aggregations.Aggregation
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import me.jayer.hdata.elasticsearch8.EsReadConfig
import me.jayer.hdata.elasticsearch8.aggregateFieldName
import me.jayer.hdata.elasticsearch8.buildAggregateSchema
import me.jayer.hdata.elasticsearch8.buildEsClient
import me.jayer.hdata.elasticsearch8.parseEsAggregations
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.elasticsearch.client.RestClient
import java.io.StringReader

/**
 * ES 聚合下推：把 `count`/`min`/`max`/`sum`/`avg` 翻译成 ES 原生 aggregation，在 ES 侧算完返回单行。
 *
 * 聚合是对整个索引（或 `scan_query` 过滤后的结果集）的全局计算，所以不走 slice 并行——
 * 每个索引一次 `size(0)` 的聚合查询即可，结果就是聚合后的一行。
 *
 * @author wuya
 */
class EsAggregateFn(private val config: EsReadConfig) : DoFn<String, Row>() {

    @Transient
    private var client: ElasticsearchClient? = null

    @Transient
    private var restClient: RestClient? = null

    @Transient
    private var query: Query? = null

    @Transient
    private var schema: Schema? = null

    @Setup
    fun setup() {
        val (c, rc) = buildEsClient(config.connectionUri, config.apiKey, config.username, config.password)
        client = c
        restClient = rc
        query = if (config.scanQuery.isNotBlank()) {
            Query.of { it.withJson(StringReader(config.scanQuery)) }
        } else {
            Query.of { it.matchAll { m -> m } }
        }
        schema = buildAggregateSchema(parseEsAggregations(config.aggregations))
    }

    @Teardown
    fun tearDown() {
        runCatching { restClient?.close() }
        restClient = null
        client = null
    }

    @ProcessElement
    fun processElement(@Element index: String, receiver: OutputReceiver<Row>) {
        val specs = parseEsAggregations(config.aggregations)
        val aggMap = specs.filter { it.op != "count" }.associate { spec ->
            val name = aggregateFieldName(spec)
            name to when (spec.op) {
                "min" -> Aggregation.of { it.min { m -> m.field(spec.column) } }
                "max" -> Aggregation.of { it.max { m -> m.field(spec.column) } }
                "sum" -> Aggregation.of { it.sum { s -> s.field(spec.column) } }
                "avg" -> Aggregation.of { it.avg { a -> a.field(spec.column) } }
                else -> error("不支持的聚合: ${spec.op}")
            }
        }
        val resp = checkNotNull(client).search({ b ->
            b.trackTotalHits(TrackHits.of { it.enabled(true) }).size(0).query(checkNotNull(query))
            if (aggMap.isNotEmpty()) b.aggregations(aggMap)
            b
        }, Map::class.java)
        val count = resp.hits().total()?.value() ?: 0L
        val aggResults = resp.aggregations() ?: emptyMap()
        val builder = Row.withSchema(checkNotNull(schema))
        specs.forEach { spec ->
            when (spec.op) {
                "count" -> builder.addValue(count)
                "min" -> builder.addValue(aggResults[aggregateFieldName(spec)]?.min()?.value())
                "max" -> builder.addValue(aggResults[aggregateFieldName(spec)]?.max()?.value())
                "sum" -> builder.addValue(aggResults[aggregateFieldName(spec)]?.sum()?.value())
                "avg" -> builder.addValue(aggResults[aggregateFieldName(spec)]?.avg()?.value())
            }
        }
        receiver.output(builder.build())
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
