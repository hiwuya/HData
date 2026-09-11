package me.jayer.hdata.elasticsearch8.transform

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch.core.SearchRequest
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
 * Builds an aggregate search request as a top-level function so tests can verify it without a cluster.
 *
 * [indexExpression] is a comma-separated multi-index expression; aggregation is global and returns one row.
 */
internal fun buildAggregateSearchRequest(
    indexExpression: String,
    query: Query,
    aggMap: Map<String, Aggregation>,
): SearchRequest = SearchRequest.of { b ->
    b.index(indexExpression)
        .trackTotalHits(TrackHits.of { it.enabled(true) })
        .size(0)
        .query(query)
    if (aggMap.isNotEmpty()) b.aggregations(aggMap)
    b
}

/**
 * Elasticsearch aggregate pushdown for `count`, `min`, `max`, `sum`, and `avg`, returning one result row.
 *
 * Aggregation covers every configured index (after `scan_query`) and uses one `size(0)` request rather than slices.
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
    fun processElement(@Element indexExpression: String, receiver: OutputReceiver<Row>) {
        val specs = parseEsAggregations(config.aggregations)
        val aggMap = specs.filter { it.op != "count" }.associate { spec ->
            val name = aggregateFieldName(spec)
            name to when (spec.op) {
                "min" -> Aggregation.of { it.min { m -> m.field(spec.column) } }
                "max" -> Aggregation.of { it.max { m -> m.field(spec.column) } }
                "sum" -> Aggregation.of { it.sum { s -> s.field(spec.column) } }
                "avg" -> Aggregation.of { it.avg { a -> a.field(spec.column) } }
                else -> error("Unsupported aggregation: ${spec.op}")
            }
        }
        val resp = checkNotNull(client)
            .search(buildAggregateSearchRequest(indexExpression, checkNotNull(query), aggMap), Map::class.java)
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
