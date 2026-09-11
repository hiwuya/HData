package me.jayer.hdata.elasticsearch6

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.aggregations.AggregationBuilders
import org.elasticsearch.search.aggregations.metrics.ParsedSingleValueNumericMetricsAggregation
import org.elasticsearch.search.builder.SearchSourceBuilder

/**
 * Builds the aggregation search request. It is extracted as a top-level function so it can be unit-tested without a
 * real cluster; [indexExpression] is a comma-separated multi-index expression (aggregation has global semantics, so all
 * indices are combined into one query that outputs a single row).
 */
internal fun buildAggregateSearchRequest(indexExpression: String, source: SearchSourceBuilder): SearchRequest =
    SearchRequest(*indexExpression.split(",").map { it.trim() }.filter { it.isNotEmpty() }.toTypedArray())
        .source(source)

/**
 * ES 6.x push-down aggregation: translates `count`/`min`/`max`/`sum`/`avg` into ES native aggregations, computed on the
 * ES side and returned as a single row.
 *
 * Aggregation is a global computation over **all configured indices** (or the result set filtered by [scanQuery]), so it
 * does not use slice parallelism — the provider dispatches all indices as one comma-separated element, and a single
 * `size(0)` aggregation query here returns the one and only row.
 *
 * @author wuya
 */
class Elasticsearch6AggregateFn(
    private val nodes: List<String>,
    private val username: String,
    private val password: String,
    private val aggregations: List<String>,
    private val scanQuery: String,
) : DoFn<String, Row>() {

    @Transient
    private var client: RestHighLevelClient? = null

    @Transient
    private var schema: Schema? = null

    @Setup
    fun setup() {
        client = newClient()
        schema = buildAggregateSchema(parseEs6Aggregations(aggregations))
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.close() }
        client = null
    }

    @ProcessElement
    fun processElement(@Element indexExpression: String, receiver: OutputReceiver<Row>) {
        val c = checkNotNull(client) { "ES client is not initialized" }
        val specs = parseEs6Aggregations(aggregations)
        val query = if (scanQuery.isBlank()) QueryBuilders.matchAllQuery() else QueryBuilders.wrapperQuery(scanQuery)
        val source = SearchSourceBuilder().apply {
            query(query)
            size(0)
            specs.filter { it.op != "count" }.forEach { spec ->
                val name = aggregateFieldName(spec)
                val agg = when (spec.op) {
                    "min" -> AggregationBuilders.min(name).field(spec.column)
                    "max" -> AggregationBuilders.max(name).field(spec.column)
                    "sum" -> AggregationBuilders.sum(name).field(spec.column)
                    "avg" -> AggregationBuilders.avg(name).field(spec.column)
                    else -> error("Unsupported aggregation: ${spec.op}")
                }
                aggregation(agg)
            }
        }
        val resp = c.search(buildAggregateSearchRequest(indexExpression, source), RequestOptions.DEFAULT)
        val count = resp.hits.totalHits
        val aggResult = resp.aggregations
        val builder = Row.withSchema(checkNotNull(schema))
        specs.forEach { spec ->
            when (spec.op) {
                "count" -> builder.addValue(count)
                else -> {
                    val name = aggregateFieldName(spec)
                    val a = aggResult?.get(name) as? ParsedSingleValueNumericMetricsAggregation
                    builder.addValue(a?.value())
                }
            }
        }
        receiver.output(builder.build())
    }

    private fun newClient(): RestHighLevelClient {
        val hosts = parseElasticsearch6Hosts(nodes)
        val builder = RestClient.builder(*hosts)
        if (username.isNotBlank()) {
            val creds = org.apache.http.impl.client.BasicCredentialsProvider()
            creds.setCredentials(
                org.apache.http.auth.AuthScope.ANY,
                org.apache.http.auth.UsernamePasswordCredentials(username, password),
            )
            builder.setHttpClientConfigCallback { it.setDefaultCredentialsProvider(creds) }
        }
        return RestHighLevelClient(builder)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
