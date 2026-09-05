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
 * 构造聚合搜索请求。抽成顶层函数是为了能脱离真实集群做单测；
 * [indexExpression] 是逗号分隔的多索引表达式（聚合是全局语义，所有索引合在一次查询里，只输出一行）。
 */
internal fun buildAggregateSearchRequest(indexExpression: String, source: SearchSourceBuilder): SearchRequest =
    SearchRequest(*indexExpression.split(",").map { it.trim() }.filter { it.isNotEmpty() }.toTypedArray())
        .source(source)

/**
 * ES 6.x 聚合下推：把 `count`/`min`/`max`/`sum`/`avg` 翻译成 ES 原生 aggregation，在 ES 侧算完返回单行。
 *
 * 聚合是对**全部配置索引**（或 [scanQuery] 过滤后的结果集）的全局计算，所以不走 slice 并行——
 * provider 把所有索引合成一个逗号分隔的元素下发，这里一次 `size(0)` 的聚合查询返回唯一一行。
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
        val c = checkNotNull(client) { "ES 客户端未初始化" }
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
                    else -> error("不支持的聚合: ${spec.op}")
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
