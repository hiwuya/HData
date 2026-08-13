package me.jayer.hdata.elasticsearch6

import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchScrollRequest
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestClient
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.unit.TimeValue
import org.elasticsearch.common.xcontent.XContentType
import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.builder.SearchSourceBuilder
import org.slf4j.LoggerFactory
import java.util.concurrent.TimeUnit

/**
 * 按索引用 scroll 翻页读 ES 6.x 的 Splittable DoFn。
 *
 * 元素是索引名（粒度到索引），限制用 `OffsetRange(0,1)` 一次性认领整段，
 * 因此单个索引由单线程 scroll 读完（仍可多索并行）。读本身不可中断续跑，
 * `@ProcessElement` 用 `tryClaim(range.to - 1)` 一次性认领。
 *
 * @author wuya
 */
@DoFn.BoundedPerElement
class Elasticsearch6ReadFn(
    private val nodes: List<String>,
    private val username: String,
    private val password: String,
    private val schema: Schema,
    private val fields: List<EsField>,
    private val scanQuery: String,
    private val scrollSize: Int,
    private val scrollTimeoutMinutes: Long,
) : DoFn<String, Row>() {

    @Transient
    private var client: RestHighLevelClient? = null

    @Setup
    fun setup() {
        client = newClient()
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.close() }
        client = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(@Element index: String): OffsetRange = OffsetRange(0, 1)

    @SplitRestriction
    fun splitRestriction(
        @Element index: String,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        receiver.output(restriction)
    }

    @ProcessElement
    fun processElement(
        @Element index: String,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        val range = tracker.currentRestriction()
        if (range.to <= range.from) {
            return
        }
        if (!tracker.tryClaim(range.to - 1)) {
            return
        }
        val c = checkNotNull(client) { "ES 客户端未初始化" }
        val documentMode = fields.isEmpty()
        val query = if (scanQuery.isBlank()) {
            QueryBuilders.matchAllQuery()
        } else {
            QueryBuilders.wrapperQuery(scanQuery)
        }
        val timeValue = TimeValue(scrollTimeoutMinutes, TimeUnit.MINUTES)
        val searchRequest = SearchRequest(index).apply { scroll(timeValue) }
        searchRequest.source(
            SearchSourceBuilder().apply {
                query(query)
                size(scrollSize)
            },
        )

        var response = c.search(searchRequest, RequestOptions.DEFAULT)
        var scrollId: String? = response.scrollId
        var count = 0L
        try {
            while (true) {
                val hits: Array<SearchHit> = response.hits.hits
                if (hits.isEmpty()) {
                    break
                }
                for (hit in hits) {
                    val row = if (documentMode) {
                        Row.withSchema(schema).addValue(hit.sourceAsString).build()
                    } else {
                        rowFromSource(schema, fields, hit.sourceAsMap ?: emptyMap())
                    }
                    receiver.output(row)
                    count++
                }
                val nextId = scrollId ?: break
                response = c.searchScroll(SearchScrollRequest(nextId).scroll(timeValue), RequestOptions.DEFAULT)
                scrollId = response.scrollId
            }
        } finally {
            scrollId?.let { sid ->
                runCatching { c.clearScroll(org.elasticsearch.action.search.ClearScrollRequest().apply { addScrollId(sid) }, RequestOptions.DEFAULT) }
            }
        }
        RECORDS_READ.inc(count)
        LOGGER.info("索引[{}] 用 scroll 读完 {} 条", index, count)
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun rowFromSource(schema: Schema, fields: List<EsField>, source: Map<String, Any>): Row {
        val builder = Row.withSchema(schema)
        fields.forEach { f -> builder.addValue(coerce(f.type, source[f.name])) }
        return builder.build()
    }

    private fun coerce(type: EsFieldType, raw: Any?): Any? {
        if (raw == null) {
            return null
        }
        return when (type) {
            EsFieldType.STRING -> raw.toString()
            EsFieldType.INT32 -> (raw as? Number)?.toInt() ?: raw.toString().toIntOrNull()
            EsFieldType.INT64 -> (raw as? Number)?.toLong() ?: raw.toString().toLongOrNull()
            EsFieldType.DOUBLE -> (raw as? Number)?.toDouble() ?: raw.toString().toDoubleOrNull()
            EsFieldType.BOOLEAN -> raw as? Boolean ?: raw.toString().toBoolean()
            EsFieldType.DATETIME -> when (raw) {
                is Long -> org.joda.time.Instant.ofEpochMilli(raw)
                is Number -> org.joda.time.Instant.ofEpochMilli(raw.toLong())
                is String -> org.joda.time.Instant.parse(raw)
                else -> null
            }
            EsFieldType.BYTES -> when (raw) {
                is ByteArray -> raw
                is String -> java.util.Base64.getDecoder().decode(raw)
                else -> null
            }
        }
    }

    private fun newClient(): RestHighLevelClient {
        val hosts = nodes.map { org.apache.http.HttpHost.create(it) }.toTypedArray()
        val builder = RestClient.builder(*hosts)
        if (username.isNotBlank() && password.isNotBlank()) {
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
        private val LOGGER = LoggerFactory.getLogger(Elasticsearch6ReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(Elasticsearch6ReadFn::class.java, "records_read")
    }
}
