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
 * 按 **slice** 并行读 ES 6.x 的 Splittable DoFn。
 *
 * 元素是索引名，限制是 slice 下标区间 `[0, scan_slices)`，`@ProcessElement` 逐个 slice 认领；
 * 每个 slice 跑自己的 scroll，各 slice 的文档互不重叠。
 *
 * 重构前限制固定 `OffsetRange(0, 1)` 加 `tryClaim(range.to - 1)`：一个索引只能由
 * **一个 worker 从头顺序 scroll 到尾**，索引再大也只能干等。ES 原生的 slice 正是为这个场景准备的。
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
    private val scanSlices: Int,
    private val limit: Long = -1,
) : DoFn<String, Row>() {

    @Transient
    private var client: RestHighLevelClient? = null

    /** 测试注入用的客户端工厂；生产路径为 null，理由见 [Es6ClientFactory]。 */
    internal var clientFactory: Es6ClientFactory? = null

    @Setup
    fun setup() {
        client = clientFactory?.create() ?: newClient()
    }

    @Teardown
    fun tearDown() {
        runCatching { client?.close() }
        client = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(@Element index: String): OffsetRange = OffsetRange(0, effectiveSlices().toLong())

    @SplitRestriction
    fun splitRestriction(
        @Element index: String,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        if (restriction.to <= restriction.from) {
            return
        }
        restriction.split(1, 1).forEach { receiver.output(it) }
    }

    @ProcessElement
    fun processElement(
        @Element index: String,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        val range = tracker.currentRestriction()
        var slice = range.from
        while (slice < range.to) {
            if (!tracker.tryClaim(slice)) {
                return
            }
            readSlice(index, slice.toInt(), receiver)
            slice++
        }
    }

    private fun readSlice(index: String, slice: Int, receiver: OutputReceiver<Row>) {
        val c = checkNotNull(client) { "ES 客户端未初始化" }
        val documentMode = fields.isEmpty()
        val query = if (scanQuery.isBlank()) {
            QueryBuilders.matchAllQuery()
        } else {
            QueryBuilders.wrapperQuery(scanQuery)
        }
        val timeValue = TimeValue(scrollTimeoutMinutes, TimeUnit.MINUTES)
        // LIMIT 必须是全局的：退化为单 slice 后，这里在扫到第 N 条时停止翻页。
        var remaining = if (limit > 0) limit else Long.MAX_VALUE
        val slices = effectiveSlices()
        val searchRequest = SearchRequest(index).apply { scroll(timeValue) }
        searchRequest.source(
            SearchSourceBuilder().apply {
                query(query)
                size(pageSize(scrollSize, remaining))
                // schema_fields 下推成 `_source` 投影（fetchSource includes），ES 服务端裁剪、少拉数据；
                // document 模式（整行 JSON 一列，fields 为空）不裁剪，读完整 _source
                if (!documentMode) {
                    fetchSource(sourceFieldNames(fields), null)
                }
                // 只有一个 slice 时不带 slice 参数：ES 要求 max >= 2
                if (slices > 1) {
                    slice(org.elasticsearch.search.slice.SliceBuilder(slice, slices))
                }
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
                    remaining--
                    if (remaining <= 0) break
                }
                if (remaining <= 0) break
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
        LOGGER.info("索引[{}] slice[{}/{}] 用 scroll 读完 {} 条", index, slice, slices, count)
    }

    /** 限行数时强制单 slice，保证 limit 对整结果集生效（否则会变成"每 slice 各读 limit 条"）。 */
    private fun effectiveSlices(): Int = if (limit > 0) 1 else scanSlices

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun rowFromSource(schema: Schema, fields: List<EsField>, source: Map<String, Any>): Row {
        val builder = Row.withSchema(schema)
        fields.forEach { f -> builder.addValue(esRowValue(f.type, source[f.name])) }
        return builder.build()
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
        private val LOGGER = LoggerFactory.getLogger(Elasticsearch6ReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(Elasticsearch6ReadFn::class.java, "records_read")

        /** [remaining] 是总剩余条数，可能超过 Int；ES 的 Int `size` 只约束当前页。 */
        internal fun pageSize(batchSize: Int, remaining: Long): Int {
            require(batchSize > 0) { "batchSize 必须 > 0" }
            require(remaining > 0) { "remaining 必须 > 0" }
            return minOf(batchSize.toLong(), remaining).toInt()
        }

        /** 由 `schema_fields` 推导要下推给 ES 的 `_source` includes；空表示不裁剪（document 模式）。 */
        fun sourceFieldNames(fields: List<EsField>): Array<String> =
            fields.map { it.name }.toTypedArray()
    }
}
