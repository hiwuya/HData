package me.jayer.hdata.elasticsearch8.transform

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.FieldSort
import co.elastic.clients.elasticsearch._types.FieldValue
import co.elastic.clients.elasticsearch._types.SortOptions
import co.elastic.clients.elasticsearch._types.SortOrder
import co.elastic.clients.elasticsearch._types.Time
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import co.elastic.clients.elasticsearch.core.search.Hit
import me.jayer.hdata.elasticsearch8.EsClientFactory
import me.jayer.hdata.elasticsearch8.EsReadConfig
import me.jayer.hdata.elasticsearch8.buildEsClient
import me.jayer.hdata.elasticsearch8.buildSchema
import me.jayer.hdata.elasticsearch8.convertValue
import me.jayer.hdata.elasticsearch8.parseSchemaFields
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.elasticsearch.client.RestClient
import org.slf4j.LoggerFactory
import java.io.StringReader
import tools.jackson.databind.json.JsonMapper

/**
 * 按 **slice** 并行读 Elasticsearch 8.x 的 Splittable DoFn。
 *
 * 元素是索引名，限制是 slice 下标区间 `[0, scan_slices)`，`@ProcessElement` 逐个 slice 认领；
 * 每个 slice 用 PIT + `search_after` 翻页读自己那一份。
 *
 * 重构前这里没有任何并行度可言：限制固定 `OffsetRange(0, 1)` 加 `tryClaim(range.to - 1)`，
 * 一个索引由**一个 worker 从头顺序读到尾**，索引再大也只能干等。
 * ES 原生的 slice 正是为这个场景准备的：把一次查询按文档 ID 哈希切成 N 份，各份互不重叠。
 *
 * 另外，`Query` / `SortOptions` 这些客户端对象**不可序列化**，重构前它们是 DoFn 的普通字段，
 * 提交作业时就会炸；现在都挪到 `@Setup` 里构造。
 *
 * @author wuya
 */
@DoFn.BoundedPerElement
class EsReadFn(
    private val config: EsReadConfig,
    private val schemaFields: List<String>,
) : DoFn<String, Row>() {

    @Transient
    private var client: ElasticsearchClient? = null

    /** 测试注入用的客户端工厂；生产路径为 null，理由见 [EsClientFactory]。 */
    internal var clientFactory: EsClientFactory? = null

    @Transient
    private var restClient: RestClient? = null

    @Transient
    private var schema: Schema? = null

    @Transient
    private var fields: List<Pair<String, String>>? = null

    @Transient
    private var query: Query? = null

    @Transient
    private var sortOptions: List<SortOptions>? = null

    @Transient
    private var jsonMapper: JsonMapper? = null

    /** 下推到 `_source` 的字段白名单；`schemaFields` 为空（document 模式）时为 null 表示不裁剪。 */
    @Transient
    private var sourceIncludes: List<String>? = null

    @Setup
    fun setup() {
        val factory = clientFactory
        if (factory != null) {
            client = factory.create()
        } else {
            val (c, rc) = buildEsClient(config.connectionUri, config.apiKey, config.username, config.password)
            client = c
            restClient = rc
        }
        schema = buildSchema(schemaFields)
        fields = parseSchemaFields(schemaFields)
        // schema_fields 上声明的字段名直接下推成 `_source` includes（ES 服务端裁剪，少拉数据）。
        // 注意：下推的是"声明要哪些字段"，是投影下推；若某行缺字段，mapRow 已经把缺失值当 null 处理。
        sourceIncludes = sourceFieldNames(schemaFields)
        jsonMapper = JsonMapper.builder().build()
        query = if (config.scanQuery.isNotBlank()) {
            Query.of { it.withJson(StringReader(config.scanQuery)) }
        } else {
            Query.of { it.matchAll { m -> m } }
        }
        // 按 _shard_doc 排序是 PIT + search_after 的推荐做法，比 _doc 更稳定
        sortOptions = listOf(
            SortOptions.of { s -> s.field(FieldSort.of { f -> f.field("_shard_doc").order(SortOrder.Asc) }) },
        )
    }

    @Teardown
    fun tearDown() {
        runCatching { restClient?.close() }
        restClient = null
        client = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(@Element index: String): OffsetRange =
        OffsetRange(0, effectiveSlices().toLong())

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

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun readSlice(index: String, slice: Int, receiver: OutputReceiver<Row>) {
        val c = checkNotNull(client) { "ES 客户端未初始化" }
        val keepAlive = Time.of { it.time("${config.keepAliveMinutes}m") }
        var pitId = c.openPointInTime { b -> b.index(index).keepAlive(keepAlive) }.id()
        var lastSort: List<FieldValue>? = null
        var count = 0L
        // LIMIT 必须是全局的：退化为单 slice 后，这里在扫到第 N 条时停止翻页。
        var remaining = if (config.limit > 0) config.limit else Long.MAX_VALUE
        try {
            while (true) {
                // 每页 size 压到剩余条数，避免多拉数据
                val pageSize = pageSize(config.batchSize, remaining)
                val resp = c.search({ b ->
                    b.pit { p -> p.id(pitId).keepAlive(keepAlive) }
                        .size(pageSize)
                        .sort(checkNotNull(sortOptions))
                        .query(checkNotNull(query))
                        // schema_fields 下推成 `_source` includes：服务端裁剪投影字段，少拉数据
                        .let { if (sourceIncludes != null) it.source { s -> s.filter { f -> f.includes(sourceIncludes) } } else it }
                        // 只有一个 slice 时不带 slice 参数：ES 要求 max >= 2
                        .let { if (effectiveSlices() > 1) it.slice { s -> s.id(slice.toString()).max(effectiveSlices()) } else it }
                        .let { if (lastSort != null) it.searchAfter(lastSort) else it }
                }, Map::class.java)
                resp.pitId()?.takeIf { it.isNotBlank() }?.let { pitId = it }
                val hits: List<Hit<Map<*, *>>> = resp.hits().hits()
                if (hits.isEmpty()) break
                for (hit in hits) {
                    receiver.output(mapRow(hit.source()))
                    count++
                    remaining--
                    if (remaining <= 0) break
                }
                if (remaining <= 0) break
                lastSort = hits.last().sort()
                if (lastSort.isNullOrEmpty()) break
            }
            RECORDS_READ.inc(count)
            LOGGER.info("index[{}] slice[{}/{}] 读出 {} 条（_source 投影 {} 字段）", index, slice, effectiveSlices(), count, sourceIncludes?.size ?: -1)
        } finally {
            runCatching { c.closePointInTime { b -> b.id(pitId) } }
        }
    }

    /** 限行数时强制单 slice，保证 limit 对整结果集生效（否则会变成"每 slice 各读 limit 条"）。 */
    private fun effectiveSlices(): Int = if (config.limit > 0) 1 else config.scanSlices

    private fun mapRow(source: Map<*, *>?): Row {
        val target = checkNotNull(schema)
        val declared = checkNotNull(fields)
        if (declared.isEmpty()) {
            val json = if (source == null) "{}" else checkNotNull(jsonMapper).writeValueAsString(source)
            return Row.withSchema(target).addValue(json).build()
        }
        val builder = Row.withSchema(target)
        declared.forEach { (name, type) -> builder.addValue(convertValue(source?.get(name), type)) }
        return builder.build()
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(EsReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(EsReadFn::class.java, "records_read")

        /**
         * 由 `schema_fields` 推导要下推给 ES 的 `_source` includes。
         * 为空（document 模式，整行 JSON 一个列）返回 null，表示不裁剪、读取完整 `_source`。
         */
        fun sourceFieldNames(schemaFields: List<String>): List<String>? =
            parseSchemaFields(schemaFields).map { it.first }.takeIf { it.isNotEmpty() }

        /** [remaining] 是总剩余条数，可能超过 Int；ES 的 Int `size` 只约束当前页。 */
        internal fun pageSize(batchSize: Int, remaining: Long): Int {
            require(batchSize > 0) { "batchSize 必须 > 0" }
            require(remaining > 0) { "remaining 必须 > 0" }
            return minOf(batchSize.toLong(), remaining).toInt()
        }
    }
}
