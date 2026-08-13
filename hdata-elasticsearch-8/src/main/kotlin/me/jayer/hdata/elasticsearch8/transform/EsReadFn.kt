package me.jayer.hdata.elasticsearch8.transform

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.FieldSort
import co.elastic.clients.elasticsearch._types.SortOptions
import co.elastic.clients.elasticsearch._types.SortOrder
import co.elastic.clients.elasticsearch._types.Time
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import co.elastic.clients.elasticsearch.core.search.Hit
import co.elastic.clients.elasticsearch._types.FieldValue
import com.fasterxml.jackson.databind.ObjectMapper
import me.jayer.hdata.elasticsearch8.EsReadConfig
import me.jayer.hdata.elasticsearch8.buildSchema
import me.jayer.hdata.elasticsearch8.buildEsClient
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

/**
 * 按 index 切分、用 PIT + search_after 翻页读取 Elasticsearch 8.x 的 Splittable DoFn。
 *
 * 每个元素是一个 index 名，限制统一为 `OffsetRange(0, 1)`，即一个 index 一个分片（整索引顺序读）。
 * 读取本身不可中断续跑，所以 `@ProcessElement` 用 `tryClaim(range.to - 1)` 一次性认领整段。
 *
 * @author wuya
 */
@DoFn.BoundedPerElement
class EsReadFn(
    private val config: EsReadConfig,
    private val schema: Schema,
    private val schemaFields: List<Pair<String, String>>,
) : DoFn<String, Row>() {

    @Transient
    private var client: ElasticsearchClient? = null

    @Transient
    private var restClient: RestClient? = null

    private val jsonMapper = ObjectMapper()

    private val query: Query =
        if (config.scanQuery.isNotBlank()) {
            Query.of { it.withJson(StringReader(config.scanQuery)) }
        } else {
            Query.of { it.matchAll { m -> m } }
        }

    private val sortOptions: List<SortOptions> = listOf(
        SortOptions.of { s -> s.field(FieldSort.of { f -> f.field("_doc").order(SortOrder.Asc) }) },
    )

    @Setup
    fun setup() {
        val (c, rc) = buildEsClient(config.connectionUri, config.apiKey, config.username, config.password)
        client = c
        restClient = rc
    }

    @Teardown
    fun tearDown() {
        runCatching { restClient?.close() }
        restClient = null
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
        if (restriction.to <= restriction.from) return
        restriction.split(1, 1).forEach { receiver.output(it) }
    }

    @ProcessElement
    fun processElement(
        @Element index: String,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        val range = tracker.currentRestriction()
        if (range.to <= range.from) return
        if (!tracker.tryClaim(range.to - 1)) return

        val c = checkNotNull(client) { "ES 客户端未初始化" }
        val pitId = c.openPointInTime { b -> b.index(index).keepAlive(Time.of { it.time("1m") }) }.id()
        var lastSort: List<FieldValue>? = null
        var count = 0L
        try {
            while (true) {
                val resp = c.search({ b ->
                    b.pit { p -> p.id(pitId).keepAlive(Time.of { t -> t.time("1m") }) }
                        .size(config.batchSize)
                        .sort(sortOptions)
                        .query(query)
                        .let { if (lastSort != null) it.searchAfter(lastSort) else it }
                }, Map::class.java)
                val hits: List<Hit<Map<*, *>>> = resp.hits().hits()
                if (hits.isEmpty()) break
                for (hit in hits) {
                    receiver.output(mapRow(hit.source()))
                    count++
                }
                lastSort = hits.last().sort()
                if (lastSort == null) break
            }
            RECORDS_READ.inc(count)
            LOGGER.info("index[{}] 读取 {} 条", index, count)
        } finally {
            runCatching { c.closePointInTime { b -> b.id(pitId) } }
        }
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun mapRow(source: Map<*, *>?): Row {
        if (schemaFields.isEmpty()) {
            val json = if (source == null) "{}" else jsonMapper.writeValueAsString(source)
            return Row.withSchema(schema).addValue(json).build()
        }
        val builder = Row.withSchema(schema)
        schemaFields.forEach { (name, type) ->
            builder.addValue(convertValue(source?.get(name), type))
        }
        return builder.build()
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(EsReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(EsReadFn::class.java, "records_read")
    }
}
