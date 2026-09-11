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
 * A Splittable DoFn that reads ES 6.x in parallel by **slice**.
 *
 * The element is the index name, the restriction is the slice-index range `[0, scan_slices)`, and `@ProcessElement`
 * claims one slice at a time; each slice runs its own scroll, and the documents across slices do not overlap.
 *
 * Before the refactor the restriction was fixed to `OffsetRange(0, 1)` plus `tryClaim(range.to - 1)`: one index could
 * only be **scrolled from start to end sequentially by a single worker**, so no matter how large the index, everyone
 * else just waited. ES's native slice is designed exactly for this scenario.
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

    /** The client factory used for test injection; null on the production path, see [Es6ClientFactory] for the reason. */
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
        val c = checkNotNull(client) { "ES client is not initialized" }
        val documentMode = fields.isEmpty()
        val query = if (scanQuery.isBlank()) {
            QueryBuilders.matchAllQuery()
        } else {
            QueryBuilders.wrapperQuery(scanQuery)
        }
        val timeValue = TimeValue(scrollTimeoutMinutes, TimeUnit.MINUTES)
        // LIMIT must be global: after degrading to a single slice, this stops paging once the Nth record is scanned.
        var remaining = if (limit > 0) limit else Long.MAX_VALUE
        val slices = effectiveSlices()
        val searchRequest = SearchRequest(index).apply { scroll(timeValue) }
        searchRequest.source(
            SearchSourceBuilder().apply {
                query(query)
                size(pageSize(scrollSize, remaining))
                // schema_fields are pushed down as a `_source` projection (fetchSource includes), so the ES server
                // prunes and pulls less data; document mode (the whole row as one JSON column, empty fields) does not
                // prune and reads the full _source.
                if (!documentMode) {
                    fetchSource(sourceFieldNames(fields), null)
                }
                // Do not include the slice parameter when there is only one slice: ES requires max >= 2.
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
        LOGGER.info("index[{}] slice[{}/{}] finished scrolling {} records", index, slice, slices, count)
    }

    /** Force a single slice when limiting rows, so limit applies to the whole result set (otherwise it becomes "each slice reads limit records"). */
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

        /** [remaining] is the total remaining count, which may exceed Int; ES's Int `size` only bounds the current page. */
        internal fun pageSize(batchSize: Int, remaining: Long): Int {
            require(batchSize > 0) { "batchSize must be > 0" }
            require(remaining > 0) { "remaining must be > 0" }
            return minOf(batchSize.toLong(), remaining).toInt()
        }

        /** Derives from `schema_fields` the `_source` includes to push down to ES; empty means no pruning (document mode). */
        fun sourceFieldNames(fields: List<EsField>): Array<String> =
            fields.map { it.name }.toTypedArray()
    }
}
