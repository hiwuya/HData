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
 * Splittable DoFn that reads Elasticsearch 8.x in parallel by slice.
 *
 * Elements are index names and restrictions are slice-index ranges. `@ProcessElement` claims each slice,
 * then reads it through PIT plus `search_after` paging.
 *
 * A fixed one-element restriction would serialize every index on one worker. Native slices partition a query
 * by document-ID hash into non-overlapping parts.
 *
 * Client objects such as `Query` and `SortOptions` are not serializable, so setup creates them on the worker.
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

    /** Client-factory injection point for tests; production uses null. See [EsClientFactory]. */
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

    /** `_source` projection; null reads the full source when document mode has no schema fields. */
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
        // Push declared schema fields to `_source` includes so Elasticsearch performs the projection.
        // Missing projected fields are mapped to null by mapRow.
        sourceIncludes = sourceFieldNames(schemaFields)
        jsonMapper = JsonMapper.builder().build()
        query = if (config.scanQuery.isNotBlank()) {
            Query.of { it.withJson(StringReader(config.scanQuery)) }
        } else {
            Query.of { it.matchAll { m -> m } }
        }
        // `_shard_doc` is the recommended, stable sort for PIT plus search_after.
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
        val c = checkNotNull(client) { "Elasticsearch client is not initialized" }
        val keepAlive = Time.of { it.time("${config.keepAliveMinutes}m") }
        var pitId = c.openPointInTime { b -> b.index(index).keepAlive(keepAlive) }.id()
        var lastSort: List<FieldValue>? = null
        var count = 0L
        // Limit mode uses one slice, so stopping after N records preserves global limit semantics.
        var remaining = if (config.limit > 0) config.limit else Long.MAX_VALUE
        try {
            while (true) {
                // Do not fetch more records than the remaining limit.
                val pageSize = pageSize(config.batchSize, remaining)
                val resp = c.search({ b ->
                    b.pit { p -> p.id(pitId).keepAlive(keepAlive) }
                        .size(pageSize)
                        .sort(checkNotNull(sortOptions))
                        .query(checkNotNull(query))
                        // `_source` includes lets Elasticsearch trim projected fields server-side.
                        .let { if (sourceIncludes != null) it.source { s -> s.filter { f -> f.includes(sourceIncludes) } } else it }
                        // Do not send a slice parameter for one slice; Elasticsearch requires max >= 2.
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
            LOGGER.info("index[{}] slice[{}/{}] read {} records (_source projection: {} fields)", index, slice, effectiveSlices(), count, sourceIncludes?.size ?: -1)
        } finally {
            runCatching { c.closePointInTime { b -> b.id(pitId) } }
        }
    }

    /** Limit mode forces one slice so the limit applies to the complete result set. */
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
         * Derives `_source` includes from `schema_fields`; null in document mode reads the full source.
         */
        fun sourceFieldNames(schemaFields: List<String>): List<String>? =
            parseSchemaFields(schemaFields).map { it.first }.takeIf { it.isNotEmpty() }

        /** [remaining] can exceed Int; Elasticsearch's Int `size` applies only to the current page. */
        internal fun pageSize(batchSize: Int, remaining: Long): Int {
            require(batchSize > 0) { "batchSize must be > 0" }
            require(remaining > 0) { "remaining must be > 0" }
            return minOf(batchSize.toLong(), remaining).toInt()
        }
    }
}
