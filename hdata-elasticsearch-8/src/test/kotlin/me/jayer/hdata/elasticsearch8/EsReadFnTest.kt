package me.jayer.hdata.elasticsearch8

import co.elastic.clients.elasticsearch.ElasticsearchClient
import co.elastic.clients.elasticsearch._types.FieldValue
import co.elastic.clients.elasticsearch.core.ClosePointInTimeRequest
import co.elastic.clients.elasticsearch.core.ClosePointInTimeResponse
import co.elastic.clients.elasticsearch.core.OpenPointInTimeRequest
import co.elastic.clients.elasticsearch.core.OpenPointInTimeResponse
import co.elastic.clients.elasticsearch.core.SearchRequest
import co.elastic.clients.elasticsearch.core.SearchResponse
import co.elastic.clients.elasticsearch.core.search.HitsMetadata
import co.elastic.clients.elasticsearch.core.search.Hit
import co.elastic.clients.util.ObjectBuilder
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.elasticsearch8.transform.EsReadFn
import me.jayer.hdata.core.testing.CollectingOutputReceiver
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.beam.sdk.values.Row
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import tools.jackson.databind.node.ObjectNode
import java.util.function.Function
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Splitting and serializability of the ES 8.x read side.
 *
 * @author wuya
 */
class EsReadFnTest {

    private val config = EsReadConfig(connectionUri = "http://localhost:9200", index = "orders")

    private fun fn(config: EsReadConfig) = EsReadFn(config, config.schemaFields)

    @Test
    fun `the DoFn is serializable for submission`() {
        // Before the refactor, Query / SortOptions were plain DoFn fields; neither client object is
        // serializable, so the job would blow up at submission time — a test that only calls
        // processElement would never catch that.
        SerializableUtils.ensureSerializable(fn(config.copy(scanQuery = """{"match_all":{}}""")))
    }

    @Test
    fun `the source produced by the provider is serializable for submission`() {
        val transform = EsReadProvider().from(
            TransformConfig(
                "ReadFromElasticsearch8",
                SpecMappers.CONFIG.readTree(
                    """{"connection_uri": "http://localhost:9200", "index": "orders", "scan_slices": 4}"""
                ) as ObjectNode,
            )
        )

        SerializableUtils.ensureSerializable(transform)
    }

    @Test
    fun `the initial restriction covers every slice`() {
        assertEquals(OffsetRange(0, 4), fn(config.copy(scanSlices = 4)).getInitialRestriction("orders"))
        // no splitting by default
        assertEquals(OffsetRange(0, 1), fn(config).getInitialRestriction("orders"))
    }

    @Test
    fun `each slice becomes one split, end to end with no gaps`() {
        val receiver = CollectingOutputReceiver<OffsetRange>()
        fn(config.copy(scanSlices = 4)).splitRestriction("orders", OffsetRange(0, 4), receiver)
        val splits = receiver.outputs

        assertEquals(4, splits.size)
        assertEquals(listOf(0L, 1L, 2L, 3L), splits.map { it.from })
        assertEquals(listOf(1L, 2L, 3L, 4L), splits.map { it.to })
    }

    @Test
    fun `an empty range produces no split`() {
        val receiver = CollectingOutputReceiver<OffsetRange>()
        fn(config).splitRestriction("orders", OffsetRange(0, 0), receiver)
        val splits = receiver.outputs

        assertTrue(splits.isEmpty())
    }

    @Test
    fun `an invalid scan_slices errors`() {
        assertFailsWith<IllegalArgumentException> { config.copy(scanSlices = 0).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(keepAliveMinutes = 0).validate() }
    }

    @Test
    fun `a valid limit passes validation`() {
        config.copy(limit = -1).validate()
        config.copy(limit = 1).validate()
        config.copy(limit = 1000).validate()
        config.copy(limit = Int.MAX_VALUE.toLong() + 1).validate()

        assertEquals(1000, EsReadFn.pageSize(1000, Int.MAX_VALUE.toLong() + 1))
        assertEquals(7, EsReadFn.pageSize(1000, 7))
    }

    @Test
    fun `an invalid limit errors`() {
        assertFailsWith<IllegalArgumentException> { config.copy(limit = 0).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(limit = -2).validate() }
    }

    @Test
    fun `limit greater than 0 forces a single slice, preserving global semantics`() {
        // Even with 4 slices declared, a row limit must collapse to a single slice, otherwise it
        // degrades into "each slice reads limit rows on its own".
        assertEquals(OffsetRange(0, 1), fn(config.copy(scanSlices = 4, limit = 100)).getInitialRestriction("orders"))
        // With no limit, splitting still follows the declared slice count.
        assertEquals(OffsetRange(0, 4), fn(config.copy(scanSlices = 4, limit = -1)).getInitialRestriction("orders"))
    }

    @Test
    fun `an invalid scan_query JSON errors at graph-construction time`() {
        val error = assertFailsWith<IllegalArgumentException> {
            config.copy(scanQuery = "{match_all").validate()
        }

        assertTrue("scan_query" in error.message!!)
    }

    @Test
    fun `an unrecognized schema_fields type errors`() {
        assertFailsWith<IllegalArgumentException> { config.copy(schemaFields = listOf("id:UUID")).validate() }
    }

    @Test
    fun `the _source projection is derived from schema_fields, empty means no pruning`() {
        // schema_fields' field names are pushed down directly as `_source` includes (projection pushdown),
        // letting the ES server do the pruning.
        assertEquals(listOf("id", "amount"), EsReadFn.sourceFieldNames(listOf("id:STRING", "amount:DOUBLE")))
        // Degrading to document mode (the whole row as one JSON column) returns null, reading the full _source.
        assertEquals(null, EsReadFn.sourceFieldNames(emptyList()))
    }

    /**
     * One page of search results.
     *
     * `sort()` returns an empty list: the read side uses that to decide "no more pages", so one slice
     * issues exactly one search — that keeps the test's response sequence lined up one-to-one with slices.
     */
    private fun responseOf(vararg sources: Map<String, Any?>): SearchResponse<Map<*, *>> {
        val hits = sources.map { source ->
            val hit = mock<Hit<Map<*, *>>>()
            whenever(hit.source()).doReturn(source)
            whenever(hit.sort()).doReturn(emptyList<FieldValue>())
            hit
        }
        val metadata = mock<HitsMetadata<Map<*, *>>>()
        whenever(metadata.hits()).doReturn(hits)
        val response = mock<SearchResponse<Map<*, *>>>()
        whenever(response.hits()).doReturn(metadata)
        whenever(response.pitId()).doReturn(null)
        return response
    }

    private fun clientReturning(responses: List<SearchResponse<Map<*, *>>>): ElasticsearchClient {
        val pit = mock<OpenPointInTimeResponse>()
        whenever(pit.id()).doReturn("pit-1")
        val client = mock<ElasticsearchClient>()
        whenever(
            client.openPointInTime(
                anyOrNull<Function<OpenPointInTimeRequest.Builder, ObjectBuilder<OpenPointInTimeRequest>>>()
            )
        ).doReturn(pit)
        whenever(
            client.closePointInTime(
                anyOrNull<Function<ClosePointInTimeRequest.Builder, ObjectBuilder<ClosePointInTimeRequest>>>()
            )
        ).doReturn(mock<ClosePointInTimeResponse>())
        var call = 0
        whenever(
            client.search<Map<*, *>>(
                anyOrNull<Function<SearchRequest.Builder, ObjectBuilder<SearchRequest>>>(),
                anyOrNull<Class<Map<*, *>>>(),
            )
        ).thenAnswer { responses[call++] }
        return client
    }

    private fun fieldFn(
        slices: Int = 1,
        limit: Long = -1,
    ): Pair<EsReadFn, org.apache.beam.sdk.schemas.Schema> {
        val fields = listOf("id:STRING", "amount:DOUBLE")
        val fn = EsReadFn(config.copy(schemaFields = fields, scanSlices = slices, limit = limit), fields)
        return fn to buildSchema(fields)
    }

    @Test
    fun `processElement claims one slice at a time, reading out every document in each`() {
        val (fn, _) = fieldFn(slices = 2)
        val client = clientReturning(
            listOf(
                responseOf(mapOf("id" to "a1", "amount" to 1.0), mapOf("id" to "a2", "amount" to 2.0)),
                responseOf(mapOf("id" to "b1", "amount" to 3.0)),
            )
        )
        fn.clientFactory = EsClientFactory { client }
        fn.setup()
        val receiver = CollectingOutputReceiver<Row>()

        fn.processElement("orders", OffsetRangeTracker(OffsetRange(0, 2)), receiver)

        assertEquals(listOf("a1", "a2", "b1"), receiver.outputs.map { it.getString("id") })
        verify(
            client,
            times(2)
        ).search<Map<*, *>>(
            anyOrNull<Function<SearchRequest.Builder, ObjectBuilder<SearchRequest>>>(),
            anyOrNull<Class<Map<*, *>>>(),
        )
    }

    @Test
    fun `once limit is reached, reading stops and no further page is fetched`() {
        val (fn, _) = fieldFn(slices = 4, limit = 1)
        val client = clientReturning(
            listOf(responseOf(mapOf("id" to "a1", "amount" to 1.0), mapOf("id" to "a2", "amount" to 2.0)))
        )
        fn.clientFactory = EsClientFactory { client }
        fn.setup()
        val receiver = CollectingOutputReceiver<Row>()

        fn.processElement("orders", OffsetRangeTracker(OffsetRange(0, 1)), receiver)

        assertEquals(listOf("a1"), receiver.outputs.map { it.getString("id") })
        verify(
            client,
            times(1)
        ).search<Map<*, *>>(
            anyOrNull<Function<SearchRequest.Builder, ObjectBuilder<SearchRequest>>>(),
            anyOrNull<Class<Map<*, *>>>(),
        )
    }

    @Test
    fun `a refused claim stops immediately, without even opening a PIT`() {
        val (fn, _) = fieldFn(slices = 2)
        val client = clientReturning(listOf(responseOf(mapOf("id" to "a1", "amount" to 1.0))))
        fn.clientFactory = EsClientFactory { client }
        fn.setup()
        val receiver = CollectingOutputReceiver<Row>()
        // The runner refuses this claim when it splits off the remaining work at runtime; the DoFn must
        // stop here rather than keep reading.
        val refusing = object : OffsetRangeTracker(OffsetRange(0, 2)) {
            override fun tryClaim(position: Long): Boolean = false
        }

        fn.processElement("orders", refusing, receiver)

        assertEquals(0, receiver.outputs.size)
        verify(
            client,
            never()
        ).openPointInTime(
            anyOrNull<Function<OpenPointInTimeRequest.Builder, ObjectBuilder<OpenPointInTimeRequest>>>()
        )
    }
}
