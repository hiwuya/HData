package me.jayer.hdata.elasticsearch6

import me.jayer.hdata.core.testing.CollectingOutputReceiver
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.beam.sdk.values.Row
import org.elasticsearch.action.search.SearchRequest
import org.elasticsearch.action.search.SearchResponse
import org.elasticsearch.client.RequestOptions
import org.elasticsearch.client.RestHighLevelClient
import org.elasticsearch.common.bytes.BytesArray
import org.elasticsearch.search.SearchHit
import org.elasticsearch.search.SearchHits
import org.mockito.kotlin.anyOrNull
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The ES 6.x read side's splitting and serializability.
 *
 * @author wuya
 */
class Elasticsearch6ReadFnTest {

    private val config = Elasticsearch6ReadConfig(connectionUri = "http://localhost:9200", index = "orders")

    private fun fn(slices: Int = 1, limit: Long = -1) = Elasticsearch6ReadFn(
        config.nodes(),
        config.username,
        config.password,
        DOCUMENT_SCHEMA,
        emptyList(),
        config.scanQuery,
        config.scrollSize,
        config.scrollTimeoutMinutes,
        slices,
        limit,
    )

    @Test
    fun `the DoFn ships serializable`() {
        SerializableUtils.ensureSerializable(fn(4))
    }

    @Test
    fun `the initial restriction covers every slice`() {
        assertEquals(OffsetRange(0, 4), fn(4).getInitialRestriction("orders"))
        assertEquals(OffsetRange(0, 1), fn().getInitialRestriction("orders"))
    }

    @Test
    fun `each slice is split into one contiguous piece, end to end`() {
        val receiver = CollectingOutputReceiver<OffsetRange>()
        fn(3).splitRestriction("orders", OffsetRange(0, 3), receiver)
        val splits = receiver.outputs

        assertEquals(listOf(0L, 1L, 2L), splits.map { it.from })
        assertEquals(listOf(1L, 2L, 3L), splits.map { it.to })
    }

    @Test
    fun `an empty range produces no split`() {
        val receiver = CollectingOutputReceiver<OffsetRange>()
        fn().splitRestriction("orders", OffsetRange(0, 0), receiver)
        val splits = receiver.outputs

        assertTrue(splits.isEmpty())
    }

    @Test
    fun `an invalid scan_slices errors out`() {
        assertFailsWith<IllegalArgumentException> { config.copy(scanSlices = 0).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(scrollTimeoutMinutes = 0).validate() }
    }

    @Test
    fun `a valid limit value passes validation`() {
        config.copy(limit = -1).validate()
        config.copy(limit = 1).validate()
        config.copy(limit = 1000).validate()
        config.copy(limit = Int.MAX_VALUE.toLong() + 1).validate()

        assertEquals(1000, Elasticsearch6ReadFn.pageSize(1000, Int.MAX_VALUE.toLong() + 1))
        assertEquals(7, Elasticsearch6ReadFn.pageSize(1000, 7))
    }

    @Test
    fun `an invalid limit value errors out`() {
        assertFailsWith<IllegalArgumentException> { config.copy(limit = 0).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(limit = -2).validate() }
    }

    @Test
    fun `limit greater than 0 forces a single slice to preserve global semantics`() {
        // Even with 4 slices declared, limiting rows must converge to a single slice, otherwise it would become "each slice reads limit records".
        assertEquals(OffsetRange(0, 1), fn(slices = 4, limit = 100).getInitialRestriction("orders"))
        // When unlimited, it still splits by the declared number of slices.
        assertEquals(OffsetRange(0, 4), fn(slices = 4, limit = -1).getInitialRestriction("orders"))
    }

    @Test
    fun `the _source projection is derived from schema_fields`() {
        // Field names from schema_fields are pushed down directly as `_source` includes (projection push-down), letting the ES server prune.
        assertEquals(
            listOf("id", "amount"),
            Elasticsearch6ReadFn.sourceFieldNames(listOf(EsField("id", EsFieldType.STRING), EsField("amount", EsFieldType.DOUBLE))).toList(),
        )
    }

    private fun hit(json: String): SearchHit = SearchHit(0).sourceRef(BytesArray(json))

    /** When [scrollId] is null the read side does not page further, so one scroll ends it. */
    private fun responseOf(vararg hits: SearchHit): SearchResponse {
        val response = mock<SearchResponse>()
        whenever(response.hits).doReturn(SearchHits(hits, hits.size.toLong(), 1.0f))
        whenever(response.scrollId).doReturn(null)
        return response
    }

    /** Makes `search` return [responses] in order; in field mode the read side takes columns per schema_fields. */
    private fun clientReturning(responses: List<SearchResponse>): RestHighLevelClient {
        var call = 0
        val client = mock<RestHighLevelClient>()
        whenever(client.search(anyOrNull<SearchRequest>(), anyOrNull<RequestOptions>()))
            .thenAnswer { responses[call++] }
        return client
    }

    private fun fieldFn(slices: Int = 1, limit: Long = -1): Triple<Elasticsearch6ReadFn, org.apache.beam.sdk.schemas.Schema, List<EsField>> {
        val fields = parseSchemaFields(listOf("id:STRING", "amount:DOUBLE"))
        val schema = buildSchema(fields)
        val fn = Elasticsearch6ReadFn(
            config.nodes(),
            config.username,
            config.password,
            schema,
            fields,
            config.scanQuery,
            config.scrollSize,
            config.scrollTimeoutMinutes,
            slices,
            limit,
        )
        return Triple(fn, schema, fields)
    }

    @Test
    fun `processElement claims one slice at a time, reading every slice's documents`() {
        val (fn, _, _) = fieldFn(slices = 2)
        val client = clientReturning(
            listOf(
                responseOf(hit("""{"id":"a1","amount":1.0}"""), hit("""{"id":"a2","amount":2.0}""")),
                responseOf(hit("""{"id":"b1","amount":3.0}""")),
            )
        )
        fn.clientFactory = Es6ClientFactory { client }
        fn.setup()
        val receiver = CollectingOutputReceiver<Row>()

        fn.processElement("orders", OffsetRangeTracker(OffsetRange(0, 2)), receiver)

        assertEquals(listOf("a1", "a2", "b1"), receiver.outputs.map { it.getString("id") })
        verify(client, times(2)).search(anyOrNull<SearchRequest>(), anyOrNull<RequestOptions>())
    }

    @Test
    fun `with limit in effect, reading stops after limit records without further paging`() {
        val (fn, _, _) = fieldFn(slices = 4, limit = 1)
        val client = clientReturning(
            listOf(responseOf(hit("""{"id":"a1","amount":1.0}"""), hit("""{"id":"a2","amount":2.0}""")))
        )
        fn.clientFactory = Es6ClientFactory { client }
        fn.setup()
        val receiver = CollectingOutputReceiver<Row>()

        fn.processElement("orders", OffsetRangeTracker(OffsetRange(0, 1)), receiver)

        assertEquals(listOf("a1"), receiver.outputs.map { it.getString("id") })
        verify(client, never()).searchScroll(anyOrNull(), anyOrNull<RequestOptions>())
    }

    @Test
    fun `stops immediately when a claim is rejected, issuing no request`() {
        val (fn, _, _) = fieldFn(slices = 2)
        val client = clientReturning(listOf(responseOf(hit("""{"id":"a1","amount":1.0}"""))))
        fn.clientFactory = Es6ClientFactory { client }
        fn.setup()
        val receiver = CollectingOutputReceiver<Row>()
        // When the runtime splits off the remaining work, this claim is rejected, and it must stop rather than keep reading.
        val refusing = object : OffsetRangeTracker(OffsetRange(0, 2)) {
            override fun tryClaim(position: Long): Boolean = false
        }

        fn.processElement("orders", refusing, receiver)

        assertEquals(0, receiver.outputs.size)
        verify(client, never()).search(anyOrNull<SearchRequest>(), anyOrNull<RequestOptions>())
    }
}
