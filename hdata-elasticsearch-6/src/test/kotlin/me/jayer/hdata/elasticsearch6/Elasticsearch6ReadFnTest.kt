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
 * ES 6.x 读取端的切分与可序列化。
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
    fun `DoFn 可以序列化下发`() {
        SerializableUtils.ensureSerializable(fn(4))
    }

    @Test
    fun `初始限制覆盖全部 slice`() {
        assertEquals(OffsetRange(0, 4), fn(4).getInitialRestriction("orders"))
        assertEquals(OffsetRange(0, 1), fn().getInitialRestriction("orders"))
    }

    @Test
    fun `每个 slice 切成一份，首尾相接`() {
        val receiver = CollectingOutputReceiver<OffsetRange>()
        fn(3).splitRestriction("orders", OffsetRange(0, 3), receiver)
        val splits = receiver.outputs

        assertEquals(listOf(0L, 1L, 2L), splits.map { it.from })
        assertEquals(listOf(1L, 2L, 3L), splits.map { it.to })
    }

    @Test
    fun `空区间不产出切分`() {
        val receiver = CollectingOutputReceiver<OffsetRange>()
        fn().splitRestriction("orders", OffsetRange(0, 0), receiver)
        val splits = receiver.outputs

        assertTrue(splits.isEmpty())
    }

    @Test
    fun `scan_slices 非法时报错`() {
        assertFailsWith<IllegalArgumentException> { config.copy(scanSlices = 0).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(scrollTimeoutMinutes = 0).validate() }
    }

    @Test
    fun `limit 合法取值通过校验`() {
        config.copy(limit = -1).validate()
        config.copy(limit = 1).validate()
        config.copy(limit = 1000).validate()
        config.copy(limit = Int.MAX_VALUE.toLong() + 1).validate()

        assertEquals(1000, Elasticsearch6ReadFn.pageSize(1000, Int.MAX_VALUE.toLong() + 1))
        assertEquals(7, Elasticsearch6ReadFn.pageSize(1000, 7))
    }

    @Test
    fun `limit 非法取值报错`() {
        assertFailsWith<IllegalArgumentException> { config.copy(limit = 0).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(limit = -2).validate() }
    }

    @Test
    fun `limit 大于 0 时强制单 slice，保证全局语义`() {
        // 即便声明了 4 个 slice，限行数也必须收敛成单 slice，否则会变成"每 slice 各读 limit 条"
        assertEquals(OffsetRange(0, 1), fn(slices = 4, limit = 100).getInitialRestriction("orders"))
        // 不限制时仍按声明的 slice 数切分
        assertEquals(OffsetRange(0, 4), fn(slices = 4, limit = -1).getInitialRestriction("orders"))
    }

    @Test
    fun `_source 投影由 schema_fields 推导`() {
        // schema_fields 上的字段名直接下推成 `_source` includes（投影下推），让 ES 服务端裁剪
        assertEquals(
            listOf("id", "amount"),
            Elasticsearch6ReadFn.sourceFieldNames(listOf(EsField("id", EsFieldType.STRING), EsField("amount", EsFieldType.DOUBLE))).toList(),
        )
    }

    private fun hit(json: String): SearchHit = SearchHit(0).sourceRef(BytesArray(json))

    /** [scrollId] 为 null 时读端不会再翻页，一条 scroll 就结束。 */
    private fun responseOf(vararg hits: SearchHit): SearchResponse {
        val response = mock<SearchResponse>()
        whenever(response.hits).doReturn(SearchHits(hits, hits.size.toLong(), 1.0f))
        whenever(response.scrollId).doReturn(null)
        return response
    }

    /** 让 `search` 依次返回 [responses]；用 field 模式，读端会按 schema_fields 取列。 */
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
    fun `processElement 逐个 slice 认领，每个 slice 的文档都读出来`() {
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
    fun `limit 生效时读完 limit 条就停，不再翻页`() {
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
    fun `认领被拒时立刻停手，不发任何请求`() {
        val (fn, _, _) = fieldFn(slices = 2)
        val client = clientReturning(listOf(responseOf(hit("""{"id":"a1","amount":1.0}"""))))
        fn.clientFactory = Es6ClientFactory { client }
        fn.setup()
        val receiver = CollectingOutputReceiver<Row>()
        // 运行时把剩下的活切走时就会拒掉本次认领，此时必须停手而不是继续读
        val refusing = object : OffsetRangeTracker(OffsetRange(0, 2)) {
            override fun tryClaim(position: Long): Boolean = false
        }

        fn.processElement("orders", refusing, receiver)

        assertEquals(0, receiver.outputs.size)
        verify(client, never()).search(anyOrNull<SearchRequest>(), anyOrNull<RequestOptions>())
    }
}
