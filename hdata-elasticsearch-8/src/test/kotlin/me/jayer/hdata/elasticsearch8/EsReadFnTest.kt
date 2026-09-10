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
 * ES 8.x 读取端的切分与可序列化。
 *
 * @author wuya
 */
class EsReadFnTest {

    private val config = EsReadConfig(connectionUri = "http://localhost:9200", index = "orders")

    private fun fn(config: EsReadConfig) = EsReadFn(config, config.schemaFields)

    @Test
    fun `DoFn 可以序列化下发`() {
        // 重构前 Query / SortOptions 是 DoFn 的普通字段，这两个客户端对象都不可序列化，
        // 作业在提交阶段就会炸——而单测里只调 processElement 的话永远发现不了
        SerializableUtils.ensureSerializable(fn(config.copy(scanQuery = """{"match_all":{}}""")))
    }

    @Test
    fun `provider 生成的 source 可以序列化下发`() {
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
    fun `初始限制覆盖全部 slice`() {
        assertEquals(OffsetRange(0, 4), fn(config.copy(scanSlices = 4)).getInitialRestriction("orders"))
        // 默认不切分
        assertEquals(OffsetRange(0, 1), fn(config).getInitialRestriction("orders"))
    }

    @Test
    fun `每个 slice 切成一份，首尾相接`() {
        val receiver = CollectingOutputReceiver<OffsetRange>()
        fn(config.copy(scanSlices = 4)).splitRestriction("orders", OffsetRange(0, 4), receiver)
        val splits = receiver.outputs

        assertEquals(4, splits.size)
        assertEquals(listOf(0L, 1L, 2L, 3L), splits.map { it.from })
        assertEquals(listOf(1L, 2L, 3L, 4L), splits.map { it.to })
    }

    @Test
    fun `空区间不产出切分`() {
        val receiver = CollectingOutputReceiver<OffsetRange>()
        fn(config).splitRestriction("orders", OffsetRange(0, 0), receiver)
        val splits = receiver.outputs

        assertTrue(splits.isEmpty())
    }

    @Test
    fun `scan_slices 非法时报错`() {
        assertFailsWith<IllegalArgumentException> { config.copy(scanSlices = 0).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(keepAliveMinutes = 0).validate() }
    }

    @Test
    fun `limit 合法取值通过校验`() {
        config.copy(limit = -1).validate()
        config.copy(limit = 1).validate()
        config.copy(limit = 1000).validate()
        config.copy(limit = Int.MAX_VALUE.toLong() + 1).validate()

        assertEquals(1000, EsReadFn.pageSize(1000, Int.MAX_VALUE.toLong() + 1))
        assertEquals(7, EsReadFn.pageSize(1000, 7))
    }

    @Test
    fun `limit 非法取值报错`() {
        assertFailsWith<IllegalArgumentException> { config.copy(limit = 0).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(limit = -2).validate() }
    }

    @Test
    fun `limit 大于 0 时强制单 slice，保证全局语义`() {
        // 即便声明了 4 个 slice，限行数也必须收敛成单 slice，否则会变成"每 slice 各读 limit 条"
        assertEquals(OffsetRange(0, 1), fn(config.copy(scanSlices = 4, limit = 100)).getInitialRestriction("orders"))
        // 不限制时仍按声明的 slice 数切分
        assertEquals(OffsetRange(0, 4), fn(config.copy(scanSlices = 4, limit = -1)).getInitialRestriction("orders"))
    }

    @Test
    fun `scan_query 不是合法 JSON 时在构图阶段就报错`() {
        val error = assertFailsWith<IllegalArgumentException> {
            config.copy(scanQuery = "{match_all").validate()
        }

        assertTrue("scan_query" in error.message!!)
    }

    @Test
    fun `schema_fields 类型不认识时报错`() {
        assertFailsWith<IllegalArgumentException> { config.copy(schemaFields = listOf("id:UUID")).validate() }
    }

    @Test
    fun `_source 投影由 schema_fields 推导，空字段表示不裁剪`() {
        // schema_fields 上的字段名直接下推成 `_source` includes（投影下推），让 ES 服务端裁剪
        assertEquals(listOf("id", "amount"), EsReadFn.sourceFieldNames(listOf("id:STRING", "amount:DOUBLE")))
        // 退化成 document 模式（整行 JSON 一列）时返回 null，读取完整 _source
        assertEquals(null, EsReadFn.sourceFieldNames(emptyList()))
    }

    /**
     * 一页搜索结果。
     *
     * `sort()` 返回空列表：读端据此判定"翻不动了"，一个 slice 只发一次 search，
     * 这样测试里的响应序列才能和 slice 一一对应。
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
    fun `processElement 逐个 slice 认领，每个 slice 的文档都读出来`() {
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
    fun `limit 生效时读完 limit 条就停，不再翻页`() {
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
    fun `认领被拒时立刻停手，连 PIT 都不开`() {
        val (fn, _) = fieldFn(slices = 2)
        val client = clientReturning(listOf(responseOf(mapOf("id" to "a1", "amount" to 1.0))))
        fn.clientFactory = EsClientFactory { client }
        fn.setup()
        val receiver = CollectingOutputReceiver<Row>()
        // 运行时把剩下的活切走时就会拒掉本次认领，此时必须停手而不是继续读
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
