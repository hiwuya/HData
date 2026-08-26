package me.jayer.hdata.elasticsearch8

import co.elastic.clients.elasticsearch._types.aggregations.Aggregation
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import me.jayer.hdata.elasticsearch8.transform.buildAggregateSearchRequest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * 聚合搜索请求的构造。这里曾漏掉 `.index(...)`：请求不带索引名，对真实 ES 直接 400，
 * 而逻辑层测试发现不了——所以请求必须脱离真实集群被单测钉住。
 */
class EsAggregateRequestTest {

    private fun query(): Query = Query.of { it.matchAll { m -> m } }

    @Test
    fun `聚合请求带索引名且 size 为 0 track_total_hits 开启`() {
        val req = buildAggregateSearchRequest(
            "orders_a,orders_b",
            query(),
            mapOf("min_age" to Aggregation.of { it.min { m -> m.field("age") } }),
        )

        // 回归钉子：请求必须带上索引名（曾漏掉 .index(...)，对真实 ES 直接 400）；
        // 客户端把逗号表达式原样保留，由服务端拆分
        assertEquals("orders_a,orders_b", req.index().joinToString(","))
        assertEquals(0, req.size())
        assertNotNull(req.trackTotalHits())
        assertEquals(setOf("min_age"), req.aggregations().keys)
    }

    @Test
    fun `纯 count 不携带 aggregation`() {
        val req = buildAggregateSearchRequest("orders", query(), emptyMap())

        assertEquals(listOf("orders"), req.index())
        assertTrue(req.aggregations().isEmpty())
    }
}
