package me.jayer.hdata.elasticsearch6

import org.elasticsearch.index.query.QueryBuilders
import org.elasticsearch.search.builder.SearchSourceBuilder
import me.jayer.hdata.elasticsearch6.buildAggregateSearchRequest
import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Building the aggregation search request: a multi-index expression is split into an index array and size is pushed
 * down to 0, all verified without a real cluster.
 */
class Elasticsearch6AggregateRequestTest {

    @Test
    fun `逗号分隔的多索引表达式拆成索引数组`() {
        val source = SearchSourceBuilder().apply {
            query(QueryBuilders.matchAllQuery())
            size(0)
        }
        val req = buildAggregateSearchRequest("orders_a, orders_b", source)

        assertEquals(listOf("orders_a", "orders_b"), req.indices().toList())
        assertEquals(0, req.source().size())
    }

    @Test
    fun `单索引原样传递`() {
        val req = buildAggregateSearchRequest("orders", SearchSourceBuilder().size(0))

        assertEquals(listOf("orders"), req.indices().toList())
    }
}
