package me.jayer.hdata.elasticsearch8

import co.elastic.clients.elasticsearch._types.aggregations.Aggregation
import co.elastic.clients.elasticsearch._types.query_dsl.Query
import me.jayer.hdata.elasticsearch8.transform.buildAggregateSearchRequest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Construction of the aggregate search request. `.index(...)` was missing at one point: a request with
 * no index name gets a flat 400 from a real ES cluster, and a logic-layer test cannot catch that — so the
 * request must be pinned down by a unit test without touching a real cluster.
 */
class EsAggregateRequestTest {

    private fun query(): Query = Query.of { it.matchAll { m -> m } }

    @Test
    fun `the aggregate request carries the index name, size 0, and track_total_hits on`() {
        val req = buildAggregateSearchRequest(
            "orders_a,orders_b",
            query(),
            mapOf("min_age" to Aggregation.of { it.min { m -> m.field("age") } }),
        )

        // Regression pin: the request must carry the index name (`.index(...)` was missing once, which
        // gets a flat 400 from a real ES); the client keeps the comma expression as-is, split server-side.
        assertEquals("orders_a,orders_b", req.index().joinToString(","))
        assertEquals(0, req.size())
        assertNotNull(req.trackTotalHits())
        assertEquals(setOf("min_age"), req.aggregations().keys)
    }

    @Test
    fun `a plain count carries no aggregation`() {
        val req = buildAggregateSearchRequest("orders", query(), emptyMap())

        assertEquals(listOf("orders"), req.index())
        assertTrue(req.aggregations().isEmpty())
    }
}
