package me.jayer.hdata.mongodb.internal

import org.bson.BsonDocument
import org.bson.Document
import org.bson.types.ObjectId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * Translation of the `_id` partition boundaries.
 *
 * Getting this logic wrong means missing or duplicated data, and **neither failure makes the job fail** —
 * the job exits successfully as usual, just with incorrect results. So we cover it in detail here.
 *
 * @author wuya
 */
class MongoBucketsTest {

    private fun filters(vararg ranges: Pair<Any?, Any?>, userFilter: Document? = null): List<Document> =
        MongoBuckets.rangesToFilters(ranges.toList(), userFilter).map { Document.parse(it) }

    /** Extract the `_id` clause from a filter condition (possibly wrapped in `$and`). */
    private fun idCondition(filter: Document): Document {
        val and = filter["\$and"]
        @Suppress("UNCHECKED_CAST")
        val clauses = if (and != null) and as List<Document> else listOf(filter)
        return clauses.first { it.containsKey("_id") }["_id"] as Document
    }

    @Test
    fun `adjacent partitions join end to end with no overlap and no gap`() {
        val result = filters(1 to 9, 10 to 19, 20 to 29)

        assertEquals(3, result.size)
        val conditions = result.map { idCondition(it) }
        // First partition [1, 10), second [10, 20): the previous partition's upper bound is exactly the next partition's lower bound
        assertEquals(1, conditions[0]["\$gte"])
        assertEquals(10, conditions[0]["\$lt"])
        assertEquals(10, conditions[1]["\$gte"])
        assertEquals(20, conditions[1]["\$lt"])
    }

    @Test
    fun `the last bucket uses a closed interval, otherwise the largest document is lost`() {
        val conditions = filters(1 to 9, 10 to 19).map { idCondition(it) }

        assertEquals(19, conditions.last()["\$lte"])
        assertTrue(!conditions.last().containsKey("\$lt"))
    }

    @Test
    fun `a single bucket is just one closed interval`() {
        val condition = idCondition(filters(1 to 100).single())

        assertEquals(1, condition["\$gte"])
        assertEquals(100, condition["\$lte"])
    }

    @Test
    fun `the user query condition is and-ed together with the id range`() {
        val result = filters(1 to 9, 10 to 19, userFilter = Document("status", "PAID"))

        result.forEach { filter ->
            @Suppress("UNCHECKED_CAST")
            val clauses = filter["\$and"] as List<Document>
            assertTrue(clauses.any { it["status"] == "PAID" }, "The user filter condition must be preserved on every partition")
            assertTrue(clauses.any { it.containsKey("_id") })
        }
    }

    @Test
    fun `ObjectId boundaries survive a JSON round-trip as ObjectId`() {
        // Using relaxed-mode JSON would write ObjectId as a plain string, and the query would match no documents —
        // the result would be every partition reading 0 rows: the job succeeds but all data is lost
        val a = ObjectId("650000000000000000000001")
        val b = ObjectId("650000000000000000000009")

        val json = MongoBuckets.rangesToFilters(listOf(a to b), null).single()
        val parsed = MongoBuckets.parse(json)

        assertTrue(parsed.getDocument("_id").isObjectId("\$gte"), "The boundary must be restored as ObjectId, actual: $json")
        assertEquals(a, parsed.getDocument("_id").getObjectId("\$gte").value)
    }

    @Test
    fun `Long boundaries are not downgraded to Int during the round-trip`() {
        val json = MongoBuckets.rangesToFilters(listOf(3_000_000_000L to 4_000_000_000L), null).single()
        val parsed = MongoBuckets.parse(json)

        assertEquals(3_000_000_000L, parsed.getDocument("_id").getInt64("\$gte").value)
    }

    @Test
    fun `with no boundaries it degrades to an empty condition`() {
        val json = MongoBuckets.toJson(BsonDocument())

        assertEquals("{}", Document.parse(json).toJson())
    }
}
