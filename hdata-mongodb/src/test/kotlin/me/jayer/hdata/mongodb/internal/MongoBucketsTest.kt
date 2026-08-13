package me.jayer.hdata.mongodb.internal

import org.bson.BsonDocument
import org.bson.Document
import org.bson.types.ObjectId
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `_id` 分片边界的翻译。
 *
 * 这段逻辑算错的后果是漏数据或读重复数据，而**两种后果都不会让作业失败**——
 * 作业照常成功退出，只是结果不对。所以这里覆盖得细一点。
 *
 * @author wuya
 */
class MongoBucketsTest {

    private fun filters(vararg ranges: Pair<Any?, Any?>, userFilter: Document? = null): List<Document> =
        MongoBuckets.rangesToFilters(ranges.toList(), userFilter).map { Document.parse(it) }

    /** 从（可能被 `$and` 包着的）过滤条件里挖出 `_id` 那一段。 */
    private fun idCondition(filter: Document): Document {
        val and = filter["\$and"]
        @Suppress("UNCHECKED_CAST")
        val clauses = if (and != null) and as List<Document> else listOf(filter)
        return clauses.first { it.containsKey("_id") }["_id"] as Document
    }

    @Test
    fun `相邻分片首尾相接，不重不漏`() {
        val result = filters(1 to 9, 10 to 19, 20 to 29)

        assertEquals(3, result.size)
        val conditions = result.map { idCondition(it) }
        // 第一片 [1, 10)，第二片 [10, 20)：上一片的上界正好是下一片的下界
        assertEquals(1, conditions[0]["\$gte"])
        assertEquals(10, conditions[0]["\$lt"])
        assertEquals(10, conditions[1]["\$gte"])
        assertEquals(20, conditions[1]["\$lt"])
    }

    @Test
    fun `最后一片用闭区间，否则最大的那个文档会被漏掉`() {
        val conditions = filters(1 to 9, 10 to 19).map { idCondition(it) }

        assertEquals(19, conditions.last()["\$lte"])
        assertTrue(!conditions.last().containsKey("\$lt"))
    }

    @Test
    fun `只有一个桶时就是一个闭区间`() {
        val condition = idCondition(filters(1 to 100).single())

        assertEquals(1, condition["\$gte"])
        assertEquals(100, condition["\$lte"])
    }

    @Test
    fun `用户的查询条件与 id 区间 and 在一起`() {
        val result = filters(1 to 9, 10 to 19, userFilter = Document("status", "PAID"))

        result.forEach { filter ->
            @Suppress("UNCHECKED_CAST")
            val clauses = filter["\$and"] as List<Document>
            assertTrue(clauses.any { it["status"] == "PAID" }, "用户过滤条件必须保留在每个分片上")
            assertTrue(clauses.any { it.containsKey("_id") })
        }
    }

    @Test
    fun `ObjectId 边界经过 JSON 往返后仍然是 ObjectId`() {
        // 用宽松模式的 JSON 会把 ObjectId 写成普通字符串，查询就匹配不上任何文档——
        // 结果是分片全部读出 0 条，作业成功但数据全丢
        val a = ObjectId("650000000000000000000001")
        val b = ObjectId("650000000000000000000009")

        val json = MongoBuckets.rangesToFilters(listOf(a to b), null).single()
        val parsed = MongoBuckets.parse(json)

        assertTrue(parsed.getDocument("_id").isObjectId("\$gte"), "边界必须还原成 ObjectId，实际: $json")
        assertEquals(a, parsed.getDocument("_id").getObjectId("\$gte").value)
    }

    @Test
    fun `Long 边界不会在往返中降级成 Int`() {
        val json = MongoBuckets.rangesToFilters(listOf(3_000_000_000L to 4_000_000_000L), null).single()
        val parsed = MongoBuckets.parse(json)

        assertEquals(3_000_000_000L, parsed.getDocument("_id").getInt64("\$gte").value)
    }

    @Test
    fun `没有边界时退化成空条件`() {
        val json = MongoBuckets.toJson(BsonDocument())

        assertEquals("{}", Document.parse(json).toJson())
    }
}
