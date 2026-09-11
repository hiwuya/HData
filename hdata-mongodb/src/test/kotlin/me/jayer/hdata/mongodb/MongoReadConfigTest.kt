package me.jayer.hdata.mongodb

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Binding and validation of [MongoReadConfig].
 *
 * @author wuya
 */
class MongoReadConfigTest {

    private val minimal = MongoReadConfig(
        connectionUri = "mongodb://localhost:27017",
        database = "mydb",
        collection = "orders",
    )

    private fun cfg(json: String): MongoReadConfig =
        TransformConfig("test", SpecMappers.CONFIG.readTree(json) as ObjectNode).bind(MongoReadConfig::class.java)

    @Test
    fun `config binds in snake_case`() {
        val config = cfg(
            """
            {
              "connection_uri": "mongodb://localhost:27017",
              "database": "mydb",
              "collection": "orders",
              "schema_fields": ["id:STRING"],
              "filter": "{\"status\": \"PAID\"}",
              "partition_num": 8,
              "fetch_size": 500
            }
            """.trimIndent()
        )

        assertEquals(listOf("id:STRING"), config.schemaFields)
        assertEquals(8, config.partitionNum)
        assertEquals(500, config.fetchSize)
        config.validate()
    }

    @Test
    fun `default values`() {
        val config = cfg("""{"connection_uri": "mongodb://localhost:27017", "database": "d", "collection": "c"}""")

        assertEquals(1000, config.fetchSize)
        // When not specified, the partition count is auto-estimated from the document count
        assertNull(config.partitionNum)
        assertEquals("", config.filter)
    }

    @Test
    fun `an empty required field fails validation`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(connectionUri = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(database = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(collection = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(fetchSize = 0).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(partitionNum = 0).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(partitionNum = 1001).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(connectionUri = "not-a-uri").validate() }
    }

    @Test
    fun `an invalid JSON filter fails at graph-construction time`() {
        // If this only blew up at runtime, the job would already be queued and running before the misconfiguration was caught
        val error = assertFailsWith<IllegalArgumentException> {
            minimal.copy(filter = "{status: PAID").validate()
        }

        assertTrue("filter" in error.message!!)
    }

    @Test
    fun `an unknown schema_fields type fails validation`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(schemaFields = listOf("id:UUID")).validate() }
    }

    @Test
    fun `valid limit values pass validation`() {
        minimal.copy(limit = -1).validate()
        minimal.copy(limit = 1).validate()
        minimal.copy(limit = 1000).validate()
        minimal.copy(limit = Int.MAX_VALUE.toLong() + 1).validate()
    }

    @Test
    fun `invalid limit values fail validation`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(limit = 0).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(limit = -2).validate() }
    }

    @Test
    fun `valid aggregate values pass validation`() {
        minimal.copy(
            aggregate = listOf(
                MongoAggregateSpec("count", "", "total"),
                MongoAggregateSpec("min", "amount", "min_amount"),
                MongoAggregateSpec("max", "amount", "max_amount"),
                MongoAggregateSpec("sum", "amount", "sum_amount"),
                MongoAggregateSpec("avg", "amount", "avg_amount"),
            )
        ).validate()
    }

    @Test
    fun `an invalid aggregate type fails validation`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(aggregate = listOf(MongoAggregateSpec("mean", "amount", "m"))).validate()
        }
    }

    @Test
    fun `aggregate other than count must have a column`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(aggregate = listOf(MongoAggregateSpec("min", "", "m"))).validate()
        }
    }

    @Test
    fun `aggregate count cannot carry a concrete field`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(aggregate = listOf(MongoAggregateSpec("count", "amount", "c"))).validate()
        }
    }

    @Test
    fun `aggregate and schema_fields are mutually exclusive`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(schemaFields = listOf("id:STRING"), aggregate = listOf(MongoAggregateSpec("count", "", "c"))).validate()
        }
    }

    @Test
    fun `aggregate and limit are mutually exclusive and aliases cannot repeat`() {
        // Aggregation is global semantics, so limit is meaningless for it; accepting it without effect would be a hidden trap
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(aggregate = listOf(MongoAggregateSpec("count", "", "total")), limit = 5).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(
                aggregate = listOf(
                    MongoAggregateSpec("count", "", "total"),
                    MongoAggregateSpec("sum", "amount", "total"),
                ),
            ).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(aggregate = listOf(MongoAggregateSpec("count", "", "total")), fetchSize = 10).validate()
        }
    }

    @Test
    fun `limit rejects a partition_num that would be force-overridden`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(limit = 5, partitionNum = 2).validate() }
    }
}
