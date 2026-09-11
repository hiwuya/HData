package me.jayer.hdata.jdbc

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * @author wuya
 * @date 2022-08-30
 */
class JdbcConfigTest {

    private inline fun <reified T : Any> bind(json: String): T =
        TransformConfig("Jdbc", SpecMappers.CONFIG.readTree(json) as ObjectNode).bind(T::class.java)

    // ---------- binding ----------

    @Test
    fun `read config binds by snake_case`() {
        val config = bind<JdbcReadConfig>(
            """
            {
              "url": "jdbc:h2:mem:x", "user": "sa", "password": "",
              "driver_class": "org.h2.Driver",
              "tables": ["t_order"], "where": "id > 1",
              "partition_column": "id", "partition_num": 4, "fetch_size": 100,
              "connection_properties": {"maximumPoolSize": "2"}
            }
            """.trimIndent()
        )

        assertEquals("jdbc:h2:mem:x", config.url)
        assertEquals("org.h2.Driver", config.driverClass)
        assertEquals(listOf("t_order"), config.tables)
        assertEquals("id", config.partitionColumn)
        assertEquals(4, config.partitionNum)
        assertEquals(100, config.fetchSize)
        assertEquals(mapOf("maximumPoolSize" to "2"), config.connectionProperties)
    }

    @Test
    fun `read defaults`() {
        val config = bind<JdbcReadConfig>("""{"url": "jdbc:h2:mem:x", "tables": ["t"]}""")

        assertEquals(listOf("*"), config.columns)
        assertEquals(10000, config.fetchSize)
        assertNull(config.partitionNum)
        assertEquals("", config.where)
    }

    @Test
    fun `write config binds by snake_case`() {
        val config = bind<JdbcWriteConfig>(
            """
            {
              "url": "jdbc:h2:mem:x", "table": "t_order",
              "batch_size": 500, "retry_max_attempts": 5,
              "retry_initial_seconds": 1, "retry_max_seconds": 30
            }
            """.trimIndent()
        )

        assertEquals("t_order", config.table)
        assertEquals(500, config.batchSize)
        assertEquals(5, config.retryMaxAttempts)
        assertEquals(1L, config.retryInitialSeconds)
        assertEquals(30L, config.retryMaxSeconds)
    }

    // ---------- validation ----------

    @Test
    fun `read errors when url is blank`() {
        val error = assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(tables = listOf("t")).validate()
        }
        assertTrue("url" in error.message!!)
    }

    @Test
    fun `connection_properties cannot override explicit connection fields`() {
        assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(
                url = "jdbc:h2:mem:x",
                tables = listOf("t"),
                connectionProperties = mapOf("jdbcUrl" to "jdbc:h2:mem:other"),
            ).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(
                url = "jdbc:h2:mem:x",
                tables = listOf("t"),
                connectionProperties = mapOf("" to "x"),
            ).validate()
        }
    }

    @Test
    fun `read requires at least one of tables or query`() {
        val error = assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(url = "jdbc:h2:mem:x").validate()
        }
        assertTrue("tables" in error.message!! && "query" in error.message!!)
    }

    @Test
    fun `read rejects tables and query set together`() {
        val error = assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(url = "jdbc:h2:mem:x", tables = listOf("t"), query = "select 1").validate()
        }
        assertTrue("must not both be set" in error.message!!)
    }

    @Test
    fun `query mode rejects table-read config that would have no effect`() {
        val base = JdbcReadConfig(url = "jdbc:h2:mem:x", query = "select 1")
        assertFailsWith<IllegalArgumentException> { base.copy(columns = listOf("id")).validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(where = "id > 0").validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(partitionColumn = "id").validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(partitionNum = 2).validate() }
    }

    @Test
    fun `aggregation pushdown config validation`() {
        val base = JdbcReadConfig(url = "jdbc:h2:mem:x", user = "sa", tables = listOf("t_order"))
        // count/min/max/sum/avg are all supported
        base.copy(aggregations = listOf("count", "min:id", "max:id", "sum:id", "avg:id")).validate()
        // aggregation mode rejects config that would have no effect
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("count"), columns = listOf("id")).validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("count"), partitionColumn = "id").validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("count"), partitionNum = 2).validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("count"), limit = 5).validate() }
        // an unsupported aggregation fails right away
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("median")).validate() }
        // multi-table aggregation is not supported
        assertFailsWith<IllegalArgumentException> { base.copy(tables = listOf("a", "b"), aggregations = listOf("count")).validate() }
        // duplicate output column names (two aggregations landing on the same column) put two identically named columns into the result set, so fail right away
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("count", "count:id")).validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("min:id", "min:id")).validate() }
    }

    @Test
    fun `table and column names must not be blank`() {
        assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(url = "jdbc:h2:mem:x", tables = listOf("t", " ")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(url = "jdbc:h2:mem:x", tables = listOf("t"), columns = listOf("id", " ")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(url = "jdbc:h2:mem:x", tables = listOf("t"), columns = listOf("id", "id")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(url = "jdbc:h2:mem:x", tables = listOf("t", "t")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(url = "jdbc:h2:mem:x", tables = listOf("t_\${0-1}", "t_1")).validate()
        }
    }

    @Test
    fun `read numeric parameters must be positive`() {
        assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(url = "jdbc:h2:mem:x", tables = listOf("t"), fetchSize = 0).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(url = "jdbc:h2:mem:x", tables = listOf("t"), partitionNum = 0).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(url = "jdbc:h2:mem:x", tables = listOf("t"), partitionNum = 10_001).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(url = "jdbc:h2:mem:x", tables = listOf("t"), columns = emptyList()).validate()
        }
    }

    @Test
    fun `multi-table read rejects a limit that can't guarantee global semantics`() {
        assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(
                url = "jdbc:h2:mem:test",
                tables = listOf("orders_1", "orders_2"),
                limit = 10,
            ).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(
                url = "jdbc:h2:mem:test",
                tables = listOf("orders"),
                limit = 10,
                partitionNum = 2,
            ).validate()
        }
    }

    @Test
    fun `write table must not be blank`() {
        val error = assertFailsWith<IllegalArgumentException> {
            JdbcWriteConfig(url = "jdbc:h2:mem:x").validate()
        }
        assertTrue("table" in error.message!!)
    }

    @Test
    fun `write retry parameter validation`() {
        assertFailsWith<IllegalArgumentException> {
            JdbcWriteConfig(url = "jdbc:h2:mem:x", table = "t", batchSize = 0).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            JdbcWriteConfig(url = "jdbc:h2:mem:x", table = "t", retryMaxAttempts = 0).validate()
        }
        // max must not be smaller than initial, otherwise the backoff interval is empty
        assertFailsWith<IllegalArgumentException> {
            JdbcWriteConfig(url = "jdbc:h2:mem:x", table = "t", retryInitialSeconds = 60, retryMaxSeconds = 10).validate()
        }
    }

    // ---------- connection properties ----------

    @Test
    fun `dataSourceProperties maps to keys Hikari recognizes`() {
        val properties = JdbcReadConfig(
            url = "jdbc:h2:mem:x",
            user = "sa",
            password = "pwd",
            driverClass = "org.h2.Driver",
            connectionProperties = mapOf("maximumPoolSize" to "3"),
        ).dataSourceProperties()

        assertEquals("jdbc:h2:mem:x", properties["jdbcUrl"])
        assertEquals("sa", properties["dataSource.user"])
        assertEquals("pwd", properties["dataSource.password"])
        assertEquals("org.h2.Driver", properties["driverClassName"])
        assertEquals("3", properties["maximumPoolSize"])
    }

    @Test
    fun `a blank username, password, or driver isn't written into the properties map`() {
        val properties = JdbcReadConfig(url = "jdbc:h2:mem:x").dataSourceProperties()

        assertEquals(setOf("jdbcUrl"), properties.keys.map { it.toString() }.toSet())
    }
}
