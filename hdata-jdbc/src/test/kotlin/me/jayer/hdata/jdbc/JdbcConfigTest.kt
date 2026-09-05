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

    // ---------- 绑定 ----------

    @Test
    fun `读端配置按 snake_case 绑定`() {
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
    fun `读端默认值`() {
        val config = bind<JdbcReadConfig>("""{"url": "jdbc:h2:mem:x", "tables": ["t"]}""")

        assertEquals(listOf("*"), config.columns)
        assertEquals(10000, config.fetchSize)
        assertNull(config.partitionNum)
        assertEquals("", config.where)
    }

    @Test
    fun `写端配置按 snake_case 绑定`() {
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

    // ---------- 校验 ----------

    @Test
    fun `读端 url 为空时报错`() {
        val error = assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(tables = listOf("t")).validate()
        }
        assertTrue("url" in error.message!!)
    }

    @Test
    fun `connection_properties 不能覆盖显式连接字段`() {
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
    fun `读端 tables 与 query 至少填一个`() {
        val error = assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(url = "jdbc:h2:mem:x").validate()
        }
        assertTrue("tables" in error.message!! && "query" in error.message!!)
    }

    @Test
    fun `读端 tables 与 query 不能同时填`() {
        val error = assertFailsWith<IllegalArgumentException> {
            JdbcReadConfig(url = "jdbc:h2:mem:x", tables = listOf("t"), query = "select 1").validate()
        }
        assertTrue("不能同时" in error.message!!)
    }

    @Test
    fun `query 模式拒绝不会生效的表读取配置`() {
        val base = JdbcReadConfig(url = "jdbc:h2:mem:x", query = "select 1")
        assertFailsWith<IllegalArgumentException> { base.copy(columns = listOf("id")).validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(where = "id > 0").validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(partitionColumn = "id").validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(partitionNum = 2).validate() }
    }

    @Test
    fun `聚合下推配置校验`() {
        val base = JdbcReadConfig(url = "jdbc:h2:mem:x", user = "sa", tables = listOf("t_order"))
        // count/min/max/sum/avg 都支持
        base.copy(aggregations = listOf("count", "min:id", "max:id", "sum:id", "avg:id")).validate()
        // 聚合模式拒绝不会生效的配置
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("count"), columns = listOf("id")).validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("count"), partitionColumn = "id").validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("count"), partitionNum = 2).validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("count"), limit = 5).validate() }
        // 不支持的聚合直接报错
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("median")).validate() }
        // 多表聚合不支持
        assertFailsWith<IllegalArgumentException> { base.copy(tables = listOf("a", "b"), aggregations = listOf("count")).validate() }
        // 输出列名重复（两条聚合落到同一列）会让结果集出现同名列，直接报错
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("count", "count:id")).validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(aggregations = listOf("min:id", "min:id")).validate() }
    }

    @Test
    fun `表名和列名不能是空字符串`() {
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
    fun `读端数值参数必须为正`() {
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
    fun `多表读取拒绝无法保证全局语义的 limit`() {
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
    fun `写端 table 不能为空`() {
        val error = assertFailsWith<IllegalArgumentException> {
            JdbcWriteConfig(url = "jdbc:h2:mem:x").validate()
        }
        assertTrue("table" in error.message!!)
    }

    @Test
    fun `写端重试参数校验`() {
        assertFailsWith<IllegalArgumentException> {
            JdbcWriteConfig(url = "jdbc:h2:mem:x", table = "t", batchSize = 0).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            JdbcWriteConfig(url = "jdbc:h2:mem:x", table = "t", retryMaxAttempts = 0).validate()
        }
        // max 不能小于 initial，否则退避区间是空的
        assertFailsWith<IllegalArgumentException> {
            JdbcWriteConfig(url = "jdbc:h2:mem:x", table = "t", retryInitialSeconds = 60, retryMaxSeconds = 10).validate()
        }
    }

    // ---------- 连接属性 ----------

    @Test
    fun `dataSourceProperties 映射到 Hikari 认识的键`() {
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
    fun `空的用户名密码与驱动不会写进属性表`() {
        val properties = JdbcReadConfig(url = "jdbc:h2:mem:x").dataSourceProperties()

        assertEquals(setOf("jdbcUrl"), properties.keys.map { it.toString() }.toSet())
    }
}
