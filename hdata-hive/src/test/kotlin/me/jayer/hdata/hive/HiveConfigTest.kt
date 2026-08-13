package me.jayer.hdata.hive

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * [HiveReadConfig] / [HiveWriteConfig] 的绑定与校验。
 *
 * @author wuya
 */
class HiveConfigTest {

    private inline fun <reified T : Any> bind(json: String): T =
        TransformConfig("test", SpecMappers.CONFIG.readTree(json) as ObjectNode).bind(T::class.java)

    private val read = HiveReadConfig(url = "jdbc:hive2://localhost:10000", table = "t")

    private val write = HiveWriteConfig(url = "jdbc:hive2://localhost:10000", table = "t")

    @Test
    fun `读端配置按 snake_case 绑定`() {
        val config = bind<HiveReadConfig>(
            """
            {
              "url": "jdbc:hive2://localhost:10000/default",
              "user": "hive",
              "database": "default",
              "table": "orders",
              "partitions": ["dt='2024-01-01'"],
              "where": "id > 0",
              "columns": ["id", "name"],
              "fetch_size": 500
            }
            """.trimIndent()
        )

        assertEquals(listOf("dt='2024-01-01'"), config.partitions)
        assertEquals("id > 0", config.where)
        assertEquals(listOf("id", "name"), config.columns)
        assertEquals(500, config.fetchSize)
        config.validate()
    }

    @Test
    fun `读端默认值`() {
        val config = bind<HiveReadConfig>("""{"url": "jdbc:hive2://h", "table": "t"}""")

        assertEquals(1000, config.fetchSize)
        assertEquals(emptyList(), config.partitions)
        assertEquals(emptyList(), config.columns)
    }

    @Test
    fun `表名带上库名`() {
        assertEquals("t", read.qualifiedTable)
        assertEquals("db.t", read.copy(database = "db").qualifiedTable)
        assertEquals("db.t", write.copy(database = "db").qualifiedTable)
    }

    @Test
    fun `连接属性喂给 Hikari`() {
        val props = read.copy(user = "u", password = "p").dataSourceProperties()

        assertEquals("jdbc:hive2://localhost:10000", props["jdbcUrl"])
        assertEquals("u", props["username"])
        assertEquals("p", props["password"])
    }

    @Test
    fun `没配用户名时不往属性里塞空值`() {
        // Hikari 遇到空字符串的 username 会当成真的用户名去认证
        val props = read.dataSourceProperties()

        assertEquals(null, props["username"])
    }

    @Test
    fun `必填项为空时报错`() {
        assertFailsWith<IllegalArgumentException> { read.copy(url = "").validate() }
        assertFailsWith<IllegalArgumentException> { read.copy(table = "").validate() }
        assertFailsWith<IllegalArgumentException> { read.copy(fetchSize = 0).validate() }
        assertFailsWith<IllegalArgumentException> { write.copy(url = "").validate() }
        assertFailsWith<IllegalArgumentException> { write.copy(table = "").validate() }
        assertFailsWith<IllegalArgumentException> { write.copy(batchSize = 0).validate() }
    }

    @Test
    fun `写端默认值`() {
        val config = bind<HiveWriteConfig>("""{"url": "jdbc:hive2://h", "table": "t"}""")

        assertEquals(1000, config.batchSize)
    }
}
