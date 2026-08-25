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
 * [MongoReadConfig] 的绑定与校验。
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
    fun `配置按 snake_case 绑定`() {
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
    fun `默认值`() {
        val config = cfg("""{"connection_uri": "mongodb://localhost:27017", "database": "d", "collection": "c"}""")

        assertEquals(1000, config.fetchSize)
        // 不指定就按文档数自动估算分片数
        assertNull(config.partitionNum)
        assertEquals("", config.filter)
    }

    @Test
    fun `必填项为空时报错`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(connectionUri = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(database = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(collection = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(fetchSize = 0).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(partitionNum = 0).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(partitionNum = 1001).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(connectionUri = "not-a-uri").validate() }
    }

    @Test
    fun `filter 不是合法 JSON 时在构图阶段就报错`() {
        // 留到运行期才炸的话，作业已经排队跑起来了才发现配置写错
        val error = assertFailsWith<IllegalArgumentException> {
            minimal.copy(filter = "{status: PAID").validate()
        }

        assertTrue("filter" in error.message!!)
    }

    @Test
    fun `schema_fields 类型不认识时报错`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(schemaFields = listOf("id:UUID")).validate() }
    }

    @Test
    fun `limit 合法取值通过校验`() {
        minimal.copy(limit = -1).validate()
        minimal.copy(limit = 1).validate()
        minimal.copy(limit = 1000).validate()
    }

    @Test
    fun `limit 非法取值报错`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(limit = 0).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(limit = -2).validate() }
    }
}
