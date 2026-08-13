package me.jayer.hdata.hbase

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

class HBaseReadConfigTest {

    private fun cfg(json: String): HBaseReadConfig {
        val node = SpecMappers.CONFIG.readTree(json) as ObjectNode
        return TransformConfig("test", node).bind(HBaseReadConfig::class.java)
    }

    @Test
    fun `读端配置按 snake_case 绑定`() {
        val config = cfg(
            """
            {
              "zookeeper_quorum": "localhost:2181",
              "table": "mytable",
              "rowkey_field": "rk",
              "family": "cf",
              "schema_fields": ["name:STRING", "age:INT32"],
              "scan_caching": 200
            }
            """.trimIndent()
        )

        assertEquals("localhost:2181", config.zookeeperQuorum)
        assertEquals("mytable", config.table)
        assertEquals("rk", config.rowkeyField)
        assertEquals("cf", config.family)
        assertEquals(listOf("name:STRING", "age:INT32"), config.schemaFields)
        assertEquals(200, config.scanCaching)
    }

    @Test
    fun `读端默认值`() {
        val config = cfg(
            """
            {
              "zookeeper_quorum": "localhost:2181",
              "table": "mytable"
            }
            """.trimIndent()
        )

        assertEquals("rowkey", config.rowkeyField)
        assertEquals("cf", config.family)
        assertEquals(100, config.scanCaching)
        assertTrue(config.schemaFields == null)
    }

    @Test
    fun `provider from 返回非空 transform`() {
        val node = SpecMappers.CONFIG.readTree(
            """
            {
              "zookeeper_quorum": "localhost:2181",
              "table": "mytable",
              "schema_fields": ["name:STRING"]
            }
            """.trimIndent()
        ) as ObjectNode
        val transform = HBaseReadProvider().from(TransformConfig("test", node))
        assertNotNull(transform)
    }

    @Test
    fun `读端 zookeeper_quorum 为空报错`() {
        val ex = assertThrows(IllegalArgumentException::class.java) {
            HBaseReadConfig(table = "t").validate()
        }
        assertTrue("zookeeper_quorum" in ex.message!!)
    }

    @Test
    fun `读端 table 为空报错`() {
        val ex = assertThrows(IllegalArgumentException::class.java) {
            HBaseReadConfig(zookeeperQuorum = "q").validate()
        }
        assertTrue("table" in ex.message!!)
    }

    @Test
    fun `读端 scan_caching 必须为正`() {
        assertThrows(IllegalArgumentException::class.java) {
            HBaseReadConfig(zookeeperQuorum = "q", table = "t", scanCaching = 0).validate()
        }
    }

    @Test
    fun `读端非法 schema_fields 类型报错`() {
        assertThrows(IllegalArgumentException::class.java) {
            HBaseReadConfig(
                zookeeperQuorum = "q",
                table = "t",
                schemaFields = listOf("name:BADTYPE"),
            ).validate()
        }
    }

    @Test
    fun `读端合法配置 validate 不抛异常`() {
        HBaseReadConfig(
            zookeeperQuorum = "q",
            table = "t",
            schemaFields = listOf("name:STRING", "age:INT32"),
        ).validate()
    }
}
