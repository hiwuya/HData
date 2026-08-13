package me.jayer.hdata.hbase

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Assertions.assertTrue
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

class HBaseWriteConfigTest {

    private fun cfg(json: String): HBaseWriteConfig {
        val node = SpecMappers.CONFIG.readTree(json) as ObjectNode
        return TransformConfig("test", node).bind(HBaseWriteConfig::class.java)
    }

    @Test
    fun `写端配置按 snake_case 绑定`() {
        val config = cfg(
            """
            {
              "zookeeper_quorum": "localhost:2181",
              "table": "mytable",
              "rowkey_field": "rk",
              "family": "cf",
              "schema_fields": ["name:STRING", "age:INT32"],
              "batch_size": 500
            }
            """.trimIndent()
        )

        assertEquals("localhost:2181", config.zookeeperQuorum)
        assertEquals("mytable", config.table)
        assertEquals("rk", config.rowkeyField)
        assertEquals("cf", config.family)
        assertEquals(listOf("name:STRING", "age:INT32"), config.schemaFields)
        assertEquals(500, config.batchSize)
    }

    @Test
    fun `写端默认值`() {
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
        assertEquals(1000, config.batchSize)
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
        val transform = HBaseWriteProvider().from(TransformConfig("test", node))
        assertNotNull(transform)
    }

    @Test
    fun `写端 zookeeper_quorum 为空报错`() {
        val ex = assertThrows(IllegalArgumentException::class.java) {
            HBaseWriteConfig(table = "t").validate()
        }
        assertTrue("zookeeper_quorum" in ex.message!!)
    }

    @Test
    fun `写端 table 为空报错`() {
        val ex = assertThrows(IllegalArgumentException::class.java) {
            HBaseWriteConfig(zookeeperQuorum = "q").validate()
        }
        assertTrue("table" in ex.message!!)
    }

    @Test
    fun `写端 batch_size 必须为正`() {
        assertThrows(IllegalArgumentException::class.java) {
            HBaseWriteConfig(zookeeperQuorum = "q", table = "t", batchSize = 0).validate()
        }
    }

    @Test
    fun `写端非法 schema_fields 类型报错`() {
        assertThrows(IllegalArgumentException::class.java) {
            HBaseWriteConfig(
                zookeeperQuorum = "q",
                table = "t",
                schemaFields = listOf("name:BADTYPE"),
            ).validate()
        }
    }

    @Test
    fun `写端合法配置 validate 不抛异常`() {
        HBaseWriteConfig(
            zookeeperQuorum = "q",
            table = "t",
            schemaFields = listOf("name:STRING", "age:INT32"),
        ).validate()
    }
}
