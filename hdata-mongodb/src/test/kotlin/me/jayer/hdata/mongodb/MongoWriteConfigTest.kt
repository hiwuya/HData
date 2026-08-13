package me.jayer.hdata.mongodb

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

class MongoWriteConfigTest {

    private fun config(json: String): TransformConfig =
        TransformConfig("test", SpecMappers.CONFIG.readTree(json) as ObjectNode)

    @Test
    fun `写端配置按 snake_case 绑定`() {
        val node = config(
            """
            {
              "connection_uri": "mongodb://localhost:27017",
              "database": "mydb",
              "collection": "orders",
              "schema_fields": ["id:STRING", "amount:DOUBLE"],
              "batch_size": 500
            }
            """.trimIndent()
        )

        val cfg = node.bind(MongoWriteConfig::class.java)
        assertEquals("mongodb://localhost:27017", cfg.connectionUri)
        assertEquals("mydb", cfg.database)
        assertEquals("orders", cfg.collection)
        assertEquals(listOf("id:STRING", "amount:DOUBLE"), cfg.schemaFields)
        assertEquals(500, cfg.batchSize)
    }

    @Test
    fun `写端默认值`() {
        val node = config(
            """
            {
              "connection_uri": "mongodb://localhost:27017",
              "database": "mydb",
              "collection": "orders"
            }
            """.trimIndent()
        )

        val cfg = node.bind(MongoWriteConfig::class.java)
        assertEquals(emptyList<String>(), cfg.schemaFields)
        assertEquals(1000, cfg.batchSize)
    }

    @Test
    fun `from 返回非空 transform 且触发 validate`() {
        val node = config(
            """
            {
              "connection_uri": "mongodb://localhost:27017",
              "database": "mydb",
              "collection": "orders"
            }
            """.trimIndent()
        )

        val transform = MongoWriteProvider().from(node)
        assertNotNull(transform)
    }

    @Test
    fun `写端空连接信息报错`() {
        assertThrows(IllegalArgumentException::class.java) {
            MongoWriteConfig(database = "mydb", collection = "orders").validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            MongoWriteConfig(connectionUri = "mongodb://localhost:27017", collection = "orders").validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            MongoWriteConfig(connectionUri = "mongodb://localhost:27017", database = "mydb").validate()
        }
    }

    @Test
    fun `写端 batch_size 必须为正`() {
        assertThrows(IllegalArgumentException::class.java) {
            MongoWriteConfig(
                connectionUri = "mongodb://localhost:27017",
                database = "mydb",
                collection = "orders",
                batchSize = 0,
            ).validate()
        }
    }

    @Test
    fun `写端合法配置不抛异常`() {
        MongoWriteConfig(
            connectionUri = "mongodb://localhost:27017",
            database = "mydb",
            collection = "orders",
            batchSize = 1,
        ).validate()
    }
}
