package me.jayer.hdata.mongodb

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.util.SerializableUtils
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * [MongoWriteConfig] 的绑定与校验。
 *
 * @author wuya
 */
class MongoWriteConfigTest {

    private val minimal = MongoWriteConfig(
        connectionUri = "mongodb://localhost:27017",
        database = "mydb",
        collection = "orders",
    )

    private fun cfg(json: String): MongoWriteConfig =
        TransformConfig("test", SpecMappers.CONFIG.readTree(json) as ObjectNode).bind(MongoWriteConfig::class.java)

    @Test
    fun `配置按 snake_case 绑定`() {
        val config = cfg(
            """
            {
              "connection_uri": "mongodb://localhost:27017",
              "database": "mydb",
              "collection": "orders",
              "schema_fields": ["id:STRING", "amount:DOUBLE"],
              "upsert_keys": ["id"],
              "batch_size": 500
            }
            """.trimIndent()
        )

        assertEquals(listOf("id"), config.upsertKeys)
        assertTrue(config.upsert)
        assertEquals(500, config.batchSize)
        config.validate()
    }

    @Test
    fun `默认是纯插入`() {
        val config = cfg("""{"connection_uri": "mongodb://localhost:27017", "database": "d", "collection": "c"}""")

        assertEquals(1000, config.batchSize)
        assertFalse(config.upsert)
    }

    @Test
    fun `upsert_keys 不在 schema_fields 里时报错`() {
        val error = assertFailsWith<IllegalArgumentException> {
            minimal.copy(schemaFields = listOf("id:STRING"), upsertKeys = listOf("order_no")).validate()
        }

        assertTrue("order_no" in error.message!!)
    }

    @Test
    fun `必填项为空时报错`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(connectionUri = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(database = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(collection = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(batchSize = 0).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(connectionUri = "not-a-uri").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(upsertKeys = listOf("", "id")).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(upsertKeys = listOf("id", "id")).validate() }
    }

    @Test
    fun `provider 生成的 sink 可以序列化下发`() {
        val transform = MongoWriteProvider().from(
            TransformConfig(
                "WriteToMongoDb",
                SpecMappers.CONFIG.readTree(
                    """{"connection_uri": "mongodb://localhost:27017", "database": "d", "collection": "c"}"""
                ) as ObjectNode,
            )
        )

        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }
}
