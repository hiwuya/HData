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
 * Binding and validation of [MongoWriteConfig].
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
    fun `config binds in snake_case`() {
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
    fun `the default is a plain insert`() {
        val config = cfg("""{"connection_uri": "mongodb://localhost:27017", "database": "d", "collection": "c"}""")

        assertEquals(1000, config.batchSize)
        assertFalse(config.upsert)
    }

    @Test
    fun `upsert_keys missing from schema_fields fails validation`() {
        val error = assertFailsWith<IllegalArgumentException> {
            minimal.copy(schemaFields = listOf("id:STRING"), upsertKeys = listOf("order_no")).validate()
        }

        assertTrue("order_no" in error.message!!)
    }

    @Test
    fun `an empty required field fails validation`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(connectionUri = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(database = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(collection = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(batchSize = 0).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(connectionUri = "not-a-uri").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(upsertKeys = listOf("", "id")).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(upsertKeys = listOf("id", "id")).validate() }
    }

    @Test
    fun `the sink produced by the provider can be serialized and shipped`() {
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
