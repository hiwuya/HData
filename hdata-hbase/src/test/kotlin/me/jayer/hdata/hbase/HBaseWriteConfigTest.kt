package me.jayer.hdata.hbase

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.util.SerializableUtils
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Binding and validation of [HBaseWriteConfig].
 *
 * @author wuya
 */
class HBaseWriteConfigTest {

    private val minimal = HBaseWriteConfig(
        zookeeperQuorum = "localhost:2181",
        table = "mytable",
        schemaFields = listOf("name:STRING"),
    )

    private fun cfg(json: String): HBaseWriteConfig =
        TransformConfig("test", SpecMappers.CONFIG.readTree(json) as ObjectNode).bind(HBaseWriteConfig::class.java)

    @Test
    fun `binds configuration with snake_case keys`() {
        val config = cfg(
            """
            {
              "zookeeper_quorum": "localhost:2181",
              "table": "mytable",
              "rowkey_field": "rk",
              "rowkey_format": "bytes",
              "family": "cf",
              "schema_fields": ["name:STRING", "age:INT32"],
              "batch_size": 500
            }
            """.trimIndent()
        )

        assertEquals("rk", config.rowkeyField)
        assertEquals("bytes", config.rowkeyFormat)
        assertEquals(500, config.batchSize)
        config.validate()
    }

    @Test
    fun `uses defaults`() {
        val config = cfg("""{"zookeeper_quorum": "localhost:2181", "table": "mytable"}""")

        assertEquals("rowkey", config.rowkeyField)
        assertEquals("cf", config.family)
        assertEquals(1000, config.batchSize)
    }

    @Test
    fun `provider sink is serializable`() {
        val transform = HBaseWriteProvider().from(
            TransformConfig(
                "WriteToHBase",
                SpecMappers.CONFIG.readTree(
                    """{"zookeeper_quorum": "localhost:2181", "table": "t", "schema_fields": ["name:STRING"]}"""
                ) as ObjectNode,
            )
        )

        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }

    @Test
    fun `rejects empty schema_fields instead of writing empty puts`() {
        val error = assertFailsWith<IllegalArgumentException> { minimal.copy(schemaFields = null).validate() }

        assertTrue("schema_fields" in error.message!!)
    }

    @Test
    fun `rejects blank required fields`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(zookeeperQuorum = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(table = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(rowkeyField = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(batchSize = 0).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(rowkeyFormat = "utf8").validate() }
    }

    @Test
    fun `rejects duplicate fields and rowkey collisions`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(schemaFields = listOf("cf:name:STRING", "ext:name:STRING")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(schemaFields = listOf("rowkey:STRING")).validate()
        }
    }

    @Test
    fun `properties cannot override explicit zookeeper settings`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(properties = mapOf("hbase.zookeeper.quorum" to "other:2181")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(properties = mapOf("zookeeper.znode.parent" to "/other")).validate()
        }
    }
}
