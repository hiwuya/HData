package me.jayer.hdata.hbase

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.hadoop.hbase.util.Bytes
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

/**
 * Binding and validation of [HBaseReadConfig], including its [org.apache.hadoop.hbase.client.Scan] output.
 *
 * @author wuya
 */
class HBaseReadConfigTest {

    private val minimal = HBaseReadConfig(
        zookeeperQuorum = "localhost:2181",
        table = "mytable",
        schemaFields = listOf("name:STRING", "age:INT32"),
    )

    private fun cfg(json: String): HBaseReadConfig =
        TransformConfig("test", SpecMappers.CONFIG.readTree(json) as ObjectNode).bind(HBaseReadConfig::class.java)

    @Test
    fun `binds configuration with snake_case keys`() {
        val config = cfg(
            """
            {
              "zookeeper_quorum": "localhost:2181",
              "zookeeper_znode_parent": "/hbase-unsecure",
              "table": "mytable",
              "rowkey_field": "rk",
              "rowkey_format": "bytes",
              "family": "cf",
              "schema_fields": ["name:STRING", "age:INT32"],
              "scan_start_row": "a",
              "scan_stop_row": "b",
              "scan_caching": 200,
              "properties": {"hbase.rpc.timeout": "60000"}
            }
            """.trimIndent()
        )

        assertEquals("/hbase-unsecure", config.zookeeperZnodeParent)
        assertEquals("bytes", config.rowkeyFormat)
        assertEquals("a", config.scanStartRow)
        assertEquals(200, config.scanCaching)
        assertEquals(mapOf("hbase.rpc.timeout" to "60000"), config.properties)
        config.validate()
    }

    @Test
    fun `uses defaults`() {
        val config = cfg("""{"zookeeper_quorum": "localhost:2181", "table": "mytable"}""")

        assertEquals("rowkey", config.rowkeyField)
        assertEquals("string", config.rowkeyFormat)
        assertEquals("cf", config.family)
        assertEquals(100, config.scanCaching)
        // Full-table scans bypass the block cache so one sync cannot evict online workload data.
        assertFalse(config.scanCacheBlocks)
    }

    @Test
    fun `scan requests only declared columns`() {
        // The scan must project families and qualifiers instead of fetching every cell and discarding it.
        val scan = minimal.copy(schemaFields = listOf("name:STRING", "ext:tag:STRING")).scan()

        assertTrue(scan.hasFamilies())
        assertEquals(
            setOf("cf", "ext"),
            scan.familyMap.keys.map { Bytes.toString(it) }.toSet(),
        )
        assertEquals(setOf("name"), scan.familyMap[Bytes.toBytes("cf")]!!.map { Bytes.toString(it) }.toSet())
        assertEquals(setOf("tag"), scan.familyMap[Bytes.toBytes("ext")]!!.map { Bytes.toString(it) }.toSet())
    }

    @Test
    fun `scan includes rowkey bounds and caching settings`() {
        val scan = minimal.copy(scanStartRow = "20220101", scanStopRow = "20220201", scanCaching = 500).scan()

        assertContentEqualsBytes("20220101", scan.startRow)
        assertContentEqualsBytes("20220201", scan.stopRow)
        assertEquals(500, scan.caching)
        assertFalse(scan.cacheBlocks)
    }

    @Test
    fun `empty rowkey bounds scan the whole table`() {
        val scan = minimal.scan()

        assertEquals(0, scan.startRow.size)
        assertEquals(0, scan.stopRow.size)
    }

    @Test
    fun `rejects empty schema_fields instead of scanning every column`() {
        val error = assertFailsWith<IllegalArgumentException> { minimal.copy(schemaFields = null).validate() }

        assertTrue("schema_fields" in error.message!!)
    }

    @Test
    fun `rejects blank required fields`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(zookeeperQuorum = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(table = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(rowkeyField = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(scanCaching = 0).validate() }
    }

    @Test
    fun `rejects reversed rowkey bounds`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanStartRow = "b", scanStopRow = "a").validate()
        }
    }

    @Test
    fun `compares rowkey bounds in UTF-8 byte order`() {
        // U+E000 sorts after U+10000 as UTF-16 but its UTF-8 bytes sort before F0...; HBase compares the latter.
        minimal.copy(scanStartRow = "\uE000", scanStopRow = "\uD800\uDC00").validate()
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanStartRow = "\uD800\uDC00", scanStopRow = "\uE000").validate()
        }
    }

    @Test
    fun `rejects an invalid rowkey_format`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(rowkeyFormat = "utf8").validate() }
    }

    @Test
    fun `rejects duplicate columns and rowkey collisions`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(schemaFields = listOf("cf:name:STRING", "ext:name:STRING")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(schemaFields = listOf("rowkey:STRING")).validate()
        }
    }

    @Test
    fun `writes zookeeper settings to Configuration`() {
        val conf = minimal.copy(
            zookeeperZnodeParent = "/hbase-unsecure",
            properties = mapOf("hbase.rpc.timeout" to "60000"),
        ).configuration()

        assertEquals("localhost:2181", conf.get("hbase.zookeeper.quorum"))
        assertEquals("/hbase-unsecure", conf.get("zookeeper.znode.parent"))
        assertEquals("60000", conf.get("hbase.rpc.timeout"))
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

    private fun assertContentEqualsBytes(expected: String, actual: ByteArray) =
        assertEquals(expected, Bytes.toString(actual))
}
