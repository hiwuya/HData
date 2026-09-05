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
 * [HBaseReadConfig] 的绑定、校验，以及它构造出的 [org.apache.hadoop.hbase.client.Scan]。
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
    fun `配置按 snake_case 绑定`() {
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
    fun `默认值`() {
        val config = cfg("""{"zookeeper_quorum": "localhost:2181", "table": "mytable"}""")

        assertEquals("rowkey", config.rowkeyField)
        assertEquals("string", config.rowkeyFormat)
        assertEquals("cf", config.family)
        assertEquals(100, config.scanCaching)
        // 全表扫描默认不进块缓存，否则一次同步就能把在线业务的热点数据全挤出去
        assertFalse(config.scanCacheBlocks)
    }

    @Test
    fun `scan 只请求声明过的列`() {
        // 重构前是光秃秃的 Scan(startKey, stopKey)：把每行所有列族所有列都拉下来再丢掉
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
    fun `scan 带上起止 rowkey 与 caching 设置`() {
        val scan = minimal.copy(scanStartRow = "20220101", scanStopRow = "20220201", scanCaching = 500).scan()

        assertContentEqualsBytes("20220101", scan.startRow)
        assertContentEqualsBytes("20220201", scan.stopRow)
        assertEquals(500, scan.caching)
        assertFalse(scan.cacheBlocks)
    }

    @Test
    fun `不留起止 rowkey 时扫全表`() {
        val scan = minimal.scan()

        assertEquals(0, scan.startRow.size)
        assertEquals(0, scan.stopRow.size)
    }

    @Test
    fun `schema_fields 为空时报错，不允许退化成整表全列扫描`() {
        val error = assertFailsWith<IllegalArgumentException> { minimal.copy(schemaFields = null).validate() }

        assertTrue("schema_fields" in error.message!!)
    }

    @Test
    fun `必填项为空时报错`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(zookeeperQuorum = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(table = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(rowkeyField = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(scanCaching = 0).validate() }
    }

    @Test
    fun `起止 rowkey 反了时报错`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanStartRow = "b", scanStopRow = "a").validate()
        }
    }

    @Test
    fun `rowkey 边界按实际 UTF-8 字节顺序比较`() {
        // U+E000 在 UTF-16 字符串顺序中大于 U+10000，但 UTF-8 编码 EE... 小于 F0...。
        // HBase 比较的是后者，配置校验必须与实际 Scan 一致。
        minimal.copy(scanStartRow = "\uE000", scanStopRow = "\uD800\uDC00").validate()
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(scanStartRow = "\uD800\uDC00", scanStopRow = "\uE000").validate()
        }
    }

    @Test
    fun `rowkey_format 取值非法时报错`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(rowkeyFormat = "utf8").validate() }
    }

    @Test
    fun `读取字段不能重名或与 rowkey 撞名`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(schemaFields = listOf("cf:name:STRING", "ext:name:STRING")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(schemaFields = listOf("rowkey:STRING")).validate()
        }
    }

    @Test
    fun `zookeeper 配置落到 Configuration 上`() {
        val conf = minimal.copy(
            zookeeperZnodeParent = "/hbase-unsecure",
            properties = mapOf("hbase.rpc.timeout" to "60000"),
        ).configuration()

        assertEquals("localhost:2181", conf.get("hbase.zookeeper.quorum"))
        assertEquals("/hbase-unsecure", conf.get("zookeeper.znode.parent"))
        assertEquals("60000", conf.get("hbase.rpc.timeout"))
    }

    @Test
    fun `properties 不能覆盖显式 zookeeper 配置`() {
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
