package me.jayer.hdata.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import java.io.Serializable

/**
 * `ReadFromHBase` 的配置，键名对齐 Flink HBase connector（`zookeeper.quorum` /
 * `zookeeper.znode.parent` / `properties.*`）。
 *
 * ```yaml
 * - type: ReadFromHBase
 *   config:
 *     zookeeper_quorum: "localhost:2181"
 *     table: "mytable"
 *     family: "cf"
 *     schema_fields: ["name:STRING", "age:INT32", "ext:tag:STRING"]
 *     scan_start_row: "20220101"
 *     scan_stop_row: "20220201"
 * ```
 *
 * 读出行的 schema：`rowkey_field` 在最前，之后按 [schemaFields] 顺序。
 *
 * @author wuya
 */
data class HBaseReadConfig(
    val zookeeperQuorum: String = "",
    val zookeeperZnodeParent: String = "",
    val table: String = "",
    val rowkeyField: String = "rowkey",
    /** `string`(默认) 或 `bytes`。二进制 rowkey 必须用 `bytes`，否则会被 UTF-8 解码破坏。 */
    val rowkeyFormat: String = "string",
    /** [schemaFields] 里没写列族的条目默认落在这个列族。 */
    val family: String = "cf",
    val schemaFields: List<String>? = null,
    /** 起始 rowkey（含），留空表示从头扫。 */
    val scanStartRow: String = "",
    /** 结束 rowkey（不含），留空表示扫到尾。 */
    val scanStopRow: String = "",
    val scanCaching: Int = 100,
    /**
     * 是否让扫到的块进入 RegionServer 的块缓存。全表扫描默认关掉，
     * 否则一次同步就能把在线业务的热点数据全部挤出缓存。
     */
    val scanCacheBlocks: Boolean = false,
    /** 透传给 HBase 的属性，对应 Flink 的 `properties.*`。 */
    val properties: Map<String, String> = emptyMap(),
) : Serializable {

    fun validate() {
        require(zookeeperQuorum.isNotBlank()) { "zookeeper_quorum 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(rowkeyField.isNotBlank()) { "rowkey_field 不能为空" }
        require(family.isNotBlank()) { "family 不能为空" }
        require(scanCaching > 0) { "scan_caching 必须 > 0" }
        validateConnectionProperties(properties)
        val resolvedRowkeyFormat = RowkeyFormat.of(rowkeyFormat)
        val columns = columns()
        require(columns.isNotEmpty()) {
            "schema_fields 不能为空：不声明要读哪些列，扫描会把所有列族整表拉下来"
        }
        buildReadSchema(rowkeyField, resolvedRowkeyFormat, columns)
        if (scanStartRow.isNotBlank() && scanStopRow.isNotBlank()) {
            require(Bytes.compareTo(Bytes.toBytes(scanStartRow), Bytes.toBytes(scanStopRow)) < 0) {
                "scan_start_row 按 HBase UTF-8 字节顺序必须小于 scan_stop_row"
            }
        }
    }

    fun columns(): List<HBaseColumn> = parseColumns(schemaFields, family)

    fun configuration(): Configuration =
        HBaseConnections.newConfiguration(zookeeperQuorum, zookeeperZnodeParent, properties)

    /**
     * 只请求声明过的列。
     *
     * 重构前这里是一个光秃秃的 `Scan(startKey, stopKey)`：不加 `addColumn` 就等于
     * **把每一行的所有列族所有列都拉过来**，再在客户端把用不上的丢掉。宽表上这是数量级的浪费。
     */
    fun scan(): Scan = Scan().apply {
        columns().forEach { addColumn(it.familyBytes, it.qualifierBytes) }
        if (scanStartRow.isNotBlank()) {
            withStartRow(Bytes.toBytes(scanStartRow))
        }
        if (scanStopRow.isNotBlank()) {
            withStopRow(Bytes.toBytes(scanStopRow))
        }
        caching = scanCaching
        cacheBlocks = scanCacheBlocks
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/** 显式连接字段拥有唯一来源，避免 properties 中的重复键制造与配置表面不一致的行为。 */
internal fun validateConnectionProperties(properties: Map<String, String>) {
    require(properties.keys.none { it.isBlank() }) { "properties 不能包含空键" }
    require("hbase.zookeeper.quorum" !in properties) {
        "properties.hbase.zookeeper.quorum 与 zookeeper_quorum 重复，请只使用 zookeeper_quorum"
    }
    require("zookeeper.znode.parent" !in properties) {
        "properties.zookeeper.znode.parent 与 zookeeper_znode_parent 重复，请只使用 zookeeper_znode_parent"
    }
}
