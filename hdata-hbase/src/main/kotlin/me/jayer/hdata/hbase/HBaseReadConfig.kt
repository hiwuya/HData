package me.jayer.hdata.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import java.io.Serializable

/**
 * Configuration for `ReadFromHBase`, with keys aligned to the Flink HBase connector.
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
 * Output schema: `rowkey_field` first, followed by [schemaFields].
 *
 * @author wuya
 */
data class HBaseReadConfig(
    val zookeeperQuorum: String = "",
    val zookeeperZnodeParent: String = "",
    val table: String = "",
    val rowkeyField: String = "rowkey",
    /** `string` (default) or `bytes`; binary row keys must use `bytes` to avoid UTF-8 corruption. */
    val rowkeyFormat: String = "string",
    /** The default family for [schemaFields] entries without an explicit family. */
    val family: String = "cf",
    val schemaFields: List<String>? = null,
    /** Inclusive start row key; empty starts at the beginning. */
    val scanStartRow: String = "",
    /** Exclusive stop row key; empty scans to the end. */
    val scanStopRow: String = "",
    val scanCaching: Int = 100,
    /**
     * Whether scanned blocks enter the RegionServer block cache. Disabled by default for full scans.
     */
    val scanCacheBlocks: Boolean = false,
    /** HBase properties passed through from Flink-style `properties.*`. */
    val properties: Map<String, String> = emptyMap(),
) : Serializable {

    fun validate() {
        require(zookeeperQuorum.isNotBlank()) { "zookeeper_quorum must not be blank" }
        require(table.isNotBlank()) { "table must not be blank" }
        require(rowkeyField.isNotBlank()) { "rowkey_field must not be blank" }
        require(family.isNotBlank()) { "family must not be blank" }
        require(scanCaching > 0) { "scan_caching must be > 0" }
        validateConnectionProperties(properties)
        val resolvedRowkeyFormat = RowkeyFormat.of(rowkeyFormat)
        val columns = columns()
        require(columns.isNotEmpty()) {
            "schema_fields must not be empty; otherwise the scan would fetch every column family"
        }
        buildReadSchema(rowkeyField, resolvedRowkeyFormat, columns)
        if (scanStartRow.isNotBlank() && scanStopRow.isNotBlank()) {
            require(Bytes.compareTo(Bytes.toBytes(scanStartRow), Bytes.toBytes(scanStopRow)) < 0) {
                "scan_start_row must precede scan_stop_row in HBase UTF-8 byte order"
            }
        }
    }

    fun columns(): List<HBaseColumn> = parseColumns(schemaFields, family)

    fun configuration(): Configuration =
        HBaseConnections.newConfiguration(zookeeperQuorum, zookeeperZnodeParent, properties)

    /**
     * Requests only declared columns.
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

/** Explicit connection fields have a single source of truth. */
internal fun validateConnectionProperties(properties: Map<String, String>) {
    require(properties.keys.none { it.isBlank() }) { "properties must not contain a blank key" }
    require("hbase.zookeeper.quorum" !in properties) {
        "properties.hbase.zookeeper.quorum duplicates zookeeper_quorum; use zookeeper_quorum only"
    }
    require("zookeeper.znode.parent" !in properties) {
        "properties.zookeeper.znode.parent duplicates zookeeper_znode_parent; use zookeeper_znode_parent only"
    }
}
