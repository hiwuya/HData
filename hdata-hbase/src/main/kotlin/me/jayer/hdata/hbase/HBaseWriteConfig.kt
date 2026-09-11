package me.jayer.hdata.hbase

import org.apache.hadoop.conf.Configuration
import java.io.Serializable

/**
 * Configuration for `WriteToHBase`.
 *
 * ```yaml
 * - type: WriteToHBase
 *   config:
 *     zookeeper_quorum: "localhost:2181"
 *     table: "mytable"
 *     family: "cf"
 *     schema_fields: ["name:STRING", "age:INT32"]
 *     batch_size: 1000
 * ```
 *
 * [batchSize] corresponds to Flink's `sink.buffer-flush.max-rows`.
 *
 * @author wuya
 */
data class HBaseWriteConfig(
    val zookeeperQuorum: String = "",
    val zookeeperZnodeParent: String = "",
    val table: String = "",
    val rowkeyField: String = "rowkey",
    /** `string` (default) or `bytes`, matching the input row key field type. */
    val rowkeyFormat: String = "string",
    val family: String = "cf",
    val schemaFields: List<String>? = null,
    /** Commit after this many rows, corresponding to Flink's `sink.buffer-flush.max-rows`. */
    val batchSize: Int = 1000,
    /** HBase properties passed through from Flink-style `properties.*`. */
    val properties: Map<String, String> = emptyMap(),
) : Serializable {

    fun validate() {
        require(zookeeperQuorum.isNotBlank()) { "zookeeper_quorum must not be blank" }
        require(table.isNotBlank()) { "table must not be blank" }
        require(rowkeyField.isNotBlank()) { "rowkey_field must not be blank" }
        require(family.isNotBlank()) { "family must not be blank" }
        require(batchSize > 0) { "batch_size must be > 0" }
        validateConnectionProperties(properties)
        val resolvedRowkeyFormat = RowkeyFormat.of(rowkeyFormat)
        val columns = columns()
        require(columns.isNotEmpty()) { "schema_fields must not be empty; otherwise every row would create an empty Put" }
        buildReadSchema(rowkeyField, resolvedRowkeyFormat, columns)
    }

    fun columns(): List<HBaseColumn> = parseColumns(schemaFields, family)

    fun configuration(): Configuration =
        HBaseConnections.newConfiguration(zookeeperQuorum, zookeeperZnodeParent, properties)

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
