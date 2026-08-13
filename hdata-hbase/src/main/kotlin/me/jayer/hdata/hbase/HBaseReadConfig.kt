package me.jayer.hdata.hbase

import java.io.Serializable

/**
 * `ReadFromHBase` 的配置。配置键对齐 Flink HBase connector：
 *
 * ```yaml
 * - type: ReadFromHBase
 *   config:
 *     zookeeper_quorum: "localhost:2181"
 *     table: "mytable"
 *     rowkey_field: "rowkey"      # 默认 rowkey
 *     family: "cf"                # 默认 cf
 *     schema_fields: ["name:STRING", "age:INT32"]
 *     scan_caching: 100
 * ```
 *
 * 读出行的 schema：固定第一列 `rowkey`(STRING) + [schemaFields] 里声明的列（缺省类型 STRING）。
 */
data class HBaseReadConfig(
    val zookeeperQuorum: String = "",
    val table: String = "",
    val rowkeyField: String = "rowkey",
    val family: String = "cf",
    val schemaFields: List<String>? = null,
    val scanCaching: Int = 100,
) : Serializable {

    fun validate() {
        require(zookeeperQuorum.isNotBlank()) { "zookeeper_quorum 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(rowkeyField.isNotBlank()) { "rowkey_field 不能为空" }
        require(family.isNotBlank()) { "family 不能为空" }
        require(scanCaching > 0) { "scan_caching 必须 > 0" }
        parseSchemaFields(schemaFields)
    }
}
