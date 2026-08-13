package me.jayer.hdata.hbase

import java.io.Serializable

/**
 * `WriteToHBase` 的配置。
 *
 * ```yaml
 * - type: WriteToHBase
 *   config:
 *     zookeeper_quorum: "localhost:2181"
 *     table: "mytable"
 *     rowkey_field: "rowkey"      # 默认 rowkey
 *     family: "cf"                # 默认 cf
 *     schema_fields: ["name:STRING", "age:INT32"]
 *     batch_size: 1000            # 默认 1000
 * ```
 *
 * 输入行必须包含 [rowkeyField] 字段；其余按 [schemaFields] 写入 [family] 列族。
 */
data class HBaseWriteConfig(
    val zookeeperQuorum: String = "",
    val table: String = "",
    val rowkeyField: String = "rowkey",
    val family: String = "cf",
    val schemaFields: List<String>? = null,
    val batchSize: Int = 1000,
) : Serializable {

    fun validate() {
        require(zookeeperQuorum.isNotBlank()) { "zookeeper_quorum 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(rowkeyField.isNotBlank()) { "rowkey_field 不能为空" }
        require(family.isNotBlank()) { "family 不能为空" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
        parseSchemaFields(schemaFields)
    }
}
