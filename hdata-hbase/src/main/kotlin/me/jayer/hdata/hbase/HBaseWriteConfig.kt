package me.jayer.hdata.hbase

import org.apache.hadoop.conf.Configuration
import java.io.Serializable

/**
 * `WriteToHBase` 的配置。
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
 * [batchSize] 对应 Flink 的 `sink.buffer-flush.max-rows`，这里沿用 HData 其他连接器的命名。
 *
 * @author wuya
 */
data class HBaseWriteConfig(
    val zookeeperQuorum: String = "",
    val zookeeperZnodeParent: String = "",
    val table: String = "",
    val rowkeyField: String = "rowkey",
    /** `string`(默认) 或 `bytes`，需与输入行 rowkey 字段的类型一致。 */
    val rowkeyFormat: String = "string",
    val family: String = "cf",
    val schemaFields: List<String>? = null,
    /** 攒够这么多行提交一次；对应 Flink 的 `sink.buffer-flush.max-rows`。 */
    val batchSize: Int = 1000,
    /** 透传给 HBase 的属性，对应 Flink 的 `properties.*`。 */
    val properties: Map<String, String> = emptyMap(),
) : Serializable {

    fun validate() {
        require(zookeeperQuorum.isNotBlank()) { "zookeeper_quorum 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(rowkeyField.isNotBlank()) { "rowkey_field 不能为空" }
        require(family.isNotBlank()) { "family 不能为空" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
        val resolvedRowkeyFormat = RowkeyFormat.of(rowkeyFormat)
        val columns = columns()
        require(columns.isNotEmpty()) { "schema_fields 不能为空，否则每行只会写出一个空的 Put" }
        buildReadSchema(rowkeyField, resolvedRowkeyFormat, columns)
    }

    fun columns(): List<HBaseColumn> = parseColumns(schemaFields, family)

    fun configuration(): Configuration =
        HBaseConnections.newConfiguration(zookeeperQuorum, zookeeperZnodeParent, properties)

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
