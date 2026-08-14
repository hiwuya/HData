package me.jayer.hdata.iceberg

import java.io.Serializable

/**
 * `WriteToIceberg` 的配置。
 *
 * 写入时按 `schema_fields` 声明目标表结构，表不存在则自动创建（无分区）；
 * 每个 bundle 累积的行落成一个数据文件再 `append`（或 overwrite）。
 *
 * @author wuya
 */
data class IcebergWriteConfig(
    override val warehouse: String,
    override val catalogName: String = "hdata",
    override val table: String,
    val schemaFields: List<String>,
    val writeMode: String = "append",
) : IcebergConnectionConfig {

    fun validate() {
        require(warehouse.isNotBlank()) { "warehouse 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(schemaFields.isNotEmpty()) { "schema_fields 不能为空" }
        require(writeMode in setOf("append", "overwrite")) { "write_mode 只能是 append / overwrite" }
    }

    fun outputSchema() = me.jayer.hdata.iceberg.internal.schemaOf(schemaFields)

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
