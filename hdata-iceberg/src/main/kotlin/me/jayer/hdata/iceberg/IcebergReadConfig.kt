package me.jayer.hdata.iceberg

import me.jayer.hdata.iceberg.internal.parseSchemaFields
import org.apache.beam.sdk.schemas.Schema
import java.io.Serializable

/**
 * Iceberg 连接信息（共享）：warehouse 目录 + catalog 名 + 表名。
 *
 * @author wuya
 */
interface IcebergConnectionConfig : Serializable {
    val warehouse: String
    val catalogName: String
    val table: String
}

/**
 * `ReadFromIceberg` 的配置。
 *
 * 读端**不连库即可构图**：输出 schema 由 `schema_fields`（`name:TYPE`）声明；
 * 运行时从 `warehouse` 下的 HadoopCatalog 加载表，用 IcebergGenerics 扫描并逐行映射。
 *
 * @author wuya
 */
data class IcebergReadConfig(
    override val warehouse: String,
    override val catalogName: String = "hdata",
    override val table: String,
    val schemaFields: List<String>,
) : IcebergConnectionConfig {

    fun validate() {
        require(warehouse.isNotBlank()) { "warehouse 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(schemaFields.isNotEmpty()) { "schema_fields 不能为空" }
        parseSchemaFields(schemaFields)
    }

    fun outputSchema(): Schema = Schema.builder().apply {
        parseSchemaFields(schemaFields).forEach { (name, type) -> addNullableField(name, type) }
    }.build()

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
