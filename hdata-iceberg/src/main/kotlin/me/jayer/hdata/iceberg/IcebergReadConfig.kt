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
    /**
     * 单个数据文件内部进一步切分的目标大小（字节）。文件大于它时切成多个 split 并行读，
     * 并行度来自 row-group / 同步块粒度；默认 128MB，与 Iceberg 的默认 split size 对齐。
     */
    val splitSize: Long = 128 * 1024 * 1024,
) : IcebergConnectionConfig {

    fun validate() {
        require(warehouse.isNotBlank()) { "warehouse 不能为空" }
        require(catalogName.isNotBlank()) { "catalog_name 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(schemaFields.isNotEmpty()) { "schema_fields 不能为空" }
        require(splitSize > 0) { "split_size 必须 > 0" }
        val fields = parseSchemaFields(schemaFields)
        require(fields.map { it.first }.distinct().size == fields.size) { "schema_fields 字段名不能重复" }
    }

    fun outputSchema(): Schema = Schema.builder().apply {
        parseSchemaFields(schemaFields).forEach { (name, type) -> addNullableField(name, type) }
    }.build()

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
