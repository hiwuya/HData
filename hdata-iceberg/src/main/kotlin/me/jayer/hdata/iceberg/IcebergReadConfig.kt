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
    /**
     * 过滤下推（类 SQL 的 WHERE）：`age >= 40 AND name = 'bob'`、`id IN (1, 2, 3)`、`age IS NOT NULL`。
     * 下推到 Iceberg 的 TableScan 做 manifest 级裁剪（分区/文件粒度直接砍掉不匹配的 split），
     * 读端再对每行用 `Evaluator` 求残留谓词，数据列与分区列都能正确过滤。
     */
    val filter: String = "",
    /**
     * 最多读多少行；`-1` 表示不限制。Iceberg 没有原生全局 LIMIT，且读是分文件并行的，
     * 所以限行数时退化为单 split（整表第一个数据文件），由读端截断到 limit 行
     * （与 JDBC/ES 的"限行数退化为单分区/单 slice"一致；多文件时只取第一个文件的头 limit 行）。
     */
    val limit: Long = -1,
    /**
     * 聚合下推：`["count", "min:age", "max:age", "sum:amount", "avg:amount"]`。COUNT 取自数据文件元数据
     * `recordCount`；MIN/MAX/SUM/AVG 投影对应列逐文件累加，再全局归并成一行。
     * 配置非空时输出聚合后的一行，[schemaFields] 的逐行 schema 不参与输出（本连接器 schema_fields 必填，
     * 聚合模式下仅作为占位）；与 [limit] 互斥——聚合是全局语义，limit 没有意义，同配直接报错。
     */
    val aggregations: List<String> = emptyList(),
) : IcebergConnectionConfig {

    fun validate() {
        require(warehouse.isNotBlank()) { "warehouse 不能为空" }
        require(catalogName.isNotBlank()) { "catalog_name 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(schemaFields.isNotEmpty()) { "schema_fields 不能为空" }
        require(splitSize > 0) { "split_size 必须 > 0" }
        require(limit == -1L || limit > 0) { "limit 必须 > 0（或不限制时留空/传 -1）" }
        if (filter.isNotBlank()) parseIcebergFilter(filter) // 解析失败在构图阶段就报错
        if (aggregations.isNotEmpty()) {
            require(limit == -1L) { "aggregations 模式不使用 limit，请从配置中移除" }
            parseAggregations(aggregations)
        }
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
