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
    /** 普通逐行读取的输出 schema；聚合模式直接从表元数据推导，必须留空。 */
    val schemaFields: List<String> = emptyList(),
    /**
     * 单个数据文件内部进一步切分的目标大小（字节）。文件大于它时切成多个 split 并行读，
     * 并行度来自 row-group / 同步块粒度；默认 128MB，与 Iceberg 的默认 split size 对齐。
     */
    val splitSize: Long = DEFAULT_SPLIT_SIZE,
    /**
     * 过滤下推（类 SQL 的 WHERE）：`age >= 40 AND name = 'bob'`、`id IN (1, 2, 3)`、`age IS NOT NULL`。
     * 下推到 Iceberg 的 TableScan 做 manifest 级裁剪（分区/文件粒度直接砍掉不匹配的 split），
     * 读端再对每行用 `Evaluator` 求残留谓词，数据列与分区列都能正确过滤。
     */
    val filter: String = "",
    /**
     * 最多读多少行；`-1` 表示不限制。Iceberg 没有原生全局 LIMIT，所以限行数时退化为单 worker，
     * 按当前快照跨文件顺序读取，在真正产出 limit 条匹配记录后停止。
     */
    val limit: Long = -1,
    /**
     * 聚合下推：`["count", "min:age", "max:age", "sum:amount", "avg:amount"]`。COUNT 取自数据文件元数据
     * `recordCount`；MIN/MAX/SUM/AVG 投影对应列逐文件累加，再全局归并成一行。
     * 配置非空时输出聚合后的一行，schema 直接从 Iceberg 表元数据推导，所以 [schemaFields] 必须留空；
     * 与 [limit] 互斥——聚合是全局语义，limit 没有意义，同配直接报错。
     */
    val aggregations: List<String> = emptyList(),
) : IcebergConnectionConfig {

    fun validate() {
        require(warehouse.isNotBlank()) { "warehouse 不能为空" }
        require(catalogName.isNotBlank()) { "catalog_name 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(splitSize > 0) { "split_size 必须 > 0" }
        require(limit == -1L || limit > 0) { "limit 必须 > 0（或不限制时留空/传 -1）" }
        if (filter.isNotBlank()) parseIcebergFilter(filter) // 解析失败在构图阶段就报错
        if (aggregations.isNotEmpty()) {
            require(limit == -1L) { "aggregations 模式不使用 limit，请从配置中移除" }
            require(schemaFields.isEmpty()) { "aggregations 模式不使用 schema_fields，请从配置中移除" }
            require(splitSize == DEFAULT_SPLIT_SIZE) {
                "aggregations 模式按数据文件聚合，不使用 split_size，请从配置中移除"
            }
            parseAggregations(aggregations)
        } else {
            require(schemaFields.isNotEmpty()) { "普通读取需要 schema_fields" }
            if (limit > 0) {
                require(splitSize == DEFAULT_SPLIT_SIZE) {
                    "limit 模式强制单 worker 顺序读取，不使用 split_size，请从配置中移除"
                }
            }
        }
        val fields = parseSchemaFields(schemaFields)
        require(fields.map { it.first }.distinct().size == fields.size) { "schema_fields 字段名不能重复" }
    }

    fun outputSchema(): Schema = Schema.builder().apply {
        parseSchemaFields(schemaFields).forEach { (name, type) -> addNullableField(name, type) }
    }.build()

    companion object {
        private const val serialVersionUID: Long = 1
        const val DEFAULT_SPLIT_SIZE: Long = 128L * 1024 * 1024
    }
}
