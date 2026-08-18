package me.jayer.hdata.iceberg

import java.io.Serializable

/** 已有数据怎么处理。 */
enum class IcebergWriteMode {

    /** 新数据文件追加进去，表里原有的数据原样保留。 */
    APPEND,

    /** 先把表清空（一次原子提交），再写入本次的数据。 */
    OVERWRITE,
    ;

    companion object {
        fun of(name: String): IcebergWriteMode = entries.firstOrNull { it.name.equals(name.trim(), ignoreCase = true) }
            ?: throw IllegalArgumentException(
                "write_mode 取值非法: $name，可选: ${entries.joinToString { it.name.lowercase() }}"
            )
    }
}

/**
 * `WriteToIceberg` 的配置。
 *
 * 写入时按 `schema_fields` 声明目标表结构，表不存在则自动创建（无分区）；
 * 每个 bundle 累积的行落成一个数据文件再 `append`。
 * `write_mode: overwrite` 会在**所有写入之前**把表清空一次，见
 * [me.jayer.hdata.iceberg.transform.IcebergTruncateFn]。
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
        mode()
    }

    fun mode(): IcebergWriteMode = IcebergWriteMode.of(writeMode)

    fun outputSchema() = me.jayer.hdata.iceberg.internal.schemaOf(schemaFields)

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
