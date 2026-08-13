package me.jayer.hdata.hive

import java.io.Serializable

/**
 * `ReadFromHive` 的配置。配置键对齐 Flink Hive catalog connector 的常用选项。
 *
 * ```yaml
 * - type: ReadFromHive
 *   config:
 *     url: "jdbc:hive2://localhost:10000/default"
 *     user: hive
 *     password: hive
 *     database: default
 *     table: orders
 *     partitions: ["dt='2024-01-01'", "dt='2024-01-02'"]
 *     schema_fields: ["id:INT64", "name:STRING", "amount:DOUBLE"]
 * ```
 *
 * 读出行的 schema 由 [schemaFields] 决定；未提供时尝试用构图阶段的 `DESCRIBE` 推断列名与类型，
 * 仍失败则回退成单列 `value`(STRING)。
 */
data class HiveReadConfig(
    val url: String = "",
    val user: String = "",
    val password: String = "",
    val database: String = "",
    val table: String = "",
    /** 显式分区谓词片段（直接拼进 WHERE），为空则在构图阶段通过 `SHOW PARTITIONS` 发现。 */
    val partitions: List<String> = emptyList(),
    /** `name:TYPE` 列表，TYPE ∈ STRING/INT32/INT64/DOUBLE/BOOLEAN/DATETIME/BYTES。 */
    val schemaFields: List<String> = emptyList(),
) : Serializable {

    fun validate() {
        require(url.isNotBlank()) { "url 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
    }
}

/**
 * `WriteToHive` 的配置。
 *
 * ```yaml
 * - type: WriteToHive
 *   config:
 *     url: "jdbc:hive2://localhost:10000/default"
 *     table: orders
 *     batch_size: 1000
 * ```
 *
 * 输入行按 [schemaFields] 或 `DESCRIBE` 推断出的列顺序拼成 `INSERT INTO`，写失败且开了死信时进死信流。
 */
data class HiveWriteConfig(
    val url: String = "",
    val user: String = "",
    val password: String = "",
    val database: String = "",
    val table: String = "",
    val batchSize: Int = 1000,
    val schemaFields: List<String> = emptyList(),
) : Serializable {

    fun validate() {
        require(url.isNotBlank()) { "url 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
    }
}
