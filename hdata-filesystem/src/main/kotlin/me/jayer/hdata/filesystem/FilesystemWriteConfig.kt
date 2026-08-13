package me.jayer.hdata.filesystem

import java.io.Serializable

/**
 * `WriteToFilesystem` 的配置。配置键对齐 Flink filesystem connector 的 sink 侧。
 *
 * ```yaml
 * - type: WriteToFilesystem
 *   config:
 *     path: "hdfs:///data/output"
 *     default_fs: "hdfs://namenode:8020"
 *     file_format: text
 *     batch_size: 1000
 * ```
 *
 * 输入行必须包含 `content`(STRING) 字段（`text` 格式），或按 `schema_fields` 列出的字段（`csv`/`xlsx` 格式）。
 */
data class FilesystemWriteConfig(
    val path: String = "",
    val defaultFs: String = "file:///",
    val fileFormat: String = "text",
    val schemaFields: List<String> = emptyList(),
    val header: Boolean = false,
    val sheet: String = "",
    val batchSize: Int = 1000,
    val encoding: String = "UTF-8",
    val fileName: String = "",
) : Serializable {

    fun validate() {
        require(path.isNotBlank()) { "path 不能为空" }
        require(fileFormat in setOf("text", "csv", "xlsx")) { "file_format 取值非法: $fileFormat" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
        if (fileFormat in setOf("csv", "xlsx")) {
            require(schemaFields.isNotEmpty()) { "file_format=$fileFormat 需要 schema_fields" }
        }
    }
}
