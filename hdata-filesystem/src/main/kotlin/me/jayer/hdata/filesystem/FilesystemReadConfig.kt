package me.jayer.hdata.filesystem

import java.io.Serializable

/**
 * `ReadFromFilesystem` 的配置。配置键对齐 Flink filesystem connector：
 *
 * - `path`：目录或文件通配符，例如 `hdfs:///data/files_*.csv` 或 `file:///tmp/input`。
 * - `default_fs`：默认文件系统 URI，`file:///`(默认) 或 `hdfs://namenode:8020`。
 * - `file_format`：`text`(默认，每行一条记录，`content` STRING 列) 或 `csv`(按 `schema_fields` 解析)。
 * - `schema_fields`：`csv` 格式用，形如 `["name:string", "age:int"]`。
 * - `encoding`：文件编码，默认 UTF-8。
 *
 * ```yaml
 * - type: ReadFromFilesystem
 *   config:
 *     path: "hdfs:///data/files_*.csv"
 *     default_fs: "hdfs://namenode:8020"
 *     file_format: csv
 *     schema_fields: ["name:string", "age:int"]
 * ```
 */
data class FilesystemReadConfig(
    val path: String = "",
    val defaultFs: String = "file:///",
    val fileFormat: String = "text",
    val schemaFields: List<String> = emptyList(),
    val encoding: String = "UTF-8",
) : Serializable {

    fun validate() {
        require(path.isNotBlank()) { "path 不能为空" }
        require(fileFormat in setOf("text", "csv")) { "file_format 取值非法: $fileFormat" }
        if (fileFormat == "csv") {
            require(schemaFields.isNotEmpty()) { "file_format=csv 需要 schema_fields" }
        }
    }
}
