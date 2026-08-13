package me.jayer.hdata.filesystem

import java.io.Serializable

/**
 * `ReadFromFilesystem` 的配置。配置键对齐 Flink filesystem connector：
 *
 * - `path`：目录或文件通配符，例如 `hdfs:///data/files_*.csv` 或 `file:///tmp/input`。
 * - `default_fs`：默认文件系统 URI，`file:///`(默认) 或 `hdfs://namenode:8020`。
 * - `file_format`：`text`(默认，每行一条记录，`content` STRING 列)、`csv`(按 `schema_fields` 解析，
 *   支持引号/转义/内嵌换行) 或 `xlsx`(按工作表解析，列按 `schema_fields` 顺序映射)。
 * - `schema_fields`：`csv`/`xlsx` 格式用，形如 `["name:string", "age:int"]`。
 * - `header`：`csv`/`xlsx` 用，为 `true` 时跳过首行（表头）。默认 `false`。
 * - `sheet`：`xlsx` 用，工作表名；留空取第一个工作表。
 * - `encoding`：文本编码，默认 UTF-8（仅 `text`/`csv` 生效）。
 *
 * ```yaml
 * - type: ReadFromFilesystem
 *   config:
 *     path: "hdfs:///data/files_*.csv"
 *     default_fs: "hdfs://namenode:8020"
 *     file_format: csv
 *     schema_fields: ["name:string", "age:int"]
 *     header: true
 * ```
 */
data class FilesystemReadConfig(
    val path: String = "",
    val defaultFs: String = "file:///",
    val fileFormat: String = "text",
    val schemaFields: List<String> = emptyList(),
    val header: Boolean = false,
    val sheet: String = "",
    val encoding: String = "UTF-8",
) : Serializable {

    fun validate() {
        require(path.isNotBlank()) { "path 不能为空" }
        require(fileFormat in setOf("text", "csv", "xlsx")) { "file_format 取值非法: $fileFormat" }
        if (fileFormat in setOf("csv", "xlsx")) {
            require(schemaFields.isNotEmpty()) { "file_format=$fileFormat 需要 schema_fields" }
        }
    }
}
