package me.jayer.hdata.filesystem

import java.io.Serializable

/**
 * `ReadFromFilesystem` 的配置，键名对齐 Flink filesystem connector。
 *
 * - `path`：目录或通配符（注意 Kotlin 的块注释可嵌套，这里不写出斜杠紧跟星号的字面量），
 *   例如 `file:///tmp/input` 下的所有 csv、或 `hdfs://namenode:8020/data` 下的所有 csv。
 *   走 Beam 的 `FileSystems`，scheme 决定用哪个文件系统。
 * - `file_format`：`text`(默认，每行一条记录，`content` STRING 列)、`csv` 或 `xlsx`。
 * - `schema_fields`：`csv`/`xlsx` 用，形如 `["name:string", "age:int"]`。
 * - `header`：`csv`/`xlsx` 用，为 `true` 时跳过首行。
 * - `csv_delimiter` / `csv_quote`：CSV 的分隔符与引号字符，默认 `,` 与 `"`。
 *
 * ```yaml
 * - type: ReadFromFilesystem
 *   config:
 *     path: "file:///tmp/input/all-*.csv"
 *     file_format: csv
 *     schema_fields: ["name:string", "age:int"]
 *     header: true
 * ```
 *
 * @author wuya
 */
data class FilesystemReadConfig(
    val path: String = "",
    /** Hadoop 的 `fs.defaultFS`，只在 [path] 用 `hdfs://` 且需要额外配置时有意义。 */
    val defaultFs: String = "file:///",
    val fileFormat: String = TEXT,
    val schemaFields: List<String> = emptyList(),
    val header: Boolean = false,
    /** `xlsx` 用，工作表名；留空取第一个工作表。 */
    val sheet: String = "",
    val encoding: String = "UTF-8",
    val csvDelimiter: String = ",",
    val csvQuote: String = "\"",
) : Serializable {

    fun validate() {
        require(path.isNotBlank()) { "path 不能为空" }
        require(fileFormat in FORMATS) { "file_format 取值非法: $fileFormat，可选 ${FORMATS.joinToString()}" }
        require(csvDelimiter.length == 1) { "csv_delimiter 必须是单个字符，收到: \"$csvDelimiter\"" }
        require(csvQuote.length == 1) { "csv_quote 必须是单个字符，收到: \"$csvQuote\"" }
        runCatching { java.nio.charset.Charset.forName(encoding) }
            .onFailure { throw IllegalArgumentException("encoding 不是合法的字符集: $encoding", it) }
        if (fileFormat != TEXT) {
            require(schemaFields.isNotEmpty()) { "file_format=$fileFormat 需要 schema_fields" }
        }
        FilesystemSchemas.build(schemaFields)
    }

    companion object {
        private const val serialVersionUID: Long = 1

        const val TEXT = "text"
        const val CSV = "csv"
        const val XLSX = "xlsx"

        val FORMATS = listOf(TEXT, CSV, XLSX)
    }
}
