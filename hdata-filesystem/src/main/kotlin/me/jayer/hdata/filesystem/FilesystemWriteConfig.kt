package me.jayer.hdata.filesystem

import java.io.Serializable
import java.nio.charset.StandardCharsets

/**
 * `WriteToFilesystem` 的配置，键名对齐 Flink filesystem connector 的 sink 侧。
 *
 * ```yaml
 * - type: WriteToFilesystem
 *   config:
 *     path: "file:///tmp/output"
 *     file_format: csv
 *     schema_fields: ["name:string", "age:int"]
 *     header: true
 *     num_shards: 1
 * ```
 *
 * [path] 是**目录**，实际文件名由 [filePrefix] + 分片号 + 扩展名拼出（Beam 的标准命名）。
 * 需要固定成一个文件时把 [numShards] 设为 1。
 *
 * @author wuya
 */
data class FilesystemWriteConfig(
    /** 输出目录。 */
    val path: String = "",
    val defaultFs: String = "file:///",
    val fileFormat: String = FilesystemReadConfig.TEXT,
    val schemaFields: List<String> = emptyList(),
    val header: Boolean = false,
    val sheet: String = "",
    val encoding: String = "UTF-8",
    val csvDelimiter: String = ",",
    val csvQuote: String = "\"",
    /** 输出文件名前缀。 */
    val filePrefix: String = "output",
    /**
     * 输出分片数。0 表示交给 runner 决定（吞吐最好）；设成 1 会把所有数据汇到一个 worker 上，
     * 只在确实需要单个文件时才这么用。
     */
    val numShards: Int = 0,
) : Serializable {

    fun validate() {
        require(path.isNotBlank()) { "path 不能为空" }
        require(defaultFs.isNotBlank()) { "default_fs 不能为空" }
        FilesystemPaths.validateDefaultFs(defaultFs)
        require(filePrefix.isNotBlank()) { "file_prefix 不能为空" }
        require(fileFormat in FilesystemReadConfig.FORMATS) {
            "file_format 取值非法: $fileFormat，可选 ${FilesystemReadConfig.FORMATS.joinToString()}"
        }
        require(numShards >= 0) { "num_shards 不能为负" }
        require(csvDelimiter.length == 1) { "csv_delimiter 必须是单个字符，收到: \"$csvDelimiter\"" }
        require(csvQuote.length == 1) { "csv_quote 必须是单个字符，收到: \"$csvQuote\"" }
        val charset = runCatching { java.nio.charset.Charset.forName(encoding) }
            .onFailure { throw IllegalArgumentException("encoding 不是合法的字符集: $encoding", it) }
            .getOrThrow()
        if (fileFormat == FilesystemReadConfig.XLSX) {
            require(charset == StandardCharsets.UTF_8) { "file_format=xlsx 不使用 encoding，请移除非 UTF-8 配置" }
        }
        if (fileFormat == FilesystemReadConfig.TEXT) {
            require(schemaFields.isEmpty() && !header && sheet.isBlank()) {
                "file_format=text 不使用 schema_fields/header/sheet，请从配置中移除"
            }
        }
        if (fileFormat != FilesystemReadConfig.CSV) {
            require(csvDelimiter == "," && csvQuote == "\"") {
                "file_format=$fileFormat 不使用 csv_delimiter/csv_quote，请从配置中移除"
            }
        }
        if (fileFormat == FilesystemReadConfig.CSV) {
            require(sheet.isBlank()) { "file_format=csv 不使用 sheet，请从配置中移除" }
        }
        if (fileFormat != FilesystemReadConfig.TEXT) {
            require(schemaFields.isNotEmpty()) { "file_format=$fileFormat 需要 schema_fields" }
        }
        if (fileFormat == FilesystemReadConfig.XLSX) {
            require(numShards == 1) {
                "file_format=xlsx 必须设 num_shards: 1——一个工作簿就是一个完整的 zip 容器，切成多份没有意义"
            }
        }
        FilesystemSchemas.build(schemaFields)
    }

    fun suffix(): String = when (fileFormat) {
        FilesystemReadConfig.CSV -> ".csv"
        FilesystemReadConfig.XLSX -> ".xlsx"
        else -> ".txt"
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
