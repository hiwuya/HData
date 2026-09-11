package me.jayer.hdata.filesystem

import java.io.Serializable
import java.nio.charset.StandardCharsets

/**
 * Config of `WriteToFilesystem`; the key names follow the sink side of the Flink filesystem connector.
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
 * [path] is a **directory**; the actual file name is [filePrefix] + shard number + extension
 * (Beam's standard naming). Set [numShards] to 1 when you need everything in a single file.
 *
 * @author wuya
 */
data class FilesystemWriteConfig(
    /** Output directory. */
    val path: String = "",
    val defaultFs: String = "file:///",
    /** Hadoop filesystem options, including S3A endpoint and credentials. */
    val hadoopConf: Map<String, String> = emptyMap(),
    val fileFormat: String = FilesystemReadConfig.TEXT,
    val schemaFields: List<String> = emptyList(),
    val header: Boolean = false,
    val sheet: String = "",
    val encoding: String = "UTF-8",
    val csvDelimiter: String = ",",
    val csvQuote: String = "\"",
    /** Output file name prefix. */
    val filePrefix: String = "output",
    /**
     * Number of output shards. 0 means the runner decides (best throughput); setting it to 1
     * funnels all data onto a single worker, so only do that when one file is really required.
     */
    val numShards: Int = 0,
) : Serializable {

    fun validate() {
        require(path.isNotBlank()) { "path must not be blank" }
        require(defaultFs.isNotBlank()) { "default_fs must not be blank" }
        FilesystemPaths.validateDefaultFs(defaultFs)
        require(hadoopConf.keys.none { it.isBlank() }) { "hadoop_conf must not contain a blank key" }
        require(filePrefix.isNotBlank()) { "file_prefix must not be blank" }
        require(fileFormat in FilesystemReadConfig.FORMATS) {
            "invalid file_format: $fileFormat, valid values: ${FilesystemReadConfig.FORMATS.joinToString()}"
        }
        require(numShards >= 0) { "num_shards must not be negative" }
        require(csvDelimiter.length == 1) { "csv_delimiter must be a single character, got: \"$csvDelimiter\"" }
        require(csvQuote.length == 1) { "csv_quote must be a single character, got: \"$csvQuote\"" }
        val charset = runCatching { java.nio.charset.Charset.forName(encoding) }
            .onFailure { throw IllegalArgumentException("encoding is not a valid charset: $encoding", it) }
            .getOrThrow()
        if (fileFormat == FilesystemReadConfig.XLSX) {
            require(charset == StandardCharsets.UTF_8) { "file_format=xlsx does not use encoding, remove any non-UTF-8 setting" }
        }
        if (fileFormat == FilesystemReadConfig.TEXT) {
            require(schemaFields.isEmpty() && !header && sheet.isBlank()) {
                "file_format=text does not use schema_fields/header/sheet, remove them from the config"
            }
        }
        if (fileFormat != FilesystemReadConfig.CSV) {
            require(csvDelimiter == "," && csvQuote == "\"") {
                "file_format=$fileFormat does not use csv_delimiter/csv_quote, remove them from the config"
            }
        }
        if (fileFormat == FilesystemReadConfig.CSV) {
            require(sheet.isBlank()) { "file_format=csv does not use sheet, remove it from the config" }
        }
        if (fileFormat != FilesystemReadConfig.TEXT) {
            require(schemaFields.isNotEmpty()) { "file_format=$fileFormat requires schema_fields" }
        }
        if (fileFormat == FilesystemReadConfig.XLSX) {
            require(numShards == 1) {
                "file_format=xlsx requires num_shards: 1 -- a workbook is one complete zip container, so splitting it into several parts makes no sense"
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
