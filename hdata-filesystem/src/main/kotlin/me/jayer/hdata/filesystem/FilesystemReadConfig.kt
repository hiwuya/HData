package me.jayer.hdata.filesystem

import java.io.Serializable
import java.nio.charset.StandardCharsets

/**
 * Config of `ReadFromFilesystem`; the key names follow the Flink filesystem connector.
 *
 * - `path`: a directory or a wildcard (note that Kotlin block comments nest, so the literal for a
 *   slash immediately followed by a star is not written out here); for example all csv files under
 *   `file:///tmp/input`, or all csv files under `hdfs://namenode:8020/data`.
 *   Matching goes through Beam's `FileSystems`, so the scheme decides which filesystem is used.
 * - `file_format`: `text` (default, one record per line with a single `content` STRING column),
 *   `csv` or `xlsx`.
 * - `schema_fields`: used by `csv`/`xlsx`, in the form `["name:string", "age:int"]`.
 * - `header`: used by `csv`/`xlsx`; when `true` the first line is skipped.
 * - `csv_delimiter` / `csv_quote`: the delimiter and quote characters of CSV, default `,` and `"`.
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
    /** Hadoop's `fs.defaultFS`; only meaningful when [path] uses `hdfs://` and needs extra configuration. */
    val defaultFs: String = "file:///",
    /** Hadoop filesystem options, including S3A endpoint and credentials. */
    val hadoopConf: Map<String, String> = emptyMap(),
    val fileFormat: String = TEXT,
    val schemaFields: List<String> = emptyList(),
    val header: Boolean = false,
    /** Used by `xlsx`: the sheet name; blank means the first sheet. */
    val sheet: String = "",
    val encoding: String = "UTF-8",
    val csvDelimiter: String = ",",
    val csvQuote: String = "\"",
) : Serializable {

    fun validate() {
        require(path.isNotBlank()) { "path must not be blank" }
        require(defaultFs.isNotBlank()) { "default_fs must not be blank" }
        FilesystemPaths.validateDefaultFs(defaultFs)
        require(hadoopConf.keys.none { it.isBlank() }) { "hadoop_conf must not contain a blank key" }
        require(fileFormat in FORMATS) { "invalid file_format: $fileFormat, valid values: ${FORMATS.joinToString()}" }
        require(csvDelimiter.length == 1) { "csv_delimiter must be a single character, got: \"$csvDelimiter\"" }
        require(csvQuote.length == 1) { "csv_quote must be a single character, got: \"$csvQuote\"" }
        val charset = runCatching { java.nio.charset.Charset.forName(encoding) }
            .onFailure { throw IllegalArgumentException("encoding is not a valid charset: $encoding", it) }
            .getOrThrow()
        if (fileFormat == TEXT) {
            require(charset == StandardCharsets.UTF_8) {
                "file_format=text reuses Beam TextIO.readFiles, which only supports UTF-8; got encoding=$encoding"
            }
            require(schemaFields.isEmpty() && !header && sheet.isBlank()) {
                "file_format=text does not use schema_fields/header/sheet, remove them from the config"
            }
        }
        if (fileFormat != CSV) {
            require(csvDelimiter == "," && csvQuote == "\"") {
                "file_format=$fileFormat does not use csv_delimiter/csv_quote, remove them from the config"
            }
        }
        if (fileFormat == CSV) require(sheet.isBlank()) { "file_format=csv does not use sheet, remove it from the config" }
        if (fileFormat == XLSX) {
            require(charset == StandardCharsets.UTF_8) { "file_format=xlsx does not use encoding, remove any non-UTF-8 setting" }
        }
        if (fileFormat != TEXT) {
            require(schemaFields.isNotEmpty()) { "file_format=$fileFormat requires schema_fields" }
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
