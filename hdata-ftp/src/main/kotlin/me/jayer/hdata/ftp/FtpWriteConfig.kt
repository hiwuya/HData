package me.jayer.hdata.ftp

import java.io.Serializable

/**
 * Config for `WriteToFtp`.
 *
 * ```yaml
 * - type: WriteToFtp
 *   config:
 *     host: "localhost"
 *     user: "u"
 *     password: "p"
 *     path: "/upload"
 *     file_prefix: "orders"
 *     file_format: csv
 *     schema_fields: ["name:string", "age:int"]
 * ```
 *
 * The output is **sharded**: each parallel write unit produces a `<file_prefix>-<shard-number><extension>` file.
 * Before the refactor every instance appended to the same `file_name` via `appendFile` — concurrent
 * appends from multiple instances would interleave the contents, and re-running the job would append
 * after the previous run's result rather than overwriting it. Sharding is the only sane approach for
 * distributed writes.
 *
 * @author wuya
 */
data class FtpWriteConfig(
    val host: String = "",
    val hostName: String = "",
    val port: Int = 21,
    val user: String = "",
    val username: String = "",
    val password: String = "",
    /** Remote directory. */
    val path: String = "",
    /** Output file name prefix. */
    val filePrefix: String = "hdata-output",
    val fileFormat: String = FtpReadConfig.TEXT,
    val schemaFields: List<String>? = null,
    val header: Boolean = false,
    val encoding: String = "UTF-8",
    val csvDelimiter: String = ",",
    val csvQuote: String = "\"",
    val batchSize: Int = 1000,
    val timeoutMillis: Int = 30_000,
) : Serializable {

    fun validate() {
        connection.validate()
        require(path.isNotBlank()) { "path must not be empty" }
        require(filePrefix.isNotBlank()) { "file_prefix must not be empty" }
        require(fileFormat in FtpReadConfig.FORMATS) {
            "file_format is invalid: $fileFormat, expected one of ${FtpReadConfig.FORMATS.joinToString()}"
        }
        require(batchSize > 0) { "batch_size must be > 0" }
        require(csvDelimiter.length == 1) { "csv_delimiter must be a single character" }
        require(csvQuote.length == 1) { "csv_quote must be a single character" }
        runCatching { java.nio.charset.Charset.forName(encoding) }
            .onFailure { throw IllegalArgumentException("encoding is not a valid charset: $encoding", it) }
        if (fileFormat == FtpReadConfig.CSV) {
            require(!schemaFields.isNullOrEmpty()) { "file_format=csv requires schema_fields" }
        } else {
            require(schemaFields.isNullOrEmpty() && !header) {
                "file_format=text does not use schema_fields/header; remove them from the config"
            }
            require(csvDelimiter == "," && csvQuote == "\"") {
                "file_format=text does not use csv_delimiter/csv_quote; remove them from the config"
            }
        }
        val names = schemaFields.orEmpty().map { parseSchemaField(it).first }
        require(names.size == names.distinct().size) { "schema_fields has duplicate field names: $names" }
    }

    val connection: FtpConnection
        get() = FtpConnection(host, hostName, port, user, username, password, timeoutMillis)

    /** Header field names for csv; a single `content` column in text mode. */
    val outputFieldNames: List<String>
        get() = if (fileFormat == FtpReadConfig.CSV && !schemaFields.isNullOrEmpty()) {
            schemaFields.map { parseSchemaField(it).first }
        } else {
            listOf("content")
        }

    fun suffix(): String = if (fileFormat == FtpReadConfig.CSV) ".csv" else ".txt"

    /** Final remote path for a given shard. */
    fun shardPath(shard: String): String {
        val dir = if (path.endsWith("/")) path.dropLast(1) else path
        return "$dir/$filePrefix-$shard${suffix()}"
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
