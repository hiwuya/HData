package me.jayer.hdata.ftp

import org.apache.beam.sdk.schemas.Schema
import java.io.Serializable
import java.nio.file.FileSystems

/**
 * Parse a `name:type` field declaration into (field name, Beam field type).
 *
 * Unknown types raise an error directly. Before the refactor this was
 * `else -> Schema.FieldType.STRING`, which silently treated typos like `age:intt`
 * as STRING, only failing downstream.
 */
fun parseSchemaField(spec: String): Pair<String, Schema.FieldType> {
    val idx = spec.indexOf(':')
    val name = if (idx >= 0) spec.substring(0, idx).trim() else spec.trim()
    require(name.isNotBlank()) { "schema_fields entry must have a non-empty field name: $spec" }
    val type = if (idx >= 0) spec.substring(idx + 1).trim().lowercase() else "string"
    val fieldType = when (type) {
        "string" -> Schema.FieldType.STRING
        "int", "int32" -> Schema.FieldType.INT32
        "long", "int64" -> Schema.FieldType.INT64
        "float" -> Schema.FieldType.FLOAT
        "double" -> Schema.FieldType.DOUBLE
        "boolean", "bool" -> Schema.FieldType.BOOLEAN
        else -> throw IllegalArgumentException(
            "schema_fields unsupported type: $type, expected one of string/int/long/float/double/boolean"
        )
    }
    return name to fieldType
}

/**
 * Config for `ReadFromFtp`; key names align with the Flink filesystem connector's FTP usage.
 *
 * - `path`: remote directory or a single file path.
 * - `file_pattern`: optional glob, e.g. `*.csv`.
 * - `file_format`: `text` (default; one record per line, a `content` STRING column) or `csv`.
 * - `schema_fields`: for `csv`, e.g. `["name:string", "age:int"]`.
 *
 * @author wuya
 */
data class FtpReadConfig(
    val host: String = "",
    val hostName: String = "",
    val port: Int = 21,
    val user: String = "",
    val username: String = "",
    val password: String = "",
    val path: String = "",
    val filePattern: String? = null,
    val fileFormat: String = TEXT,
    val schemaFields: List<String>? = null,
    val header: Boolean = false,
    val encoding: String = "UTF-8",
    val csvDelimiter: String = ",",
    val csvQuote: String = "\"",
    /** Connection and read/write timeout (milliseconds). Defaults to 30s to avoid the job hanging forever if the peer becomes unresponsive. */
    val timeoutMillis: Int = 30_000,
) : Serializable {

    fun validate() {
        connection.validate()
        require(path.isNotBlank()) { "path must not be empty" }
        filePattern?.let { pattern ->
            require(pattern.isNotBlank()) { "file_pattern must not be an empty string; remove this config when not filtering files" }
            runCatching { FileSystems.getDefault().getPathMatcher("glob:$pattern") }
                .onFailure { throw IllegalArgumentException("file_pattern is not a valid glob: $pattern", it) }
        }
        require(fileFormat in FORMATS) { "file_format is invalid: $fileFormat, expected one of ${FORMATS.joinToString()}" }
        require(csvDelimiter.length == 1) { "csv_delimiter must be a single character" }
        require(csvQuote.length == 1) { "csv_quote must be a single character" }
        runCatching { java.nio.charset.Charset.forName(encoding) }
            .onFailure { throw IllegalArgumentException("encoding is not a valid charset: $encoding", it) }
        if (fileFormat == CSV) {
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
        buildReadSchema(this)
    }

    val connection: FtpConnection
        get() = FtpConnection(host, hostName, port, user, username, password, timeoutMillis)

    companion object {
        private const val serialVersionUID: Long = 1

        const val TEXT = "text"
        const val CSV = "csv"

        val FORMATS = listOf(TEXT, CSV)
    }
}

/** Fixed schema for `text` mode. */
val FTP_TEXT_SCHEMA: Schema = Schema.builder().addNullableStringField("content").build()

fun buildReadSchema(config: FtpReadConfig): Schema =
    if (config.fileFormat == FtpReadConfig.CSV && !config.schemaFields.isNullOrEmpty()) {
        val builder = Schema.builder()
        config.schemaFields.map { parseSchemaField(it) }.forEach { (name, type) -> builder.addNullableField(name, type) }
        builder.build()
    } else {
        FTP_TEXT_SCHEMA
    }
