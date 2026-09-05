package me.jayer.hdata.ftp

import org.apache.beam.sdk.schemas.Schema
import java.io.Serializable
import java.nio.file.FileSystems

/**
 * 把一个 `name:type` 字段声明解析成 (字段名, Beam 字段类型)。
 *
 * 类型不认识时**直接报错**。重构前这里是 `else -> Schema.FieldType.STRING`，
 * 把 `age:intt` 这种拼写错误静默当成 STRING，一直到下游对不上号才发作。
 */
fun parseSchemaField(spec: String): Pair<String, Schema.FieldType> {
    val idx = spec.indexOf(':')
    val name = if (idx >= 0) spec.substring(0, idx).trim() else spec.trim()
    require(name.isNotBlank()) { "schema_fields 条目的字段名不能为空: $spec" }
    val type = if (idx >= 0) spec.substring(idx + 1).trim().lowercase() else "string"
    val fieldType = when (type) {
        "string" -> Schema.FieldType.STRING
        "int", "int32" -> Schema.FieldType.INT32
        "long", "int64" -> Schema.FieldType.INT64
        "float" -> Schema.FieldType.FLOAT
        "double" -> Schema.FieldType.DOUBLE
        "boolean", "bool" -> Schema.FieldType.BOOLEAN
        else -> throw IllegalArgumentException(
            "schema_fields 不支持的类型: $type，可选 string/int/long/float/double/boolean"
        )
    }
    return name to fieldType
}

/**
 * `ReadFromFtp` 的配置，键名对齐 Flink filesystem connector 的 FTP 用法。
 *
 * - `path`：远程目录或单个文件路径。
 * - `file_pattern`：可选 glob，例如 `*.csv`。
 * - `file_format`：`text`(默认，每行一条记录，`content` STRING 列) 或 `csv`。
 * - `schema_fields`：`csv` 用，形如 `["name:string", "age:int"]`。
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
    /** 连接与读写超时（毫秒）。默认 30 秒，避免对端假死时作业一直挂着。 */
    val timeoutMillis: Int = 30_000,
) : Serializable {

    fun validate() {
        connection.validate()
        require(path.isNotBlank()) { "path 不能为空" }
        filePattern?.let { pattern ->
            require(pattern.isNotBlank()) { "file_pattern 不能为空字符串；不筛选文件时请移除该配置" }
            runCatching { FileSystems.getDefault().getPathMatcher("glob:$pattern") }
                .onFailure { throw IllegalArgumentException("file_pattern 不是合法的 glob: $pattern", it) }
        }
        require(fileFormat in FORMATS) { "file_format 取值非法: $fileFormat，可选 ${FORMATS.joinToString()}" }
        require(csvDelimiter.length == 1) { "csv_delimiter 必须是单个字符" }
        require(csvQuote.length == 1) { "csv_quote 必须是单个字符" }
        runCatching { java.nio.charset.Charset.forName(encoding) }
            .onFailure { throw IllegalArgumentException("encoding 不是合法的字符集: $encoding", it) }
        if (fileFormat == CSV) {
            require(!schemaFields.isNullOrEmpty()) { "file_format=csv 需要 schema_fields" }
        } else {
            require(schemaFields.isNullOrEmpty() && !header) {
                "file_format=text 不使用 schema_fields/header，请从配置中移除"
            }
            require(csvDelimiter == "," && csvQuote == "\"") {
                "file_format=text 不使用 csv_delimiter/csv_quote，请从配置中移除"
            }
        }
        val names = schemaFields.orEmpty().map { parseSchemaField(it).first }
        require(names.size == names.distinct().size) { "schema_fields 字段名不能重复: $names" }
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

/** `text` 模式的固定 schema。 */
val FTP_TEXT_SCHEMA: Schema = Schema.builder().addNullableStringField("content").build()

fun buildReadSchema(config: FtpReadConfig): Schema =
    if (config.fileFormat == FtpReadConfig.CSV && !config.schemaFields.isNullOrEmpty()) {
        val builder = Schema.builder()
        config.schemaFields.map { parseSchemaField(it) }.forEach { (name, type) -> builder.addNullableField(name, type) }
        builder.build()
    } else {
        FTP_TEXT_SCHEMA
    }
