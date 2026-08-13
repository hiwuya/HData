package me.jayer.hdata.ftp

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.io.Serializable

/** 把一个 `name:type` 字段声明解析成 (字段名, Beam 字段类型)。 */
fun parseSchemaField(spec: String): Pair<String, Schema.FieldType> {
    val idx = spec.indexOf(':')
    val name = if (idx >= 0) spec.substring(0, idx).trim() else spec.trim()
    val type = if (idx >= 0) spec.substring(idx + 1).trim().lowercase() else "string"
    val fieldType = when (type) {
        "string" -> Schema.FieldType.STRING
        "int", "int32" -> Schema.FieldType.INT32
        "long", "int64" -> Schema.FieldType.INT64
        "float" -> Schema.FieldType.FLOAT
        "double" -> Schema.FieldType.DOUBLE
        "boolean", "bool" -> Schema.FieldType.BOOLEAN
        else -> Schema.FieldType.STRING
    }
    return name to fieldType
}

/**
 * `ReadFromFtp` 的配置。配置键对齐 Flink FTP connector：
 * `path`(远程目录或单文件路径)、`file_pattern`(可选 glob，如 `*.csv`)、
 * `file_format`(默认 `text`：每行一条记录带 `content` STRING 字段；或 `csv` 配合 `schema_fields`)、
 * `schema_fields`(csv 用，List&lt;String&gt; `name:type`)、`encoding`(默认 UTF-8)。
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
    val fileFormat: String = "text",
    val schemaFields: List<String>? = null,
    val encoding: String = "UTF-8",
) : Serializable {

    fun validate() {
        require(host.isNotBlank() || hostName.isNotBlank()) { "host 不能为空" }
        require(path.isNotBlank()) { "path 不能为空" }
        require(fileFormat in setOf("text", "csv")) { "file_format 取值非法: $fileFormat" }
        if (fileFormat == "csv") {
            require(!schemaFields.isNullOrEmpty()) { "file_format=csv 需要 schema_fields" }
        }
    }

    val connection: FtpConnection
        get() = FtpConnection(host, hostName, port, user, username, password)
}

/** 根据配置推导读出的 Beam schema：`text` 模式是单 `content` 列，`csv` 模式按 `schema_fields` 建。 */
fun buildReadSchema(config: FtpReadConfig): Schema =
    if (config.fileFormat == "csv" && !config.schemaFields.isNullOrEmpty()) {
        val builder = Schema.builder()
        config.schemaFields.map { parseSchemaField(it) }.forEach { (name, type) -> builder.addNullableField(name, type) }
        builder.build()
    } else {
        Schema.builder().addNullableField("content", Schema.FieldType.STRING).build()
    }

/** 把一行文本解析成 Row（按 csv 字段类型）。 */
fun csvLineToRow(schema: Schema, line: String): Row {
    val values = line.split(',')
    val row = Row.withSchema(schema)
    schema.fieldNames.forEachIndexed { index, _ ->
        val raw = values.getOrNull(index)?.takeIf { it.isNotBlank() }
        row.addValue(raw)
    }
    return row.build()
}
