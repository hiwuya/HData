package me.jayer.hdata.ftp

import java.io.Serializable

/**
 * `WriteToFtp` 的配置。配置键对齐 Flink FTP connector 的 sink 侧：
 * `path`(远程目录)、`file_name`(可选，默认 `hdata-output.txt`)、
 * `file_format`(默认 `text`：写 `content` STRING 字段作为一行；或 `csv` 配合 `schema_fields`)、
 * `batch_size`(攒多少行上传一次，默认 1000)。
 *
 * 输入行：text 模式需要 `content`(STRING) 字段；csv 模式需要 `schema_fields` 里声明的字段。
 */
data class FtpWriteConfig(
    val host: String = "",
    val hostName: String = "",
    val port: Int = 21,
    val user: String = "",
    val username: String = "",
    val password: String = "",
    val path: String = "",
    val fileName: String? = null,
    val fileFormat: String = "text",
    val schemaFields: List<String>? = null,
    val encoding: String = "UTF-8",
    val batchSize: Int = 1000,
) : Serializable {

    fun validate() {
        require(host.isNotBlank() || hostName.isNotBlank()) { "host 不能为空" }
        require(path.isNotBlank()) { "path 不能为空" }
        require(fileFormat in setOf("text", "csv")) { "file_format 取值非法: $fileFormat" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
        if (fileFormat == "csv") {
            require(!schemaFields.isNullOrEmpty()) { "file_format=csv 需要 schema_fields" }
        }
    }

    val connection: FtpConnection
        get() = FtpConnection(host, hostName, port, user, username, password)

    /** 输出字段名：text 模式是 `content`，csv 模式是 `schema_fields` 解析出的名字。 */
    val outputFieldNames: List<String>
        get() = if (fileFormat == "csv" && !schemaFields.isNullOrEmpty()) {
            schemaFields.map { parseSchemaField(it).first }
        } else {
            listOf("content")
        }

    val actualFileName: String
        get() = (fileName ?: "hdata-output.txt").let { if (path.endsWith("/")) "$path$it" else "$path/$it" }
}
