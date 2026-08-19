package me.jayer.hdata.ftp

import java.io.Serializable

/**
 * `WriteToFtp` 的配置。
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
 * 输出是**分片**的：每个并行写入单元产出一个 `<file_prefix>-<分片号><扩展名>` 文件。
 * 重构前是所有实例往同一个 `file_name` 上 `appendFile`——多实例并发追加会把内容交错在一起，
 * 而且重跑作业是往上一次的结果后面接着追加，不是覆盖。分片是分布式写入唯一说得通的做法。
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
    /** 远程目录。 */
    val path: String = "",
    /** 输出文件名前缀。 */
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
        require(path.isNotBlank()) { "path 不能为空" }
        require(filePrefix.isNotBlank()) { "file_prefix 不能为空" }
        require(fileFormat in FtpReadConfig.FORMATS) {
            "file_format 取值非法: $fileFormat，可选 ${FtpReadConfig.FORMATS.joinToString()}"
        }
        require(batchSize > 0) { "batch_size 必须 > 0" }
        require(csvDelimiter.length == 1) { "csv_delimiter 必须是单个字符" }
        require(csvQuote.length == 1) { "csv_quote 必须是单个字符" }
        runCatching { java.nio.charset.Charset.forName(encoding) }
            .onFailure { throw IllegalArgumentException("encoding 不是合法的字符集: $encoding", it) }
        if (fileFormat == FtpReadConfig.CSV) {
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
    }

    val connection: FtpConnection
        get() = FtpConnection(host, hostName, port, user, username, password, timeoutMillis)

    /** csv 的表头字段名；text 模式下是单列 `content`。 */
    val outputFieldNames: List<String>
        get() = if (fileFormat == FtpReadConfig.CSV && !schemaFields.isNullOrEmpty()) {
            schemaFields.map { parseSchemaField(it).first }
        } else {
            listOf("content")
        }

    fun suffix(): String = if (fileFormat == FtpReadConfig.CSV) ".csv" else ".txt"

    /** 某个分片的最终远程路径。 */
    fun shardPath(shard: String): String {
        val dir = if (path.endsWith("/")) path.dropLast(1) else path
        return "$dir/$filePrefix-$shard${suffix()}"
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
