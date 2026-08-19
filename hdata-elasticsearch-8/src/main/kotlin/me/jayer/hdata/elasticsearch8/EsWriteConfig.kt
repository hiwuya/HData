package me.jayer.hdata.elasticsearch8

import java.io.Serializable

/**
 * `WriteToElasticsearch8` 的配置。
 *
 * ```yaml
 * - type: WriteToElasticsearch8
 *   config:
 *     connection_uri: "http://localhost:9200"
 *     index: "orders"
 *     schema_fields: ["id:STRING", "amount:DOUBLE"]
 *     batch_size: 1000
 * ```
 *
 * 输入行按 `schema_fields` 构建文档；没有 `schema_fields` 时取 `value`(STRING) 字段，当作原始 JSON 文档写入。
 */
data class EsWriteConfig(
    val connectionUri: String = "",
    val index: String = "",
    val apiKey: String = "",
    val username: String = "",
    val password: String = "",
    val schemaFields: List<String> = emptyList(),
    val batchSize: Int = 1000,
) : Serializable {

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri 不能为空" }
        parseEsHosts(connectionUri)
        require(index.isNotBlank()) { "index 不能为空" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
        val fields = parseSchemaFields(schemaFields)
        require(fields.map { it.first }.distinct().size == fields.size) { "schema_fields 字段名不能重复" }
        fields.forEach { (_, type) -> fieldType(type) }
    }
}
