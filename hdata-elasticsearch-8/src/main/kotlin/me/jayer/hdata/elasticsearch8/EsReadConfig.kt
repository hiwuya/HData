package me.jayer.hdata.elasticsearch8

import java.io.Serializable

/**
 * `ReadFromElasticsearch8` 的配置，配置键对齐 Flink Elasticsearch connector。
 *
 * ```yaml
 * - type: ReadFromElasticsearch8
 *   config:
 *     connection_uri: "http://localhost:9200"
 *     index: "orders"
 *     schema_fields: ["id:STRING", "amount:DOUBLE"]
 *     batch_size: 1000
 * ```
 *
 * 读出的行 schema 由 `schema_fields` 决定；为空时退化为单 `document`(STRING) 列（存放 `_source` 的 JSON）。
 */
data class EsReadConfig(
    val connectionUri: String = "",
    val index: String = "",
    val indices: List<String> = emptyList(),
    val apiKey: String = "",
    val username: String = "",
    val password: String = "",
    val schemaFields: List<String> = emptyList(),
    val batchSize: Int = 1000,
    val scanQuery: String = "",
) : Serializable {

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri 不能为空" }
        require(index.isNotBlank() || indices.isNotEmpty()) { "index/indices 至少要填一个" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
    }
}
