package me.jayer.hdata.elasticsearch6

import java.io.Serializable

/**
 * `WriteToElasticsearch6` 的配置。
 *
 * ```yaml
 * - type: WriteToElasticsearch6
 *   config:
 *     connection_uri: "http://localhost:9200"
 *     index: "orders"
 *     schema_fields: ["id:INT64", "name:STRING"]
 *     batch_size: 1000
 * ```
 *
 * 给了 [schemaFields] 时按字段写；缺省时输入行必须带 `value`(STRING) 列，作为原始 JSON 写入。
 */
data class Elasticsearch6WriteConfig(
    val connectionUri: String = "",
    val index: String = "",
    val username: String = "",
    val password: String = "",
    val schemaFields: List<String> = emptyList(),
    val batchSize: Int = 1000,
) : Serializable {

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri 不能为空" }
        require(index.isNotBlank()) { "index 不能为空" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
    }

    fun nodes(): List<String> = connectionUri.split(",").map { it.trim() }.filter { it.isNotBlank() }
}
