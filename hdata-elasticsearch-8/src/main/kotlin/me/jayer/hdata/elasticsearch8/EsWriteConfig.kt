package me.jayer.hdata.elasticsearch8

import java.io.Serializable

/**
 * Configuration for `WriteToElasticsearch8`.
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
 * Input rows form documents from `schema_fields`; without it, the STRING `value` field is raw JSON.
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
        require(connectionUri.isNotBlank()) { "connection_uri must not be blank" }
        parseEsHosts(connectionUri)
        validateEsAuthentication(apiKey, username, password)
        require(index.isNotBlank()) { "index must not be blank" }
        require(batchSize > 0) { "batch_size must be > 0" }
        val fields = parseSchemaFields(schemaFields)
        require(fields.map { it.first }.distinct().size == fields.size) { "schema_fields field names must not be duplicated" }
        fields.forEach { (_, type) -> fieldType(type) }
    }
}
