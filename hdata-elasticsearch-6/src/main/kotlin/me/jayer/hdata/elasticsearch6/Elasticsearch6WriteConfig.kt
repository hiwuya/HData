package me.jayer.hdata.elasticsearch6

import java.io.Serializable

/**
 * Config for `WriteToElasticsearch6`.
 *
 * ```yaml
 * - type: WriteToElasticsearch6
 *   config:
 *     connection_uri: "http://localhost:9200"
 *     index: "orders"
 *     schema_fields: ["id:INT64", "name:STRING"]
 *     batch_size: 1000
 *     doc_type: "_doc"
 * ```
 *
 * When [schemaFields] is given, it writes by field; by default the input row must carry a `value`(STRING) column,
 * written as raw JSON. [docType] defaults to `_doc`, the community-recommended single-type-per-index convention for
 * clusters that still enforce ES 6's mandatory mapping type.
 */
data class Elasticsearch6WriteConfig(
    val connectionUri: String = "",
    val index: String = "",
    val username: String = "",
    val password: String = "",
    val schemaFields: List<String> = emptyList(),
    val batchSize: Int = 1000,
    /** ES 6.x still requires a mapping type per document; `_doc` is the recommended single-type convention. */
    val docType: String = "_doc",
) : Serializable {

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri must not be empty" }
        require(password.isBlank() || username.isNotBlank()) { "username must also be configured when password is set" }
        require(index.isNotBlank()) { "index must not be empty" }
        require(batchSize > 0) { "batch_size must be > 0" }
        require(docType.isNotBlank()) { "doc_type must not be empty" }
        require(nodes().isNotEmpty()) { "connection_uri must contain at least one valid node" }
        parseElasticsearch6Hosts(nodes())
        val fields = parseSchemaFields(schemaFields)
        require(fields.map { it.name }.distinct().size == fields.size) { "schema_fields field names must not be duplicated" }
    }

    fun nodes(): List<String> = connectionUri.split(",".toRegex(), Int.MAX_VALUE).map(String::trim)
}
