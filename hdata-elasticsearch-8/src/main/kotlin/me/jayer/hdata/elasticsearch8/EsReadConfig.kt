package me.jayer.hdata.elasticsearch8

import java.io.Serializable
import me.jayer.hdata.elasticsearch8.parseEsAggregations

/**
 * Configuration for `ReadFromElasticsearch8`, with keys aligned to the Flink Elasticsearch connector.
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
 * `schema_fields` determines the output schema; when omitted, a single STRING `document` column contains `_source` JSON.
 */
data class EsReadConfig(
    val connectionUri: String = "",
    val index: String = "",
    val indices: List<String> = emptyList(),
    val apiKey: String = "",
    val username: String = "",
    val password: String = "",
    val schemaFields: List<String> = emptyList(),
    /** Number of records fetched per page. */
    val batchSize: Int = 1000,
    /** ES Query DSL JSON; empty uses match_all. */
    val scanQuery: String = "",
    /**
     * Number of slices used to read each index in parallel. The default, 1, disables slicing.
     *
     * Elasticsearch slices partition a query by document-ID hash. Use no more slices than shards,
     * because larger values can make slice sizes noticeably uneven.
     */
    val scanSlices: Int = 1,
    /** PIT lifetime in minutes. It expires if a slice waits longer than this between pages. */
    val keepAliveMinutes: Int = 5,
    /**
     * Maximum records to read; `-1` means unlimited. PIT plus search_after has no global limit,
     * so limit mode uses a single slice and stops paging after the requested number of records.
     */
    val limit: Long = -1,
    /**
     * Aggregate pushdown, such as `count`, `min:age`, or `avg:age`. Elasticsearch calculates one
     * result row; it does not use schema_fields, limit, or scan_slices.
     */
    val aggregations: List<String> = emptyList(),
) : Serializable {

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri must not be blank" }
        parseEsHosts(connectionUri)
        validateEsAuthentication(apiKey, username, password)
        require(index.isNotBlank() || indices.isNotEmpty()) { "at least one of index and indices must be set" }
        require(index.isBlank() || indices.isEmpty()) { "index and indices must not both be set" }
        require(indices.none { it.isBlank() }) { "indices must not contain a blank index name" }
        require(indices.distinct().size == indices.size) { "indices must not contain duplicates" }
        require(batchSize > 0) { "batch_size must be > 0" }
        require(scanSlices >= 1) { "scan_slices must be >= 1" }
        require(keepAliveMinutes > 0) { "keep_alive_minutes must be > 0" }
        require(limit == -1L || limit > 0) { "limit must be > 0, or -1 for unlimited" }
        require(limit <= 0 || indices.size <= 1) {
            "limit is global; reading multiple indices with limit is not supported"
        }
        require(limit <= 0 || scanSlices == 1) {
            "limit mode requires a single slice; remove scan_slices"
        }
        if (aggregations.isNotEmpty()) {
            // Elasticsearch computes a single aggregate row, so projection, limits, and slices do not apply.
            require(schemaFields.isEmpty()) { "aggregations does not use schema_fields; remove it" }
            require(limit == -1L) { "aggregations does not use limit; remove it" }
            require(scanSlices == 1) { "aggregations does not use scan_slices; remove it" }
            require(batchSize == 1000) { "aggregations does not use batch_size; remove it" }
            require(keepAliveMinutes == 5) { "aggregations does not use keep_alive_minutes; remove it" }
            parseEsAggregations(aggregations)
        }
        val fields = parseSchemaFields(schemaFields)
        require(fields.map { it.first }.distinct().size == fields.size) { "schema_fields field names must not be duplicated" }
        fields.forEach { (_, type) -> fieldType(type) }
        if (scanQuery.isNotBlank()) {
            runCatching { tools.jackson.databind.json.JsonMapper.builder().build().readTree(scanQuery) }
                .onFailure { throw IllegalArgumentException("scan_query is not valid JSON: ${it.message}", it) }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
