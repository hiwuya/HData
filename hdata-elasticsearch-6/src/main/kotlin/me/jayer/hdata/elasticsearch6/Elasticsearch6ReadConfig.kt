package me.jayer.hdata.elasticsearch6

import java.io.Serializable

/**
 * Config for `ReadFromElasticsearch6`. The config keys align with the Flink Elasticsearch connector:
 *
 * ```yaml
 * - type: ReadFromElasticsearch6
 *   config:
 *     connection_uri: "http://localhost:9200"
 *     index: "orders"
 *     schema_fields: ["id:INT64", "name:STRING", "amount:DOUBLE"]
 *     scroll_size: 1000
 * ```
 *
 * Output rows are built per [schema_fields]; by default it degrades to a single `document`(STRING) column, with the
 * entire `_source` output as JSON.
 */
data class Elasticsearch6ReadConfig(
    /** Comma-separated node addresses, e.g. `http://node1:9200,http://node2:9200`. */
    val connectionUri: String = "",
    /** A single index; mutually exclusive with [indices]. */
    val index: String = "",
    /** Multiple indices; mutually exclusive with [index]. */
    val indices: List<String> = emptyList(),
    val username: String = "",
    val password: String = "",
    /** A `name:TYPE` list (TYPE ∈ STRING/INT32/INT64/DOUBLE/BOOLEAN/DATETIME/BYTES); by default it degrades to a `document` column. */
    val schemaFields: List<String> = emptyList(),
    /** Optional query DSL string; by default a full scan. */
    val scanQuery: String = "",
    val scrollSize: Int = 1000,
    val scrollTimeoutMinutes: Long = 1,
    /**
     * How many slices to cut each index into for parallel reads. 1 (default) means no splitting.
     *
     * ES's slice hashes on document ID to cut one scroll into several non-overlapping parts; the number of slices is
     * **recommended to equal the index's shard count**.
     */
    val scanSlices: Int = 1,
    /**
     * The maximum number of records to read; `-1` means unlimited. ES's scroll has no native "global limit", so when
     * limiting rows it degrades to a single slice (to preserve global semantics, otherwise it would become "each slice
     * reads limit records"), stops paging after the Nth record is scanned, and squeezes each page's size to the
     * remaining count.
     */
    val limit: Long = -1,
    /**
     * Push-down aggregation: `["count", "min:age", "max:age", "sum:age", "avg:age"]`. Translated into ES native
     * aggregations, computed on the ES side and returned as a single row (no slice parallelism). When non-empty,
     * schema_fields/limit/scan slices are ignored.
     */
    val aggregations: List<String> = emptyList(),
) : Serializable {

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri must not be empty" }
        require(password.isBlank() || username.isNotBlank()) { "username must also be configured when password is set" }
        require(index.isNotBlank() || indices.isNotEmpty()) { "at least one of index or indices must be provided" }
        require(index.isBlank() || indices.isEmpty()) { "index and indices must not be configured at the same time" }
        require(indices.none { it.isBlank() }) { "indices must not contain an empty index name" }
        require(indices.distinct().size == indices.size) { "indices must not be duplicated, otherwise the same index would be read multiple times" }
        require(nodes().isNotEmpty()) { "connection_uri must contain at least one valid node" }
        parseElasticsearch6Hosts(nodes())
        require(scrollSize > 0) { "scroll_size must be > 0" }
        require(scrollTimeoutMinutes > 0) { "scroll_timeout_minutes must be > 0" }
        require(scanSlices >= 1) { "scan_slices must be >= 1" }
        require(limit == -1L || limit > 0) { "limit must be > 0 (or left empty / set to -1 for unlimited)" }
        require(limit <= 0 || indices.size <= 1) {
            "limit is a global row-count cap and does not yet support reading multiple indices at once; otherwise it would degrade to reading $limit records from each index"
        }
        require(limit <= 0 || scanSlices == 1) {
            "limit mode forces a single slice and does not use scan_slices, please remove it from the config"
        }
        if (aggregations.isNotEmpty()) {
            require(schemaFields.isEmpty()) { "aggregations mode does not use schema_fields, please remove it from the config" }
            require(limit == -1L) { "aggregations mode does not use limit, please remove it from the config" }
            require(scanSlices == 1) { "aggregations mode does not use scan_slices, please remove it from the config" }
            require(scrollSize == 1000) { "aggregations mode does not use scroll_size, please remove it from the config" }
            require(scrollTimeoutMinutes == 1L) {
                "aggregations mode does not use scroll_timeout_minutes, please remove it from the config"
            }
            parseEs6Aggregations(aggregations)
        }
        val fields = parseSchemaFields(schemaFields)
        require(fields.map { it.name }.distinct().size == fields.size) { "schema_fields field names must not be duplicated" }
    }

    fun nodes(): List<String> = connectionUri.split(",".toRegex(), Int.MAX_VALUE).map(String::trim)

    fun indexList(): List<String> = if (index.isNotBlank()) listOf(index) else indices
}
