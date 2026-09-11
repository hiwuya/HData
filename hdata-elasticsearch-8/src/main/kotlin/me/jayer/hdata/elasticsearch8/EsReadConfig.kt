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
     * 把每个索引切成几个 slice 并行读。1(默认) 表示不切分。
     *
     * ES 的 slice 按文档 ID 哈希把一次查询切成互不重叠的若干份，切分数**建议等于索引的分片数**：
     * 大于分片数时各 slice 的数据量会明显不均。
     */
    val scanSlices: Int = 1,
    /** PIT 的存活时间（分钟）。单个 slice 两次翻页之间超过这个时间，PIT 会过期。 */
    val keepAliveMinutes: Int = 5,
    /**
     * 最多读多少条；`-1` 表示不限制。ES 的 PIT + search_after 翻页没有原生的"全局 limit"，
     * 所以限行数时退化为单 slice（保证全局语义，否则会变成"每 slice 各读 limit 条"），
     * 并在扫到第 N 条后停止翻页、把每页 size 压到剩余条数。
     */
    val limit: Long = -1,
    /**
     * 聚合下推：`["count", "min:age", "max:age", "sum:age", "avg:age"]`。翻译成 ES 原生 aggregation，
     * 在 ES 侧算完返回单行（不走 slice 并行）。配置非空时忽略 schema_fields/limit/扫描切片。
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
            // 聚合是 ES 侧算完返回单行，schema_fields/limit/扫描切片都没意义
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
