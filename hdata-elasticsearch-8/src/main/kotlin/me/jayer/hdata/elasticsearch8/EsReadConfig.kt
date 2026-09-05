package me.jayer.hdata.elasticsearch8

import java.io.Serializable
import me.jayer.hdata.elasticsearch8.parseEsAggregations

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
    /** 每次翻页取多少条。 */
    val batchSize: Int = 1000,
    /** 查询条件（ES Query DSL 的 JSON），留空表示 match_all。 */
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
        require(connectionUri.isNotBlank()) { "connection_uri 不能为空" }
        parseEsHosts(connectionUri)
        validateEsAuthentication(apiKey, username, password)
        require(index.isNotBlank() || indices.isNotEmpty()) { "index/indices 至少要填一个" }
        require(index.isBlank() || indices.isEmpty()) { "index 与 indices 不能同时配置" }
        require(indices.none { it.isBlank() }) { "indices 不能包含空索引名" }
        require(indices.distinct().size == indices.size) { "indices 不能重复，否则同一索引会被读取多次" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
        require(scanSlices >= 1) { "scan_slices 必须 >= 1" }
        require(keepAliveMinutes > 0) { "keep_alive_minutes 必须 > 0" }
        require(limit == -1L || limit > 0) { "limit 必须 > 0（或不限制时留空/传 -1）" }
        require(limit <= 0 || indices.size <= 1) {
            "limit 是全局行数上限，暂不支持同时读取多个 indices；否则会退化成每个索引各取 $limit 条"
        }
        require(limit <= 0 || scanSlices == 1) {
            "limit 模式强制单 slice，不使用 scan_slices，请从配置中移除"
        }
        if (aggregations.isNotEmpty()) {
            // 聚合是 ES 侧算完返回单行，schema_fields/limit/扫描切片都没意义
            require(schemaFields.isEmpty()) { "aggregations 模式不使用 schema_fields，请从配置中移除" }
            require(limit == -1L) { "aggregations 模式不使用 limit，请从配置中移除" }
            require(scanSlices == 1) { "aggregations 模式不使用 scan_slices，请从配置中移除" }
            require(batchSize == 1000) { "aggregations 模式不使用 batch_size，请从配置中移除" }
            require(keepAliveMinutes == 5) { "aggregations 模式不使用 keep_alive_minutes，请从配置中移除" }
            parseEsAggregations(aggregations)
        }
        val fields = parseSchemaFields(schemaFields)
        require(fields.map { it.first }.distinct().size == fields.size) { "schema_fields 字段名不能重复" }
        fields.forEach { (_, type) -> fieldType(type) }
        if (scanQuery.isNotBlank()) {
            runCatching { tools.jackson.databind.json.JsonMapper.builder().build().readTree(scanQuery) }
                .onFailure { throw IllegalArgumentException("scan_query 不是合法的 JSON: ${it.message}", it) }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
