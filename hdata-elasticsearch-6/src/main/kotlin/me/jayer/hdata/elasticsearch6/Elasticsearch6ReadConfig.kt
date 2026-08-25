package me.jayer.hdata.elasticsearch6

import java.io.Serializable

/**
 * `ReadFromElasticsearch6` 的配置。配置键对齐 Flink Elasticsearch connector：
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
 * 输出行按 [schema_fields] 构建；缺省时退化为单 `document`(STRING) 列，整条 `_source` 作为 JSON 输出。
 */
data class Elasticsearch6ReadConfig(
    /** 逗号分隔的节点地址，例如 `http://node1:9200,http://node2:9200`。 */
    val connectionUri: String = "",
    /** 单索引；与 [indices] 二选一。 */
    val index: String = "",
    /** 多索引；与 [index] 二选一。 */
    val indices: List<String> = emptyList(),
    val username: String = "",
    val password: String = "",
    /** `name:TYPE` 列表（TYPE ∈ STRING/INT32/INT64/DOUBLE/BOOLEAN/DATETIME/BYTES）；缺省退化为 `document` 列。 */
    val schemaFields: List<String> = emptyList(),
    /** 可选 query DSL 字符串；缺省全量扫描。 */
    val scanQuery: String = "",
    val scrollSize: Int = 1000,
    val scrollTimeoutMinutes: Long = 1,
    /**
     * 把每个索引切成几个 slice 并行读。1(默认) 表示不切分。
     *
     * ES 的 slice 按文档 ID 哈希把一次 scroll 切成互不重叠的若干份，切分数**建议等于索引的分片数**。
     */
    val scanSlices: Int = 1,
    /**
     * 最多读多少条；`-1` 表示不限制。ES 的 scroll 没有原生的"全局 limit"，
     * 所以限行数时退化为单 slice（保证全局语义，否则会变成"每 slice 各读 limit 条"），
     * 并在扫到第 N 条后停止翻页、把每页 size 压到剩余条数。
     */
    val limit: Long = -1,
) : Serializable {

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri 不能为空" }
        require(index.isNotBlank() || indices.isNotEmpty()) { "index 或 indices 至少填一个" }
        require(index.isBlank() || indices.isEmpty()) { "index 与 indices 不能同时配置" }
        require(indices.none { it.isBlank() }) { "indices 不能包含空索引名" }
        require(nodes().isNotEmpty()) { "connection_uri 至少要包含一个有效节点" }
        parseElasticsearch6Hosts(nodes())
        require(scrollSize > 0) { "scroll_size 必须 > 0" }
        require(scrollTimeoutMinutes > 0) { "scroll_timeout_minutes 必须 > 0" }
        require(scanSlices >= 1) { "scan_slices 必须 >= 1" }
        require(limit == -1L || limit > 0) { "limit 必须 > 0（或不限制时留空/传 -1）" }
        require(limit <= Int.MAX_VALUE) { "limit 超过 ES 单次翻页上限" }
        val fields = parseSchemaFields(schemaFields)
        require(fields.map { it.name }.distinct().size == fields.size) { "schema_fields 字段名不能重复" }
    }

    fun nodes(): List<String> = connectionUri.split(",".toRegex(), Int.MAX_VALUE).map(String::trim)

    fun indexList(): List<String> = if (index.isNotBlank()) listOf(index) else indices
}
