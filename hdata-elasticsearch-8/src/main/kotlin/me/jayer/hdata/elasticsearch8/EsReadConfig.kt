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
) : Serializable {

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri 不能为空" }
        parseEsHosts(connectionUri)
        require(index.isNotBlank() || indices.isNotEmpty()) { "index/indices 至少要填一个" }
        require(index.isBlank() || indices.isEmpty()) { "index 与 indices 不能同时配置" }
        require(indices.none { it.isBlank() }) { "indices 不能包含空索引名" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
        require(scanSlices >= 1) { "scan_slices 必须 >= 1" }
        require(keepAliveMinutes > 0) { "keep_alive_minutes 必须 > 0" }
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
