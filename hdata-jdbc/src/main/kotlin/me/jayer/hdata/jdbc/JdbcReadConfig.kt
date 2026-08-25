package me.jayer.hdata.jdbc

import java.io.Serializable
import me.jayer.hdata.jdbc.internal.parseJdbcAggregations

/**
 * `ReadFromJdbc` 的配置。
 *
 * ```yaml
 * - type: ReadFromJdbc
 *   config:
 *     url: jdbc:mysql://127.0.0.1:3306/demo
 *     user: root
 *     password: ${MYSQL_PASSWORD}
 *     tables: ["t_order_${00-15}"]
 *     where: "created_at >= '2022-01-01'"
 *     partition_column: id
 *     partition_num: 8
 *     limit: 1000
 * ```
 *
 * @author wuya
 * @date 2022-07-29
 */
data class JdbcReadConfig(
    override val url: String = "",
    override val user: String = "",
    override val password: String = "",
    override val driverClass: String = "",
    override val connectionProperties: Map<String, String> = emptyMap(),
    val columns: List<String> = listOf("*"),
    /** 支持 `t_order_${00-15}` 这样的区间写法，见 [me.jayer.hdata.jdbc.util.JdbcUtils.resolveTables]。 */
    val tables: List<String> = emptyList(),
    val where: String = "",
    val partitionColumn: String = "",
    /** 不填时按分区列的取值范围自动估算。 */
    val partitionNum: Int? = null,
    /** 直接给一条 SQL，此时 [tables] / [where] / 分区都不生效。 */
    val query: String = "",
    /** 最多读多少行；`-1` 表示不限制。下推成 SQL 的 `LIMIT`（仅 [tables] 模式，[query] 模式忽略）。 */
    val limit: Long = -1,
    val fetchSize: Int = 10000,
    /**
     * 聚合下推：`["count", "min:age", "max:age", "sum:age", "avg:age"]`。翻译成 DB 原生聚合 SQL，
     * 在数据源侧算完返回单行。配置非空时忽略 columns/分区/limit，只支持单表或 query。
     */
    val aggregations: List<String> = emptyList(),
) : JdbcConnectionConfig, Serializable {

    companion object {
        private const val serialVersionUID: Long = 1
        private const val MAX_PARTITION_NUM: Int = 10_000
    }

    fun validate() {
        validateConnection()
        require(tables.isNotEmpty() || query.isNotBlank()) { "tables 与 query 至少要填一个" }
        require(tables.isEmpty() || query.isBlank()) { "tables 与 query 不能同时填写" }
        require(fetchSize > 0) { "fetch_size 必须 > 0" }
        require(columns.isNotEmpty()) { "columns 不能为空" }
        require(columns.none { it.isBlank() }) { "columns 不能包含空列名" }
        require(tables.none { it.isBlank() }) { "tables 不能包含空表名" }
        require(partitionNum == null || partitionNum > 0) { "partition_num 必须 > 0" }
        require(partitionNum == null || partitionNum <= MAX_PARTITION_NUM) {
            "partition_num 不能超过 $MAX_PARTITION_NUM，过多并发连接会压垮源数据库"
        }
        require(limit == -1L || limit > 0) { "limit 必须 > 0（或不限制时留空/传 -1）" }
        if (aggregations.isNotEmpty()) {
            // 聚合是 DB 侧算完返回单行，columns/分区/limit 都没意义
            require(columns == listOf("*")) { "aggregations 模式不使用 columns，请从配置中移除" }
            require(partitionColumn.isBlank()) { "aggregations 模式不使用 partition_column，请从配置中移除" }
            require(partitionNum == null) { "aggregations 模式不使用 partition_num，请从配置中移除" }
            require(limit == -1L) { "aggregations 模式不使用 limit，请从配置中移除" }
            require(tables.size <= 1) { "aggregations 只支持单表或 query（多表聚合需指定具体表）" }
            parseJdbcAggregations(aggregations) // 拒绝不支持的聚合（sum/avg 允许）
        }
        if (query.isNotBlank()) {
            require(columns == listOf("*")) { "query 模式不使用 columns，请从配置中移除" }
            require(where.isBlank()) { "query 模式不使用 where，请从配置中移除" }
            require(partitionColumn.isBlank()) { "query 模式不使用 partition_column，请从配置中移除" }
            require(partitionNum == null) { "query 模式不使用 partition_num，请从配置中移除" }
            require(limit == -1L) { "query 模式不使用 limit，请从配置中移除" }
        }
    }
}
