package me.jayer.hdata.jdbc

import java.io.Serializable

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
    val fetchSize: Int = 10000,
) : JdbcConnectionConfig, Serializable {

    companion object {
        private const val serialVersionUID: Long = 1
    }

    fun validate() {
        validateConnection()
        require(tables.isNotEmpty() || query.isNotBlank()) { "tables 与 query 至少要填一个" }
        require(tables.isEmpty() || query.isBlank()) { "tables 与 query 不能同时填写" }
        require(fetchSize > 0) { "fetch_size 必须 > 0" }
        require(columns.isNotEmpty()) { "columns 不能为空" }
        require(partitionNum == null || partitionNum > 0) { "partition_num 必须 > 0" }
    }
}
