package me.jayer.hdata.hive

import java.io.Serializable
import java.util.Properties

/**
 * `ReadFromHive` 的配置，键名对齐 Flink Hive connector 的常用选项。
 *
 * ```yaml
 * - type: ReadFromHive
 *   config:
 *     url: "jdbc:hive2://localhost:10000/default"
 *     user: hive
 *     database: default
 *     table: orders
 *     partitions: ["dt='2024-01-01'"]   # 留空则 SHOW PARTITIONS 自动发现
 *     fetch_size: 1000
 * ```
 *
 * schema 在构图阶段用 `SELECT ... WHERE 1 = 0` 的结果集元数据推断，
 * 与 `ReadFromJdbc` 走的是同一套类型映射——重构前 Hive 模块自己维护了一份更弱的映射，
 * 推不出来还会**静默回退成单列 `value`(STRING)**，读出来的数据完全对不上。
 *
 * @author wuya
 */
data class HiveReadConfig(
    val url: String = "",
    val user: String = "",
    val password: String = "",
    val database: String = "",
    val table: String = "",
    /** 显式分区谓词，例如 `dt='2024-01-01'`；留空则在构图阶段 `SHOW PARTITIONS` 发现。 */
    val partitions: List<String> = emptyList(),
    /** 额外的 WHERE 条件，与分区谓词 and 在一起。 */
    val where: String = "",
    /** 只读这些列；留空读全部。 */
    val columns: List<String> = emptyList(),
    /** 游标每次往返取多少行。 */
    val fetchSize: Int = 1000,
) : Serializable {

    fun validate() {
        require(url.isNotBlank()) { "url 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(fetchSize > 0) { "fetch_size 必须 > 0" }
    }

    val qualifiedTable: String
        get() = if (database.isNotBlank()) "$database.$table" else table

    fun dataSourceProperties(): Properties = hiveDataSourceProperties(url, user, password)

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * `WriteToHive` 的配置。
 *
 * ```yaml
 * - type: WriteToHive
 *   config:
 *     url: "jdbc:hive2://localhost:10000/default"
 *     table: orders
 *     batch_size: 1000
 * ```
 *
 * @author wuya
 */
data class HiveWriteConfig(
    val url: String = "",
    val user: String = "",
    val password: String = "",
    val database: String = "",
    val table: String = "",
    val batchSize: Int = 1000,
) : Serializable {

    fun validate() {
        require(url.isNotBlank()) { "url 不能为空" }
        require(table.isNotBlank()) { "table 不能为空" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
    }

    val qualifiedTable: String
        get() = if (database.isNotBlank()) "$database.$table" else table

    fun dataSourceProperties(): Properties = hiveDataSourceProperties(url, user, password)

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/** Hikari 需要的连接属性。用连接池而不是每个分区 `DriverManager.getConnection` 各开一条。 */
internal fun hiveDataSourceProperties(url: String, user: String, password: String): Properties = Properties().apply {
    this["jdbcUrl"] = url
    if (user.isNotBlank()) this["username"] = user
    if (password.isNotBlank()) this["password"] = password
    this["maximumPoolSize"] = "4"
}
