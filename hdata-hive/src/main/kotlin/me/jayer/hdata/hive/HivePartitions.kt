package me.jayer.hdata.hive

import org.slf4j.LoggerFactory
import java.sql.Connection

/**
 * 分区发现，以及把 `SHOW PARTITIONS` 的输出翻译成 SQL 谓词。
 *
 * `SHOW PARTITIONS db.t` 返回的是 `dt=2024-01-01/hr=01` 这种**分区规格**，
 * 重构前的代码把它原样拼进 `WHERE`：
 *
 * ```sql
 * SELECT ... FROM t WHERE dt=2024-01-01/hr=01
 * ```
 *
 * 这既不是合法 SQL（日期没加引号会被当成减法），多级分区的 `/` 更是直接语法错误。
 *
 * @author wuya
 */
object HivePartitions {

    private val LOGGER = LoggerFactory.getLogger(HivePartitions::class.java)

    /**
     * `dt=2024-01-01/hr=01` -> `dt='2024-01-01' AND hr='01'`。
     *
     * 值一律加引号：Hive 对数字分区列会做隐式转换，加引号是安全的；不加引号则字符串分区必错。
     * 值里的单引号做转义，避免分区名带引号时把谓词拼坏。
     */
    fun toPredicate(spec: String): String = spec.split('/')
        .filter { it.isNotBlank() }
        .joinToString(" AND ") { part ->
            val idx = part.indexOf('=')
            require(idx > 0) { "无法解析分区规格片段: $part（完整规格: $spec）" }
            val column = part.substring(0, idx).trim()
            val value = part.substring(idx + 1).trim().replace("'", "''")
            "$column = '$value'"
        }

    /**
     * 列出要读的分区谓词。显式配置优先；否则 `SHOW PARTITIONS`；表没分区就返回一个空谓词（读整表）。
     *
     * 重构前这里是 `runCatching { ... }.getOrDefault(emptyList())`——**任何异常都被吞掉**，
     * 然后回落成 `listOf("")` 读整张表。权限不足或表名写错时，一个本该按分区裁剪的读取
     * 会静默变成全表扫描，只有在账单或时长上才看得出来。
     */
    fun discover(connection: Connection, config: HiveReadConfig): List<String> {
        if (config.partitions.isNotEmpty()) {
            return config.partitions
        }
        val specs = try {
            connection.createStatement().use { st ->
                st.executeQuery("SHOW PARTITIONS ${config.qualifiedTable}").use { rs ->
                    generateSequence { if (rs.next()) rs.getString(1) else null }
                        .filterNotNull()
                        .filter { it.isNotBlank() }
                        .toList()
                }
            }
        } catch (e: Exception) {
            // 非分区表上 SHOW PARTITIONS 本来就会报错，这是唯一可以接受的失败
            LOGGER.info("SHOW PARTITIONS {} 失败（多半是非分区表），按整表读取: {}", config.qualifiedTable, e.message)
            return listOf("")
        }
        if (specs.isEmpty()) {
            LOGGER.info("表[{}] 没有分区，按整表读取", config.qualifiedTable)
            return listOf("")
        }
        LOGGER.info("表[{}] 发现 {} 个分区", config.qualifiedTable, specs.size)
        return specs.map { toPredicate(it) }
    }
}
