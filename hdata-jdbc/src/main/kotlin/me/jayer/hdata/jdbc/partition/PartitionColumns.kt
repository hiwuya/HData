package me.jayer.hdata.jdbc.partition

import me.jayer.hdata.jdbc.internal.JdbcColumn
import me.jayer.hdata.jdbc.internal.JdbcMetadata
import me.jayer.hdata.jdbc.internal.SelectSql
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.sql.Connection

/**
 * 选定的分区列。
 *
 * @author wuya
 * @date 2022-08-17
 */
data class PartitionColumn(
    /** 库里实际的列名，用它拼 SQL 才能兼容大小写敏感的库。 */
    val name: String,
    val converter: PartitionConverter<out Any>,
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 分区列的选取与校验。
 *
 * @author wuya
 * @date 2022-08-17
 */
object PartitionColumns {

    private val LOGGER = LoggerFactory.getLogger(PartitionColumns::class.java)

    /**
     * @param requested 用户指定的分区列，为空时自动从主键里挑
     * @return 选不出可用的分区列时返回 null，此时退化为单分区读
     */
    fun resolve(
        connection: Connection,
        table: String,
        columns: List<JdbcColumn>,
        requested: String,
        probe: SelectSql,
    ): PartitionColumn? {
        if (requested.isNotBlank()) {
            return resolveRequested(connection, columns, requested, probe)
        }
        return resolveFromPrimaryKey(connection, table, columns, probe)
    }

    private fun resolveRequested(
        connection: Connection,
        columns: List<JdbcColumn>,
        requested: String,
        probe: SelectSql,
    ): PartitionColumn {
        // 先精确匹配；H2 / PostgreSQL / Oracle 会规范化未加引号的标识符大小写，所以再退一步忽略大小写
        val column = columns.firstOrNull { it.label == requested }
            ?: columns.firstOrNull { it.label.equals(requested, ignoreCase = true) }
        requireNotNull(column) {
            "未知的分区列[$requested]，可选列: ${columns.map { it.label }}"
        }
        val converter = converterOf(column)
        requireNotNull(converter) {
            "分区列[$requested] 的类型 ${column.describe()} 不支持分区，" +
                "支持的类型: ${PartitionConverters.entries.map { it.type.javaObjectType.canonicalName }}"
        }
        return PartitionColumn(column.label, converter)
    }

    private fun resolveFromPrimaryKey(
        connection: Connection,
        table: String,
        columns: List<JdbcColumn>,
        probe: SelectSql,
    ): PartitionColumn? {
        val primaryKeys = JdbcMetadata.primaryKeyColumns(connection, table)
        if (primaryKeys.isEmpty()) {
            LOGGER.warn("表[{}] 没有主键、也没有指定 partition_column，将单分区读取", table)
            return null
        }
        // 复合主键取首列：它是索引前缀，按它切分区才走得动索引
        val name = primaryKeys.first()
        val column = columns.firstOrNull { it.label.equals(name, ignoreCase = true) }
        if (column == null) {
            LOGGER.warn("表[{}] 的主键列[{}] 不在查询结果里，将单分区读取", table, name)
            return null
        }
        val converter = converterOf(column)
        if (converter == null) {
            LOGGER.warn("表[{}] 的主键列 {} 类型不支持分区，将单分区读取", table, column.describe())
            return null
        }
        LOGGER.info("表[{}] 未指定 partition_column，自动使用主键首列: {}", table, column.label)
        return PartitionColumn(column.label, converter)
    }

    /**
     * 分区列若含 NULL，不会走 `col >= ? AND col < ?` 那类数值区间查询，而是交给读取端单独补一条
     * `col IS NULL` 查询（见 [me.jayer.hdata.jdbc.transform.JdbcPartitionedReadFn]），对齐 Trino
     * 把 NULL 行放进一个独立 split 的行为，不再静默丢数据。
     */

    private fun converterOf(column: JdbcColumn): PartitionConverter<out Any>? {
        val javaType = column.javaType() ?: return null
        return PartitionConverters.entries
            .firstOrNull { it.type.javaObjectType == javaType }
            ?.partitionConverter
    }
}
