package me.jayer.hdata.jdbc.partition

import me.jayer.hdata.jdbc.internal.JdbcColumn
import me.jayer.hdata.jdbc.internal.JdbcMetadata
import me.jayer.hdata.jdbc.internal.SelectSql
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.sql.Connection

/**
 * The selected partition column.
 *
 * @author wuya
 * @date 2022-08-17
 */
data class PartitionColumn(
    /** The actual column name in the database; using it to build SQL keeps case-sensitive databases working. */
    val name: String,
    val converter: PartitionConverter<out Any>,
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * Selection and validation of the partition column.
 *
 * @author wuya
 * @date 2022-08-17
 */
object PartitionColumns {

    private val LOGGER = LoggerFactory.getLogger(PartitionColumns::class.java)

    /**
     * @param requested the partition column given by the user; when empty one is picked from the primary key automatically
     * @return null when no usable partition column can be selected, in which case we degrade to a single-partition read
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
        // Try an exact match first; H2 / PostgreSQL / Oracle normalize the case of unquoted identifiers, so also retry case-insensitively
        val column = columns.firstOrNull { it.label == requested }
            ?: columns.firstOrNull { it.label.equals(requested, ignoreCase = true) }
        requireNotNull(column) {
            "unknown partition column [$requested], available columns: ${columns.map { it.label }}"
        }
        val converter = converterOf(column)
        requireNotNull(converter) {
            "the type ${column.describe()} of partition column [$requested] does not support partitioning, " +
                "supported types: ${PartitionConverters.entries.map { it.type.javaObjectType.canonicalName }}"
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
            LOGGER.warn("table [{}] has no primary key and no partition_column was given; reading with a single partition", table)
            return null
        }
        // For a composite primary key take the first column: it is the index prefix, so partitioning on it can use the index
        val name = primaryKeys.first()
        val column = columns.firstOrNull { it.label.equals(name, ignoreCase = true) }
        if (column == null) {
            LOGGER.warn("the primary key column [{}] of table [{}] is not in the query result; reading with a single partition", table, name)
            return null
        }
        val converter = converterOf(column)
        if (converter == null) {
            LOGGER.warn("the type of primary key column {} of table [{}] does not support partitioning; reading with a single partition", table, column.describe())
            return null
        }
        LOGGER.info("no partition_column given for table [{}], automatically using the first primary key column: {}", table, column.label)
        return PartitionColumn(column.label, converter)
    }

    /**
     * When the partition column contains NULL, we do not use a numeric range query such as `col >= ? AND col < ?`; instead the read
     * side issues a separate `col IS NULL` query (see [me.jayer.hdata.jdbc.transform.JdbcPartitionedReadFn]), matching Trino's
     * behaviour of putting NULL rows into a dedicated split, so no data is silently lost.
     */

    private fun converterOf(column: JdbcColumn): PartitionConverter<out Any>? {
        val javaType = column.javaType() ?: return null
        return PartitionConverters.entries
            .firstOrNull { it.type.javaObjectType == javaType }
            ?.partitionConverter
    }
}
