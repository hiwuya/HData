package io.jayer.hdata.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.jayer.hdata.jdbc.handler.AbstractListResultSetHandler
import io.jayer.hdata.jdbc.handler.TableSchemaHandler
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import java.sql.Connection
import java.sql.ResultSet
import java.sql.Types
import java.util.*

/**
 * @author wuya
 * @date 2022-08-04
 */
object JdbcUtils {

    fun createDataSource(properties: Properties): HikariDataSource {
        return HikariDataSource(HikariConfig(properties))
    }

    fun inferBeamSchema(connection: Connection, query: String): Schema {
        return SchemaConverter.convertToBeamSchema(getQuerySchema(connection, query))
    }

    fun getQuerySchema(connection: Connection, query: String): List<Column> {
        return SqlRunner.query(connection, query, TableSchemaHandler())
    }

    fun getTableSchema(connection: Connection, table: String): List<Column> {
        return getQuerySchema(connection, "SELECT * FROM $table WHERE 1 < 0")
    }

    fun getPrimaryKeys(connection: Connection, table: String): List<Pair<String, Int>> {
        return connection.metaData.getPrimaryKeys(connection.catalog, null, table).use { rs ->
            val handler = object : AbstractListResultSetHandler<Pair<String, Int>>() {
                override fun handleRow(rs: ResultSet): Pair<String, Int> {
                    val seq = rs.getShort("KEY_SEQ").toInt()
                    val name = rs.getString("COLUMN_NAME")
                    return Pair(name, seq)
                }
            }
            handler.handle(rs)
        }
    }

    fun getNumericPrimaryKey(connection: Connection, table: String): String? {
        val numericColumns = getTableSchema(connection, table).filter {
            it.type == Types.INTEGER || it.type == Types.BIGINT
        }.map { it.label }

        return getPrimaryKeys(connection, table).filter { it.second == 1 && it.first in numericColumns }
            .map { it.first }
            .firstOrNull()
    }

    fun queryPartitionRange(
        connection: Connection,
        table: String,
        where: String,
        partitionColumn: String,
        partitionHelper: PartitionHelpers.PartitionHelper
    ): OffsetRange? {
        var sql = "SELECT min($partitionColumn), max($partitionColumn) FROM $table"
        if (where.isNotBlank()) {
            sql += " WHERE $where"
        }

        return SqlRunner.query(connection, sql, object : AbstractListResultSetHandler<OffsetRange?>() {
            override fun handleRow(rs: ResultSet): OffsetRange? {
                return partitionHelper.mapToOffsetRange(rs)
            }
        }).first()
    }
}