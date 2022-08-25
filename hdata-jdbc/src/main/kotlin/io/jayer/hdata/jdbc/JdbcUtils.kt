package io.jayer.hdata.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import io.jayer.hdata.jdbc.handler.AbstractListResultSetHandler
import io.jayer.hdata.jdbc.handler.AbstractListResultSetMetaDataHandler
import io.jayer.hdata.jdbc.type.JdbcTypeRegistry
import org.apache.beam.sdk.schemas.Schema
import java.sql.Connection
import java.sql.JDBCType
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.util.*

/**
 * @author wuya
 * @date 2022-08-04
 */
object JdbcUtils {

    fun createDataSource(properties: Properties): HikariDataSource {
        return HikariDataSource(HikariConfig(properties))
    }

    fun inferBeamSchema(columnMetas: List<JdbcColumnMeta>): Schema {
        return Schema.builder().addFields(columnMetas.map {
            val fieldType = JdbcTypeRegistry.getFieldType(it)
            requireNotNull(fieldType) { "Type ${it.typeName}[${it.typeClass}] is not supported" }
            Schema.Field.of(it.label, fieldType).withNullable(it.nullable)
        }).build()
    }

    fun getQuerySchema(connection: Connection, query: String): List<JdbcColumnMeta> {
        return SqlRunner.query(connection, query, object : AbstractListResultSetMetaDataHandler<JdbcColumnMeta>() {
            override fun handleRow(metaData: ResultSetMetaData, index: Int) = JdbcColumnMeta.from(metaData, index)
        })
    }

    fun getTableSchema(connection: Connection, table: String): List<JdbcColumnMeta> {
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
            it.type == JDBCType.INTEGER || it.type == JDBCType.BIGINT
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
    ): Pair<Any?, Any?> {
        var sql = "SELECT min($partitionColumn), max($partitionColumn) FROM $table"
        if (where.isNotBlank()) {
            sql += " WHERE $where"
        }

        return SqlRunner.query(connection, sql, object : AbstractListResultSetHandler<Pair<Any?, Any?>>() {
            override fun handleRow(rs: ResultSet): Pair<Any?, Any?> {
                val min = rs.getObject(1)
                val max = rs.getObject(2)
                return Pair(min, max)
            }
        }).first()
    }
}