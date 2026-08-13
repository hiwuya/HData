package me.jayer.hdata.jdbc.util

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import me.jayer.hdata.jdbc.JdbcColumnMeta
import me.jayer.hdata.jdbc.handler.AbstractListResultSetHandler
import me.jayer.hdata.jdbc.handler.AbstractListResultSetMetaDataHandler
import me.jayer.hdata.jdbc.statement.SelectStatement
import me.jayer.hdata.jdbc.type.JdbcTypeRegistry
import org.apache.beam.sdk.schemas.Schema
import java.sql.Connection
import java.sql.ResultSet
import java.sql.ResultSetMetaData
import java.util.*

/**
 * @author wuya
 * @date 2022-08-04
 */
object JdbcUtils {

    private val TABLE_NAME_REGEXP = "\\\$\\{(\\d+)-(\\d+)\\}".toRegex()

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
        return SqlRunner.query(
            connection,
            query,
            object :
                AbstractListResultSetMetaDataHandler<JdbcColumnMeta>() {
                override fun handleRow(metaData: ResultSetMetaData, index: Int) =
                    JdbcColumnMeta.from(metaData, index)
            })
    }

    fun getTableSchema(connection: Connection, table: String): List<JdbcColumnMeta> {
        return getQuerySchema(
            connection,
            SelectStatement(table = table, columns = listOf("*")).buildSql()
        )
    }

    fun getPrimaryKeys(connection: Connection, table: String): List<String> {
        return connection.metaData.getPrimaryKeys(connection.catalog, null, table).use { rs ->
            val handler = object : AbstractListResultSetHandler<String>() {
                override fun handleRow(rs: ResultSet): String {
                    return rs.getString("COLUMN_NAME")
                }
            }
            handler.handle(rs)
        }
    }

    fun queryPartitionRange(
        connection: Connection,
        statement: SelectStatement,
        partitionColumn: String,
    ): Pair<Any?, Any?> {
        val sql = statement.columns("min($partitionColumn)", "max($partitionColumn)").buildSql()
        return SqlRunner.query(
            connection,
            sql,
            object : AbstractListResultSetHandler<Pair<Any?, Any?>>() {
                override fun handleRow(rs: ResultSet): Pair<Any?, Any?> {
                    val min = rs.getObject(1)
                    val max = rs.getObject(2)
                    return Pair(min, max)
                }
            }).first()
    }

    fun resolveTables(tables: List<String>): List<String> {
        return tables.flatMap { table ->
            val matchResult = TABLE_NAME_REGEXP.find(table)
            if (matchResult != null) {
                val padLength = matchResult.groupValues[1].length
                val from = matchResult.groupValues[1].toInt()
                val to = matchResult.groupValues[2].toInt()
                require(from <= to) { "Invalid table range: $table, range from should be <= to, actual: from[$from] > to[$to]" }
                IntRange(from, to).map { index ->
                    TABLE_NAME_REGEXP.replace(table, index.toString().padStart(padLength, '0'))
                }
            } else {
                listOf(table)
            }
        }
    }
}