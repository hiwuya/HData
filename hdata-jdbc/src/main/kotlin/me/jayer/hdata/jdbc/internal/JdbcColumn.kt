package me.jayer.hdata.jdbc.internal

import java.io.Serializable
import java.sql.JDBCType
import java.sql.ResultSetMetaData

/**
 * JDBC metadata of one column.
 *
 * @author wuya
 * @date 2022-08-12
 */
data class JdbcColumn(
    val label: String,
    val type: JDBCType,
    /** Database type name reported by the driver, for example MySQL's `BIGINT UNSIGNED` or PostgreSQL's `_int4` array. */
    val typeName: String,
    /** Fully qualified Java type name reported by the driver; type mapping dispatches mainly on it. */
    val typeClass: String,
    val precision: Int,
    val scale: Int,
    val nullable: Boolean,
    val autoIncrement: Boolean,
    val signed: Boolean,
) : Serializable {

    /** Java class reported by the driver; returns null instead of throwing when it is unavailable (custom types, for example). */
    fun javaType(): Class<*>? = runCatching { Class.forName(typeClass) }.getOrNull()

    fun describe(): String = "$label($typeName/$typeClass)"

    companion object {
        private const val serialVersionUID: Long = 1

        fun from(metaData: ResultSetMetaData, index: Int): JdbcColumn = JdbcColumn(
            label = metaData.getColumnLabel(index),
            type = jdbcTypeOf(metaData.getColumnType(index)),
            typeName = metaData.getColumnTypeName(index) ?: "",
            typeClass = metaData.getColumnClassName(index) ?: "",
            precision = metaData.getPrecision(index),
            scale = metaData.getScale(index),
            // columnNullableUnknown is treated as nullable: guessing non-nullable makes Beam fail at runtime on a null value
            nullable = metaData.isNullable(index) != ResultSetMetaData.columnNoNulls,
            autoIncrement = metaData.isAutoIncrement(index),
            signed = metaData.isSigned(index),
        )

        /** Drivers may report type codes outside the JDBC spec (PostgreSQL's -100, for example); degrade to OTHER in that case. */
        private fun jdbcTypeOf(code: Int): JDBCType =
            runCatching { JDBCType.valueOf(code) }.getOrDefault(JDBCType.OTHER)
    }
}
