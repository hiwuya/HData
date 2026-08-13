package me.jayer.hdata.jdbc.internal

import java.io.Serializable
import java.sql.JDBCType
import java.sql.ResultSetMetaData

/**
 * 一列的 JDBC 元数据。
 *
 * @author wuya
 * @date 2022-08-12
 */
data class JdbcColumn(
    val label: String,
    val type: JDBCType,
    /** 驱动上报的数据库类型名，例如 MySQL 的 `BIGINT UNSIGNED`、PostgreSQL 数组的 `_int4`。 */
    val typeName: String,
    /** 驱动上报的 Java 类型全名，类型映射主要按它来分派。 */
    val typeClass: String,
    val precision: Int,
    val scale: Int,
    val nullable: Boolean,
    val autoIncrement: Boolean,
    val signed: Boolean,
) : Serializable {

    /** 驱动上报的 Java 类，取不到时（例如自定义类型）返回 null 而不是抛出。 */
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
            // columnNullableUnknown 按可空处理：猜错成非空的话，Beam 会在运行期因为 null 值直接失败
            nullable = metaData.isNullable(index) != ResultSetMetaData.columnNoNulls,
            autoIncrement = metaData.isAutoIncrement(index),
            signed = metaData.isSigned(index),
        )

        /** 驱动可能上报 JDBC 规范之外的 type code（例如 PostgreSQL 的 -100），此时退化为 OTHER。 */
        private fun jdbcTypeOf(code: Int): JDBCType =
            runCatching { JDBCType.valueOf(code) }.getOrDefault(JDBCType.OTHER)
    }
}
