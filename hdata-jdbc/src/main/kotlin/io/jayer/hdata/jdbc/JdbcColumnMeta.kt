package io.jayer.hdata.jdbc

import java.sql.JDBCType
import java.sql.ResultSetMetaData
import kotlin.reflect.KClass

/**
 * @author wuya
 * @date 2022-08-12
 */
data class JdbcColumnMeta(
    val label: String,
    val type: JDBCType,
    val typeName: String,
    val typeClass: KClass<*>,
    val precision: Int,
    val scale: Int,
    val nullable: Boolean,
    val autoIncrement: Boolean,
    val signed: Boolean,
    val displaySize: Int,
) {
    companion object {
        fun from(metaData: ResultSetMetaData, index: Int): JdbcColumnMeta {
            return JdbcColumnMeta(
                label = metaData.getColumnLabel(index),
                type = JDBCType.valueOf(metaData.getColumnType(index)),
                typeName = metaData.getColumnTypeName(index),
                typeClass = Class.forName(metaData.getColumnClassName(index)).kotlin,
                precision = metaData.getPrecision(index),
                scale = metaData.getScale(index),
                nullable = metaData.isNullable(index) == ResultSetMetaData.columnNullable,
                autoIncrement = metaData.isAutoIncrement(index),
                signed = metaData.isSigned(index),
                displaySize = metaData.getColumnDisplaySize(index)
            )
        }
    }
}
