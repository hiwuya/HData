package me.jayer.hdata.jdbc

import java.io.Serializable
import java.sql.JDBCType
import java.sql.ResultSetMetaData

/**
 * @author wuya
 * @date 2022-08-12
 */
data class JdbcColumnMeta(
    val label: String,
    val type: JDBCType,
    val typeName: String,
    val typeClass: String,
    val precision: Int,
    val scale: Int,
    val nullable: Boolean,
    val autoIncrement: Boolean,
    val signed: Boolean,
    val displaySize: Int,
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1

        fun from(metaData: ResultSetMetaData, index: Int): me.jayer.hdata.jdbc.JdbcColumnMeta {
            return me.jayer.hdata.jdbc.JdbcColumnMeta(
                label = metaData.getColumnLabel(index),
                type = JDBCType.valueOf(metaData.getColumnType(index)),
                typeName = metaData.getColumnTypeName(index),
                typeClass = metaData.getColumnClassName(index),
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
