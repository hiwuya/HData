package io.jayer.hdata.jdbc

import java.sql.ResultSetMetaData

/**
 * @author wuya
 * @date 2022-08-12
 */
data class Column(
    val label: String,
    val type: Int,
    val typeName: String,
    val className: String,
    val precision: Int,
    val scale: Int,
    val nullable: Boolean,
    val autoIncrement: Boolean,
    val signed: Boolean,
    val displaySize: Int,
) {
    companion object {
        fun from(metaData: ResultSetMetaData, index: Int): Column {
            return Column(
                label = metaData.getColumnLabel(index),
                type = metaData.getColumnType(index),
                typeName = metaData.getColumnTypeName(index),
                className = metaData.getColumnClassName(index),
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
