package io.jayer.hdata.jdbc.handler

import java.sql.ResultSetMetaData
import java.sql.SQLException

/**
 * @author wuya
 * @date 2022-08-11
 */
abstract class AbstractListResultSetMetaDataHandler<T> : ResultSetMetaDataHandler<List<T>> {

    @Throws(SQLException::class)
    abstract fun handleRow(metaData: ResultSetMetaData, index: Int): T

    override fun handle(metaData: ResultSetMetaData): List<T> {
        return IntRange(1, metaData.columnCount).map { handleRow(metaData, it) }
    }
}