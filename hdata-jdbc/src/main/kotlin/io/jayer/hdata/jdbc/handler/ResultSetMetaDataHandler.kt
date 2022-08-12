package io.jayer.hdata.jdbc.handler

import java.sql.ResultSetMetaData
import java.sql.SQLException

/**
 * @author wuya
 * @date 2022-08-11
 */
interface ResultSetMetaDataHandler<T> {

    @Throws(SQLException::class)
    fun handle(metaData: ResultSetMetaData): T
}