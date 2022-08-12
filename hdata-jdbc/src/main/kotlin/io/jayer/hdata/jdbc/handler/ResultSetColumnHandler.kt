package io.jayer.hdata.jdbc.handler

import java.sql.ResultSet
import java.sql.SQLException

/**
 * @author wuya
 * @date 2022-08-11
 */
interface ResultSetColumnHandler<T> {

    @Throws(SQLException::class)
    fun handle(rs: ResultSet, index: Int): T

}