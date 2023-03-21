package me.jayer.hdata.jdbc.type

import java.io.Serializable
import java.sql.ResultSet
import java.sql.SQLException

/**
 * @author wuya
 * @date 2022-08-25
 */
fun interface ResultSetGetter : Serializable {
    @Throws(SQLException::class)
    fun getResult(rs: ResultSet, index: Int): Any?
}