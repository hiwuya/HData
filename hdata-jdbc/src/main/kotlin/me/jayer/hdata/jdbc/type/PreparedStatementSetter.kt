package me.jayer.hdata.jdbc.type

import org.apache.beam.sdk.values.Row
import java.io.Serializable
import java.sql.PreparedStatement
import java.sql.SQLException

/**
 * @author wuya
 * @date 2022-08-25
 */
fun interface PreparedStatementSetter : Serializable {
    @Throws(SQLException::class)
    fun setParameter(ps: PreparedStatement, row: Row, index: Int)
}