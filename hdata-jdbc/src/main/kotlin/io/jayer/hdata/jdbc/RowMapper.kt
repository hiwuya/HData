package io.jayer.hdata.jdbc

import org.apache.beam.sdk.values.Row
import java.io.Serializable
import java.sql.ResultSet

/**
 * @author wuya
 * @date 2022-08-10
 */
interface RowMapper : Serializable {

    fun mapRow(rs: ResultSet): Row
}