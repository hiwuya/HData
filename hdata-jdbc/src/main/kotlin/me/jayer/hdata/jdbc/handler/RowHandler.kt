package me.jayer.hdata.jdbc.handler

import me.jayer.hdata.jdbc.type.ResultSetGetter
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.io.Serializable
import java.sql.ResultSet

/**
 * @author wuya
 * @date 2022-08-12
 */
class RowHandler(private val schema: Schema, private val resultSetGetters: List<ResultSetGetter>) :
    ResultSetHandler<Row>, Serializable {

    companion object {
        private const val serialVersionUID: Long = 1
    }

    override fun handle(rs: ResultSet): Row {
        val row = Row.withSchema(schema)
        for (i in 0 until schema.fieldCount) {
            val value = resultSetGetters[i].getResult(rs, i + 1)
            if (rs.wasNull()) {
                row.addValue(null)
            } else {
                row.addValue(value)
            }
        }

        return row.build()
    }
}