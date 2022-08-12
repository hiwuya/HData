package io.jayer.hdata.jdbc.handler

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.io.Serializable
import java.sql.ResultSet

/**
 * @author wuya
 * @date 2022-08-12
 */
class RowHandler(val schema: Schema) : ResultSetHandler<Row>, Serializable {

    companion object {
        private const val serialVersionUID: Long = 1
    }

    private val columnHandlers = mutableListOf<ResultSetColumnHandler<Any?>>()

    override fun handle(rs: ResultSet): Row {
        if (columnHandlers.isEmpty()) {
            columnHandlers.addAll(ColumnsHandler().handle(rs.metaData))
        }

        val row = Row.withSchema(schema)
        for (i in 0 until schema.fieldCount) {
            val value = columnHandlers[i].handle(rs, i + 1)
            if (rs.wasNull()) {
                row.addValue(null)
            } else {
                row.addValue(value)
            }
        }

        return row.build()
    }
}