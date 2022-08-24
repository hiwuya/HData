package io.jayer.hdata.jdbc.handler

import io.jayer.hdata.jdbc.JdbcColumnMeta
import io.jayer.hdata.jdbc.type.JdbcTypeRegistry
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.io.Serializable
import java.sql.ResultSet
import java.sql.ResultSetMetaData

/**
 * @author wuya
 * @date 2022-08-12
 */
class RowHandler(private val schema: Schema) : ResultSetHandler<Row>, Serializable {

    companion object {
        private const val serialVersionUID: Long = 1
    }

    private val resultSetGetters = mutableListOf<JdbcTypeRegistry.ResultSetGetter>()

    override fun handle(rs: ResultSet): Row {
        if (resultSetGetters.isEmpty()) {
            val handler = object : AbstractListResultSetMetaDataHandler<JdbcTypeRegistry.ResultSetGetter>() {
                override fun handleRow(metaData: ResultSetMetaData, index: Int): JdbcTypeRegistry.ResultSetGetter {
                    return JdbcTypeRegistry.getResultSetGetter(JdbcColumnMeta.from(metaData, index))!!
                }
            }
            resultSetGetters.addAll(handler.handle(rs.metaData))
        }

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