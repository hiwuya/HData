package io.jayer.hdata.jdbc

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.sql.ResultSet

/**
 * @author wuya
 * @date 2022-08-10
 */
class BeamRowMapper(private val schema: Schema) : RowMapper {
    companion object {
        private const val serialVersionUID: Long = 1
    }

    private val fieldExtractors = mutableListOf<ResultSetFieldExtractor>()

    override fun mapRow(rs: ResultSet): Row {
        if (fieldExtractors.isEmpty()) {
            val metaData = rs.metaData
            fieldExtractors.addAll(IntRange(1, schema.fieldCount).map { i ->
                JdbcUtils.createResultSetFieldExtractor(metaData, i)
            })
        }

        val row = Row.withSchema(schema)
        for (i in 0 until schema.fieldCount) {
            val value = fieldExtractors[i].invoke(rs, i + 1)
            if (rs.wasNull()) {
                row.addValue(null)
            } else {
                row.addValue(value)
            }
        }

        return row.build()
    }
}