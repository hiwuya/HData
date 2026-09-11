package me.jayer.hdata.jdbc.internal

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.io.Serializable
import java.sql.ResultSet

/**
 * One row of a `ResultSet` -> a Beam `Row`.
 *
 * How each column is read is resolved at graph construction time ([TypeMappings.resolve]); at runtime we just read values in order.
 *
 * @author wuya
 * @date 2022-08-12
 */
class RowMapper(
    private val schema: Schema,
    private val readers: List<ResultSetReader>,
) : Serializable {

    init {
        require(schema.fieldCount == readers.size) {
            "schema field count [${schema.fieldCount}] does not match the number of readers [${readers.size}]"
        }
    }

    fun map(rs: ResultSet): Row {
        val builder = Row.withSchema(schema)
        for (index in 0 until schema.fieldCount) {
            val value = readers[index].read(rs, index + 1)
            // Primitive getters such as getInt/getLong return 0 for NULL, so only wasNull can tell them apart
            builder.addValue(if (rs.wasNull()) null else value)
        }
        return builder.build()
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * Beam `Row` -> parameters of a `PreparedStatement`.
 *
 * The write side is likewise resolved at graph construction time; before the refactor it looked up the registry and built a new lambda per row and column.
 */
class RowBinder(private val writers: List<PreparedStatementWriter>) : Serializable {

    fun bind(ps: java.sql.PreparedStatement, row: Row) {
        for (index in writers.indices) {
            writers[index].write(ps, index + 1, row.getValue<Any?>(index))
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1

        fun of(schema: Schema): RowBinder =
            RowBinder(schema.fields.map { TypeMappings.writerOf(it.type) })
    }
}
