package me.jayer.hdata.hbase

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import java.io.Serializable

/**
 * Converts between Beam [Row] and HBase [Result] or [Put].
 *
 * Precomputes family and qualifier bytes once and keeps the conversion testable without an HBase connection.
 *
 * @author wuya
 */
class HBaseRowCodec private constructor(
    private val rowkeyField: String,
    private val rowkeyFormat: RowkeyFormat,
    private val columns: List<Resolved>,
) : Serializable {

    /** Precomputed family and qualifier bytes to avoid per-row allocation. */
    private class Resolved(
        val column: HBaseColumn,
        val family: ByteArray,
        val qualifier: ByteArray,
    ) : Serializable {
        companion object {
            private const val serialVersionUID: Long = 1
        }
    }

    val schema: Schema = buildReadSchema(rowkeyField, rowkeyFormat, columns.map { it.column })

    fun toRow(result: Result): Row {
        val builder = Row.withSchema(schema).addValue(rowkeyFormat.decode(result.row))
        columns.forEach { resolved ->
            builder.addValue(resolved.column.type.decode(resolved.column, result.getValue(resolved.family, resolved.qualifier)))
        }
        return builder.build()
    }

    fun toPut(row: Row): Put {
        require(row.schema.hasField(rowkeyField)) {
            "row written to HBase is missing rowkey field[$rowkeyField]; available fields: ${row.schema.fieldNames}"
        }
        val put = Put(rowkeyFormat.encode(row.getValue<Any?>(rowkeyField)))
        var cells = 0
        columns.forEach { resolved ->
            val name = resolved.column.fieldName
            require(row.schema.hasField(name)) {
                "row written to HBase is missing schema_fields field[$name]; available fields: ${row.schema.fieldNames}"
            }
            val bytes = resolved.column.type.encode(resolved.column, row.getValue<Any?>(name))
            if (bytes != null) {
                put.addColumn(resolved.family, resolved.qualifier, bytes)
                cells++
            }
        }
        // HBase rejects empty Put requests; report the offending row before submission.
        require(cells > 0) {
            "rowkey[${row.getValue<Any?>(rowkeyField)}] has null values for every schema_fields column; " +
                "HBase does not accept an empty Put"
        }
        return put
    }

    companion object {
        private const val serialVersionUID: Long = 1

        fun of(rowkeyField: String, rowkeyFormat: String, schemaFields: List<String>?, defaultFamily: String): HBaseRowCodec {
            val columns = parseColumns(schemaFields, defaultFamily)
            return HBaseRowCodec(
                rowkeyField,
                RowkeyFormat.of(rowkeyFormat),
                columns.map { Resolved(it, it.familyBytes, it.qualifierBytes) },
            )
        }
    }
}
