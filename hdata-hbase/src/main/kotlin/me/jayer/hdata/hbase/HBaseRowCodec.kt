package me.jayer.hdata.hbase

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Result
import java.io.Serializable

/**
 * Beam [Row] 与 HBase [Result] / [Put] 的互转。
 *
 * 从 DoFn 里拆出来有两个好处：列族/列名的字节只解析一次（重构前每行每列都 `Bytes.toBytes` 一遍），
 * 以及这段逻辑不再需要一个真的 HBase 连接才能测。
 *
 * @author wuya
 */
class HBaseRowCodec private constructor(
    private val rowkeyField: String,
    private val rowkeyFormat: RowkeyFormat,
    private val columns: List<Resolved>,
) : Serializable {

    /** 预解析好列族/列名的字节，避免逐行重复分配。 */
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
            "写 HBase 的行缺少 rowkey 字段[$rowkeyField]，现有字段: ${row.schema.fieldNames}"
        }
        val put = Put(rowkeyFormat.encode(row.getValue<Any?>(rowkeyField)))
        var cells = 0
        columns.forEach { resolved ->
            val name = resolved.column.fieldName
            if (!row.schema.hasField(name)) {
                return@forEach
            }
            val bytes = resolved.column.type.encode(resolved.column, row.getValue<Any?>(name))
            if (bytes != null) {
                put.addColumn(resolved.family, resolved.qualifier, bytes)
                cells++
            }
        }
        // HBase 拒收空 Put，与其让它在提交时报一句没头没尾的错，不如当场说清楚是哪一行
        require(cells > 0) {
            "rowkey[${row.getValue<Any?>(rowkeyField)}] 在 schema_fields 声明的列上全是 null（或这些字段压根不存在），" +
                "HBase 不接受空的 Put"
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
