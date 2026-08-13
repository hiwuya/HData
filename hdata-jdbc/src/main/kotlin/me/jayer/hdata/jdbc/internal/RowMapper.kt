package me.jayer.hdata.jdbc.internal

import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.io.Serializable
import java.sql.ResultSet

/**
 * `ResultSet` 的一行 -> Beam `Row`。
 *
 * 每列的读取方式在构图期就解析好了（[TypeMappings.resolve]），运行期只是按序取值。
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
            "schema 字段数[${schema.fieldCount}] 与读取器数量[${readers.size}] 不一致"
        }
    }

    fun map(rs: ResultSet): Row {
        val builder = Row.withSchema(schema)
        for (index in 0 until schema.fieldCount) {
            val value = readers[index].read(rs, index + 1)
            // getInt/getLong 这类原始类型取值遇到 NULL 会返回 0，必须靠 wasNull 才能分辨
            builder.addValue(if (rs.wasNull()) null else value)
        }
        return builder.build()
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * Beam `Row` -> `PreparedStatement` 的参数。
 *
 * 写入方式同样在构图期解析好；重构前是逐行逐列去查一次注册表并新建 lambda。
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
