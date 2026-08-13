package me.jayer.hdata.jdbc.internal

import me.jayer.hdata.core.extension.toSqlDate
import me.jayer.hdata.core.extension.toSqlTime
import me.jayer.hdata.core.extension.toTimestamp
import me.jayer.hdata.core.type.FieldTypes
import org.apache.beam.sdk.schemas.Schema
import java.io.Serializable
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.Date
import java.sql.JDBCType
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.SQLXML
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

/** 从 [ResultSet] 的第 [index] 列取值，null 由调用方按 `wasNull` 判定。 */
fun interface ResultSetReader : Serializable {
    fun read(rs: ResultSet, index: Int): Any?
}

/** 把值写进 [PreparedStatement] 的第 [index] 个占位符（1 起）。 */
fun interface PreparedStatementWriter : Serializable {
    fun write(ps: PreparedStatement, index: Int, value: Any?)
}

/**
 * 单值换算。必须是可序列化的接口而不是普通 Kotlin lambda：
 * [PreparedStatementWriter] 会捕获它并随 DoFn 一起下发，捕获一个 `Function1` 会让整个 DoFn 无法序列化。
 */
fun interface ValueConverter : Serializable {
    fun convert(value: Any): Any
}

/**
 * JDBC 类型与 Beam 类型的映射表。
 *
 * 重构前这里是一个可变的全局注册表：`init` 里往 `MutableList` 塞规则，`registerJdbcType` 对外
 * 公开却没人调用，每次查类型都要线性扫一遍并对每条规则做一次 `Class.forName`。
 * 现在改成不可变的规则表，并且**每列只解析一次**，解析结果（[ColumnCodec]）随 DoFn 一起序列化下发，
 * 运行期不再有查表开销。
 *
 * @author wuya
 * @date 2022-08-23
 */
object TypeMappings {

    /** 一列解析好的读写方式。 */
    data class ColumnCodec(
        val fieldType: Schema.FieldType,
        val reader: ResultSetReader,
    ) : Serializable {
        companion object {
            private const val serialVersionUID: Long = 1
        }
    }

    private class Rule(
        val matches: (JdbcColumn) -> Boolean,
        val fieldType: (JdbcColumn) -> Schema.FieldType,
        val reader: (JdbcColumn) -> ResultSetReader,
    )

    /** 大对象读完要 free，否则某些驱动会一直占着服务端资源。 */
    private val ClobReader = ResultSetReader { rs, i ->
        rs.getClob(i)?.let { clob ->
            try {
                clob.getSubString(1, clob.length().toInt())
            } finally {
                runCatching { clob.free() }
            }
        }
    }

    private val BlobReader = ResultSetReader { rs, i ->
        rs.getBlob(i)?.let { blob ->
            try {
                blob.getBytes(1, blob.length().toInt())
            } finally {
                runCatching { blob.free() }
            }
        }
    }

    private fun rule(
        matches: (JdbcColumn) -> Boolean,
        fieldType: Schema.FieldType,
        reader: ResultSetReader,
    ) = Rule(matches, { fieldType }, { reader })

    private fun byClass(
        type: Class<*>,
        fieldType: Schema.FieldType,
        reader: ResultSetReader,
    ) = rule({ it.javaType() == type }, fieldType, reader)

    /**
     * 规则按顺序匹配，先命中者胜。
     *
     * MySQL 用 `BIT` 同时表示位串和布尔，只能靠 precision 区分，所以这两条要排在按类查找之前。
     * 见 https://dev.mysql.com/doc/connector-j/en/connector-j-reference-type-conversions.html
     */
    private val RULES: List<Rule> = listOf(
        rule({ it.type == JDBCType.BIT && it.precision > 1 }, FieldTypes.BYTES) { rs, i -> rs.getBytes(i) },
        rule({ it.type == JDBCType.BIT && it.precision <= 1 }, FieldTypes.BOOLEAN) { rs, i -> rs.getBoolean(i) },

        // PostgreSQL / H2 / Oracle 等会直接上报标准的 BOOLEAN
        byClass(java.lang.Boolean::class.java, FieldTypes.BOOLEAN) { rs, i -> rs.getBoolean(i) },
        byClass(java.lang.Byte::class.java, FieldTypes.BYTE) { rs, i -> rs.getByte(i) },
        byClass(java.lang.Short::class.java, FieldTypes.INT16) { rs, i -> rs.getShort(i) },
        byClass(java.lang.Integer::class.java, FieldTypes.INT32) { rs, i -> rs.getInt(i) },
        byClass(java.lang.Long::class.java, FieldTypes.INT64) { rs, i -> rs.getLong(i) },
        byClass(java.lang.Float::class.java, FieldTypes.FLOAT) { rs, i -> rs.getFloat(i) },
        byClass(java.lang.Double::class.java, FieldTypes.DOUBLE) { rs, i -> rs.getDouble(i) },
        byClass(BigInteger::class.java, FieldTypes.DECIMAL) { rs, i -> rs.getBigDecimal(i) },
        byClass(BigDecimal::class.java, FieldTypes.DECIMAL) { rs, i -> rs.getBigDecimal(i) },
        byClass(Date::class.java, FieldTypes.DATE) { rs, i -> rs.getDate(i)?.toLocalDate() },
        byClass(LocalDate::class.java, FieldTypes.DATE) { rs, i -> rs.getDate(i)?.toLocalDate() },
        byClass(Time::class.java, FieldTypes.TIME) { rs, i -> rs.getTime(i)?.toLocalTime() },
        byClass(LocalTime::class.java, FieldTypes.TIME) { rs, i -> rs.getTime(i)?.toLocalTime() },
        // 驱动报 LocalDateTime 的（MySQL 的 DATETIME）当墙上时间处理，不牵扯时区
        byClass(LocalDateTime::class.java, FieldTypes.DATETIME) { rs, i -> rs.getTimestamp(i)?.toLocalDateTime() },
        // 驱动报 java.sql.Timestamp 的按时间点处理。注意 TIMESTAMP WITHOUT TIME ZONE 也会走到这里，
        // 此时 toInstant() 按 **JVM 默认时区** 解释墙上时间——跨时区同步要保证两端 JVM 时区一致
        byClass(Timestamp::class.java, FieldTypes.TIMESTAMP) { rs, i -> rs.getTimestamp(i)?.toInstant() },

        byClass(SQLXML::class.java, FieldTypes.STRING) { rs, i ->
            val xml = rs.getSQLXML(i)
            try {
                xml?.string
            } finally {
                xml?.free()
            }
        },

        // MySQL 把 TEXT/CLOB 的 class 报成 java.lang.String，H2 / PostgreSQL 则报 java.sql.Clob
        byClass(java.sql.Clob::class.java, FieldTypes.STRING, ClobReader),
        byClass(java.sql.NClob::class.java, FieldTypes.STRING, ClobReader),
        byClass(java.sql.Blob::class.java, FieldTypes.BYTES, BlobReader),

        Rule(
            { it.javaType() == java.lang.String::class.java },
            { FieldTypes.STRING },
            { column ->
                when (column.type) {
                    JDBCType.CLOB, JDBCType.NCLOB -> ClobReader

                    JDBCType.NCHAR, JDBCType.NVARCHAR, JDBCType.LONGNVARCHAR ->
                        ResultSetReader { rs, i -> rs.getNString(i) }

                    else -> ResultSetReader { rs, i -> rs.getString(i) }
                }
            },
        ),

        Rule(
            { it.javaType() == ByteArray::class.java },
            { FieldTypes.BYTES },
            { column ->
                when (column.type) {
                    JDBCType.BLOB, JDBCType.LONGVARBINARY -> BlobReader

                    else -> ResultSetReader { rs, i -> rs.getBytes(i) }
                }
            },
        ),
    )

    /**
     * 解析一列的读写方式。
     *
     * 数组列走单独的分支：元素类型要从数组本身的元数据推断，而不是像重构前那样拿
     * `JDBCType.valueOf(columnMeta.typeName)` 去猜——PostgreSQL 的数组类型名是 `_int4` 这种，
     * 那句代码必然抛 IllegalArgumentException。
     */
    fun resolve(column: JdbcColumn): ColumnCodec? {
        if (column.type == JDBCType.ARRAY) {
            return resolveArray(column)
        }
        val rule = RULES.firstOrNull { it.matches(column) } ?: return null
        return ColumnCodec(rule.fieldType(column), rule.reader(column))
    }

    private fun resolveArray(column: JdbcColumn): ColumnCodec? {
        // 数组元素的类型只能在运行期从 java.sql.Array 的元数据拿到，构图期先按字符串兜底，
        // 真正的元素读取交给 ArrayReader 在拿到 ResultSet 时再决定。
        val elementType = arrayElementFieldType(column) ?: return null
        return ColumnCodec(Schema.FieldType.array(elementType), ArrayReader(elementType))
    }

    /** 从 `_int4` / `INTEGER ARRAY` 这类数组类型名里推断元素类型。 */
    private fun arrayElementFieldType(column: JdbcColumn): Schema.FieldType? {
        val name = column.typeName.removePrefix("_").substringBefore(" ").uppercase()
        return when (name) {
            "BOOL", "BOOLEAN" -> FieldTypes.BOOLEAN
            "INT2", "SMALLINT" -> FieldTypes.INT16
            "INT4", "INT", "INTEGER" -> FieldTypes.INT32
            "INT8", "BIGINT" -> FieldTypes.INT64
            "FLOAT4", "REAL" -> FieldTypes.FLOAT
            "FLOAT8", "DOUBLE", "DOUBLE PRECISION" -> FieldTypes.DOUBLE
            "NUMERIC", "DECIMAL" -> FieldTypes.DECIMAL
            "DATE" -> FieldTypes.DATE
            "TIME" -> FieldTypes.TIME
            "TIMESTAMP" -> FieldTypes.TIMESTAMP
            "TEXT", "VARCHAR", "CHAR", "BPCHAR", "CHARACTER VARYING" -> FieldTypes.STRING
            else -> null
        }
    }

    /**
     * 按 JDBC 约定读数组：[java.sql.Array.getResultSet] 每行一个元素，第 1 列是下标，第 2 列才是值。
     * 重构前这段代码遍历的是**列**而不是行，而且最后返回的是一个 handler 对象，读出来的永远不对。
     */
    private class ArrayReader(private val elementType: Schema.FieldType) : ResultSetReader {

        override fun read(rs: ResultSet, index: Int): Any? {
            val array = rs.getArray(index) ?: return null
            return try {
                array.resultSet.use { elements ->
                    val elementColumn = JdbcColumn.from(elements.metaData, 2)
                    val reader = resolve(elementColumn)?.reader
                        ?: throw UnsupportedOperationException("数组元素类型 ${elementColumn.describe()} 暂不支持")
                    buildList {
                        while (elements.next()) {
                            val value = reader.read(elements, 2)
                            add(if (elements.wasNull()) null else value)
                        }
                    }
                }
            } finally {
                runCatching { array.free() }
            }
        }

        companion object {
            private const val serialVersionUID: Long = 1
        }
    }

    /**
     * 按 Beam 字段类型解析出写入方式。写入端每列只解析一次，不再像重构前那样逐行逐列查表。
     */
    fun writerOf(fieldType: Schema.FieldType): PreparedStatementWriter {
        if (fieldType.typeName == Schema.TypeName.ARRAY) {
            val elementType = fieldType.collectionElementType!!
            val sqlTypeName = sqlTypeNameOf(elementType)
            val convert = valueConverterOf(elementType)
            return PreparedStatementWriter { ps, index, value ->
                if (value == null) {
                    ps.setNull(index, java.sql.Types.ARRAY)
                } else {
                    @Suppress("UNCHECKED_CAST")
                    val elements = (value as Collection<Any?>).map { it?.let(convert::convert) }.toTypedArray()
                    ps.setArray(index, ps.connection.createArrayOf(sqlTypeName, elements))
                }
            }
        }
        val convert = valueConverterOf(fieldType)
        return PreparedStatementWriter { ps, index, value ->
            ps.setObject(index, value?.let(convert::convert))
        }
    }

    /**
     * Beam 侧的逻辑类型是 `java.time` 家族，绝大多数驱动只认 `java.sql` 家族，这里做一次换算。
     */
    private fun valueConverterOf(fieldType: Schema.FieldType): ValueConverter =
        when (fieldType.withNullable(false)) {
            FieldTypes.DATE -> ValueConverter { (it as LocalDate).toSqlDate() }
            FieldTypes.TIME -> ValueConverter { (it as LocalTime).toSqlTime() }
            FieldTypes.DATETIME -> ValueConverter { (it as LocalDateTime).toTimestamp() }
            FieldTypes.TIMESTAMP -> ValueConverter { (it as Instant).toTimestamp() }
            else -> ValueConverter { it }
        }

    /**
     * [java.sql.Connection.createArrayOf] 要的是数据库认识的类型名。这里给标准 SQL 名，
     * 驱动不认时会抛 SQLException，比重构前传 Beam 的 `INT32` 至少还有一线希望。
     */
    private fun sqlTypeNameOf(fieldType: Schema.FieldType): String = when (fieldType.withNullable(false)) {
        FieldTypes.BOOLEAN -> "BOOLEAN"
        FieldTypes.BYTE, FieldTypes.INT16 -> "SMALLINT"
        FieldTypes.INT32 -> "INTEGER"
        FieldTypes.INT64 -> "BIGINT"
        FieldTypes.FLOAT -> "REAL"
        FieldTypes.DOUBLE -> "DOUBLE PRECISION"
        FieldTypes.DECIMAL -> "NUMERIC"
        FieldTypes.DATE -> "DATE"
        FieldTypes.TIME -> "TIME"
        FieldTypes.DATETIME, FieldTypes.TIMESTAMP -> "TIMESTAMP"
        else -> "VARCHAR"
    }
}
