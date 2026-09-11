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

/** Reads the value of column [index] from [ResultSet]; the caller decides nullability via `wasNull`. */
fun interface ResultSetReader : Serializable {
    fun read(rs: ResultSet, index: Int): Any?
}

/** Writes a value into placeholder [index] of [PreparedStatement] (1-based). */
fun interface PreparedStatementWriter : Serializable {
    fun write(ps: PreparedStatement, index: Int, value: Any?)
}

/**
 * Single-value conversion. It must be a serializable interface rather than a plain Kotlin lambda:
 * [PreparedStatementWriter] captures it and ships it along with the DoFn, and capturing a `Function1` makes the whole DoFn unserializable.
 */
fun interface ValueConverter : Serializable {
    fun convert(value: Any): Any
}

/**
 * Mapping table between JDBC types and Beam types.
 *
 * Before the refactor this was a mutable global registry: `init` pushed rules into a `MutableList`, `registerJdbcType` was
 * public yet nobody called it, and every type lookup linearly scanned the rules and did a `Class.forName` per rule.
 * It is now an immutable rule table that is **resolved once per column**; the result ([ColumnCodec]) is serialized and shipped
 * with the DoFn, so there is no lookup cost at runtime.
 *
 * @author wuya
 * @date 2022-08-23
 */
object TypeMappings {

    /** Resolved read/write strategy for one column. */
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

    /** Large objects must be freed after reading, otherwise some drivers keep holding server-side resources. */
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
     * Rules are matched in order, first match wins.
     *
     * MySQL uses `BIT` for both bit strings and booleans and only precision can tell them apart, so these two rules must come before the by-class lookup.
     * See https://dev.mysql.com/doc/connector-j/en/connector-j-reference-type-conversions.html
     */
    private val RULES: List<Rule> = listOf(
        rule({ it.type == JDBCType.BIT && it.precision > 1 }, FieldTypes.BYTES) { rs, i -> rs.getBytes(i) },
        rule({ it.type == JDBCType.BIT && it.precision <= 1 }, FieldTypes.BOOLEAN) { rs, i -> rs.getBoolean(i) },

        // PostgreSQL / H2 / Oracle and others report the standard BOOLEAN directly
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
        // Drivers reporting LocalDateTime (MySQL's DATETIME) are treated as wall-clock time, with no time zone involved
        byClass(LocalDateTime::class.java, FieldTypes.DATETIME) { rs, i -> rs.getTimestamp(i)?.toLocalDateTime() },
        // Drivers reporting java.sql.Timestamp are treated as a point in time. Note that TIMESTAMP WITHOUT TIME ZONE also lands here,
        // in which case toInstant() interprets the wall-clock time in the **JVM default time zone** — cross-time-zone syncing must keep both JVMs on the same zone
        byClass(Timestamp::class.java, FieldTypes.TIMESTAMP) { rs, i -> rs.getTimestamp(i)?.toInstant() },

        byClass(SQLXML::class.java, FieldTypes.STRING) { rs, i ->
            val xml = rs.getSQLXML(i)
            try {
                xml?.string
            } finally {
                xml?.free()
            }
        },

        // MySQL reports the class of TEXT/CLOB as java.lang.String, while H2 / PostgreSQL report java.sql.Clob
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
     * Resolves how one column is read and written.
     *
     * Array columns take a separate branch: the element type is inferred from the array's own metadata, instead of guessing with
     * `JDBCType.valueOf(columnMeta.typeName)` as before the refactor — PostgreSQL array type names look like `_int4`,
     * so that line would inevitably throw IllegalArgumentException.
     */
    fun resolve(column: JdbcColumn): ColumnCodec? {
        if (column.type == JDBCType.ARRAY) {
            return resolveArray(column)
        }
        val rule = RULES.firstOrNull { it.matches(column) } ?: return null
        return ColumnCodec(rule.fieldType(column), rule.reader(column))
    }

    private fun resolveArray(column: JdbcColumn): ColumnCodec? {
        // The array element type is only available at runtime from the java.sql.Array metadata; at graph construction time we fall
        // back to string, and the actual element read is decided by ArrayReader once it gets hold of the ResultSet.
        val elementType = arrayElementFieldType(column) ?: return null
        return ColumnCodec(Schema.FieldType.array(elementType), ArrayReader(elementType))
    }

    /** Infers the element type from array type names such as `_int4` / `INTEGER ARRAY`. */
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
     * Reads an array following the JDBC convention: [java.sql.Array.getResultSet] has one element per row, column 1 is the index
     * and column 2 is the value. Before the refactor this code iterated over **columns** instead of rows and returned a handler object, so the values were always wrong.
     */
    private class ArrayReader(private val elementType: Schema.FieldType) : ResultSetReader {

        override fun read(rs: ResultSet, index: Int): Any? {
            val array = rs.getArray(index) ?: return null
            return try {
                array.resultSet.use { elements ->
                    val elementColumn = JdbcColumn.from(elements.metaData, 2)
                    val reader = resolve(elementColumn)?.reader
                        ?: throw UnsupportedOperationException("array element type ${elementColumn.describe()} is not supported yet")
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
     * Resolves the write strategy from the Beam field type. The write side resolves each column once, instead of a per-row, per-column table lookup as before the refactor.
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
     * Beam's logical types are the `java.time` family, while most drivers only know the `java.sql` family, so we convert here.
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
     * [java.sql.Connection.createArrayOf] wants a type name the database understands. We pass a standard SQL name here;
     * a driver that does not know it throws SQLException, which at least has a chance compared to passing Beam's `INT32`.
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
