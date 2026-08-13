package me.jayer.hdata.jdbc.type

import me.jayer.hdata.core.extension.toSqlDate
import me.jayer.hdata.core.extension.toSqlTime
import me.jayer.hdata.core.extension.toTimestamp
import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.jdbc.JdbcColumnMeta
import me.jayer.hdata.jdbc.handler.AbstractListResultSetHandler
import org.apache.beam.sdk.schemas.Schema
import java.io.Serializable
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.*
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass

/**
 * @author wuya
 * @date 2022-08-23
 */
object JdbcTypeRegistry {

    private interface TypeProvider : Serializable {
        fun predicate(columnMeta: JdbcColumnMeta): Boolean
        fun getFieldType(columnMeta: JdbcColumnMeta): Schema.FieldType
        fun getResultSetGetter(columnMeta: JdbcColumnMeta): ResultSetGetter
    }

    private val typeProviders = mutableListOf<TypeProvider>()
    private val fieldValueConverters = mutableMapOf<Schema.FieldType, FieldValueConverter>()

    init {
        // register default Schema.FieldType and ResultSetGetter
        // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-type-conversions.html
        registerJdbcType(
            { meta: JdbcColumnMeta -> meta.type == JDBCType.BIT && meta.precision > 1 },
            FieldTypes.BYTES,
            { ResultSetGetter { rs, i -> rs.getBytes(i) } }
        )

        registerJdbcType(
            { meta: JdbcColumnMeta -> meta.type == JDBCType.BIT && meta.precision <= 1 },
            FieldTypes.BOOLEAN,
            { ResultSetGetter { rs, i -> rs.getBoolean(i) } }
        )

        // 上面两条是 MySQL 的 BIT 约定；PostgreSQL / H2 等会直接报标准的 BOOLEAN 类型
        registerJdbcType(Boolean::class, FieldTypes.BOOLEAN) { rs, i -> rs.getBoolean(i) }

        registerJdbcType(
            Byte::class,
            FieldTypes.BYTE
        ) { rs, i -> rs.getByte(i) }
        registerJdbcType(
            Short::class,
            FieldTypes.INT16
        ) { rs, i -> rs.getShort(i) }
        registerJdbcType(
            Int::class,
            FieldTypes.INT32
        ) { rs, i -> rs.getInt(i) }
        registerJdbcType(Long::class, FieldTypes.INT64) { rs, i ->
            rs.getLong(
                i
            )
        }
        registerJdbcType(
            Float::class,
            FieldTypes.FLOAT
        ) { rs, i -> rs.getFloat(i) }
        registerJdbcType(
            Double::class,
            FieldTypes.DOUBLE
        ) { rs, i -> rs.getDouble(i) }
        registerJdbcType(
            arrayOf(BigInteger::class, BigDecimal::class),
            FieldTypes.DECIMAL
        ) { rs, i ->
            rs.getBigDecimal(i)
        }
        registerJdbcType(
            arrayOf(Date::class, LocalDate::class),
            FieldTypes.DATE
        ) { rs, i ->
            rs.getDate(i)?.toLocalDate()
        }
        registerJdbcType(
            arrayOf(Time::class, LocalTime::class),
            FieldTypes.TIME
        ) { rs, i ->
            rs.getTime(i)?.toLocalTime()
        }
        registerJdbcType(
            LocalDateTime::class,
            FieldTypes.DATETIME
        ) { rs, i -> rs.getTimestamp(i)?.toLocalDateTime() }
        registerJdbcType(
            Timestamp::class,
            FieldTypes.TIMESTAMP
        ) { rs, i -> rs.getTimestamp(i)?.toInstant() }

        registerJdbcType(SQLXML::class, FieldTypes.STRING) { rs, i ->
            val xml = rs.getSQLXML(i)
            try {
                xml?.string
            } finally {
                xml?.free()
            }
        }

        // String type
        registerJdbcType(
            String::class,
            FieldTypes.STRING
        ) { meta: JdbcColumnMeta ->
            ResultSetGetter { rs, i ->
                when (meta.type) {
                    JDBCType.CLOB, JDBCType.NCLOB -> {
                        val clob = rs.getClob(i)
                        clob?.getSubString(1, clob.length().toInt())
                    }

                    JDBCType.NCHAR, JDBCType.NVARCHAR -> rs.getNString(i)
                    else -> rs.getString(i)
                }
            }
        }

        // byte[] type
        registerJdbcType(
            ByteArray::class,
            FieldTypes.BYTES
        ) { meta: JdbcColumnMeta ->
            ResultSetGetter { rs, i ->
                when (meta.type) {
                    JDBCType.BLOB, JDBCType.LONGVARBINARY -> {
                        val blob = rs.getBlob(i)
                        blob?.getBytes(1, blob.length().toInt())
                    }

                    else -> rs.getBytes(i)
                }
            }
        }

        // register default RowGetter
        registerFieldValueConverter(FieldTypes.DATE) { v -> (v as LocalDate).toSqlDate() }
        registerFieldValueConverter(FieldTypes.TIME) { v -> (v as LocalTime).toSqlTime() }
        registerFieldValueConverter(FieldTypes.DATETIME) { v -> (v as LocalDateTime).toTimestamp() }
        registerFieldValueConverter(FieldTypes.TIMESTAMP) { v -> (v as Instant).toTimestamp() }
    }

    fun registerJdbcType(
        predicate: (JdbcColumnMeta) -> Boolean,
        fieldType: Schema.FieldType,
        getResultSetGetter: (JdbcColumnMeta) -> ResultSetGetter
    ) {
        typeProviders.add(object :
            TypeProvider {
            override fun predicate(columnMeta: JdbcColumnMeta) = predicate.invoke(columnMeta)
            override fun getFieldType(columnMeta: JdbcColumnMeta) = fieldType
            override fun getResultSetGetter(columnMeta: JdbcColumnMeta) = getResultSetGetter.invoke(columnMeta)
        })
    }

    fun registerJdbcType(
        typeClass: KClass<*>,
        fieldType: Schema.FieldType,
        getResultSetGetter: (JdbcColumnMeta) -> ResultSetGetter
    ) {
        registerJdbcType(
            { columnMeta: JdbcColumnMeta -> Class.forName(columnMeta.typeClass).kotlin == typeClass },
            fieldType,
            getResultSetGetter,
        )
    }

    fun registerJdbcType(typeClass: KClass<*>, fieldType: Schema.FieldType, resultSetGetter: ResultSetGetter) {
        registerJdbcType(typeClass, fieldType) { resultSetGetter }
    }

    fun registerJdbcType(
        typeClasses: Array<KClass<*>>,
        fieldType: Schema.FieldType,
        resultSetGetter: ResultSetGetter
    ) {
        typeClasses.forEach { registerJdbcType(it, fieldType, resultSetGetter) }
    }

    fun getFieldType(columnMeta: JdbcColumnMeta): Schema.FieldType? {
        return if (columnMeta.type == JDBCType.ARRAY) {
            val elementJdbcType = getFieldType(
                columnMeta.copy(
                    type = JDBCType.valueOf(columnMeta.typeName)
                )
            )
            if (elementJdbcType != null) {
                Schema.FieldType.array(elementJdbcType)
            } else {
                null
            }
        } else {
            typeProviders.filter { it.predicate(columnMeta) }.map { it.getFieldType(columnMeta) }.firstOrNull()
        }
    }

    fun getResultSetGetter(columnMeta: JdbcColumnMeta): ResultSetGetter? {
        return if (columnMeta.type == JDBCType.ARRAY) {
            val elementResultSetGetter =
                getResultSetGetter(
                    columnMeta.copy(
                        type = JDBCType.valueOf(
                            columnMeta.typeName
                        )
                    )
                )
            if (elementResultSetGetter != null) {
                ResultSetGetter { rs, i ->
                    val array = rs.getArray(i)
                    if (array == null) {
                        null
                    } else {
                        try {
                            array.resultSet.use { elements ->
                                // JDBC 约定：数组的 ResultSet 每行一个元素，第 1 列是下标，第 2 列才是值
                                val handler = object : AbstractListResultSetHandler<Any?>() {
                                    override fun handleRow(rs: ResultSet): Any? =
                                        elementResultSetGetter.getResult(rs, 2)
                                }
                                handler.handle(elements)
                            }
                        } finally {
                            array.free()
                        }
                    }
                }
            } else {
                null
            }
        } else {
            typeProviders.filter { it.predicate(columnMeta) }.map { it.getResultSetGetter(columnMeta) }.firstOrNull()
        }
    }

    fun registerFieldValueConverter(type: Schema.FieldType, fieldValueConverter: FieldValueConverter) {
        fieldValueConverters[type] = fieldValueConverter
    }

    fun getFieldValueConverter(type: Schema.FieldType): FieldValueConverter {
        return fieldValueConverters[type] ?: FieldValueConverter { v -> v }
    }

    fun getPreparedStatementSetter(type: Schema.FieldType): PreparedStatementSetter {
        return if (type.typeName == Schema.TypeName.ARRAY) {
            val elementType = type.collectionElementType!!
            val elementFieldValueConverter =
                getFieldValueConverter(elementType)
            PreparedStatementSetter { ps, row, i ->
                val values = row.getArray<Any?>(i)
                if (values == null) {
                    ps.setArray(i + 1, null)
                } else {
                    val array = ps.connection.createArrayOf(
                        elementType.typeName.name,
                        values.map { if (it != null) elementFieldValueConverter.convert(it) else it }.toTypedArray()
                    )
                    ps.setArray(i + 1, array)
                }
            }
        } else {
            PreparedStatementSetter { ps, row, i ->
                val value: Any? = row.getValue(i)
                ps.setObject(
                    i + 1,
                    if (value != null) getFieldValueConverter(type)
                        .convert(value) else null
                )
            }
        }
    }
}