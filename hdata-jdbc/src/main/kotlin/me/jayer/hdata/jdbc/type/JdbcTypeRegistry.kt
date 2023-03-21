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
        fun predicate(columnMeta: me.jayer.hdata.jdbc.JdbcColumnMeta): Boolean
        fun getFieldType(columnMeta: me.jayer.hdata.jdbc.JdbcColumnMeta): Schema.FieldType
        fun getResultSetGetter(columnMeta: me.jayer.hdata.jdbc.JdbcColumnMeta): me.jayer.hdata.jdbc.type.ResultSetGetter
    }

    private val typeProviders = mutableListOf<me.jayer.hdata.jdbc.type.JdbcTypeRegistry.TypeProvider>()
    private val fieldValueConverters = mutableMapOf<Schema.FieldType, me.jayer.hdata.jdbc.type.FieldValueConverter>()

    init {
        // register default Schema.FieldType and ResultSetGetter
        // https://dev.mysql.com/doc/connector-j/8.0/en/connector-j-reference-type-conversions.html
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            { meta: me.jayer.hdata.jdbc.JdbcColumnMeta -> meta.type == JDBCType.BIT && meta.precision > 1 },
            FieldTypes.BYTES,
            { me.jayer.hdata.jdbc.type.ResultSetGetter { rs, i -> rs.getBytes(i) } }
        )

        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            { meta: me.jayer.hdata.jdbc.JdbcColumnMeta -> meta.type == JDBCType.BIT && meta.precision <= 1 },
            FieldTypes.BOOLEAN,
            { me.jayer.hdata.jdbc.type.ResultSetGetter { rs, i -> rs.getBoolean(i) } }
        )

        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            Byte::class,
            FieldTypes.BYTE
        ) { rs, i -> rs.getByte(i) }
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            Short::class,
            FieldTypes.INT16
        ) { rs, i -> rs.getShort(i) }
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            Int::class,
            FieldTypes.INT32
        ) { rs, i -> rs.getInt(i) }
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(Long::class, FieldTypes.INT64) { rs, i ->
            rs.getLong(
                i
            )
        }
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            Float::class,
            FieldTypes.FLOAT
        ) { rs, i -> rs.getFloat(i) }
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            Double::class,
            FieldTypes.DOUBLE
        ) { rs, i -> rs.getDouble(i) }
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            arrayOf(BigInteger::class, BigDecimal::class),
            FieldTypes.DECIMAL
        ) { rs, i ->
            rs.getBigDecimal(i)
        }
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            arrayOf(Date::class, LocalDate::class),
            FieldTypes.DATE
        ) { rs, i ->
            rs.getDate(i)?.toLocalDate()
        }
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            arrayOf(Time::class, LocalTime::class),
            FieldTypes.TIME
        ) { rs, i ->
            rs.getTime(i)?.toLocalTime()
        }
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            LocalDateTime::class,
            FieldTypes.DATETIME
        ) { rs, i -> rs.getTimestamp(i)?.toLocalDateTime() }
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            Timestamp::class,
            FieldTypes.TIMESTAMP
        ) { rs, i -> rs.getTimestamp(i)?.toInstant() }

        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(SQLXML::class, FieldTypes.STRING) { rs, i ->
            val xml = rs.getSQLXML(i)
            try {
                xml?.string
            } finally {
                xml?.free()
            }
        }

        // String type
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            String::class,
            FieldTypes.STRING
        ) { meta: me.jayer.hdata.jdbc.JdbcColumnMeta ->
            me.jayer.hdata.jdbc.type.ResultSetGetter { rs, i ->
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
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            ByteArray::class,
            FieldTypes.BYTES
        ) { meta: me.jayer.hdata.jdbc.JdbcColumnMeta ->
            me.jayer.hdata.jdbc.type.ResultSetGetter { rs, i ->
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
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerFieldValueConverter(FieldTypes.DATE) { v -> (v as LocalDate).toSqlDate() }
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerFieldValueConverter(FieldTypes.TIME) { v -> (v as LocalTime).toSqlTime() }
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerFieldValueConverter(FieldTypes.DATETIME) { v -> (v as LocalDateTime).toTimestamp() }
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerFieldValueConverter(FieldTypes.TIMESTAMP) { v -> (v as Instant).toTimestamp() }
    }

    fun registerJdbcType(
        predicate: (me.jayer.hdata.jdbc.JdbcColumnMeta) -> Boolean,
        fieldType: Schema.FieldType,
        getResultSetGetter: (me.jayer.hdata.jdbc.JdbcColumnMeta) -> me.jayer.hdata.jdbc.type.ResultSetGetter
    ) {
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.typeProviders.add(object :
            me.jayer.hdata.jdbc.type.JdbcTypeRegistry.TypeProvider {
            override fun predicate(columnMeta: me.jayer.hdata.jdbc.JdbcColumnMeta) = predicate.invoke(columnMeta)
            override fun getFieldType(columnMeta: me.jayer.hdata.jdbc.JdbcColumnMeta) = fieldType
            override fun getResultSetGetter(columnMeta: me.jayer.hdata.jdbc.JdbcColumnMeta) = getResultSetGetter.invoke(columnMeta)
        })
    }

    fun registerJdbcType(
        typeClass: KClass<*>,
        fieldType: Schema.FieldType,
        getResultSetGetter: (me.jayer.hdata.jdbc.JdbcColumnMeta) -> me.jayer.hdata.jdbc.type.ResultSetGetter
    ) {
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(
            { columnMeta: me.jayer.hdata.jdbc.JdbcColumnMeta -> Class.forName(columnMeta.typeClass).kotlin == typeClass },
            fieldType,
            getResultSetGetter,
        )
    }

    fun registerJdbcType(typeClass: KClass<*>, fieldType: Schema.FieldType, resultSetGetter: me.jayer.hdata.jdbc.type.ResultSetGetter) {
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.registerJdbcType(typeClass, fieldType) { resultSetGetter }
    }

    fun registerJdbcType(
        typeClasses: Array<KClass<*>>,
        fieldType: Schema.FieldType,
        resultSetGetter: me.jayer.hdata.jdbc.type.ResultSetGetter
    ) {
        typeClasses.forEach { registerJdbcType(it, fieldType, resultSetGetter) }
    }

    fun getFieldType(columnMeta: me.jayer.hdata.jdbc.JdbcColumnMeta): Schema.FieldType? {
        return if (columnMeta.type == JDBCType.ARRAY) {
            val elementJdbcType = me.jayer.hdata.jdbc.type.JdbcTypeRegistry.getFieldType(
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
            me.jayer.hdata.jdbc.type.JdbcTypeRegistry.typeProviders.filter { it.predicate(columnMeta) }.map { it.getFieldType(columnMeta) }.firstOrNull()
        }
    }

    fun getResultSetGetter(columnMeta: me.jayer.hdata.jdbc.JdbcColumnMeta): me.jayer.hdata.jdbc.type.ResultSetGetter? {
        return if (columnMeta.type == JDBCType.ARRAY) {
            val elementResultSetGetter =
                me.jayer.hdata.jdbc.type.JdbcTypeRegistry.getResultSetGetter(
                    columnMeta.copy(
                        type = JDBCType.valueOf(
                            columnMeta.typeName
                        )
                    )
                )
            if (elementResultSetGetter != null) {
                me.jayer.hdata.jdbc.type.ResultSetGetter { rs, i ->
                    val array = rs.getArray(i)
                    if (array != null) {
                        val result = mutableListOf<Any?>()
                        val resultSet = array.resultSet
                        for (index in 1..resultSet.metaData.columnCount) {
                            result.add(elementResultSetGetter.getResult(resultSet, index))
                        }
                        result.toList()
                        object : me.jayer.hdata.jdbc.handler.AbstractListResultSetHandler<Any?>() {
                            override fun handleRow(rs: ResultSet): Any? {
                                TODO("Not yet implemented")
                            }
                        }
                    } else {
                        null
                    }
                }
            } else {
                null
            }
        } else {
            me.jayer.hdata.jdbc.type.JdbcTypeRegistry.typeProviders.filter { it.predicate(columnMeta) }.map { it.getResultSetGetter(columnMeta) }.firstOrNull()
        }
    }

    fun registerFieldValueConverter(type: Schema.FieldType, fieldValueConverter: me.jayer.hdata.jdbc.type.FieldValueConverter) {
        me.jayer.hdata.jdbc.type.JdbcTypeRegistry.fieldValueConverters[type] = fieldValueConverter
    }

    fun getFieldValueConverter(type: Schema.FieldType): me.jayer.hdata.jdbc.type.FieldValueConverter {
        return me.jayer.hdata.jdbc.type.JdbcTypeRegistry.fieldValueConverters[type] ?: me.jayer.hdata.jdbc.type.FieldValueConverter { v -> v }
    }

    fun getPreparedStatementSetter(type: Schema.FieldType): me.jayer.hdata.jdbc.type.PreparedStatementSetter {
        return if (type.typeName == Schema.TypeName.ARRAY) {
            val elementType = type.collectionElementType!!
            val elementFieldValueConverter =
                me.jayer.hdata.jdbc.type.JdbcTypeRegistry.getFieldValueConverter(elementType)
            me.jayer.hdata.jdbc.type.PreparedStatementSetter { ps, row, i ->
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
            me.jayer.hdata.jdbc.type.PreparedStatementSetter { ps, row, i ->
                val value: Any? = row.getValue(i)
                ps.setObject(
                    i + 1,
                    if (value != null) me.jayer.hdata.jdbc.type.JdbcTypeRegistry.getFieldValueConverter(type)
                        .convert(value) else null
                )
            }
        }
    }
}