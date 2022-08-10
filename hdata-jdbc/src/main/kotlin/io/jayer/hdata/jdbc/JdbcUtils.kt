package io.jayer.hdata.jdbc

import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.logicaltypes.UuidLogicalType
import org.joda.time.DateTime
import org.joda.time.LocalDate
import org.joda.time.chrono.ISOChronology
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.*
import java.sql.Date
import java.time.*
import java.util.*
import kotlin.reflect.KClass
import kotlin.reflect.full.isSubclassOf

/**
 * @author wuya
 * @date 2022-08-04
 */
object JdbcUtils {

    private fun convertToBeamFieldType(metadata: ResultSetMetaData, columnIndex: Int): Schema.FieldType {
        val columnType = metadata.getColumnType(columnIndex)
        val columnTypeName = metadata.getColumnTypeName(columnIndex)
        val columnClass = Class.forName(metadata.getColumnClassName(columnIndex)).kotlin
        val precision = metadata.getPrecision(columnIndex)

        val result = when (columnClass) {
            Boolean::class -> if (columnType == Types.BIT && precision > 1) {
                // https://bugs.mysql.com/bug.php?id=100722
                Schema.FieldType.BYTES
            } else {
                Schema.FieldType.BOOLEAN
            }

            Byte::class -> Schema.FieldType.BYTE
            ByteArray::class -> Schema.FieldType.BYTES
            Short::class -> Schema.FieldType.INT16
            Int::class -> Schema.FieldType.INT32
            Long::class -> Schema.FieldType.INT64
            Float::class -> Schema.FieldType.FLOAT
            Double::class -> Schema.FieldType.DOUBLE
            String::class -> Schema.FieldType.STRING
            BigInteger::class, BigDecimal::class -> Schema.FieldType.DECIMAL
            Date::class, Time::class, Timestamp::class, LocalDateTime::class -> Schema.FieldType.DATETIME
            UUID::class -> Schema.FieldType.logicalType(UuidLogicalType())
            else -> if (columnType == Types.JAVA_OBJECT) {
                Schema.FieldType.STRING
            } else {
                throw UnsupportedOperationException("Convert $columnTypeName to Beam schema type is not supported")
            }
        }

        return if (columnType == Types.ARRAY) Schema.FieldType.array(result) else result
    }

    fun createResultSetFieldExtractor(metadata: ResultSetMetaData, columnIndex: Int): ResultSetFieldExtractor {
        val columnType = metadata.getColumnType(columnIndex)
        val columnTypeName = metadata.getColumnTypeName(columnIndex)
        val columnClass = Class.forName(metadata.getColumnClassName(columnIndex)).kotlin
        val precision = metadata.getPrecision(columnIndex)

        val fieldExtractor = { rs: ResultSet, i: Int ->
            when (columnClass) {
                Boolean::class -> if (columnType == Types.BIT && precision > 1) {
                    // https://bugs.mysql.com/bug.php?id=100722
                    rs.getBytes(i)
                } else {
                    rs.getBoolean((i))
                }

                Byte::class -> rs.getByte(i)
                ByteArray::class -> rs.getBytes(i)
                Short::class -> rs.getShort(i)
                Int::class -> rs.getInt(i)
                Long::class -> rs.getLong(i)
                Float::class -> rs.getFloat(i)
                Double::class -> rs.getDouble(i)
                String::class -> rs.getString(i)
                BigInteger::class, BigDecimal::class -> rs.getBigDecimal(i)
                Date::class -> {
                    val date = rs.getDate(i, Calendar.getInstance(TimeZone.getTimeZone(ZoneId.systemDefault())))
                    if (date == null) {
                        null
                    } else {
                        val zdt = ZonedDateTime.of(date.toLocalDate(), LocalTime.MIDNIGHT, ZoneId.systemDefault())
                        DateTime(zdt.toInstant().toEpochMilli(), ISOChronology.getInstanceUTC())
                    }
                }

                Time::class -> {
                    val time = rs.getTime(i, Calendar.getInstance(TimeZone.getTimeZone(ZoneId.systemDefault())))
                    if (time == null) {
                        null
                    } else {
                        DateTime(time.time, ISOChronology.getInstanceUTC()).withDate(LocalDate(0L))
                    }
                }

                Timestamp::class -> {
                    val ts = rs.getTimestamp(i, Calendar.getInstance(TimeZone.getTimeZone(ZoneId.systemDefault())))
                    if (ts == null) {
                        null
                    } else {
                        DateTime(ts.toInstant().toEpochMilli(), ISOChronology.getInstanceUTC())
                    }
                }

                LocalDateTime::class -> {
                    val dateTime = rs.getObject(i) as LocalDateTime?
                    if (dateTime == null) {
                        null
                    } else {
                        DateTime(dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                    }
                }

                UUID::class -> {
                    rs.getObject(i)
                }

                else -> if (columnType == Types.JAVA_OBJECT) {
                    rs.getString(i)
                } else {
                    throw UnsupportedOperationException("Convert $columnTypeName to Beam schema type is not supported")
                }
            }
        }

        return if (columnType == Types.ARRAY) {
            { rs: ResultSet, i: Int ->
                val array = rs.getArray(i)
                if (array == null) {
                    null
                } else {
                    val list = mutableListOf<Any?>()
                    val arrayResultSet = array.resultSet
                    while (arrayResultSet.next()) {
                        list.add(fieldExtractor.invoke(arrayResultSet, 1))
                    }
                    list
                }
            }
        } else {
            fieldExtractor
        }
    }

    private fun convertToBeamSchema(metadata: ResultSetMetaData): Schema {
        val builder = Schema.builder()
        for (i in 1..metadata.columnCount) {
            val label = metadata.getColumnLabel(i)
            val fieldType = convertToBeamFieldType(metadata, i)
            val isNullable = metadata.isNullable(i) == ResultSetMetaData.columnNullable
            val field = Schema.Field.of(label, fieldType).withNullable(isNullable)
            builder.addField(field)
        }
        return builder.build()
    }

    fun inferBeamSchema(connection: Connection, query: String): Schema {
        return connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { ps ->
            convertToBeamSchema(ps.metaData)
        }
    }

    private fun getColumnTypes(connection: Connection, table: String): Map<String, KClass<*>> {
        val sql = "SELECT * FROM `${table}` WHERE 1=2"
        return connection.prepareStatement(sql).use { ps ->
            val metadata = ps.metaData
            IntRange(1, metadata.columnCount)
                .associate { metadata.getColumnLabel(it) to Class.forName(metadata.getColumnClassName(it)).kotlin }
        }
    }

    fun getFirstNumericPrimaryKey(connection: Connection, table: String): String? {
        val numericColumns = getColumnTypes(connection, table).filter { it.value.isSubclassOf(Number::class) }.keys
        return connection.metaData.getPrimaryKeys(connection.catalog, null, table).use { rs ->
            while (rs.next()) {
                val key = rs.getString("COLUMN_NAME")
                if (key in numericColumns) {
                    return key
                }
            }
            null
        }
    }

    fun queryPartitionRange(
        connection: Connection,
        table: String,
        where: String,
        partitionColumn: String
    ): OffsetRange? {
        val result = DoubleArray(2)
        var sql = "SELECT min(`$partitionColumn`), max(`$partitionColumn`) FROM `$table`"
        if (where.isNotBlank()) {
            sql += " WHERE $where"
        }

        connection.prepareStatement(sql).use { ps ->
            ps.executeQuery().use { rs ->
                while (rs.next()) {
                    result[0] = rs.getDouble(1)
                    result[1] = rs.getDouble(2)
                    if(rs.wasNull()) {
                        return null
                    }
                }
            }
        }

        return OffsetRange(result[0].toLong(), result[1].toLong() + 1)
    }
}

typealias ResultSetFieldExtractor = (ResultSet, Int) -> Any?