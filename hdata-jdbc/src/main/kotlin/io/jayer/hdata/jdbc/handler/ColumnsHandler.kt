package io.jayer.hdata.jdbc.handler

import org.joda.time.DateTime
import org.joda.time.LocalDate
import org.joda.time.chrono.ISOChronology
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.*
import java.sql.Date
import java.time.LocalDateTime
import java.time.LocalTime
import java.time.ZoneId
import java.time.ZonedDateTime
import java.util.*

/**
 * @author wuya
 * @date 2022-08-11
 */
class ColumnsHandler : AbstractListResultSetMetaDataHandler<ResultSetColumnHandler<Any?>>() {

    override fun handleRow(metaData: ResultSetMetaData, index: Int): ResultSetColumnHandler<Any?> {
        val columnType = metaData.getColumnType(index)
        val columnTypeName = metaData.getColumnTypeName(index)
        val columnClass = Class.forName(metaData.getColumnClassName(index)).kotlin
        val precision = metaData.getPrecision(index)

        val fieldExtractor = object : ResultSetColumnHandler<Any?> {
            override fun handle(rs: ResultSet, index: Int): Any? {
                return when (columnClass) {
                    Boolean::class -> if (columnType == Types.BIT && precision > 1) {
                        // https://bugs.mysql.com/bug.php?id=100722
                        rs.getBytes(index)
                    } else {
                        rs.getBoolean((index))
                    }

                    Byte::class -> rs.getByte(index)
                    ByteArray::class -> rs.getBytes(index)
                    Short::class -> rs.getShort(index)
                    Int::class -> rs.getInt(index)
                    Long::class -> rs.getLong(index)
                    Float::class -> rs.getFloat(index)
                    Double::class -> rs.getDouble(index)
                    String::class -> rs.getString(index)
                    BigInteger::class, BigDecimal::class -> rs.getBigDecimal(index)
                    Date::class -> {
                        val date = rs.getDate(index, Calendar.getInstance(TimeZone.getTimeZone(ZoneId.systemDefault())))
                        if (date == null) {
                            null
                        } else {
                            val zdt = ZonedDateTime.of(date.toLocalDate(), LocalTime.MIDNIGHT, ZoneId.systemDefault())
                            DateTime(zdt.toInstant().toEpochMilli(), ISOChronology.getInstanceUTC())
                        }
                    }

                    Time::class -> {
                        val time = rs.getTime(index, Calendar.getInstance(TimeZone.getTimeZone(ZoneId.systemDefault())))
                        if (time == null) {
                            null
                        } else {
                            DateTime(time.time, ISOChronology.getInstanceUTC()).withDate(LocalDate(0L))
                        }
                    }

                    Timestamp::class -> {
                        val ts =
                            rs.getTimestamp(index, Calendar.getInstance(TimeZone.getTimeZone(ZoneId.systemDefault())))
                        if (ts == null) {
                            null
                        } else {
                            DateTime(ts.toInstant().toEpochMilli(), ISOChronology.getInstanceUTC())
                        }
                    }

                    LocalDateTime::class -> {
                        val dateTime = rs.getObject(index) as LocalDateTime?
                        if (dateTime == null) {
                            null
                        } else {
                            DateTime(dateTime.atZone(ZoneId.systemDefault()).toInstant().toEpochMilli())
                        }
                    }

                    UUID::class -> {
                        rs.getObject(index)
                    }

                    else -> if (columnType == Types.JAVA_OBJECT) {
                        rs.getString(index)
                    } else {
                        throw UnsupportedOperationException("Convert $columnTypeName to Beam schema type is not supported")
                    }
                }
            }
        }

        return if (columnType == Types.ARRAY) {
            object : ResultSetColumnHandler<Any?> {
                override fun handle(rs: ResultSet, index: Int): Any? {
                    val array = rs.getArray(index)
                    return if (array == null) {
                        null
                    } else {
                        val list = mutableListOf<Any?>()
                        val arrayResultSet = array.resultSet
                        while (arrayResultSet.next()) {
                            list.add(fieldExtractor.handle(arrayResultSet, 1))
                        }
                        list
                    }
                }
            }
        } else {
            fieldExtractor
        }
    }
}