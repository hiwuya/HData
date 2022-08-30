package io.jayer.hdata.jdbc.partition

import io.jayer.hdata.core.extension.toSqlDate
import io.jayer.hdata.core.extension.toSqlTime
import java.math.BigDecimal
import java.math.BigInteger
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime
import kotlin.reflect.KClass

/**
 * @author wuya
 * @date 2022-08-17
 */
enum class PartitionConverters(val type: KClass<out Any>, val partitionConverter: PartitionConverter<out Any>) {
    BOOLEAN(Boolean::class, object : PartitionConverter<Boolean> {
        override fun toLong(value: Boolean) = if (value) 1L else 0L
        override fun fromLong(value: Long) = value > 0
    }),
    BYTE(Byte::class, object : PartitionConverter<Byte> {
        override fun toLong(value: Byte) = value.toLong()
        override fun fromLong(value: Long) = value.toByte()
    }),
    SHORT(Short::class, object : PartitionConverter<Short> {
        override fun toLong(value: Short) = value.toLong()
        override fun fromLong(value: Long) = value.toShort()
    }),
    INT(Int::class, object : PartitionConverter<Int> {
        override fun toLong(value: Int) = value.toLong()
        override fun fromLong(value: Long) = value.toInt()
    }),
    LONG(Long::class, object : PartitionConverter<Long> {
        override fun toLong(value: Long) = value
        override fun fromLong(value: Long) = value
    }),
    FLOAT(Float::class, object : PartitionConverter<Float> {
        override fun toLong(value: Float) = value.toLong()
        override fun fromLong(value: Long) = value.toFloat()
    }),
    DOUBLE(Double::class, object : PartitionConverter<Double> {
        override fun toLong(value: Double) = value.toLong()
        override fun fromLong(value: Long) = value.toDouble()
    }),
    BIG_INTEGER(BigInteger::class, object : PartitionConverter<BigInteger> {
        override fun toLong(value: BigInteger) = value.toLong()
        override fun fromLong(value: Long) = value.toBigInteger()
    }),
    BIG_DECIMAL(BigDecimal::class, object : PartitionConverter<BigDecimal> {
        override fun toLong(value: BigDecimal) = value.toLong()
        override fun fromLong(value: Long) = value.toBigDecimal()
    }),
    DATE(Date::class, object : PartitionConverter<Date> {
        override fun toLong(value: Date) = value.toLocalDate().toEpochDay()
        override fun fromLong(value: Long) = LocalDate.ofEpochDay(value).toSqlDate()
    }),
    TIME(Time::class, object : PartitionConverter<Time> {
        override fun toLong(value: Time) = value.toLocalTime().toSecondOfDay().toLong()
        override fun fromLong(value: Long) = LocalTime.ofSecondOfDay(value).toSqlTime()
    }),
    TIMESTAMP(Timestamp::class, object : PartitionConverter<Timestamp> {
        override fun toLong(value: Timestamp) = value.time / 1000
        override fun fromLong(value: Long) = Timestamp(value * 1000)
    }),
    LOCAL_DATE_TIME(LocalDateTime::class, object : PartitionConverter<LocalDateTime> {
        override fun toLong(value: LocalDateTime) = Timestamp.valueOf(value).time / 1000
        override fun fromLong(value: Long) = Timestamp(value * 1000).toLocalDateTime()
    });
}