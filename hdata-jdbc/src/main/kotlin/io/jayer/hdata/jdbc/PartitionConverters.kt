package io.jayer.hdata.jdbc

import java.math.BigInteger
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.reflect.KClass

/**
 * @author wuya
 * @date 2022-08-17
 */
enum class PartitionConverters(val type: KClass<out Any>, val partitionConverter: PartitionConverter<out Any>) {
    INT(Int::class, object : PartitionConverter<Int> {
        override fun toLong(value: Int) = value.toLong()
        override fun fromLong(value: Long) = value.toInt()
    }),
    LONG(Long::class, object : PartitionConverter<Long> {
        override fun toLong(value: Long) = value
        override fun fromLong(value: Long) = value
    }),
    BIG_INTEGER(BigInteger::class, object : PartitionConverter<BigInteger> {
        override fun toLong(value: BigInteger) = value.toLong()
        override fun fromLong(value: Long) = value.toBigInteger()
    }),
    DATE(Date::class, object : PartitionConverter<Date> {
        override fun toLong(value: Date) = value.time
        override fun fromLong(value: Long) = Date(value)
    }),
    TIME(Time::class, object : PartitionConverter<Time> {
        override fun toLong(value: Time) = value.time
        override fun fromLong(value: Long) = Time(value)
    }),
    TIMESTAMP(Timestamp::class, object : PartitionConverter<Timestamp> {
        override fun toLong(value: Timestamp) = value.time
        override fun fromLong(value: Long) = Timestamp(value)
    }),
    LOCAL_DATE_TIME(LocalDateTime::class, object : PartitionConverter<LocalDateTime> {
        private val defaultZoneOffset = ZoneOffset.systemDefault()
        override fun toLong(value: LocalDateTime) = value.atZone(defaultZoneOffset).toInstant().toEpochMilli()
        override fun fromLong(value: Long) = Instant.ofEpochMilli(value).atZone(defaultZoneOffset).toLocalDateTime()
    });
}