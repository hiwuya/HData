package io.jayer.hdata.core.extension

import org.joda.time.DateTime
import java.sql.Date
import java.sql.Time
import java.sql.Timestamp
import java.time.Instant
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.LocalTime

/**
 * @author wuya
 * @date 2022-08-23
 */
fun LocalTime.toSqlTime(): Time = Time.valueOf(this)

fun LocalDate.toSqlDate(): Date = Date.valueOf(this)

fun LocalDateTime.toTimestamp(): Timestamp = Timestamp.valueOf(this)

fun Instant.toTimestamp(): Timestamp = Timestamp.from(this)

fun DateTime.toTimestamp(): Timestamp = Timestamp(this.millis)

fun Timestamp.toDateTime(): DateTime = DateTime(this)