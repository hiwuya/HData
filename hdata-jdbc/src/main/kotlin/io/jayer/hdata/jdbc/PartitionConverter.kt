package io.jayer.hdata.jdbc

import java.io.Serializable

/**
 * @author wuya
 * @date 2022-08-18
 */
interface PartitionConverter<T> : Serializable {
    fun toLong(value: T): Long
    fun fromLong(value: Long): T
}