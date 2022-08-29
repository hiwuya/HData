package io.jayer.hdata.jdbc.strategy

import java.sql.SQLException

/**
 * @author wuya
 * @date 2022-08-29
 */
fun interface RetryStrategy {
    fun apply(e: SQLException): Boolean
}