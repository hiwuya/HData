package me.jayer.hdata.jdbc.strategy

import java.sql.SQLException

/**
 * @author wuya
 * @date 2022-08-29
 */
class DefaultRetryStrategy : RetryStrategy {

    companion object {
        private val ERROR_CODES_TO_RETRY = setOf("40001", "40P01")
    }

    override fun apply(e: SQLException): Boolean = ERROR_CODES_TO_RETRY.contains(e.sqlState)
}