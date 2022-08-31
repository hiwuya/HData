package io.jayer.hdata.core.exception

/**
 * @author wuya
 * @date 2022-08-31
 */
class HDataException(message: String, cause: Throwable? = null) : RuntimeException(message, cause) {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}