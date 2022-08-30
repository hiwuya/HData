package io.jayer.hdata.core.util

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper

/**
 * @author wuya
 * @date 2022-08-30
 */
object ObjectMappers {
    fun get(): ObjectMapper {
        return ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)
    }
}