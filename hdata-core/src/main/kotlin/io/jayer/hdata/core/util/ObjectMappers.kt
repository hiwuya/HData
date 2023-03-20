package io.jayer.hdata.core.util

import com.fasterxml.jackson.databind.DeserializationFeature
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.toml.TomlFactory

/**
 * @author wuya
 * @date 2022-08-30
 */
object ObjectMappers {
    fun getTomlObjectMapper(): ObjectMapper {
        return ObjectMapper(TomlFactory()).findAndRegisterModules()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
    }
}