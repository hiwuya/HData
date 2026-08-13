package me.jayer.hdata.core.util

import tools.jackson.databind.DeserializationFeature
import tools.jackson.dataformat.toml.TomlMapper

/**
 * @author wuya
 * @date 2022-08-30
 */
object ObjectMappers {
    fun getTomlObjectMapper(): TomlMapper {
        return TomlMapper.builder()
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES)
            .build()
    }
}
