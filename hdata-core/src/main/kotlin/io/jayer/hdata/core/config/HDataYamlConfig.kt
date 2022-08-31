package io.jayer.hdata.core.config

data class HDataYamlConfig(
    val sources: List<Map<String, Map<String, Any>>> = emptyList(),
    val transforms: List<Map<String, Map<String, Any>>> = emptyList(),
    val sinks: List<Map<String, Map<String, Any>>> = emptyList()
)