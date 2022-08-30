package io.jayer.hdata.core.config

data class HDataYamlConfig(
    val source: List<Map<String, Map<String, Any>>> = emptyList(),
    val transform: List<Map<String, Map<String, Any>>> = emptyList(),
    val sink: List<Map<String, Map<String, Any>>> = emptyList()
)