package io.jayer.hdata.core.config

data class HDataJobConfig(
    val sources: List<Map<String, Any>> = emptyList(),
    val transforms: List<Map<String, Any>> = emptyList(),
    val sinks: List<Map<String, Any>> = emptyList()
)