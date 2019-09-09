package io.jayer.hdata.core

data class YamlPOJO(
    var source: List<Map<String, Any>>? = emptyList(),
    var transform: List<Map<String, Any>>? = emptyList(),
    var sink: List<Map<String, Any>>? = emptyList()
)