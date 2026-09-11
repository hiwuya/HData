package me.jayer.hdata.pulsar

import java.io.Serializable

data class PulsarWriteConfig(
    val serviceUrl: String = "",
    val topic: String = "",
    val valueField: String = "value",
    val batchSize: Int = 1_000,
) : Serializable {
    fun validate() {
        require(serviceUrl.isNotBlank()) { "service_url must not be blank" }
        require(topic.isNotBlank()) { "topic must not be blank" }
        require(valueField.isNotBlank()) { "value_field must not be blank" }
        require(batchSize > 0) { "batch_size must be positive" }
    }
}
