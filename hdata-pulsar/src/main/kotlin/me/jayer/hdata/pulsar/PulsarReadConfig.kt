package me.jayer.hdata.pulsar

import java.io.Serializable

/** Bounded Pulsar topic snapshot configuration. */
data class PulsarReadConfig(
    val serviceUrl: String = "",
    val topic: String = "",
    val startPosition: String = EARLIEST,
    val maxMessages: Long = -1,
    val receiveTimeoutMillis: Long = 1_000,
) : Serializable {
    fun validate() {
        require(serviceUrl.isNotBlank()) { "service_url must not be blank" }
        require(topic.isNotBlank()) { "topic must not be blank" }
        require(startPosition in POSITIONS) { "start_position must be one of ${POSITIONS.joinToString()}" }
        require(maxMessages == -1L || maxMessages > 0) { "max_messages must be positive or -1" }
        require(receiveTimeoutMillis > 0) { "receive_timeout_millis must be positive" }
    }
    companion object {
        const val EARLIEST = "earliest"
        const val LATEST = "latest"
        val POSITIONS = listOf(EARLIEST, LATEST)
    }
}
