package me.jayer.hdata.rabbitmq

import java.io.Serializable

/**
 * Config for `ReadFromRabbitMQ`.
 *
 * ```yaml
 * - type: ReadFromRabbitMQ
 *   config:
 *     host: "localhost"
 *     port: 5672
 *     queue: "orders"
 *     max_messages: 1000
 * ```
 *
 * The read side performs a bounded snapshot: it consumes up to [maxMessages] messages from the
 * queue using `basicGet` (synchronous pull). When the queue is empty, the read finishes. Set
 * `streaming` to keep polling after an empty queue; streaming requires `max_messages: 0`.
 *
 * The output schema is fixed:
 *  - `exchange`    STRING
 *  - `routing_key` STRING
 *  - `body`        STRING   (UTF-8 decoded message body)
 *  - `message_id`  STRING   (nullable — not all producers set a message ID)
 *  - `delivery_tag` LONG    (broker-assigned delivery tag for tracing)
 *
 * @author wuya
 */
data class RabbitMQReadConfig(
    val host: String = "localhost",
    val port: Int = 5672,
    val virtualHost: String = "/",
    val username: String = "guest",
    val password: String = "guest",
    val queue: String = "",
    /** Maximum number of messages to consume; 0 means no limit (read until the queue is empty). */
    val maxMessages: Int = 1000,
    /** How long to wait (ms) for a message when the queue is empty before finishing. 0 = return immediately. */
    val waitTimeoutMs: Long = 1000,
    /** Keep the source alive after an empty poll. Requires an unlimited message count. */
    val streaming: Boolean = false,
) : Serializable {

    fun validate() {
        require(host.isNotBlank()) { "host must not be blank" }
        require(port in 1..65535) { "port must be between 1 and 65535" }
        require(virtualHost.isNotBlank()) { "virtual_host must not be blank" }
        require(username.isNotBlank()) { "username must not be blank" }
        require(queue.isNotBlank()) { "queue must not be blank" }
        require(maxMessages >= 0) { "max_messages must be >= 0" }
        require(waitTimeoutMs >= 0) { "wait_timeout_ms must be >= 0" }
        require(!streaming || maxMessages == 0) { "streaming requires max_messages to be 0" }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
