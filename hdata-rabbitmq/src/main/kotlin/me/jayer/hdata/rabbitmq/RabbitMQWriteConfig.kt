package me.jayer.hdata.rabbitmq

import java.io.Serializable

/**
 * Config for `WriteToRabbitMQ`.
 *
 * ```yaml
 * - type: WriteToRabbitMQ
 *   config:
 *     host: "localhost"
 *     port: 5672
 *     queue: "orders"
 *     body_field: "body"
 *     routing_key_field: "routing_key"
 *     batch_size: 100
 * ```
 *
 * The input row must contain the body field (STRING). The exchange, routing key, and message ID
 * fields are optional — if absent or null, sensible defaults are used (default exchange, queue
 * name as routing key, no message ID).
 *
 * When `declare_exchange` or `declare_queue` is true, the connector declares the exchange/queue
 * before writing. This makes the write side self-bootstrapping for simple topologies.
 *
 * @author wuya
 */
data class RabbitMQWriteConfig(
    val host: String = "localhost",
    val port: Int = 5672,
    val virtualHost: String = "/",
    val username: String = "guest",
    val password: String = "guest",
    /** The target exchange. Empty string = default (direct) exchange. */
    val exchange: String = "",
    /** The target queue. Must be set. */
    val queue: String = "",
    /** The routing key; defaults to [queue] if left blank. */
    val routingKey: String = "",
    /** Input row field containing the message body (STRING). */
    val bodyField: String = "body",
    /** Input row field containing the routing key; takes precedence over [routingKey] when present. */
    val routingKeyField: String = "routing_key",
    /** Input row field containing the exchange; takes precedence over [exchange] when present. */
    val exchangeField: String = "exchange",
    /** Input row field containing a message ID; absent or null means no message ID is set. */
    val messageIdField: String = "message_id",
    /** Declare the exchange before writing; no-op if it already exists. */
    val declareExchange: Boolean = false,
    /** Exchange type when [declareExchange] is true: `direct`, `fanout`, `topic`, or `headers`. */
    val exchangeType: String = "direct",
    /** Declare the queue before writing; no-op if it already exists. */
    val declareQueue: Boolean = false,
    /** Set delivery mode to 2 (persistent) when true. */
    val persistent: Boolean = false,
    /** Message TTL in milliseconds; omitted means no expiry. */
    val messageTtlMs: Long? = null,
    /** Flush and confirm after this many messages; also the max in-flight count. */
    val batchSize: Int = 100,
) : Serializable {

    fun validate() {
        require(host.isNotBlank()) { "host must not be blank" }
        require(port in 1..65535) { "port must be between 1 and 65535" }
        require(virtualHost.isNotBlank()) { "virtual_host must not be blank" }
        require(username.isNotBlank()) { "username must not be blank" }
        require(queue.isNotBlank()) { "queue must not be blank" }
        require(bodyField.isNotBlank()) { "body_field must not be blank" }
        require(routingKeyField.isNotBlank()) { "routing_key_field must not be blank" }
        require(exchangeField.isNotBlank()) { "exchange_field must not be blank" }
        require(messageIdField.isNotBlank()) { "message_id_field must not be blank" }
        require(batchSize > 0) { "batch_size must be > 0" }
        if (declareExchange) {
            require(exchangeType in EXCHANGE_TYPES) {
                "exchange_type is invalid: $exchangeType; allowed values: ${EXCHANGE_TYPES.joinToString()}"
            }
        }
        require(messageTtlMs == null || messageTtlMs > 0) { "message_ttl_ms must be > 0" }
    }

    companion object {
        private const val serialVersionUID: Long = 1

        const val EXCHANGE_DIRECT = "direct"
        const val EXCHANGE_FANOUT = "fanout"
        const val EXCHANGE_TOPIC = "topic"
        const val EXCHANGE_HEADERS = "headers"
        val EXCHANGE_TYPES = listOf(EXCHANGE_DIRECT, EXCHANGE_FANOUT, EXCHANGE_TOPIC, EXCHANGE_HEADERS)
    }
}
