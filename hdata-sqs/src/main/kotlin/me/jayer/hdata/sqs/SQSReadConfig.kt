package me.jayer.hdata.sqs

import java.io.Serializable

/**
 * Config for `ReadFromSQS`.
 *
 * ```yaml
 * - type: ReadFromSQS
 *   config:
 *     queue_url: "http://localhost:4566/000000000000/test-queue"
 *     region: us-east-1
 *     max_messages: 10
 * ```
 *
 * The read side performs a bounded snapshot: it long-polls the queue up to [maxMessages] times,
 * each time receiving up to [batchSize] messages. Messages are deleted from the queue after reading
 * (standard behavior for a bounded snapshot). For at-least-once semantics, set [visibilityTimeout]
 * high enough that a crashed reader's messages become visible again.
 *
 * The output schema is fixed:
 *  - `message_id`    STRING
 *  - `body`          STRING
 *  - `receipt_handle` STRING
 *  - `attributes`    MAP<STRING, STRING>  (SQS system attributes)
 *
 * @author wuya
 */
data class SQSReadConfig(
    /** The full SQS queue URL including account ID and queue name. */
    val queueUrl: String = "",
    /** AWS region; ignored when using a custom endpoint (e.g. LocalStack). */
    val region: String = "us-east-1",
    /** Custom endpoint override for local emulators (e.g. `http://localhost:4566`). */
    val endpointOverride: String = "",
    val accessKeyId: String = "",
    val secretAccessKey: String = "",
    /** Maximum number of ReceiveMessage calls; 0 = no limit (receive until queue is empty). */
    val maxMessages: Int = 10,
    /** Number of messages to receive per ReceiveMessage call (1-10). */
    val batchSize: Int = 10,
    /** How long (seconds) a received message is hidden from other consumers. */
    val visibilityTimeout: Int = 30,
    /** How long (seconds) to wait for messages when the queue is empty (long poll). 0 = short poll. */
    val waitTimeSeconds: Int = 20,
    /** Whether to delete messages from the queue after reading. */
    val deleteAfterRead: Boolean = true,
) : Serializable {

    fun validate() {
        require(queueUrl.isNotBlank()) { "queue_url must not be blank" }
        require(region.isNotBlank()) { "region must not be blank" }
        require(maxMessages >= 0) { "max_messages must be >= 0" }
        require(batchSize in 1..10) { "batch_size must be between 1 and 10" }
        require(visibilityTimeout in 0..43200) { "visibility_timeout must be between 0 and 43200" }
        require(waitTimeSeconds in 0..20) { "wait_time_seconds must be between 0 and 20" }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
