package me.jayer.hdata.sqs

import java.io.Serializable

/**
 * Config for `WriteToSQS`.
 *
 * ```yaml
 * - type: WriteToSQS
 *   config:
 *     queue_url: "http://localhost:4566/000000000000/test-queue"
 *     region: us-east-1
 *     batch_size: 10
 * ```
 *
 * The input row must contain a body field. Message group ID (for FIFO queues) and deduplication ID
 * are optional.
 *
 * @author wuya
 */
data class SQSWriteConfig(
    /** The full SQS queue URL including account ID and queue name. */
    val queueUrl: String = "",
    val region: String = "us-east-1",
    /** Custom endpoint override for local emulators. */
    val endpointOverride: String = "",
    val accessKeyId: String = "",
    val secretAccessKey: String = "",
    /** Input row field containing the message body. */
    val bodyField: String = "body",
    /** Input row field for FIFO message group ID; absent means non-FIFO queue. */
    val messageGroupIdField: String = "",
    /** Input row field for FIFO deduplication ID; absent means server-generated. */
    val messageDeduplicationIdField: String = "",
    /** Number of messages per SendMessageBatch call (1-10). */
    val batchSize: Int = 10,
) : Serializable {

    fun validate() {
        require(queueUrl.isNotBlank()) { "queue_url must not be blank" }
        require(region.isNotBlank()) { "region must not be blank" }
        require(bodyField.isNotBlank()) { "body_field must not be blank" }
        require(batchSize in 1..10) { "batch_size must be between 1 and 10" }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
