package me.jayer.hdata.dynamodb

import java.io.Serializable

/**
 * Config for `WriteToDynamoDB`.
 *
 * ```yaml
 * - type: WriteToDynamoDB
 *   config:
 *     table_name: "my_table"
 *     region: us-east-1
 *     batch_size: 25
 * ```
 *
 * Each input row is converted to a DynamoDB item and written via BatchWriteItem.
 * DynamoDB's BatchWriteItem supports up to 25 items per call. Failed items are
 * retried individually, then routed to the dead-letter stream.
 *
 * The input row's field names are used as DynamoDB attribute names. The row schema determines
 * the attribute value types (STRING -> S, NUMBER -> N, BYTES -> B, BOOLEAN -> BOOL, etc.).
 *
 * @author wuya
 */
data class DynamoDBWriteConfig(
    /** The DynamoDB table name to write to. */
    val tableName: String = "",
    /** AWS region; ignored when using a custom endpoint. */
    val region: String = "us-east-1",
    /** Custom endpoint override for local emulators. */
    val endpointOverride: String = "",
    val accessKeyId: String = "",
    val secretAccessKey: String = "",
    /** Number of items per BatchWriteItem call (1-25). */
    val batchSize: Int = 25,
    /** Number of retries on transient write failures. 0 = no retries. */
    val maxRetries: Int = 3,
    /** Delay in milliseconds between retries (multiplied by attempt number). */
    val retryDelayMs: Long = 500,
) : Serializable {

    fun validate() {
        require(tableName.isNotBlank()) { "table_name must not be blank" }
        require(region.isNotBlank()) { "region must not be blank" }
        require(batchSize in 1..25) { "batch_size must be between 1 and 25" }
        require(maxRetries >= 0) { "max_retries must be >= 0" }
        require(retryDelayMs > 0) { "retry_delay_ms must be > 0" }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
