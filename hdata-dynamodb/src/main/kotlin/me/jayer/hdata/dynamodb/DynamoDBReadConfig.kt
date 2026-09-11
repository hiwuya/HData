package me.jayer.hdata.dynamodb

import java.io.Serializable

/**
 * Config for `ReadFromDynamoDB`.
 *
 * ```yaml
 * - type: ReadFromDynamoDB
 *   config:
 *     table_name: "my_table"
 *     region: us-east-1
 *     consistent_read: false
 * ```
 *
 * The read side performs a bounded snapshot: a Scan (or Query, if [keyConditionExpression] is
 * provided) is executed against the DynamoDB table and all matching items are streamed. The
 * output schema is derived from the item attributes at runtime.
 *
 * Setting [parallelScanSegments] > 1 enables DynamoDB's native
 * [parallel scan](https://docs.aws.amazon.com/amazondynamodb/latest/developerguide/Scan.html#Scan.ParallelScan):
 * the table is scanned as that many independent `Segment`/`TotalSegments` slices, one per input
 * element, which Beam can schedule onto different workers concurrently — this is what gives
 * `ReadFromDynamoDB` real read parallelism instead of a single sequential Scan. It only applies to
 * a Scan; it is rejected together with [keyConditionExpression] (Query has no parallel-scan
 * equivalent).
 *
 * @author wuya
 */
data class DynamoDBReadConfig(
    /** The DynamoDB table name to read from. */
    val tableName: String = "",
    /** AWS region; ignored when using a custom endpoint (e.g. DynamoDB Local). */
    val region: String = "us-east-1",
    /** Custom endpoint override for local emulators (e.g. `http://localhost:8000`). */
    val endpointOverride: String = "",
    val accessKeyId: String = "",
    val secretAccessKey: String = "",
    /** Whether to use a consistent read (strongly consistent). Default false (eventually consistent). */
    val consistentRead: Boolean = false,
    /** Optional filter expression applied server-side (e.g. `"attribute_exists(pk)"`). */
    val filterExpression: String = "",
    /** Optional projection expression to limit returned attributes (e.g. `"pk, sk, data"`). */
    val projectionExpression: String = "",
    /** Optional key condition expression; when provided, a Query is used instead of Scan. */
    val keyConditionExpression: String = "",
    /** Expression attribute values for filter / key condition expressions, as a string map. */
    val expressionAttributeValues: Map<String, String> = emptyMap(),
    /**
     * Maximum number of items to read; 0 = no limit. Applied independently per segment when
     * [parallelScanSegments] > 1 (each segment is a separate worker with no shared counter), so
     * the realistic total is up to `max_items * parallel_scan_segments`, not an exact global cap.
     */
    val maxItems: Long = 0,
    /** Number of parallel Scan segments; 1 = a single sequential Scan (default). Scan only. */
    val parallelScanSegments: Int = 1,
) : Serializable {

    fun validate() {
        require(tableName.isNotBlank()) { "table_name must not be blank" }
        require(region.isNotBlank()) { "region must not be blank" }
        require(maxItems >= 0) { "max_items must be >= 0" }
        require(parallelScanSegments >= 1) { "parallel_scan_segments must be >= 1" }
        require(parallelScanSegments == 1 || keyConditionExpression.isBlank()) {
            "parallel_scan_segments > 1 is not supported together with key_condition_expression (parallel scan has no Query equivalent)"
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
