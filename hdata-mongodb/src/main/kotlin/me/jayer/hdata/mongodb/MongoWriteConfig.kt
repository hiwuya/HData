package me.jayer.hdata.mongodb

import java.io.Serializable

/**
 * Config for `WriteToMongoDb`, key names align with the sink side of the Flink MongoDB connector.
 *
 * ```yaml
 * - type: WriteToMongoDb
 *   config:
 *     connection_uri: "mongodb://localhost:27017"
 *     database: mydb
 *     collection: orders
 *     schema_fields: ["id:STRING", "amount:DOUBLE"]
 *     upsert_keys: ["id"]      # With this, overwrite by primary key; re-running won't create duplicate data
 *     batch_size: 1000
 * ```
 *
 * When [schemaFields] is not specified, the input row's `document` (STRING) field is written as JSON —
 * this column name matches the output of `ReadFromMongoDb` (before the refactor the reader produced
 * `document` while the writer looked for `value`, so read data could not be written back directly).
 *
 * @author wuya
 */
data class MongoWriteConfig(
    val connectionUri: String = "",
    val database: String = "",
    val collection: String = "",
    val schemaFields: List<String> = emptyList(),
    /**
     * Overwrite by primary key (upsert) using these fields. If empty, always insert —
     * **re-running the job will produce duplicate documents**.
     * Corresponds to the upsert behavior derived from the table primary key in the Flink MongoDB connector.
     */
    val upsertKeys: List<String> = emptyList(),
    /** Commit once this many rows are accumulated, corresponding to Flink's `sink.buffer-flush.max-rows`. */
    val batchSize: Int = 1000,
) : Serializable {

    val upsert: Boolean get() = upsertKeys.isNotEmpty()

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri must not be empty" }
        runCatching { com.mongodb.ConnectionString(connectionUri) }
            .onFailure { throw IllegalArgumentException("connection_uri is not a valid MongoDB URI", it) }
        require(database.isNotBlank()) { "database must not be empty" }
        require(collection.isNotBlank()) { "collection must not be empty" }
        require(batchSize > 0) { "batch_size must be > 0" }
        require(upsertKeys.none { it.isBlank() }) { "upsert_keys must not contain empty field names" }
        require(upsertKeys.distinct().size == upsertKeys.size) { "upsert_keys must not contain duplicates" }
        parseSchemaFields(schemaFields)
        if (upsert && schemaFields.isNotEmpty()) {
            val names = parseSchemaFields(schemaFields).map { it.name }.toSet()
            val unknown = upsertKeys.filterNot { it in names }
            require(unknown.isEmpty()) { "upsert_keys contains $unknown which is not in schema_fields and cannot be used as primary key" }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
