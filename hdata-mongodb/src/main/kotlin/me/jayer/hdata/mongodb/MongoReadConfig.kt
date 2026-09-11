package me.jayer.hdata.mongodb

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.beam.sdk.schemas.Schema
import java.io.Serializable

/**
 * Config for `ReadFromMongoDb`, key names align with the Flink MongoDB connector (`uri` / `scan.fetch-size` /
 * `scan.partition.*`).
 *
 * ```yaml
 * - type: ReadFromMongoDb
 *   config:
 *     connection_uri: "mongodb://localhost:27017"
 *     database: mydb
 *     collection: orders
 *     schema_fields: ["id:STRING", "amount:DOUBLE"]
 *     filter: '{"status": "PAID"}'
 *     partition_num: 8
 *     limit: 1000
 * ```
 *
 * When [schemaFields] is not specified it degenerates to a single `document` (STRING) column, where each row
 * is the document's extended JSON — this column name matches what `WriteToMongoDb` expects, so read data can
 * be written back directly.
 *
 * @author wuya
 */
data class MongoReadConfig(
    val connectionUri: String = "",
    val database: String = "",
    val collection: String = "",
    val schemaFields: List<String> = emptyList(),
    /** Query condition, a MongoDB JSON filter such as `{"status": "PAID"}`. Empty means the full collection. */
    val filter: String = "",
    /**
     * Number of partitions to read in parallel; if empty it is auto-estimated from the document count
     * (about 100k documents per partition, capped at 1000 partitions). Setting to 1 means no partitioning.
     * Corresponds to Flink's `scan.partition.*`.
     */
    val partitionNum: Int? = null,
    /** How many documents to fetch per cursor round trip, corresponding to Flink's `scan.fetch-size`. */
    val fetchSize: Int = 1000,
    /** Maximum number of documents to read; `-1` means unlimited. Pushed down as `find().limit()` (when row count is limited it falls back to single-partition read to guarantee global semantics). */
    val limit: Long = -1,
    /**
     * Push-down aggregation: push `count` / `sum` / `min` / `max` / `avg` down to the MongoDB aggregation pipeline.
     * Mutually exclusive with `schema_fields` — the aggregation result carries its own schema (decided by the `as`
     * below), and documents are no longer read out row by row.
     *
     * ```yaml
     * aggregate:
     *   - {type: count, as: total}
     *   - {type: min, column: amount, as: min_amount}
     *   - {type: max, column: amount, as: max_amount}
     * ```
     *
     * Aggregation does a per-`_id`-partition local `$group`, then merges globally across partitions (see `MongoAggregate`),
     * so it is mutually exclusive with `limit` — limit is meaningless for a global aggregation, and configuring both is an error.
     */
    val aggregate: List<MongoAggregateSpec> = emptyList(),
) : Serializable {

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri must not be empty" }
        runCatching { com.mongodb.ConnectionString(connectionUri) }
            .onFailure { throw IllegalArgumentException("connection_uri is not a valid MongoDB URI", it) }
        require(database.isNotBlank()) { "database must not be empty" }
        require(collection.isNotBlank()) { "collection must not be empty" }
        require(fetchSize > 0) { "fetch_size must be > 0" }
        require(partitionNum == null || partitionNum > 0) { "partition_num must be > 0" }
        require(partitionNum == null || partitionNum <= 1000) { "partition_num must not exceed 1000" }
        require(limit == -1L || limit > 0) { "limit must be > 0 (or leave empty / pass -1 for unlimited)" }
        require(limit <= 0 || partitionNum == null) {
            "limit mode forces a single partition and does not use partition_num; please remove it from the config"
        }
        parseSchemaFields(schemaFields)
        require(aggregate.isEmpty() || schemaFields.isEmpty()) {
            "aggregate and schema_fields are mutually exclusive: the aggregation result carries its own schema (decided by each as), so the document column must not be declared"
        }
        if (aggregate.isNotEmpty()) {
            // Aggregation is global semantics, so limit is meaningless; accepting it without effect would be a hidden
            // trap, so we error out directly
            require(limit == -1L) { "aggregate mode does not use limit; please remove it from the config" }
            require(fetchSize == 1000) { "aggregate mode does not use fetch_size; please remove it from the config" }
            val aliases = aggregate.map { it.alias }
            require(aliases.distinct().size == aliases.size) { "aggregate as (output column name) must not be duplicated: $aliases" }
        }
        aggregate.forEach { it.validate() }
        if (filter.isNotBlank()) {
            runCatching { org.bson.BsonDocument.parse(filter) }
                .onFailure { throw IllegalArgumentException("filter is not valid MongoDB query JSON: ${it.message}", it) }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * A single aggregation expression. `type` is one of `count` / `sum` / `min` / `max` / `avg`;
 * `column` is the aggregated field name (without `$`), `count` may be omitted or `*`; `as` is the output column name.
 */
data class MongoAggregateSpec(
    val type: String = "",
    /** The aggregated field name; `count` uses `*` / empty means counting rows. */
    val column: String = "",
    @JsonProperty("as")
    val alias: String = "",
) : Serializable {

    companion object {
        private const val serialVersionUID: Long = 1
        val KNOWN = setOf("count", "sum", "min", "max", "avg")
    }

    fun validate() {
        require(type in KNOWN) { "aggregate.type must be one of ${KNOWN.joinToString()}, received: $type" }
        require(alias.isNotBlank()) { "every aggregate entry must have an as (output column name)" }
        if (type == "count") {
            require(column.isBlank() || column == "*") { "aggregate.count column can only be * or empty, received: $column" }
        } else {
            require(column.isNotBlank()) { "aggregate.$type requires column (the aggregated field name)" }
        }
    }
}

/** The Beam schema for aggregation output: count is INT64, the rest are DOUBLE (all nullable). */
fun aggregateSchema(specs: List<MongoAggregateSpec>): Schema =
    Schema.builder().apply {
        specs.forEach { spec ->
            if (spec.type == "count") addNullableField(spec.alias, Schema.FieldType.INT64)
            else addNullableField(spec.alias, Schema.FieldType.DOUBLE)
        }
    }.build()
