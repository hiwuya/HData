package me.jayer.hdata.mongodb

import com.fasterxml.jackson.annotation.JsonProperty
import org.apache.beam.sdk.schemas.Schema
import java.io.Serializable

/**
 * `ReadFromMongoDb` 的配置，键名对齐 Flink MongoDB connector（`uri` / `scan.fetch-size` /
 * `scan.partition.*`）。
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
 * 不指定 [schemaFields] 时退化为单列 `document`(STRING)，每行是该文档的扩展 JSON——
 * 这个列名与 `WriteToMongoDb` 的期望一致，读出来可以直接写回去。
 *
 * @author wuya
 */
data class MongoReadConfig(
    val connectionUri: String = "",
    val database: String = "",
    val collection: String = "",
    val schemaFields: List<String> = emptyList(),
    /** 查询条件，MongoDB 的 JSON 过滤器，例如 `{"status": "PAID"}`。留空表示全量。 */
    val filter: String = "",
    /**
     * 切成几个分片并行读；留空按文档数自动估算（每片约 10 万条，上限 1000 片）。
     * 设为 1 表示不分片。对应 Flink 的 `scan.partition.*`。
     */
    val partitionNum: Int? = null,
    /** 游标每次往返取多少条，对应 Flink 的 `scan.fetch-size`。 */
    val fetchSize: Int = 1000,
    /** 最多读多少条；`-1` 表示不限制。下推成 `find().limit()`（限行数时退化为单分片读保证全局语义）。 */
    val limit: Long = -1,
    /**
     * 聚合下推：把 `count` / `sum` / `min` / `max` / `avg` 推到 MongoDB 聚合管道。
     * 与 `schema_fields` 互斥——聚合结果自带 schema（由下面的 `as` 决定），不再按文档读出。
     *
     * ```yaml
     * aggregate:
     *   - {type: count, as: total}
     *   - {type: min, column: amount, as: min_amount}
     *   - {type: max, column: amount, as: max_amount}
     * ```
     *
     * 聚合按 `_id` 分片做局部 `$group`，再跨分片全局归并（见 `MongoAggregate`），
     * 所以与 `limit` 互斥——limit 对全局聚合没有意义，同配直接报错。
     */
    val aggregate: List<MongoAggregateSpec> = emptyList(),
) : Serializable {

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri 不能为空" }
        runCatching { com.mongodb.ConnectionString(connectionUri) }
            .onFailure { throw IllegalArgumentException("connection_uri 不是合法的 MongoDB URI", it) }
        require(database.isNotBlank()) { "database 不能为空" }
        require(collection.isNotBlank()) { "collection 不能为空" }
        require(fetchSize > 0) { "fetch_size 必须 > 0" }
        require(partitionNum == null || partitionNum > 0) { "partition_num 必须 > 0" }
        require(partitionNum == null || partitionNum <= 1000) { "partition_num 不能超过 1000" }
        require(limit == -1L || limit > 0) { "limit 必须 > 0（或不限制时留空/传 -1）" }
        require(limit <= Int.MAX_VALUE) { "limit 超过 MongoDB 单次游标上限" }
        parseSchemaFields(schemaFields)
        require(aggregate.isEmpty() || schemaFields.isEmpty()) {
            "aggregate 与 schema_fields 互斥：聚合结果自带 schema（由各条 as 决定），无需再声明文档列"
        }
        if (aggregate.isNotEmpty()) {
            // 聚合是全局语义，limit 没有意义；收了又不生效等于埋坑，直接报错
            require(limit == -1L) { "aggregate 模式不使用 limit，请从配置中移除" }
            val aliases = aggregate.map { it.alias }
            require(aliases.distinct().size == aliases.size) { "aggregate 的 as（输出列名）不能重复: $aliases" }
        }
        aggregate.forEach { it.validate() }
        if (filter.isNotBlank()) {
            runCatching { org.bson.BsonDocument.parse(filter) }
                .onFailure { throw IllegalArgumentException("filter 不是合法的 MongoDB 查询 JSON: ${it.message}", it) }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 单条聚合表达式。`type` 取值 `count` / `sum` / `min` / `max` / `avg`；
 * `column` 是被聚合的字段名（不含 `$`），`count` 可省略或填 `*`；`as` 是输出列名。
 */
data class MongoAggregateSpec(
    val type: String = "",
    /** 被聚合字段名；`count` 用 `*` / 留空表示按行计数。 */
    val column: String = "",
    @JsonProperty("as")
    val alias: String = "",
) : Serializable {

    companion object {
        private const val serialVersionUID: Long = 1
        val KNOWN = setOf("count", "sum", "min", "max", "avg")
    }

    fun validate() {
        require(type in KNOWN) { "aggregate.type 必须是 ${KNOWN.joinToString()}，收到: $type" }
        require(alias.isNotBlank()) { "aggregate 每条都要有 as（输出列名）" }
        if (type == "count") {
            require(column.isBlank() || column == "*") { "aggregate.count 的 column 只能用 * 或留空，收到: $column" }
        } else {
            require(column.isNotBlank()) { "aggregate.$type 需要 column（被聚合字段名）" }
        }
    }
}

/** 聚合输出的 Beam schema：count 为 INT64，其余为 DOUBLE（均可空）。 */
fun aggregateSchema(specs: List<MongoAggregateSpec>): Schema =
    Schema.builder().apply {
        specs.forEach { spec ->
            if (spec.type == "count") addNullableField(spec.alias, Schema.FieldType.INT64)
            else addNullableField(spec.alias, Schema.FieldType.DOUBLE)
        }
    }.build()
