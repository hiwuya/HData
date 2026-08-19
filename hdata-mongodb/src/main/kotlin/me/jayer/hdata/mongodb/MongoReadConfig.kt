package me.jayer.hdata.mongodb

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
        parseSchemaFields(schemaFields)
        if (filter.isNotBlank()) {
            runCatching { org.bson.BsonDocument.parse(filter) }
                .onFailure { throw IllegalArgumentException("filter 不是合法的 MongoDB 查询 JSON: ${it.message}", it) }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
