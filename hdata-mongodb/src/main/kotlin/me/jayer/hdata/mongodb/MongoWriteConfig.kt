package me.jayer.hdata.mongodb

import java.io.Serializable

/**
 * `WriteToMongoDb` 的配置。配置键对齐 Flink MongoDB connector 的 sink 侧。
 *
 * ```yaml
 * - type: WriteToMongoDb
 *   config:
 *     connection_uri: "mongodb://localhost:27017"
 *     database: mydb
 *     collection: orders
 *     schema_fields: ["id:STRING", "amount:DOUBLE"]
 *     batch_size: 1000
 * ```
 *
 * 未指定 [schemaFields] 时，读取输入行的 `value`(STRING) 字段，按原始 JSON 写入。
 * 否则按 [schemaFields] 把每一列填进一个 `Document`。
 */
data class MongoWriteConfig(
    val connectionUri: String = "",
    val database: String = "",
    val collection: String = "",
    val schemaFields: List<String> = emptyList(),
    val batchSize: Int = 1000,
) : Serializable {

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri 不能为空" }
        require(database.isNotBlank()) { "database 不能为空" }
        require(collection.isNotBlank()) { "collection 不能为空" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
    }
}
