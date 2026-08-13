package me.jayer.hdata.mongodb

import java.io.Serializable

/**
 * `ReadFromMongoDb` 的配置。配置键对齐 Flink MongoDB connector。
 *
 * ```yaml
 * - type: ReadFromMongoDb
 *   config:
 *     connection_uri: "mongodb://localhost:27017"
 *     database: mydb
 *     collection: orders
 *     schema_fields: ["id:STRING", "amount:DOUBLE"]
 *     fetch_size: 1000
 * ```
 *
 * 未指定 [schemaFields] 时回退为单列 `document`(STRING)，即每行是该文档的 JSON 字符串。
 */
data class MongoReadConfig(
    val connectionUri: String = "",
    val database: String = "",
    val collection: String = "",
    val schemaFields: List<String> = emptyList(),
    val fetchSize: Int = 1000,
) : Serializable {

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri 不能为空" }
        require(database.isNotBlank()) { "database 不能为空" }
        require(collection.isNotBlank()) { "collection 不能为空" }
        require(fetchSize > 0) { "fetch_size 必须 > 0" }
    }
}
