package me.jayer.hdata.mongodb

import java.io.Serializable

/**
 * `WriteToMongoDb` 的配置，键名对齐 Flink MongoDB connector 的 sink 侧。
 *
 * ```yaml
 * - type: WriteToMongoDb
 *   config:
 *     connection_uri: "mongodb://localhost:27017"
 *     database: mydb
 *     collection: orders
 *     schema_fields: ["id:STRING", "amount:DOUBLE"]
 *     upsert_keys: ["id"]      # 有这个就按主键覆盖写，重跑不会造出重复数据
 *     batch_size: 1000
 * ```
 *
 * 不指定 [schemaFields] 时，读取输入行的 `document`(STRING) 字段按 JSON 写入——
 * 这个列名与 `ReadFromMongoDb` 的产出一致（重构前读端产出 `document`、写端却找 `value`，
 * 读出来的数据没法直接写回去）。
 *
 * @author wuya
 */
data class MongoWriteConfig(
    val connectionUri: String = "",
    val database: String = "",
    val collection: String = "",
    val schemaFields: List<String> = emptyList(),
    /**
     * 按这些字段做主键覆盖写（upsert）。留空则一律 insert，**重跑作业会产生重复文档**。
     * 对应 Flink MongoDB connector 里由表主键推导出的 upsert 行为。
     */
    val upsertKeys: List<String> = emptyList(),
    /** 攒够这么多行提交一次，对应 Flink 的 `sink.buffer-flush.max-rows`。 */
    val batchSize: Int = 1000,
) : Serializable {

    val upsert: Boolean get() = upsertKeys.isNotEmpty()

    fun validate() {
        require(connectionUri.isNotBlank()) { "connection_uri 不能为空" }
        runCatching { com.mongodb.ConnectionString(connectionUri) }
            .onFailure { throw IllegalArgumentException("connection_uri 不是合法的 MongoDB URI", it) }
        require(database.isNotBlank()) { "database 不能为空" }
        require(collection.isNotBlank()) { "collection 不能为空" }
        require(batchSize > 0) { "batch_size 必须 > 0" }
        require(upsertKeys.none { it.isBlank() }) { "upsert_keys 不能包含空字段名" }
        require(upsertKeys.distinct().size == upsertKeys.size) { "upsert_keys 不能重复" }
        parseSchemaFields(schemaFields)
        if (upsert && schemaFields.isNotEmpty()) {
            val names = parseSchemaFields(schemaFields).map { it.name }.toSet()
            val unknown = upsertKeys.filterNot { it in names }
            require(unknown.isEmpty()) { "upsert_keys 里的 $unknown 不在 schema_fields 中，无法作为主键" }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
