package me.jayer.hdata.debezium

import com.fasterxml.jackson.annotation.JsonProperty
import java.io.Serializable
import java.nio.file.Files
import java.util.Properties

/**
 * `ReadFromDebezium` 的配置。
 *
 * 用 Debezium 的嵌入式引擎（[io.debezium.embedded.EmbeddedEngine]）直接对接数据库的
 * binlog / WAL 做变更捕获（CDC），把每张表的变更事件映射成一行：
 *
 * | 字段 | 含义 |
 * |---|---|
 * | `op` | 操作类型：`c`(insert) / `u`(update) / `d`(delete) / `r`(snapshot/read) / `t`(truncate) |
 * | `key` | 主键（JSON 字符串） |
 * | `before` | 变更前整行（JSON 字符串，删除/插入可能为 null） |
 * | `after` | 变更后整行（JSON 字符串，删除可能为 null） |
 * | `source` | 来源元数据（JSON 字符串） |
 * | `ts_ms` | 变更发生的时间戳（毫秒） |
 *
 * 输出 schema 固定，不要求用户在 pipeline 里声明 `schema_fields`；这也意味着同一个
 * pipeline 可以捕获多张结构不同的表，每行各自携带自己的 `before` / `after` JSON。
 *
 * `connector` 支持 `mysql` / `postgres`，也可通过 `connector_class` 直接指定任意
 * Debezium 连接器类；其余引擎参数（如 `topic.prefix`、`schema.history.internal` 等）
 *可通过 `extra` 透传，优先级高于本配置自动生成的项。
 *
 * @author wuya
 */
data class DebeziumReadConfig(
    @JsonProperty("connector") val connector: String = "mysql",
    @JsonProperty("connector_class") val connectorClass: String? = null,
    @JsonProperty("host") val host: String? = null,
    @JsonProperty("port") val port: Int? = null,
    @JsonProperty("user") val user: String? = null,
    @JsonProperty("password") val password: String? = null,
    @JsonProperty("database") val database: String? = null,
    @JsonProperty("table_include") val tableInclude: String? = null,
    @JsonProperty("snapshot_mode") val snapshotMode: String? = "initial",
    @JsonProperty("server_name") val serverName: String? = "hdata",
    @JsonProperty("server_id") val serverId: Int? = 184054,
    @JsonProperty("offset_file") val offsetFile: String? = null,
    @JsonProperty("schema_history_file") val schemaHistoryFile: String? = null,
    @JsonProperty("name") val name: String? = null,
    @JsonProperty("max_records") val maxRecords: Int? = null,
    @JsonProperty("extra") val extra: Map<String, String>? = null,
) : Serializable {

    fun toProperties(): Properties {
        val p = Properties()
        p["name"] = name ?: "hdata-debezium"
        p["connector.class"] = connectorClass ?: defaultConnectorClass(connector)
        p["tasks.max"] = "1"
        if (host != null) p["database.hostname"] = host
        if (port != null) p["database.port"] = port.toString()
        if (user != null) p["database.user"] = user
        if (password != null) p["database.password"] = password
        if (database != null) p["database.dbname"] = database
        if (tableInclude != null) p["table.include.list"] = tableInclude
        if (snapshotMode != null) p["snapshot.mode"] = snapshotMode
        if (serverName != null) {
            p["topic.prefix"] = serverName
            p["database.server.name"] = serverName
        }
        if (serverId != null) p["server.id"] = serverId.toString()
        p["offset.storage"] = "org.apache.kafka.connect.storage.FileOffsetBackingStore"
        p["offset.storage.file.filename"] =
            offsetFile ?: Files.createTempFile("debezium-offsets", ".dat").toString()
        val isMySql = connectorClass?.contains("mysql", ignoreCase = true) == true || connector == "mysql"
        if (isMySql) {
            p["schema.history.internal"] = "io.debezium.storage.file.history.FileSchemaHistory"
            p["schema.history.internal.file.filename"] =
                schemaHistoryFile ?: Files.createTempFile("debezium-schema", ".dat").toString()
        }
        extra?.forEach { (k, v) -> p[k] = v }
        return p
    }

    fun validate() {
        if (connectorClass == null) {
            require(connector in setOf("mysql", "postgres")) { "connector 仅支持 mysql/postgres，或显式指定 connector_class" }
            require(!host.isNullOrBlank()) { "host 必填" }
            require(!user.isNullOrBlank()) { "user 必填" }
        }
    }

    private fun defaultConnectorClass(connector: String): String = when (connector) {
        "mysql" -> "io.debezium.connector.mysql.MySqlConnector"
        "postgres" -> "io.debezium.connector.postgresql.PostgresConnector"
        else -> throw IllegalArgumentException("未知 connector: $connector，请通过 connector_class 指定")
    }
}
