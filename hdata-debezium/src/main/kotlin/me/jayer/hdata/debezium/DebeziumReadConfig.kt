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
    /** MySQL server id；留空时实际使用 184054，非 MySQL 连接器不得配置。 */
    @JsonProperty("server_id") val serverId: Int? = null,
    @JsonProperty("offset_file") val offsetFile: String? = null,
    @JsonProperty("schema_history_file") val schemaHistoryFile: String? = null,
    @JsonProperty("name") val name: String? = null,
    @JsonProperty("max_records") val maxRecords: Int? = null,
    @JsonProperty("extra") val extra: Map<String, String>? = null,
) : Serializable {

    fun toProperties(): Properties {
        val kind = connectorKind()
        val p = Properties()
        p["name"] = name ?: "hdata-debezium"
        p["connector.class"] = connectorClass ?: defaultConnectorClass(connector)
        p["tasks.max"] = "1"
        if (host != null) p["database.hostname"] = host
        if (port != null) p["database.port"] = port.toString()
        if (user != null) p["database.user"] = user
        if (password != null) p["database.password"] = password
        if (database != null) {
            p[if (kind == "mysql") "database.include.list" else "database.dbname"] = database
        }
        if (tableInclude != null) p["table.include.list"] = tableInclude
        if (snapshotMode != null) p["snapshot.mode"] = snapshotMode
        if (serverName != null) {
            p["topic.prefix"] = serverName
        }
        if (kind == "mysql") p["database.server.id"] = (serverId ?: DEFAULT_MYSQL_SERVER_ID).toString()
        p["offset.storage"] = "org.apache.kafka.connect.storage.FileOffsetBackingStore"
        p["offset.storage.file.filename"] =
            offsetFile ?: Files.createTempFile("debezium-offsets", ".dat").toString()
        if (kind == "mysql") {
            p["schema.history.internal"] = "io.debezium.storage.file.history.FileSchemaHistory"
            p["schema.history.internal.file.filename"] =
                schemaHistoryFile ?: Files.createTempFile("debezium-schema", ".dat").toString()
        }
        extra?.forEach { (k, v) -> p[k] = v }
        return p
    }

    fun validate() {
        val kind = connectorKind()
        require(connectorClass == null || connectorClass.isNotBlank()) { "connector_class 不能为空" }
        require(port == null || port in 1..65535) { "port 必须在 1..65535 之间" }
        require(maxRecords == null || maxRecords > 0) { "max_records 必须大于 0" }
        require(serverId == null || serverId > 0) { "server_id 必须大于 0" }
        require(serverName == null || serverName.isNotBlank()) { "server_name 不能为空" }
        require(name == null || name.isNotBlank()) { "name 不能为空" }
        require(host == null || host.isNotBlank()) { "host 不能为空" }
        require(user == null || user.isNotBlank()) { "user 不能为空" }
        require(database == null || database.isNotBlank()) { "database 不能为空" }
        require(tableInclude == null || tableInclude.isNotBlank()) { "table_include 不能为空" }
        require(snapshotMode == null || snapshotMode.isNotBlank()) { "snapshot_mode 不能为空" }
        require(offsetFile == null || offsetFile.isNotBlank()) { "offset_file 不能为空" }
        require(schemaHistoryFile == null || schemaHistoryFile.isNotBlank()) { "schema_history_file 不能为空" }
        require(kind == "mysql" || serverId == null) { "server_id 只用于 MySQL 连接器，请从配置中移除" }
        require(kind == "mysql" || schemaHistoryFile == null) {
            "schema_history_file 只用于 MySQL 连接器，请从配置中移除"
        }
        require(extra.orEmpty().keys.none { it.isBlank() }) { "extra 不能包含空配置键" }
        if (connectorClass == null) {
            require(kind in setOf("mysql", "postgres")) { "connector 仅支持 mysql/postgres，或显式指定 connector_class" }
            require(!host.isNullOrBlank()) { "host 必填" }
            require(!user.isNullOrBlank()) { "user 必填" }
            if (kind == "postgres") require(!database.isNullOrBlank()) { "Postgres 的 database 必填" }
        }
    }

    private fun connectorKind(): String = connectorClass?.let {
        when {
            it.contains("mysql", ignoreCase = true) -> "mysql"
            it.contains("postgres", ignoreCase = true) -> "postgres"
            else -> "custom"
        }
    } ?: connector.trim().lowercase()

    private fun defaultConnectorClass(connector: String): String = when (connector.trim().lowercase()) {
        "mysql" -> "io.debezium.connector.mysql.MySqlConnector"
        "postgres" -> "io.debezium.connector.postgresql.PostgresConnector"
        else -> throw IllegalArgumentException("未知 connector: $connector，请通过 connector_class 指定")
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private const val DEFAULT_MYSQL_SERVER_ID = 184054
    }
}
