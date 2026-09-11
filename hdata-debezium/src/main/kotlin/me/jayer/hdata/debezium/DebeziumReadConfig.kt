package me.jayer.hdata.debezium

import com.fasterxml.jackson.annotation.JsonProperty
import java.io.Serializable
import java.nio.file.Files
import java.util.Properties

/**
 * Config for `ReadFromDebezium`.
 *
 * Uses Debezium's embedded engine ([io.debezium.embedded.EmbeddedEngine]) to hook directly into the database's
 * binlog / WAL for change data capture (CDC), mapping each table's change events into a row:
 *
 * | Field | Meaning |
 * |---|---|
 * | `op` | Operation type: `c`(insert) / `u`(update) / `d`(delete) / `r`(snapshot/read) / `t`(truncate) |
 * | `key` | Primary key (JSON string) |
 * | `before` | The whole row before the change (JSON string, may be null for delete/insert) |
 * | `after` | The whole row after the change (JSON string, may be null for delete) |
 * | `source` | Source metadata (JSON string) |
 * | `ts_ms` | The timestamp when the change occurred (milliseconds) |
 *
 * The output schema is fixed and does not require the user to declare `schema_fields` in the pipeline; this also means
 * a single pipeline can capture multiple tables of differing structures, with each row carrying its own
 * `before` / `after` JSON.
 *
 * `connector` supports `mysql` / `postgres`, and any Debezium connector class can also be specified directly via
 * `connector_class`; other engine parameters (such as `topic.prefix`, `schema.history.internal`, etc.) can be passed
 * through via `extra`, which takes precedence over the entries this config generates automatically.
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
    /** MySQL server id; when left empty 184054 is used, and it must not be configured for non-MySQL connectors. */
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
        require(connectorClass == null || connectorClass.isNotBlank()) { "connector_class must not be empty" }
        require(port == null || port in 1..65535) { "port must be between 1 and 65535" }
        require(maxRecords == null || maxRecords > 0) { "max_records must be greater than 0" }
        require(serverId == null || serverId > 0) { "server_id must be greater than 0" }
        require(serverName == null || serverName.isNotBlank()) { "server_name must not be empty" }
        require(name == null || name.isNotBlank()) { "name must not be empty" }
        require(host == null || host.isNotBlank()) { "host must not be empty" }
        require(user == null || user.isNotBlank()) { "user must not be empty" }
        require(database == null || database.isNotBlank()) { "database must not be empty" }
        require(tableInclude == null || tableInclude.isNotBlank()) { "table_include must not be empty" }
        require(snapshotMode == null || snapshotMode.isNotBlank()) { "snapshot_mode must not be empty" }
        require(offsetFile == null || offsetFile.isNotBlank()) { "offset_file must not be empty" }
        require(schemaHistoryFile == null || schemaHistoryFile.isNotBlank()) { "schema_history_file must not be empty" }
        require(kind == "mysql" || serverId == null) { "server_id is only used by the MySQL connector, please remove it from the config" }
        require(kind == "mysql" || schemaHistoryFile == null) {
            "schema_history_file is only used by the MySQL connector, please remove it from the config"
        }
        require(extra.orEmpty().keys.none { it.isBlank() }) { "extra must not contain an empty config key" }
        if (connectorClass == null) {
            require(kind in setOf("mysql", "postgres")) { "connector only supports mysql/postgres, or specify connector_class explicitly" }
            require(!host.isNullOrBlank()) { "host is required" }
            require(!user.isNullOrBlank()) { "user is required" }
            if (kind == "postgres") require(!database.isNullOrBlank()) { "database is required for Postgres" }
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
        else -> throw IllegalArgumentException("Unknown connector: $connector, please specify it via connector_class")
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private const val DEFAULT_MYSQL_SERVER_ID = 184054
    }
}
