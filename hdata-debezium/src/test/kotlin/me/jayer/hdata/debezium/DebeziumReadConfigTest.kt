package me.jayer.hdata.debezium

import org.junit.jupiter.api.Test
import java.nio.file.Files
import kotlin.test.assertFailsWith
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class DebeziumReadConfigTest {

    @Test
    fun `mysql 必填校验`() {
        assertFailsWith<IllegalArgumentException> {
            DebeziumReadConfig(connector = "mysql").validate()
        }
    }

    @Test
    fun `未知 connector 报错`() {
        assertFailsWith<IllegalArgumentException> {
            DebeziumReadConfig(connector = "oracle").validate()
        }
    }

    @Test
    fun `host user 必填`() {
        assertFailsWith<IllegalArgumentException> { DebeziumReadConfig(connector = "mysql", host = "h").validate() }
        assertFailsWith<IllegalArgumentException> { DebeziumReadConfig(connector = "mysql", user = "u").validate() }
        DebeziumReadConfig(connector = "mysql", host = "h", user = "u").validate()
    }

    @Test
    fun `toProperties 含 offset 与 schema history`() {
        val p = DebeziumReadConfig(
            connector = "mysql",
            host = "h",
            user = "u",
            database = "db",
            tableInclude = "t.*",
        ).toProperties()
        assertTrue(p["connector.class"] == "io.debezium.connector.mysql.MySqlConnector")
        assertTrue(p["database.hostname"] == "h")
        assertTrue(p["database.user"] == "u")
        assertTrue(p["database.include.list"] == "db")
        assertTrue(p["database.server.id"] == "184054")
        assertTrue(p["server.id"] == null)
        assertTrue(p["table.include.list"] == "t.*")
        assertTrue(p["offset.storage"] == "org.apache.kafka.connect.storage.FileOffsetBackingStore")
        assertTrue(p["schema.history.internal"] == "io.debezium.storage.file.history.FileSchemaHistory")
        assertTrue(p["offset.storage.file.filename"] != null)
    }

    @Test
    fun `extra 覆盖默认项`() {
        val p = DebeziumReadConfig(
            connector = "mysql",
            host = "h",
            user = "u",
            extra = mapOf("topic.prefix" to "custom"),
        ).toProperties()
        assertTrue(p["topic.prefix"] == "custom")
    }

    @Test
    fun `connector_class 给定时无需 host 或 user`() {
        // With a custom connector_class (such as an embedded test source), no real database is connected, so host/user should no longer be strictly validated.
        DebeziumReadConfig(
            connector = "mysql",
            connectorClass = "io.debezium.connector.mysql.MySqlConnector",
        ).validate()
    }

    @Test
    fun `postgres 使用 dbname 且不带 mysql server id`() {
        val config = DebeziumReadConfig(connector = "POSTGRES", host = "h", user = "u", database = "db")
        config.validate()
        val p = config.toProperties()
        assertEquals("io.debezium.connector.postgresql.PostgresConnector", p["connector.class"])
        assertEquals("db", p["database.dbname"])
        assertTrue(p["database.include.list"] == null)
        assertTrue(p["database.server.id"] == null)
    }

    @Test
    fun `非 MySQL 连接器拒绝不会生效的 MySQL 专用配置`() {
        assertFailsWith<IllegalArgumentException> {
            DebeziumReadConfig(
                connector = "postgres",
                host = "h",
                user = "u",
                database = "db",
                serverId = 7,
            ).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            DebeziumReadConfig(
                connector = "postgres",
                host = "h",
                user = "u",
                database = "db",
                schemaHistoryFile = "/tmp/history.dat",
            ).validate()
        }
    }

    @Test
    fun `边界配置在构图前被拒绝`() {
        assertFailsWith<IllegalArgumentException> {
            DebeziumReadConfig(connector = "mysql", host = "h", user = "u", port = 70000).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            DebeziumReadConfig(connector = "mysql", host = "h", user = "u", maxRecords = 0).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            DebeziumReadConfig(connector = "postgres", host = "h", user = "u").validate()
        }
        assertFailsWith<IllegalArgumentException> {
            DebeziumReadConfig(connector = "mysql", host = "h", user = "u", snapshotMode = " ").validate()
        }
        assertFailsWith<IllegalArgumentException> {
            DebeziumReadConfig(connector = "mysql", host = "h", user = "u", extra = mapOf("" to "x")).validate()
        }
    }

    @Test
    fun `offset_file 与 schema_history_file 真的生效`() {
        // These two paths used to be accepted and then discarded (defaulting to temp files), so users thought they had specified where offsets/history were persisted, but it actually had no effect.
        val offset = Files.createTempFile("off", ".dat").toString()
        val hist = Files.createTempFile("hist", ".dat").toString()
        val p = DebeziumReadConfig(
            connector = "mysql",
            host = "h",
            user = "u",
            offsetFile = offset,
            schemaHistoryFile = hist,
        ).toProperties()
        assertEquals(offset, p["offset.storage.file.filename"])
        assertEquals(hist, p["schema.history.internal.file.filename"])
    }
}
