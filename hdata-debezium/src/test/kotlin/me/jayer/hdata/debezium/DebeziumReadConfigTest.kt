package me.jayer.hdata.debezium

import org.junit.jupiter.api.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

class DebeziumReadConfigTest {

    @Test
    fun `mysql 必填校验`() = assertFailsWith<IllegalArgumentException> {
        DebeziumReadConfig(connector = "mysql").validate()
    }

    @Test
    fun `未知 connector 报错`() = assertFailsWith<IllegalArgumentException> {
        DebeziumReadConfig(connector = "oracle").validate()
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
        assertTrue(p["database.dbname"] == "db")
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
}
