package me.jayer.hdata.debezium

import org.junit.jupiter.api.Test
import java.nio.file.Files
import kotlin.test.assertFailsWith
import kotlin.test.assertEquals
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

    @Test
    fun `connector_class 给定时无需 host 或 user`() {
        // 自定义 connector_class 时（如内嵌测试源），不会连真实库，host/user 不应再被强校验
        DebeziumReadConfig(
            connector = "mysql",
            connectorClass = "io.debezium.connector.mysql.MySqlConnector",
        ).validate()
    }

    @Test
    fun `offset_file 与 schema_history_file 真的生效`() {
        // 这两个路径曾经收下就丢掉（默认走临时文件），用户以为自己指定了偏移/历史落盘位置，实际没用
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
