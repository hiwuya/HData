package me.jayer.hdata.filesystem

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

/**
 * `ReadFromFilesystem` 配置绑定（snake_case）与 [FilesystemReadConfig.validate] 校验。
 */
class FilesystemReadConfigTest {

    private fun config(yaml: String): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    @Test
    fun `read config binds snake_case keys and builds a transform`() {
        val cfg = config(
            """
            path: "/tmp/input"
            default_fs: "file:///"
            file_format: text
            encoding: UTF-8
            """.trimIndent()
        )
        val transform = FilesystemReadProvider().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `read config with schema_fields binds list`() {
        val cfg = config(
            """
            path: "/tmp/input"
            file_format: csv
            schema_fields: ["name:string", "age:int"]
            """.trimIndent()
        )
        val transform = FilesystemReadProvider().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `csv read without schema_fields fails validation`() {
        val cfg = config(
            """
            path: "/tmp/input"
            file_format: csv
            """.trimIndent()
        )
        assertFailsWith<IllegalArgumentException> {
            FilesystemReadProvider().from(cfg)
        }
    }

    @Test
    fun `validate throws on empty path`() {
        assertFailsWith<IllegalArgumentException> {
            FilesystemReadConfig(path = "").validate()
        }
    }

    @Test
    fun `validate throws on unsupported file_format`() {
        assertFailsWith<IllegalArgumentException> {
            FilesystemReadConfig(path = "/tmp/x", fileFormat = "xml").validate()
        }
    }

    @Test
    fun `validate passes for valid text config`() {
        FilesystemReadConfig(path = "/tmp/x", fileFormat = "text").validate()
    }

    @Test
    fun `csv validate requires schema_fields`() {
        assertFailsWith<IllegalArgumentException> {
            FilesystemReadConfig(path = "/tmp/x", fileFormat = "csv").validate()
        }
        FilesystemReadConfig(path = "/tmp/x", fileFormat = "csv", schemaFields = listOf("a:string")).validate()
    }

    @Test
    fun `read config with xlsx format and schema_fields builds a transform`() {
        val cfg = config(
            """
            path: "/tmp/input"
            file_format: xlsx
            schema_fields: ["name:string", "age:int"]
            header: true
            sheet: "Sheet1"
            """.trimIndent()
        )
        val transform = FilesystemReadProvider().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `xlsx read without schema_fields fails validation`() {
        val cfg = config(
            """
            path: "/tmp/input"
            file_format: xlsx
            """.trimIndent()
        )
        assertFailsWith<IllegalArgumentException> {
            FilesystemReadProvider().from(cfg)
        }
    }

    @Test
    fun `xlsx validate requires schema_fields`() {
        assertFailsWith<IllegalArgumentException> {
            FilesystemReadConfig(path = "/tmp/x", fileFormat = "xlsx").validate()
        }
        FilesystemReadConfig(path = "/tmp/x", fileFormat = "xlsx", schemaFields = listOf("a:string")).validate()
    }

    @Test
    fun `csv_delimiter 与 csv_quote 必须是单字符`() {
        assertFailsWith<IllegalArgumentException> { FilesystemReadConfig(path = "/tmp/x", csvDelimiter = "||").validate() }
        assertFailsWith<IllegalArgumentException> { FilesystemReadConfig(path = "/tmp/x", csvQuote = "").validate() }
    }

    @Test
    fun `encoding 不合法时报错`() {
        assertFailsWith<IllegalArgumentException> { FilesystemReadConfig(path = "/tmp/x", encoding = "UTF-99").validate() }
    }
}
