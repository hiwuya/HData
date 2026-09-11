package me.jayer.hdata.filesystem

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

/**
 * Config binding (snake_case) for `ReadFromFilesystem` and validation by [FilesystemReadConfig.validate].
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
    fun `csv_delimiter and csv_quote must be a single character`() {
        assertFailsWith<IllegalArgumentException> { FilesystemReadConfig(path = "/tmp/x", csvDelimiter = "||").validate() }
        assertFailsWith<IllegalArgumentException> { FilesystemReadConfig(path = "/tmp/x", csvQuote = "").validate() }
    }

    @Test
    fun `an invalid encoding raises an error`() {
        assertFailsWith<IllegalArgumentException> { FilesystemReadConfig(path = "/tmp/x", encoding = "UTF-99").validate() }
    }

    @Test
    fun `TextIO reads explicitly reject non-UTF8 so the config cannot silently fail`() {
        assertFailsWith<IllegalArgumentException> {
            FilesystemReadConfig(path = "/tmp/x", fileFormat = "text", encoding = "UTF-16").validate()
        }
    }

    @Test
    fun `parameters the current file format will not use are rejected`() {
        assertFailsWith<IllegalArgumentException> {
            FilesystemReadConfig(path = "/tmp/x", fileFormat = "text", header = true).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            FilesystemReadConfig(
                path = "/tmp/x",
                fileFormat = "csv",
                schemaFields = listOf("id:int"),
                sheet = "Sheet1",
            ).validate()
        }
    }

    @Test
    fun `default_fs and duplicate fields are validated`() {
        assertFailsWith<IllegalArgumentException> { FilesystemReadConfig(path = "/tmp/x", defaultFs = "").validate() }
        assertFailsWith<IllegalArgumentException> {
            FilesystemReadConfig(path = "/tmp/x", defaultFs = "namenode:8020").validate()
        }
        assertFailsWith<IllegalArgumentException> {
            FilesystemReadConfig(
                path = "/tmp/x",
                fileFormat = "csv",
                schemaFields = listOf("id:int", "id:string"),
            ).validate()
        }
    }
}
