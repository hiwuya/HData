package me.jayer.hdata.filesystem

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull

/**
 * `WriteToFilesystem` 配置绑定（snake_case）与 [FilesystemWriteConfig.validate] 校验。
 */
class FilesystemWriteConfigTest {

    private fun config(yaml: String): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    @Test
    fun `write config binds snake_case keys and builds a transform`() {
        val cfg = config(
            """
            path: "/tmp/output"
            default_fs: "file:///"
            file_format: text
            batch_size: 500
            encoding: UTF-8
            file_name: "out.txt"
            """.trimIndent()
        )
        val transform = FilesystemWriteProvider().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `write config with schema_fields binds list`() {
        val cfg = config(
            """
            path: "/tmp/output"
            file_format: csv
            schema_fields: ["name:string", "age:int"]
            batch_size: 100
            """.trimIndent()
        )
        val transform = FilesystemWriteProvider().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `csv write without schema_fields fails validation`() {
        val cfg = config(
            """
            path: "/tmp/output"
            file_format: csv
            """.trimIndent()
        )
        assertFailsWith<IllegalArgumentException> {
            FilesystemWriteProvider().from(cfg)
        }
    }

    @Test
    fun `validate throws on empty path`() {
        assertFailsWith<IllegalArgumentException> {
            FilesystemWriteConfig(path = "").validate()
        }
    }

    @Test
    fun `validate throws on non-positive batch_size`() {
        assertFailsWith<IllegalArgumentException> {
            FilesystemWriteConfig(path = "/tmp/x", batchSize = 0).validate()
        }
    }

    @Test
    fun `validate passes for valid text config`() {
        FilesystemWriteConfig(path = "/tmp/x", fileFormat = "text", batchSize = 10).validate()
    }
}
