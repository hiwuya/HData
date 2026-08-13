package me.jayer.hdata.ftp

import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spec.SpecMappers
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

class FtpReadConfigTest {

    private fun buildConfig(yaml: String): TransformConfig {
        val node = SpecMappers.YAML.readTree(yaml) as ObjectNode
        return TransformConfig("test", node)
    }

    @Test
    fun `valid yaml binds to provider and builds a transform`() {
        val yaml = """
            host: ftp.example.com
            port: 21
            user: alice
            password: secret
            path: /in
            file_format: text
        """.trimIndent()
        val cfg = buildConfig(yaml)
        val transform = FtpReadProvider().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `valid csv yaml binds to provider`() {
        val yaml = """
            host: ftp.example.com
            path: /in
            file_format: csv
            schema_fields:
              - id:long
              - name:string
        """.trimIndent()
        val cfg = buildConfig(yaml)
        val transform = FtpReadProvider().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `validate throws when host is blank`() {
        val cfg = FtpReadConfig(host = "", hostName = "", path = "/in")
        assertThrows(IllegalArgumentException::class.java) { cfg.validate() }
    }

    @Test
    fun `validate throws when path is blank`() {
        val cfg = FtpReadConfig(host = "h", path = "")
        assertThrows(IllegalArgumentException::class.java) { cfg.validate() }
    }

    @Test
    fun `validate throws for illegal file_format`() {
        val cfg = FtpReadConfig(host = "h", path = "/in", fileFormat = "xml")
        assertThrows(IllegalArgumentException::class.java) { cfg.validate() }
    }

    @Test
    fun `validate throws when csv has no schema_fields`() {
        val cfg = FtpReadConfig(host = "h", path = "/in", fileFormat = "csv", schemaFields = null)
        assertThrows(IllegalArgumentException::class.java) { cfg.validate() }
    }

    @Test
    fun `validate passes for valid text config`() {
        val cfg = FtpReadConfig(host = "h", path = "/in", fileFormat = "text")
        cfg.validate()
    }

    @Test
    fun `validate passes for valid csv config`() {
        val cfg = FtpReadConfig(
            host = "h",
            path = "/in",
            fileFormat = "csv",
            schemaFields = listOf("id:long", "name:string"),
        )
        cfg.validate()
    }
}
