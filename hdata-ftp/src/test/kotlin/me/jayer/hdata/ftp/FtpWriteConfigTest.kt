package me.jayer.hdata.ftp

import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spec.SpecMappers
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

class FtpWriteConfigTest {

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
            path: /out
            file_name: out.txt
            file_format: text
            batch_size: 500
        """.trimIndent()
        val cfg = buildConfig(yaml)
        val transform = FtpWriteProvider().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `valid csv yaml binds to provider`() {
        val yaml = """
            host: ftp.example.com
            path: /out
            file_format: csv
            schema_fields:
              - id:long
              - name:string
            batch_size: 100
        """.trimIndent()
        val cfg = buildConfig(yaml)
        val transform = FtpWriteProvider().from(cfg)
        assertNotNull(transform)
    }

    @Test
    fun `validate throws when host is blank`() {
        val cfg = FtpWriteConfig(host = "", hostName = "", path = "/out")
        assertThrows(IllegalArgumentException::class.java) { cfg.validate() }
    }

    @Test
    fun `validate throws when path is blank`() {
        val cfg = FtpWriteConfig(host = "h", path = "")
        assertThrows(IllegalArgumentException::class.java) { cfg.validate() }
    }

    @Test
    fun `validate throws when batch_size is not positive`() {
        val cfg = FtpWriteConfig(host = "h", path = "/out", batchSize = 0)
        assertThrows(IllegalArgumentException::class.java) { cfg.validate() }
    }

    @Test
    fun `validate throws for illegal file_format`() {
        val cfg = FtpWriteConfig(host = "h", path = "/out", fileFormat = "xml")
        assertThrows(IllegalArgumentException::class.java) { cfg.validate() }
    }

    @Test
    fun `validate throws when csv has no schema_fields`() {
        val cfg = FtpWriteConfig(host = "h", path = "/out", fileFormat = "csv", schemaFields = null)
        assertThrows(IllegalArgumentException::class.java) { cfg.validate() }
    }

    @Test
    fun `validate passes for valid text config`() {
        val cfg = FtpWriteConfig(host = "h", path = "/out", fileFormat = "text", batchSize = 10)
        cfg.validate()
    }
}
