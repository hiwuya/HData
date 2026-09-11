package me.jayer.hdata.ftp

import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spec.SpecMappers
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import java.nio.charset.Charset
import me.jayer.hdata.ftp.transform.FtpReadFn
import me.jayer.hdata.ftp.transform.FtpFile
import org.apache.beam.sdk.io.range.OffsetRange
import kotlin.test.assertEquals

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
    fun `a blank or invalid file_pattern is rejected instead of silently matching no files`() {
        assertThrows(IllegalArgumentException::class.java) {
            FtpReadConfig(host = "h", path = "/in", filePattern = " ").validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            FtpReadConfig(host = "h", path = "/in", filePattern = "[").validate()
        }
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

    @Test
    fun `validate throws when csv_delimiter is not a single character`() {
        assertThrows(IllegalArgumentException::class.java) {
            FtpReadConfig(host = "h", path = "/in", csvDelimiter = "||").validate()
        }
    }

    @Test
    fun `validate throws when csv_quote is not a single character`() {
        assertThrows(IllegalArgumentException::class.java) {
            FtpReadConfig(host = "h", path = "/in", csvQuote = "").validate()
        }
    }

    @Test
    fun `validate throws when timeout_millis is not positive`() {
        assertThrows(IllegalArgumentException::class.java) {
            FtpReadConfig(host = "h", path = "/in", timeoutMillis = 0).validate()
        }
    }

    @Test
    fun `validate throws for illegal encoding`() {
        assertThrows(IllegalArgumentException::class.java) {
            FtpReadConfig(host = "h", path = "/in", encoding = "UTF-99").validate()
        }
    }

    @Test
    fun `host_name can substitute host`() {
        FtpReadConfig(hostName = "localhost", path = "/in").validate()
    }

    @Test
    fun `connection port and conflicting aliases are rejected`() {
        assertThrows(IllegalArgumentException::class.java) {
            FtpReadConfig(host = "h", path = "/in", port = 0).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            FtpReadConfig(host = "a", hostName = "b", path = "/in").validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            FtpReadConfig(host = "h", user = "a", username = "b", path = "/in").validate()
        }
    }

    @Test
    fun `duplicate schema fields are rejected`() {
        assertThrows(IllegalArgumentException::class.java) {
            FtpReadConfig(
                host = "h",
                path = "/in",
                fileFormat = "csv",
                schemaFields = listOf("id:long", "id:string"),
            ).validate()
        }
    }

    @Test
    fun `UTF16 does not split on single-byte newlines`() {
        kotlin.test.assertFalse(FtpReadFn.byteLineCompatible(Charset.forName("UTF-16")))
        kotlin.test.assertTrue(FtpReadFn.byteLineCompatible(Charset.forName("UTF-8")))
        kotlin.test.assertTrue(FtpReadFn.byteLineCompatible(Charset.forName("GB18030")))
    }

    @Test
    fun `non-splittable formats use a single logical restriction to prevent runtime dynamic splitting`() {
        val csv = FtpReadConfig(host = "h", path = "/in", fileFormat = "csv", schemaFields = listOf("id:int"))
        val utf16 = FtpReadConfig(host = "h", path = "/in", encoding = "UTF-16")
        val text = FtpReadConfig(host = "h", path = "/in", encoding = "UTF-8")
        val file = FtpFile("/in/a", 1_000_000)

        assertEquals(OffsetRange(0, 1), FtpReadFn(csv.connection, csv).getInitialRestriction(file))
        assertEquals(OffsetRange(0, 1), FtpReadFn(utf16.connection, utf16).getInitialRestriction(file))
        assertEquals(OffsetRange(0, 1_000_000), FtpReadFn(text.connection, text).getInitialRestriction(file))
    }

    @Test
    fun `text mode rejects CSV parameters it will not use`() {
        assertThrows(IllegalArgumentException::class.java) {
            FtpReadConfig(host = "h", path = "/in", header = true).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            FtpReadConfig(host = "h", path = "/in", schemaFields = listOf("id:int")).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            FtpReadConfig(host = "h", path = "/in", csvDelimiter = "|").validate()
        }
    }
}
