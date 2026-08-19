package me.jayer.hdata.ftp

import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spec.SpecMappers
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import java.nio.charset.Charset
import me.jayer.hdata.ftp.transform.FtpReadFn

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
    fun `连接端口与冲突别名会被拒绝`() {
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
    fun `重复 schema 字段会被拒绝`() {
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
    fun `UTF16 不按单字节换行切分`() {
        kotlin.test.assertFalse(FtpReadFn.byteLineCompatible(Charset.forName("UTF-16")))
        kotlin.test.assertTrue(FtpReadFn.byteLineCompatible(Charset.forName("UTF-8")))
        kotlin.test.assertTrue(FtpReadFn.byteLineCompatible(Charset.forName("GB18030")))
    }

    @Test
    fun `text 模式拒绝不会使用的 CSV 参数`() {
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
