package me.jayer.hdata.ftp

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.util.SerializableUtils
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

/**
 * Binding and validation of [FtpWriteConfig].
 *
 * @author wuya
 */
class FtpWriteConfigTest {

    private val minimal = FtpWriteConfig(host = "localhost", path = "/upload")

    private fun cfg(json: String): FtpWriteConfig =
        TransformConfig("test", SpecMappers.CONFIG.readTree(json) as ObjectNode).bind(FtpWriteConfig::class.java)

    @Test
    fun `config binds snake_case keys`() {
        val config = cfg(
            """
            {
              "host": "localhost",
              "port": 2121,
              "user": "u",
              "password": "p",
              "path": "/upload",
              "file_prefix": "orders",
              "file_format": "csv",
              "schema_fields": ["name:string", "age:int"],
              "header": true,
              "batch_size": 500,
              "timeout_millis": 5000
            }
            """.trimIndent()
        )

        assertEquals(2121, config.port)
        assertEquals("orders", config.filePrefix)
        assertEquals(500, config.batchSize)
        assertEquals(5000, config.timeoutMillis)
        config.validate()
    }

    @Test
    fun `defaults`() {
        val config = cfg("""{"host": "localhost", "path": "/upload"}""")

        assertEquals(21, config.port)
        assertEquals("hdata-output", config.filePrefix)
        assertEquals("text", config.fileFormat)
        assertEquals(1000, config.batchSize)
        assertEquals(30_000, config.timeoutMillis)
    }

    @Test
    fun `shard path is prefix shard number and extension combined`() {
        assertEquals("/upload/hdata-output-ab12.txt", minimal.shardPath("ab12"))
        assertEquals(
            "/upload/data-ab12.csv",
            minimal.copy(filePrefix = "data", fileFormat = "csv").shardPath("ab12"),
        )
        // a trailing slash does not produce a double slash
        assertEquals("/upload/hdata-output-ab12.txt", minimal.copy(path = "/upload/").shardPath("ab12"))
    }

    @Test
    fun `csv header field names come from schema_fields`() {
        val config = minimal.copy(fileFormat = "csv", schemaFields = listOf("name:string", "age:int"))

        assertEquals(listOf("name", "age"), config.outputFieldNames)
        // in text mode it is a single content column
        assertEquals(listOf("content"), minimal.outputFieldNames)
    }

    @Test
    fun `blank required fields raise an error`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(host = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(path = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(filePrefix = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(batchSize = 0).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(timeoutMillis = 0).validate() }
    }

    @Test
    fun `host_name can substitute host`() {
        FtpWriteConfig(hostName = "localhost", path = "/upload").validate()
    }

    @Test
    fun `csv requires schema_fields`() {
        val error = assertFailsWith<IllegalArgumentException> { minimal.copy(fileFormat = "csv").validate() }

        assertTrue("schema_fields" in error.message!!)
    }

    @Test
    fun `an invalid file_format raises an error`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(fileFormat = "parquet").validate() }
    }

    @Test
    fun `csv_delimiter and csv_quote must be a single character`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(csvDelimiter = "||").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(csvQuote = "").validate() }
    }

    @Test
    fun `timeout_millis must be positive`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(timeoutMillis = 0).validate() }
    }

    @Test
    fun `an invalid encoding raises an error`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(encoding = "UTF-99").validate() }
    }

    @Test
    fun `host_name can substitute host on the write side`() {
        FtpWriteConfig(hostName = "localhost", path = "/upload").validate()
    }

    @Test
    fun `connection port conflicting aliases and duplicate fields are rejected`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(port = 65536).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(hostName = "other").validate() }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(fileFormat = "csv", schemaFields = listOf("id:long", "id:string")).validate()
        }
    }

    @Test
    fun `text mode rejects CSV parameters it will not use`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(header = true).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(schemaFields = listOf("id:int")).validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(csvQuote = "'").validate() }
    }

    @Test
    fun `the sink produced by the provider is serializable`() {
        val transform = FtpWriteProvider().from(
            TransformConfig("WriteToFtp", SpecMappers.CONFIG.readTree("""{"host": "h", "path": "/upload"}""") as ObjectNode)
        )

        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }
}
