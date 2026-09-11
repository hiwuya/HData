package me.jayer.hdata.filesystem

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
 * Binding and validation of [FilesystemWriteConfig].
 *
 * @author wuya
 */
class FilesystemWriteConfigTest {

    private val minimal = FilesystemWriteConfig(path = "file:///tmp/out")

    private fun cfg(json: String): FilesystemWriteConfig =
        TransformConfig("test", SpecMappers.CONFIG.readTree(json) as ObjectNode).bind(FilesystemWriteConfig::class.java)

    @Test
    fun `config binds snake_case keys`() {
        val config = cfg(
            """
            {
              "path": "file:///tmp/out",
              "file_format": "csv",
              "schema_fields": ["name:string", "age:int"],
              "header": true,
              "csv_delimiter": "|",
              "file_prefix": "orders",
              "num_shards": 2
            }
            """.trimIndent()
        )

        assertEquals("csv", config.fileFormat)
        assertEquals("|", config.csvDelimiter)
        assertEquals("orders", config.filePrefix)
        assertEquals(2, config.numShards)
        config.validate()
    }

    @Test
    fun `defaults`() {
        val config = cfg("""{"path": "file:///tmp/out"}""")

        assertEquals("text", config.fileFormat)
        assertEquals("output", config.filePrefix)
        // 0 means the runner decides the shard count, which gives the best throughput
        assertEquals(0, config.numShards)
        assertEquals(",", config.csvDelimiter)
    }

    @Test
    fun `the extension follows the file format`() {
        assertEquals(".txt", minimal.suffix())
        assertEquals(".csv", minimal.copy(fileFormat = "csv").suffix())
        assertEquals(".xlsx", minimal.copy(fileFormat = "xlsx").suffix())
    }

    @Test
    fun `an empty path raises an error`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(path = "").validate() }
    }

    @Test
    fun `an invalid file_format raises an error listing the valid values`() {
        val error = assertFailsWith<IllegalArgumentException> { minimal.copy(fileFormat = "parquet").validate() }

        assertTrue("xlsx" in error.message!!)
    }

    @Test
    fun `csv and xlsx require schema_fields`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(fileFormat = "csv").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(fileFormat = "xlsx", numShards = 1).validate() }
    }

    @Test
    fun `xlsx must use a single shard`() {
        // a workbook is one complete zip container, so splitting it into several parts makes no sense
        val error = assertFailsWith<IllegalArgumentException> {
            minimal.copy(fileFormat = "xlsx", schemaFields = listOf("name:string"), numShards = 3).validate()
        }

        assertTrue("num_shards" in error.message!!)
    }

    @Test
    fun `delimiter and quote must be a single character`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(csvDelimiter = "||").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(csvQuote = "").validate() }
    }

    @Test
    fun `an unrecognized encoding raises an error`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(encoding = "UTF-99").validate() }
    }

    @Test
    fun `a negative num_shards raises an error`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(numShards = -1).validate() }
    }

    @Test
    fun `default_fs and the file name prefix must not be blank`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(defaultFs = "").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(defaultFs = "namenode:8020").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(filePrefix = " ").validate() }
    }

    @Test
    fun `xlsx rejects a meaningless non-UTF8 encoding`() {
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(
                fileFormat = "xlsx",
                schemaFields = listOf("name:string"),
                numShards = 1,
                encoding = "UTF-16",
            ).validate()
        }
    }

    @Test
    fun `parameters the current file format will not use are rejected`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(header = true).validate() }
        assertFailsWith<IllegalArgumentException> {
            minimal.copy(fileFormat = "csv", schemaFields = listOf("id:int"), sheet = "Sheet1").validate()
        }
    }

    @Test
    fun `the sink produced by the provider is serializable`() {
        val transform = FilesystemWriteProvider().from(
            TransformConfig("WriteToFilesystem", SpecMappers.CONFIG.readTree("""{"path": "file:///tmp/out"}""") as ObjectNode)
        )

        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }
}
