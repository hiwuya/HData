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
 * [FilesystemWriteConfig] 的绑定与校验。
 *
 * @author wuya
 */
class FilesystemWriteConfigTest {

    private val minimal = FilesystemWriteConfig(path = "file:///tmp/out")

    private fun cfg(json: String): FilesystemWriteConfig =
        TransformConfig("test", SpecMappers.CONFIG.readTree(json) as ObjectNode).bind(FilesystemWriteConfig::class.java)

    @Test
    fun `配置按 snake_case 绑定`() {
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
    fun `默认值`() {
        val config = cfg("""{"path": "file:///tmp/out"}""")

        assertEquals("text", config.fileFormat)
        assertEquals("output", config.filePrefix)
        // 0 表示交给 runner 决定分片数，吞吐最好
        assertEquals(0, config.numShards)
        assertEquals(",", config.csvDelimiter)
    }

    @Test
    fun `扩展名跟着格式走`() {
        assertEquals(".txt", minimal.suffix())
        assertEquals(".csv", minimal.copy(fileFormat = "csv").suffix())
        assertEquals(".xlsx", minimal.copy(fileFormat = "xlsx").suffix())
    }

    @Test
    fun `path 为空时报错`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(path = "").validate() }
    }

    @Test
    fun `file_format 取值非法时报错并列出可选值`() {
        val error = assertFailsWith<IllegalArgumentException> { minimal.copy(fileFormat = "parquet").validate() }

        assertTrue("xlsx" in error.message!!)
    }

    @Test
    fun `csv 与 xlsx 需要 schema_fields`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(fileFormat = "csv").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(fileFormat = "xlsx", numShards = 1).validate() }
    }

    @Test
    fun `xlsx 必须单分片`() {
        // 一个工作簿就是一个完整的 zip 容器，切成多份没有意义
        val error = assertFailsWith<IllegalArgumentException> {
            minimal.copy(fileFormat = "xlsx", schemaFields = listOf("name:string"), numShards = 3).validate()
        }

        assertTrue("num_shards" in error.message!!)
    }

    @Test
    fun `分隔符与引号必须是单个字符`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(csvDelimiter = "||").validate() }
        assertFailsWith<IllegalArgumentException> { minimal.copy(csvQuote = "").validate() }
    }

    @Test
    fun `编码不认识时报错`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(encoding = "UTF-99").validate() }
    }

    @Test
    fun `num_shards 为负时报错`() {
        assertFailsWith<IllegalArgumentException> { minimal.copy(numShards = -1).validate() }
    }

    @Test
    fun `provider 生成的 sink 可以序列化下发`() {
        val transform = FilesystemWriteProvider().from(
            TransformConfig("WriteToFilesystem", SpecMappers.CONFIG.readTree("""{"path": "file:///tmp/out"}""") as ObjectNode)
        )

        assertNotNull(transform)
        SerializableUtils.ensureSerializable(transform)
    }
}
