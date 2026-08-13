package me.jayer.hdata.core.spi

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.ErrorHandlingSpec
import me.jayer.hdata.core.spec.SpecMappers
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * @author wuya
 * @date 2022-08-30
 */
class TransformConfigTest {

    data class SampleConfig(
        val url: String = "",
        val fetchSize: Int = 10000,
        val tables: List<String> = emptyList(),
        val connectionProperties: Map<String, String> = emptyMap(),
    )

    private fun configOf(json: String, errorHandling: ErrorHandlingSpec? = null) =
        TransformConfig("Sample", SpecMappers.CONFIG.readTree(json) as ObjectNode, errorHandling)

    @Test
    fun `配置键按 snake_case 绑定到驼峰属性`() {
        val bound = configOf("""{"url": "jdbc:x", "fetch_size": 500, "tables": ["a", "b"]}""")
            .bind(SampleConfig::class.java)

        assertEquals("jdbc:x", bound.url)
        assertEquals(500, bound.fetchSize)
        assertEquals(listOf("a", "b"), bound.tables)
    }

    @Test
    fun `未声明的键沿用默认值`() {
        val bound = configOf("""{"url": "jdbc:x"}""").bind(SampleConfig::class.java)
        assertEquals(10000, bound.fetchSize)
        assertEquals(emptyList(), bound.tables)
    }

    @Test
    fun `拼错的键会报错并带上 transform 名字`() {
        val error = assertFailsWith<HDataException> {
            configOf("""{"url": "jdbc:x", "fetch_sizee": 1}""").bind(SampleConfig::class.java)
        }
        assertTrue("Sample" in error.message!! && "fetch_sizee" in error.message!!)
    }

    @Test
    fun `自由 Map 的键不受命名策略影响`() {
        val bound = configOf("""{"connection_properties": {"maximumPoolSize": "4", "some_key": "v"}}""")
            .bind(SampleConfig::class.java)

        assertEquals(mapOf("maximumPoolSize" to "4", "some_key" to "v"), bound.connectionProperties)
    }

    @Test
    fun `类型不匹配时报错`() {
        val error = assertFailsWith<HDataException> {
            configOf("""{"fetch_size": "many"}""").bind(SampleConfig::class.java)
        }
        assertTrue("Sample" in error.message!!)
    }

    @Test
    fun `isEmpty 与 errorHandling 直通`() {
        assertTrue(configOf("{}").isEmpty)
        assertTrue(!configOf("""{"url": "x"}""").isEmpty)

        assertNull(configOf("{}").errorHandling)
        assertEquals(
            ErrorHandlingSpec("rejected"),
            configOf("{}", ErrorHandlingSpec("rejected")).errorHandling,
        )
    }
}
