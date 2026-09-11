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
    fun `config keys bind to camelCase properties by snake_case`() {
        val bound = configOf("""{"url": "jdbc:x", "fetch_size": 500, "tables": ["a", "b"]}""")
            .bind(SampleConfig::class.java)

        assertEquals("jdbc:x", bound.url)
        assertEquals(500, bound.fetchSize)
        assertEquals(listOf("a", "b"), bound.tables)
    }

    @Test
    fun `undeclared keys fall back to their default values`() {
        val bound = configOf("""{"url": "jdbc:x"}""").bind(SampleConfig::class.java)
        assertEquals(10000, bound.fetchSize)
        assertEquals(emptyList(), bound.tables)
    }

    @Test
    fun `a misspelled key errors out and includes the transform name`() {
        val error = assertFailsWith<HDataException> {
            configOf("""{"url": "jdbc:x", "fetch_sizee": 1}""").bind(SampleConfig::class.java)
        }
        assertTrue("Sample" in error.message!! && "fetch_sizee" in error.message!!)
    }

    @Test
    fun `free-form Map keys are unaffected by the naming strategy`() {
        val bound = configOf("""{"connection_properties": {"maximumPoolSize": "4", "some_key": "v"}}""")
            .bind(SampleConfig::class.java)

        assertEquals(mapOf("maximumPoolSize" to "4", "some_key" to "v"), bound.connectionProperties)
    }

    @Test
    fun `a type mismatch errors out`() {
        val error = assertFailsWith<HDataException> {
            configOf("""{"fetch_size": "many"}""").bind(SampleConfig::class.java)
        }
        assertTrue("Sample" in error.message!!)
    }

    @Test
    fun `isEmpty and errorHandling are passed straight through`() {
        assertTrue(configOf("{}").isEmpty)
        assertTrue(!configOf("""{"url": "x"}""").isEmpty)

        assertNull(configOf("{}").errorHandling)
        assertEquals(
            ErrorHandlingSpec("rejected"),
            configOf("{}", ErrorHandlingSpec("rejected")).errorHandling,
        )
    }
}
