package me.jayer.hdata.core.spec

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * The contract of [SpecMappers]: the snake_case naming strategy + `FAIL_ON_UNKNOWN_PROPERTIES` turned on.
 *
 * The latter is the foundation of every connector's "a misspelled config key errors out directly
 * instead of being silently dropped" behavior — if this switch were ever turned off, every
 * connector's unknown-key validation would quietly stop working, with no connector able to notice it
 * on its own. hdata-core pins it down right here.
 */
class SpecMappersTest {

    private data class SampleConfig(
        val fetchSize: Int = 1,
        val tableName: String = "",
    )

    @Test
    fun `the CONFIG mapper uses the snake_case naming strategy`() {
        val cfg = SpecMappers.CONFIG.readValue(
            """{"fetch_size": 5, "table_name": "t"}""",
            SampleConfig::class.java,
        )
        assertEquals(5, cfg.fetchSize)
        assertEquals("t", cfg.tableName)
    }

    @Test
    fun `the CONFIG mapper has FAIL_ON_UNKNOWN_PROPERTIES on, a misspelled key errors out directly`() {
        val error = assertFailsWith<Exception> {
            SpecMappers.CONFIG.readValue(
                """{"fetch_size": 5, "tabel": "oops"}""",
                SampleConfig::class.java,
            )
        }
        assertTrue("tabel" in error.message!!, error.message)
    }
}
