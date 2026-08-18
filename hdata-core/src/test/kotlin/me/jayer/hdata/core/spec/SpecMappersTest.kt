package me.jayer.hdata.core.spec

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * [SpecMappers] 的契约：snake_case 命名策略 + 开启 `FAIL_ON_UNKNOWN_PROPERTIES`。
 *
 * 后者是所有连接器「配置写错单词直接报错、而不是静默丢弃」的根基——一旦这个开关被关掉，
 * 各个连接器的未知键校验会悄悄全部失效，却没有连接器自己能发现。hdata-core 在这里直接钉死。
 */
class SpecMappersTest {

    private data class SampleConfig(
        val fetchSize: Int = 1,
        val tableName: String = "",
    )

    @Test
    fun `CONFIG mapper 用 snake_case 命名策略`() {
        val cfg = SpecMappers.CONFIG.readValue(
            """{"fetch_size": 5, "table_name": "t"}""",
            SampleConfig::class.java,
        )
        assertEquals(5, cfg.fetchSize)
        assertEquals("t", cfg.tableName)
    }

    @Test
    fun `CONFIG mapper 开启 FAIL_ON_UNKNOWN_PROPERTIES，写错键直接报错`() {
        val error = assertFailsWith<Exception> {
            SpecMappers.CONFIG.readValue(
                """{"fetch_size": 5, "tabel": "oops"}""",
                SampleConfig::class.java,
            )
        }
        assertTrue("tabel" in error.message!!, error.message)
    }
}
