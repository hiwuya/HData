package me.jayer.hdata.hive

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * `SHOW PARTITIONS` 的输出到 SQL 谓词的翻译。
 *
 * 重构前是把 `dt=2024-01-01/hr=01` 原样拼进 WHERE，既不是合法 SQL（日期没加引号会被当成减法），
 * 多级分区的 `/` 更是直接语法错误。
 *
 * @author wuya
 */
class HivePartitionsTest {

    @Test
    fun `单级分区的值会加引号`() {
        assertEquals("dt = '2024-01-01'", HivePartitions.toPredicate("dt=2024-01-01"))
    }

    @Test
    fun `多级分区用 AND 连起来`() {
        // 原样拼的话是 WHERE dt=2024-01-01/hr=01，直接语法错误
        assertEquals(
            "dt = '2024-01-01' AND hr = '01'",
            HivePartitions.toPredicate("dt=2024-01-01/hr=01"),
        )
    }

    @Test
    fun `值里的单引号被转义`() {
        val predicate = HivePartitions.toPredicate("name=o'brien")

        assertEquals("name = 'o''brien'", predicate)
    }

    @Test
    fun `数字分区值也加引号，靠隐式转换`() {
        // 不加引号则字符串分区必错；加引号对数字列 Hive 会隐式转换，是安全的一侧
        assertEquals("id = '42'", HivePartitions.toPredicate("id=42"))
    }

    @Test
    fun `值里带等号时只按第一个等号切开`() {
        assertEquals("expr = 'a=b'", HivePartitions.toPredicate("expr=a=b"))
    }

    @Test
    fun `值为空的分区（Hive 的 __HIVE_DEFAULT_PARTITION__）也能解析`() {
        assertEquals("dt = ''", HivePartitions.toPredicate("dt="))
    }

    @Test
    fun `格式不对时报错并带上完整规格`() {
        val error = assertFailsWith<IllegalArgumentException> { HivePartitions.toPredicate("dt") }

        assertTrue("dt" in error.message!!)
    }
}
