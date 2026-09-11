package me.jayer.hdata.hive.metastore

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

/**
 * Encode/decode of partition names, with rules aligned with Hive's `FileUtils.escapePathName` / `makePartName`.
 *
 * @author wuya
 */
class PartitionNamesTest {

    @Test
    fun `普通值不转义`() {
        assertEquals("dt=2024-01-01", PartitionNames.makePartName(listOf("dt"), listOf("2024-01-01")))
        assertEquals(
            "dt=2024-01-01/hr=01",
            PartitionNames.makePartName(listOf("dt", "hr"), listOf("2024-01-01", "01")),
        )
    }

    @Test
    fun `值里的特殊字符按 Hive 的规则转义`() {
        // These characters would break the directory structure or the parsing of a partition name, so Hive escapes them all as %XX
        assertEquals("a%2Fb", PartitionNames.escapePathName("a/b"))
        assertEquals("a%3Db", PartitionNames.escapePathName("a=b"))
        assertEquals("a%3Ab", PartitionNames.escapePathName("a:b"))
        assertEquals("a%25b", PartitionNames.escapePathName("a%b"))
        assertEquals("a%0Ab", PartitionNames.escapePathName("a\nb"))
    }

    @Test
    fun `null 与空串落到默认分区`() {
        assertEquals(PartitionNames.DEFAULT_PARTITION, PartitionNames.escapePathName(null))
        assertEquals(PartitionNames.DEFAULT_PARTITION, PartitionNames.escapePathName(""))
        assertEquals(
            "dt=__HIVE_DEFAULT_PARTITION__",
            PartitionNames.makePartName(listOf("dt"), listOf(null)),
        )
    }

    @Test
    fun `转义可以还原`() {
        listOf("a/b", "a=b", "2024-01-01", "带中文", "a%b", "ab").forEach { value ->
            assertEquals(value, PartitionNames.unescapePathName(PartitionNames.escapePathName(value)))
        }
    }

    @Test
    fun `解析分区名拿到列名与值`() {
        val name = "dt=2024-01-01/hr=01"
        assertEquals(listOf("dt", "hr"), PartitionNames.toPartitionColumnNames(name))
        assertEquals(listOf("2024-01-01", "01"), PartitionNames.toPartitionValues(name))
    }

    @Test
    fun `多级分区里带斜杠的值不会被切错`() {
        val name = PartitionNames.makePartName(listOf("path", "dt"), listOf("a/b/c", "2024-01-01"))
        assertEquals("path=a%2Fb%2Fc/dt=2024-01-01", name)
        assertEquals(listOf("a/b/c", "2024-01-01"), PartitionNames.toPartitionValues(name))
    }

    @Test
    fun `列名会转成小写`() {
        assertEquals("dt=2024-01-01", PartitionNames.makePartName(listOf("DT"), listOf("2024-01-01")))
    }

    @Test
    fun `分区名格式不对直接抛`() {
        assertFailsWith<IllegalArgumentException> { PartitionNames.toPartitionValues("dt2024-01-01") }
        assertFailsWith<IllegalArgumentException> { PartitionNames.makePartName(listOf("dt", "hr"), listOf("x")) }
    }
}
