package me.jayer.hdata.hive

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.io.range.OffsetRange
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

/**
 * 校验 [HiveReadConfig] 的 YAML 绑定（snake_case）与 [HiveReadConfig.validate]，
 * 以及 `ReadFromHive` provider 能基于配置产出 PTransform。
 */
class HiveReadConfigTest {

    private fun configOf(yaml: String): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    @Test
    fun `snake_case 配置键绑定到 HiveReadConfig 且 provider 产出 transform`() {
        val yaml = """
        |url: "jdbc:hive2://localhost:10000/default"
        |user: hive
        |password: hive
        |database: default
        |table: orders
        |partitions: ["dt='2024-01-01'", "dt='2024-01-02'"]
        |schema_fields: ["id:INT64", "name:STRING", "amount:DOUBLE"]
        """.trimMargin()

        val cfg = configOf(yaml)
        val transform = HiveReadProvider().from(cfg)
        assertNotNull(transform)

        val bound: HiveReadConfig = cfg.bind(HiveReadConfig::class.java)
        assertEquals("jdbc:hive2://localhost:10000/default", bound.url)
        assertEquals("hive", bound.user)
        assertEquals("default", bound.database)
        assertEquals("orders", bound.table)
        assertEquals(listOf("dt='2024-01-01'", "dt='2024-01-02'"), bound.partitions)
        assertEquals(listOf("id:INT64", "name:STRING", "amount:DOUBLE"), bound.schemaFields)
    }

    @Test
    fun `未提供的可选键沿用默认值`() {
        val bound: HiveReadConfig = configOf(
            """
            |url: "jdbc:hive2://x"
            |table: t
            """.trimMargin(),
        ).bind(HiveReadConfig::class.java)

        assertEquals(emptyList<String>(), bound.partitions)
        assertEquals(emptyList<String>(), bound.schemaFields)
        assertEquals("", bound.user)
        assertEquals("", bound.password)
        assertEquals("", bound.database)
    }

    @Test
    fun `拼错的键因 FAIL_ON_UNKNOWN_PROPERTIES 报错`() {
        assertThrows(Exception::class.java) {
            configOf(
                """
                |url: "jdbc:hive2://x"
                |table: t
                |tablse: oops
                """.trimMargin(),
            ).bind(HiveReadConfig::class.java)
        }
    }

    @Test
    fun `validate 在缺少必填项时抛 IllegalArgumentException`() {
        assertThrows(IllegalArgumentException::class.java) { HiveReadConfig(table = "t").validate() }
        assertThrows(IllegalArgumentException::class.java) { HiveReadConfig(url = "jdbc:x").validate() }
    }

    @Test
    fun `validate 在必填项齐全时不抛异常`() {
        HiveReadConfig(url = "jdbc:hive2://x", table = "t").validate()
    }
}
