package me.jayer.hdata.hive

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.junit.jupiter.api.Assertions.assertEquals
import org.junit.jupiter.api.Assertions.assertNotNull
import org.junit.jupiter.api.Assertions.assertThrows
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode

/**
 * 校验 [HiveWriteConfig] 的 YAML 绑定（snake_case）与 [HiveWriteConfig.validate]，
 * 以及 `WriteToHive` provider 能基于配置产出 PTransform。
 */
class HiveWriteConfigTest {

    private fun configOf(yaml: String): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    @Test
    fun `snake_case 配置键绑定到 HiveWriteConfig 且 provider 产出 transform`() {
        val yaml = """
        |url: "jdbc:hive2://localhost:10000/default"
        |user: hive
        |password: hive
        |database: default
        |table: orders
        |batch_size: 500
        |schema_fields: ["id:INT64", "name:STRING"]
        """.trimMargin()

        val cfg = configOf(yaml)
        val transform = HiveWriteProvider().from(cfg)
        assertNotNull(transform)

        val bound: HiveWriteConfig = cfg.bind(HiveWriteConfig::class.java)
        assertEquals("jdbc:hive2://localhost:10000/default", bound.url)
        assertEquals("orders", bound.table)
        assertEquals(500, bound.batchSize)
        assertEquals(listOf("id:INT64", "name:STRING"), bound.schemaFields)
    }

    @Test
    fun `batch_size 缺省时使用默认值 1000`() {
        val bound: HiveWriteConfig = configOf(
            """
            |url: "jdbc:hive2://x"
            |table: t
            """.trimMargin(),
        ).bind(HiveWriteConfig::class.java)
        assertEquals(1000, bound.batchSize)
    }

    @Test
    fun `拼错的键因 FAIL_ON_UNKNOWN_PROPERTIES 报错`() {
        assertThrows(Exception::class.java) {
            configOf(
                """
                |url: "jdbc:hive2://x"
                |table: t
                |batchsize: 1
                """.trimMargin(),
            ).bind(HiveWriteConfig::class.java)
        }
    }

    @Test
    fun `validate 在缺少必填项或 batch_size 非法时抛 IllegalArgumentException`() {
        assertThrows(IllegalArgumentException::class.java) { HiveWriteConfig(table = "t").validate() }
        assertThrows(IllegalArgumentException::class.java) { HiveWriteConfig(url = "jdbc:x").validate() }
        assertThrows(IllegalArgumentException::class.java) {
            HiveWriteConfig(url = "jdbc:x", table = "t", batchSize = 0).validate()
        }
        assertThrows(IllegalArgumentException::class.java) {
            HiveWriteConfig(url = "jdbc:x", table = "t", batchSize = -1).validate()
        }
    }

    @Test
    fun `validate 在必填项齐全且 batch_size 合法时不抛异常`() {
        HiveWriteConfig(url = "jdbc:hive2://x", table = "t", batchSize = 1).validate()
    }
}
