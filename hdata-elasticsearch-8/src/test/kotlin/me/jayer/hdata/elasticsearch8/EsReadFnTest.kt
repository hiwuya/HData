package me.jayer.hdata.elasticsearch8

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.elasticsearch8.transform.EsReadFn
import me.jayer.hdata.core.testing.CollectingOutputReceiver
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.util.SerializableUtils
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * ES 8.x 读取端的切分与可序列化。
 *
 * @author wuya
 */
class EsReadFnTest {

    private val config = EsReadConfig(connectionUri = "http://localhost:9200", index = "orders")

    private fun fn(config: EsReadConfig) = EsReadFn(config, config.schemaFields)

    @Test
    fun `DoFn 可以序列化下发`() {
        // 重构前 Query / SortOptions 是 DoFn 的普通字段，这两个客户端对象都不可序列化，
        // 作业在提交阶段就会炸——而单测里只调 processElement 的话永远发现不了
        SerializableUtils.ensureSerializable(fn(config.copy(scanQuery = """{"match_all":{}}""")))
    }

    @Test
    fun `provider 生成的 source 可以序列化下发`() {
        val transform = EsReadProvider().from(
            TransformConfig(
                "ReadFromElasticsearch8",
                SpecMappers.CONFIG.readTree(
                    """{"connection_uri": "http://localhost:9200", "index": "orders", "scan_slices": 4}"""
                ) as ObjectNode,
            )
        )

        SerializableUtils.ensureSerializable(transform)
    }

    @Test
    fun `初始限制覆盖全部 slice`() {
        assertEquals(OffsetRange(0, 4), fn(config.copy(scanSlices = 4)).getInitialRestriction("orders"))
        // 默认不切分
        assertEquals(OffsetRange(0, 1), fn(config).getInitialRestriction("orders"))
    }

    @Test
    fun `每个 slice 切成一份，首尾相接`() {
        val receiver = CollectingOutputReceiver<OffsetRange>()
        fn(config.copy(scanSlices = 4)).splitRestriction("orders", OffsetRange(0, 4), receiver)
        val splits = receiver.outputs

        assertEquals(4, splits.size)
        assertEquals(listOf(0L, 1L, 2L, 3L), splits.map { it.from })
        assertEquals(listOf(1L, 2L, 3L, 4L), splits.map { it.to })
    }

    @Test
    fun `空区间不产出切分`() {
        val receiver = CollectingOutputReceiver<OffsetRange>()
        fn(config).splitRestriction("orders", OffsetRange(0, 0), receiver)
        val splits = receiver.outputs

        assertTrue(splits.isEmpty())
    }

    @Test
    fun `scan_slices 非法时报错`() {
        assertFailsWith<IllegalArgumentException> { config.copy(scanSlices = 0).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(keepAliveMinutes = 0).validate() }
    }

    @Test
    fun `limit 合法取值通过校验`() {
        config.copy(limit = -1).validate()
        config.copy(limit = 1).validate()
        config.copy(limit = 1000).validate()
    }

    @Test
    fun `limit 非法取值报错`() {
        assertFailsWith<IllegalArgumentException> { config.copy(limit = 0).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(limit = -2).validate() }
    }

    @Test
    fun `limit 大于 0 时强制单 slice，保证全局语义`() {
        // 即便声明了 4 个 slice，限行数也必须收敛成单 slice，否则会变成"每 slice 各读 limit 条"
        assertEquals(OffsetRange(0, 1), fn(config.copy(scanSlices = 4, limit = 100)).getInitialRestriction("orders"))
        // 不限制时仍按声明的 slice 数切分
        assertEquals(OffsetRange(0, 4), fn(config.copy(scanSlices = 4, limit = -1)).getInitialRestriction("orders"))
    }

    @Test
    fun `scan_query 不是合法 JSON 时在构图阶段就报错`() {
        val error = assertFailsWith<IllegalArgumentException> {
            config.copy(scanQuery = "{match_all").validate()
        }

        assertTrue("scan_query" in error.message!!)
    }

    @Test
    fun `schema_fields 类型不认识时报错`() {
        assertFailsWith<IllegalArgumentException> { config.copy(schemaFields = listOf("id:UUID")).validate() }
    }
}
