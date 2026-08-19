package me.jayer.hdata.core.spec

import me.jayer.hdata.core.exception.HDataException
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.GlobalWindows
import org.apache.beam.sdk.transforms.windowing.Sessions
import org.apache.beam.sdk.transforms.windowing.SlidingWindows
import org.joda.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertIs
import kotlin.test.assertTrue

/**
 * @author wuya
 * @date 2022-08-30
 */
class WindowingSpecTest {

    @Test
    fun `时长支持 ms s m h d，无单位按秒`() {
        assertEquals(Duration.millis(500), WindowingSpec.parseDuration("500ms", "size"))
        assertEquals(Duration.standardSeconds(60), WindowingSpec.parseDuration("60s", "size"))
        assertEquals(Duration.standardMinutes(5), WindowingSpec.parseDuration("5m", "size"))
        assertEquals(Duration.standardHours(2), WindowingSpec.parseDuration("2h", "size"))
        assertEquals(Duration.standardDays(1), WindowingSpec.parseDuration("1d", "size"))
        assertEquals(Duration.standardSeconds(30), WindowingSpec.parseDuration("30", "size"))
    }

    @Test
    fun `时长允许小数与首尾空白`() {
        assertEquals(Duration.millis(1500), WindowingSpec.parseDuration(" 1.5s ", "size"))
    }

    @Test
    fun `非法时长报错并给出示例`() {
        val error = assertFailsWith<HDataException> { WindowingSpec.parseDuration("一分钟", "size") }
        assertTrue("一分钟" in error.message!! && "60s" in error.message!!)

        assertFailsWith<HDataException> { WindowingSpec.parseDuration("10x", "size") }
        assertFailsWith<HDataException> { WindowingSpec.parseDuration("0ms", "size") }
        assertFailsWith<HDataException> { WindowingSpec.parseDuration("0.1ms", "size") }
        assertFailsWith<HDataException> { WindowingSpec.parseDuration("999999999999999999999d", "size") }
    }

    @Test
    fun `各窗口类型构造出对应的 WindowFn`() {
        assertIs<GlobalWindows>(WindowingSpec("global").toWindowFn())
        assertIs<FixedWindows>(WindowingSpec("fixed", size = "60s").toWindowFn())
        assertIs<SlidingWindows>(WindowingSpec("sliding", size = "60s", period = "10s").toWindowFn())
        assertIs<Sessions>(WindowingSpec("sessions", gap = "30s").toWindowFn())
        // 类型名大小写不敏感，单复数都收
        assertIs<Sessions>(WindowingSpec("Session", gap = "30s").toWindowFn())
    }

    @Test
    fun `缺少必需参数时报错`() {
        assertFailsWith<IllegalArgumentException> { WindowingSpec("fixed").toWindowFn() }
        assertFailsWith<IllegalArgumentException> { WindowingSpec("sliding", size = "60s").toWindowFn() }
        assertFailsWith<IllegalArgumentException> { WindowingSpec("sessions").toWindowFn() }
    }

    @Test
    fun `拒绝当前窗口类型不会使用的配置项`() {
        assertFailsWith<IllegalArgumentException> { WindowingSpec("global", size = "1m").toWindowFn() }
        assertFailsWith<IllegalArgumentException> { WindowingSpec("fixed", size = "1m", period = "5s").toWindowFn() }
        assertFailsWith<IllegalArgumentException> { WindowingSpec("sessions", gap = "1m", size = "5m").toWindowFn() }
    }

    @Test
    fun `未知窗口类型报错并列出可选值`() {
        val error = assertFailsWith<HDataException> { WindowingSpec("tumbling", size = "60s").toWindowFn() }
        assertTrue("tumbling" in error.message!! && "fixed" in error.message!!)
    }

    @Test
    fun `窗口声明可以从 pipeline 文件解析出来`() {
        val spec = PipelineSpecLoader.parse(
            """
            pipeline:
              type: chain
              windowing:
                type: sliding
                size: 60s
                period: 10s
              transforms:
                - type: Create
                  windowing:
                    type: fixed
                    size: 5m
            """.trimIndent(),
            SpecMappers.YAML,
            "test",
        )

        assertEquals(WindowingSpec("sliding", size = "60s", period = "10s"), spec.pipeline.windowing)
        assertEquals(WindowingSpec("fixed", size = "5m"), spec.pipeline.transforms.single().windowing)
    }
}
