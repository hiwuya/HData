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
    fun `duration supports ms s m h d, no unit means seconds`() {
        assertEquals(Duration.millis(500), WindowingSpec.parseDuration("500ms", "size"))
        assertEquals(Duration.standardSeconds(60), WindowingSpec.parseDuration("60s", "size"))
        assertEquals(Duration.standardMinutes(5), WindowingSpec.parseDuration("5m", "size"))
        assertEquals(Duration.standardHours(2), WindowingSpec.parseDuration("2h", "size"))
        assertEquals(Duration.standardDays(1), WindowingSpec.parseDuration("1d", "size"))
        assertEquals(Duration.standardSeconds(30), WindowingSpec.parseDuration("30", "size"))
    }

    @Test
    fun `duration allows a decimal and leading or trailing whitespace`() {
        assertEquals(Duration.millis(1500), WindowingSpec.parseDuration(" 1.5s ", "size"))
    }

    @Test
    fun `an invalid duration errors out with an example`() {
        val error = assertFailsWith<HDataException> { WindowingSpec.parseDuration("one minute", "size") }
        assertTrue("one minute" in error.message!! && "60s" in error.message!!)

        assertFailsWith<HDataException> { WindowingSpec.parseDuration("10x", "size") }
        assertFailsWith<HDataException> { WindowingSpec.parseDuration("0ms", "size") }
        assertFailsWith<HDataException> { WindowingSpec.parseDuration("0.1ms", "size") }
        assertFailsWith<HDataException> { WindowingSpec.parseDuration("999999999999999999999d", "size") }
    }

    @Test
    fun `each window type builds the matching WindowFn`() {
        assertIs<GlobalWindows>(WindowingSpec("global").toWindowFn())
        assertIs<FixedWindows>(WindowingSpec("fixed", size = "60s").toWindowFn())
        assertIs<SlidingWindows>(WindowingSpec("sliding", size = "60s", period = "10s").toWindowFn())
        assertIs<Sessions>(WindowingSpec("sessions", gap = "30s").toWindowFn())
        // The type name is case-insensitive and accepts both singular and plural
        assertIs<Sessions>(WindowingSpec("Session", gap = "30s").toWindowFn())
    }

    @Test
    fun `a missing required parameter errors out`() {
        assertFailsWith<IllegalArgumentException> { WindowingSpec("fixed").toWindowFn() }
        assertFailsWith<IllegalArgumentException> { WindowingSpec("sliding", size = "60s").toWindowFn() }
        assertFailsWith<IllegalArgumentException> { WindowingSpec("sessions").toWindowFn() }
    }

    @Test
    fun `rejects a config key the current window type does not use`() {
        assertFailsWith<IllegalArgumentException> { WindowingSpec("global", size = "1m").toWindowFn() }
        assertFailsWith<IllegalArgumentException> { WindowingSpec("fixed", size = "1m", period = "5s").toWindowFn() }
        assertFailsWith<IllegalArgumentException> { WindowingSpec("sessions", gap = "1m", size = "5m").toWindowFn() }
    }

    @Test
    fun `an unknown window type errors out and lists the available values`() {
        val error = assertFailsWith<HDataException> { WindowingSpec("tumbling", size = "60s").toWindowFn() }
        assertTrue("tumbling" in error.message!! && "fixed" in error.message!!)
    }

    @Test
    fun `a window declaration can be parsed out of a pipeline file`() {
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
