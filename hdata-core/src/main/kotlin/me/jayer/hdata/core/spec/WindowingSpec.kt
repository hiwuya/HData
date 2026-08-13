package me.jayer.hdata.core.spec

import me.jayer.hdata.core.exception.HDataException
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.GlobalWindows
import org.apache.beam.sdk.transforms.windowing.Sessions
import org.apache.beam.sdk.transforms.windowing.SlidingWindows
import org.apache.beam.sdk.transforms.windowing.WindowFn
import org.joda.time.Duration

/**
 * 窗口声明，对齐 Beam YAML：
 *
 * ```yaml
 * windowing:
 *   type: fixed
 *   size: 60s
 * ```
 *
 * @author wuya
 * @date 2022-08-30
 */
data class WindowingSpec(
    val type: String,
    val size: String? = null,
    val period: String? = null,
    val gap: String? = null,
) {

    companion object {
        private val DURATION_REGEX = Regex("^(\\d+(?:\\.\\d+)?)\\s*(ms|s|m|h|d)?$")

        /** 支持 `500ms` / `60s` / `5m` / `2h` / `1d`，无单位时按秒解析。 */
        fun parseDuration(value: String, field: String): Duration {
            val match = DURATION_REGEX.matchEntire(value.trim())
                ?: throw HDataException("无法解析时长 $field=\"$value\"，合法示例: 500ms / 60s / 5m / 2h / 1d")
            val amount = match.groupValues[1].toDouble()
            val millis = when (match.groupValues[2]) {
                "ms" -> amount
                "", "s" -> amount * 1_000
                "m" -> amount * 60_000
                "h" -> amount * 3_600_000
                else -> amount * 86_400_000
            }
            return Duration.millis(millis.toLong())
        }
    }

    fun toWindowFn(): WindowFn<Any, out BoundedWindow> = when (type.lowercase()) {
        "global" -> GlobalWindows()
        "fixed" -> FixedWindows.of(required(size, "size"))
        "sliding" -> SlidingWindows.of(required(size, "size")).every(required(period, "period"))
        "sessions", "session" -> Sessions.withGapDuration(required(gap, "gap"))
        else -> throw HDataException("不支持的窗口类型: $type，可选: global / fixed / sliding / sessions")
    }

    private fun required(value: String?, field: String): Duration {
        require(!value.isNullOrBlank()) { "窗口类型[$type] 必须声明 $field" }
        return parseDuration(value, field)
    }
}
