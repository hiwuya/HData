package me.jayer.hdata.core.spec

import me.jayer.hdata.core.exception.HDataException
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.FixedWindows
import org.apache.beam.sdk.transforms.windowing.GlobalWindows
import org.apache.beam.sdk.transforms.windowing.Sessions
import org.apache.beam.sdk.transforms.windowing.SlidingWindows
import org.apache.beam.sdk.transforms.windowing.WindowFn
import org.joda.time.Duration
import java.math.BigDecimal

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
            val amount = match.groupValues[1].toBigDecimal()
            val multiplier = when (match.groupValues[2]) {
                "ms" -> BigDecimal.ONE
                "", "s" -> BigDecimal.valueOf(1_000)
                "m" -> BigDecimal.valueOf(60_000)
                "h" -> BigDecimal.valueOf(3_600_000)
                else -> BigDecimal.valueOf(86_400_000)
            }
            val millis = try {
                amount.multiply(multiplier).longValueExact()
            } catch (e: ArithmeticException) {
                throw HDataException("时长 $field=\"$value\" 必须能精确表示为毫秒且不能超出 Long 范围", e)
            }
            if (millis <= 0) {
                throw HDataException("时长 $field=\"$value\" 必须大于 0ms")
            }
            return Duration.millis(millis)
        }
    }

    fun toWindowFn(): WindowFn<Any, out BoundedWindow> = when (type.lowercase()) {
        "global" -> {
            rejectUnused("size" to size, "period" to period, "gap" to gap)
            GlobalWindows()
        }
        "fixed" -> {
            rejectUnused("period" to period, "gap" to gap)
            FixedWindows.of(required(size, "size"))
        }
        "sliding" -> {
            rejectUnused("gap" to gap)
            SlidingWindows.of(required(size, "size")).every(required(period, "period"))
        }
        "sessions", "session" -> {
            rejectUnused("size" to size, "period" to period)
            Sessions.withGapDuration(required(gap, "gap"))
        }
        else -> throw HDataException("不支持的窗口类型: $type，可选: global / fixed / sliding / sessions")
    }

    private fun required(value: String?, field: String): Duration {
        require(!value.isNullOrBlank()) { "窗口类型[$type] 必须声明 $field" }
        return parseDuration(value, field)
    }

    private fun rejectUnused(vararg fields: Pair<String, String?>) {
        val configured = fields.filter { it.second != null }.map { it.first }
        require(configured.isEmpty()) { "窗口类型[$type] 不使用 ${configured.joinToString()}，请从配置中移除" }
    }
}
