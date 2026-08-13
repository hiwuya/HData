package me.jayer.hdata.core.spec

/**
 * 死信（dead letter）声明，写在 transform 的 config 里，对齐 Beam YAML：
 *
 * ```yaml
 * config:
 *   error_handling:
 *     output: errors
 * ```
 *
 * 声明后该 transform 的错误输出会以 `<transformName>.<output>` 的名字暴露给 DAG，
 * 且**必须**被下游消费，否则构图阶段直接报错。
 *
 * @author wuya
 * @date 2022-08-30
 */
data class ErrorHandlingSpec(
    val output: String,
    /** 错误率阈值，暂未实现，声明后会在构图阶段报错以免静默失效。 */
    val threshold: Double? = null,
) {
    companion object {
        const val CONFIG_KEY = "error_handling"
    }
}
