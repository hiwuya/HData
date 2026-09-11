package me.jayer.hdata.core.spec

/**
 * Dead letter declaration, written in a transform's config, aligned with Beam YAML:
 *
 * ```yaml
 * config:
 *   error_handling:
 *     output: errors
 * ```
 *
 * After declaration, the transform's error output is exposed to the DAG under the name
 * `<transformName>.<output>`, and **must** be consumed by a downstream node, otherwise
 * graph construction fails immediately.
 *
 * @author wuya
 * @date 2022-08-30
 */
data class ErrorHandlingSpec(
    val output: String,
    /** Error rate threshold, not yet implemented; declaring it fails at graph construction time to avoid silently taking no effect. */
    val threshold: Double? = null,
) {
    companion object {
        const val CONFIG_KEY = "error_handling"
    }
}
