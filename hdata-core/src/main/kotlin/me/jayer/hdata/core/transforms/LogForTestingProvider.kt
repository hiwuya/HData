package me.jayer.hdata.core.transforms

import me.jayer.hdata.core.spi.RowTransform
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory

/**
 * 打印每条记录并原样透传，用来在链路中间开个观察口：
 *
 * ```yaml
 * - type: LogForTesting
 *   config:
 *     level: INFO
 *     prefix: "after mapping: "
 * ```
 *
 * @author wuya
 * @date 2022-08-30
 */
class LogForTestingProvider : TypedTransformProvider<LogForTestingConfig>(LogForTestingConfig::class.java) {

    override fun identifier(): String = "LogForTesting"

    override fun description(): String = "打印每条记录并原样透传"

    override fun create(config: LogForTestingConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> =
        LogRows(config.level.uppercase(), config.prefix)
}

data class LogForTestingConfig(val level: String = "INFO", val prefix: String = "")

private class LogRows(private val level: String, private val prefix: String) : RowTransform() {

    init {
        require(level in LEVELS) { "LogForTesting 的 level 只能是 ${LEVELS.joinToString("/")}，实际为: $level" }
    }

    override fun transform(input: PCollection<Row>): PCollection<Row> =
        input.apply(ParDo.of(LogFn(level, prefix))).setRowSchema(input.schema)

    companion object {
        private const val serialVersionUID: Long = 1
        private val LEVELS = setOf("TRACE", "DEBUG", "INFO", "WARN", "ERROR")
    }
}

private class LogFn(private val level: String, private val prefix: String) : DoFn<Row, Row>() {

    @ProcessElement
    fun processElement(@Element row: Row, receiver: OutputReceiver<Row>) {
        val message = "$prefix$row"
        when (level) {
            "TRACE" -> LOGGER.trace(message)
            "DEBUG" -> LOGGER.debug(message)
            "WARN" -> LOGGER.warn(message)
            "ERROR" -> LOGGER.error(message)
            else -> LOGGER.info(message)
        }
        receiver.output(row)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(LogFn::class.java)
    }
}
