package me.jayer.hdata.jdbc

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.jdbc.transform.JdbcWriteFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * `WriteToJdbc`：批量写入关系库。
 *
 * 声明 `error_handling` 后，批量写失败会退回逐条写以定位坏数据，写不进去的记录进死信流
 * （schema 见 [ErrorSchemas]），而不是让整个作业挂掉。
 *
 * @author wuya
 * @date 2022-08-26
 */
class JdbcWriteProvider : TypedTransformProvider<JdbcWriteConfig>(JdbcWriteConfig::class.java) {

    override fun identifier(): String = "WriteToJdbc"

    override fun description(): String = "批量写入关系库，支持死信输出"

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(
        config: JdbcWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return JdbcSink(config, context.errorHandling != null, context.transformName)
    }
}

private class JdbcSink(
    private val config: JdbcWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val inputSchema = input.schema
        val errorSchema = ErrorSchemas.of(inputSchema)
        val errors = input
            .apply("Write", ParDo.of(JdbcWriteFn(config, inputSchema, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
