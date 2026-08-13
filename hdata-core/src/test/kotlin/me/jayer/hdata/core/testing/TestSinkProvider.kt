package me.jayer.hdata.core.testing

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * 测试用的写入端：把满足条件的记录丢进死信流，其余当成写成功。
 *
 * 顺带演示连接器作者要写的东西有多少——一个 provider 加一个 [RowSink]。
 *
 * @author wuya
 * @date 2022-08-30
 */
class TestSinkProvider : TypedTransformProvider<TestSinkConfig>(TestSinkConfig::class.java) {

    override fun identifier(): String = "TestSink"

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(config: TestSinkConfig, context: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> =
        TestSink(config.failField, context.errorHandling != null, context.transformName)
}

data class TestSinkConfig(
    /** 该字段非空即视为写失败。 */
    val failField: String = "",
)

private class TestSink(
    private val failField: String,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        val errors = input
            .apply("Write", ParDo.of(TestSinkFn(failField, errorSchema, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

private class TestSinkFn(
    private val failField: String,
    private val errorSchema: Schema,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @ProcessElement
    fun processElement(@Element row: Row, receiver: OutputReceiver<Row>) {
        val shouldFail = failField.isNotBlank() && row.getValue<Any?>(failField) != null
        if (shouldFail) {
            val error = IllegalStateException("字段[$failField] 有值，模拟写入失败")
            receiver.output(ErrorSchemas.failure(errorSchema, row, error, transformName))
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
