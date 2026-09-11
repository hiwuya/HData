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
 * A test-only sink: rows matching a condition go to the dead-letter stream, everything else counts
 * as a successful write.
 *
 * Also demonstrates just how little a connector author has to write — one provider plus one [RowSink].
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
    /** A non-empty value in this field is treated as a write failure. */
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
            val error = IllegalStateException("field [$failField] has a value, simulating a write failure")
            receiver.output(ErrorSchemas.failure(errorSchema, row, error, transformName))
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
