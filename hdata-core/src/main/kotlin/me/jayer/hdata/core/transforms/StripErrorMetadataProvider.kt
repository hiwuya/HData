package me.jayer.hdata.core.transforms

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spi.RowTransform
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TransformProvider
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * Strips the error metadata from a dead-letter record, restoring it to the original record so it can be replayed after the data is fixed:
 *
 * ```yaml
 * - type: StripErrorMetadata
 *   input: WriteToJdbc.errors
 * ```
 *
 * @author wuya
 * @date 2022-08-30
 */
class StripErrorMetadataProvider : TransformProvider {

    override fun identifier(): String = "StripErrorMetadata"

    override fun description(): String = "Restores dead-letter records back to the original records"

    override fun from(config: TransformConfig): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        if (!config.isEmpty) {
            throw HDataException("transform[${config.transformName}] StripErrorMetadata does not accept any config")
        }
        return StripErrorMetadata()
    }
}

private class StripErrorMetadata : RowTransform() {

    override fun transform(input: PCollection<Row>): PCollection<Row> {
        val schema = input.schema
        if (!ErrorSchemas.isErrorSchema(schema)) {
            throw HDataException("StripErrorMetadata's input is not a dead-letter stream; its fields are: ${schema.fieldNames}")
        }
        val elementSchema = schema.getField(ErrorSchemas.ELEMENT).type.rowSchema!!
        return input.apply(ParDo.of(StripFn(elementSchema))).setRowSchema(elementSchema)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

private class StripFn(private val elementSchema: Schema) : DoFn<Row, Row>() {

    @ProcessElement
    fun processElement(@Element row: Row, receiver: OutputReceiver<Row>) {
        val element = row.getRow(ErrorSchemas.ELEMENT) ?: return
        receiver.output(Row.withSchema(elementSchema).addValues(element.values).build())
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
