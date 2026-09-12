package me.jayer.hdata.core.spi

import org.apache.beam.sdk.schemas.transforms.SchemaTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row

/**
 * The unified shape of all transforms in HData: `PCollectionRowTuple -> PCollectionRowTuple`.
 *
 * It directly extends Beam's [SchemaTransform], therefore:
 * - source / transform / sink are no longer three mutually incompatible types, but different
 *   input/output arities of the same kind of node, so the DAG builder only has to handle one contract;
 * - every node naturally supports multiple inputs (join / flatten) and multiple outputs (main output + dead letter);
 * - an HData connector is at the same time a valid Beam `SchemaTransform`, so it can be reused directly by a native Beam Java pipeline.
 *
 * Most connectors do not need to implement this class directly; using [RowSource] / [RowTransform] / [RowSink] is easier.
 *
 * @author wuya
 * @date 2022-08-30
 */
abstract class HDataTransform : SchemaTransform() {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * A read side with zero inputs and a single output.
 */
abstract class RowSource : HDataTransform() {

    protected abstract fun read(begin: PBegin): PCollection<Row>

    final override fun expand(input: PCollectionRowTuple): PCollectionRowTuple =
        PCollectionRowTuple.of(Tags.MAIN_OUTPUT, read(PBegin.`in`(input.pipeline)))

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * A processing side with a single input and a single output.
 */
abstract class RowTransform : HDataTransform() {

    protected abstract fun transform(input: PCollection<Row>): PCollection<Row>

    final override fun expand(input: PCollectionRowTuple): PCollectionRowTuple =
        PCollectionRowTuple.of(Tags.MAIN_OUTPUT, transform(input.singlePCollection))

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * A write side with a single input and no main output, optionally emitting a dead-letter stream.
 */
abstract class RowSink : HDataTransform() {

    /** @return the records that failed to write (schema see [me.jayer.hdata.core.error.ErrorSchemas]); returns null when dead letter is not supported. */
    protected abstract fun write(input: PCollection<Row>): PCollection<Row>?

    final override fun expand(input: PCollectionRowTuple): PCollectionRowTuple {
        val errors = write(input.singlePCollection)
        return if (errors == null) {
            PCollectionRowTuple.empty(input.pipeline)
        } else {
            PCollectionRowTuple.of(Tags.ERROR_OUTPUT, errors)
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
