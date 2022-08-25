package io.jayer.hdata.core.transform

import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.windowing.GlobalWindow
import org.apache.beam.sdk.values.Row
import org.joda.time.Instant

/**
 * @author wuya
 * @date 2022-08-18
 */
class Batches private constructor(val batchSize: Long) : DoFn<Row, Iterable<Row>>() {

    companion object {
        private const val serialVersionUID: Long = 1

        fun of(batchSize: Long): Batches = Batches(batchSize)
    }

    private val outputList = mutableListOf<Row>()

    @ProcessElement
    fun process(context: ProcessContext) {
        outputList.add(context.element())
        if (outputList.size >= batchSize) {
            context.output(outputList)
            outputList.clear()
        }
    }

    @FinishBundle
    fun finish(context: FinishBundleContext) {
        if (outputList.isNotEmpty()) {
            context.output(outputList, Instant.now(), GlobalWindow.INSTANCE)
        }
    }
}