package me.jayer.hdata.core.spi

import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.Row

/**
 * @author wuya
 * @date 2022-07-22
 */
abstract class StructuredTransform : PTransform<PCollection<Row>, PCollection<Row>>() {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}