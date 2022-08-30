package io.jayer.hdata.core.spi

import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.Row

/**
 * @author wuya
 * @date 2022-07-22
 */
abstract class StructuredSource : PTransform<PBegin, PCollection<Row>>() {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}