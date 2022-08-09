package io.jayer.hdata.core

import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PDone
import org.apache.beam.sdk.values.Row

/**
 * @author wuya
 * @date 2022-07-22
 */
abstract class StructuredSink : PTransform<PCollection<Row>, PDone>() {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}