package io.jayer.hdata.spi

import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.Row

/**
 * @author Jayer
 * @date 2019-07-25
 */
interface Transform : Plugin {

    fun build(): PTransform<PCollection<Row>, PCollection<Row>>

}