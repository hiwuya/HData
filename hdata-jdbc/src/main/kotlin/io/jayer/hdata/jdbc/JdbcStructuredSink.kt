package io.jayer.hdata.jdbc

import io.jayer.hdata.core.StructuredSink
import io.jayer.hdata.jdbc.transform.JdbcSinkDoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PDone
import org.apache.beam.sdk.values.Row

/**
 * @author wuya
 * @date 2022-08-26
 */
class JdbcStructuredSink(private val sinkDescriptor: JdbcSinkDescriptor) : StructuredSink() {

    override fun expand(input: PCollection<Row>): PDone {
        sinkDescriptor.validate()

        input.apply("Jdbc Sink", ParDo.of(JdbcSinkDoFn(sinkDescriptor)))
        return PDone.`in`(input.pipeline)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}