package me.jayer.hdata.jdbc

import me.jayer.hdata.core.spi.StructuredSink
import me.jayer.hdata.jdbc.transform.JdbcSinkDoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PDone
import org.apache.beam.sdk.values.Row

/**
 * @author wuya
 * @date 2022-08-26
 */
class JdbcStructuredSink(private val sinkDescriptor: me.jayer.hdata.jdbc.JdbcSinkDescriptor) : StructuredSink() {

    override fun expand(input: PCollection<Row>): PDone {
        input.apply("Jdbc Sink", ParDo.of(me.jayer.hdata.jdbc.transform.JdbcSinkDoFn(sinkDescriptor)))
        return PDone.`in`(input.pipeline)
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}