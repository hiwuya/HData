package me.jayer.hdata.hbase.transform

import me.jayer.hdata.hbase.HBaseRowCodec
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.hbase.client.Result

/**
 * Converts [Result] values emitted by Beam `HBaseIO.readAll()` into schema-bearing [Row] values.
 *
 * @author wuya
 */
class HBaseResultToRowFn(private val codec: HBaseRowCodec) : DoFn<Result, Row>() {

    @ProcessElement
    fun processElement(@Element result: Result, receiver: OutputReceiver<Row>) {
        RECORDS_READ.inc()
        receiver.output(codec.toRow(result))
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val RECORDS_READ = Metrics.counter(HBaseResultToRowFn::class.java, "records_read")
    }
}
