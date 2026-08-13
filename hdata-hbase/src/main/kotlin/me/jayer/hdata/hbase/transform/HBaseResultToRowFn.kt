package me.jayer.hdata.hbase.transform

import me.jayer.hdata.hbase.HBaseRowCodec
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.hbase.client.Result

/**
 * 把 Beam `HBaseIO.readAll()` 吐出的 [Result] 转成带 schema 的 [Row]。
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
