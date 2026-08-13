package me.jayer.hdata.hbase.transform

import me.jayer.hdata.hbase.HBaseConnections
import me.jayer.hdata.hbase.HBaseRegion
import me.jayer.hdata.hbase.buildReadSchema
import me.jayer.hdata.hbase.decodeCell
import me.jayer.hdata.hbase.parseSchemaFields
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.Result
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.util.Bytes
import org.slf4j.LoggerFactory

/**
 * 按 region 切分并行读 HBase，标准的 Splittable DoFn。
 *
 * 元素是一个 [HBaseRegion]（某段 `[startKey, stopKey)`），限制用 `OffsetRange(0, 1)` 表示"整段一次读完"，
 * 因为 HBase 的 Scan 本身不可在行级别被 Beam 续跑切分；region 已经是 HBase 天然的并行粒度。
 *
 * `@ProcessElement` 用 `Scan(startKey, stopKey)` 扫出结果，逐行发成 Beam [Row]：
 * 第一列是 rowkey（[Bytes.toString]），之后是 [schemaFields] 里声明的各列。
 */
@DoFn.BoundedPerElement
class HBaseReadFn(
    private val zookeeperQuorum: String,
    private val rowkeyField: String,
    private val family: String,
    private val schemaFields: List<String>?,
    private val scanCaching: Int,
) : DoFn<HBaseRegion, Row>() {

    @Transient
    private var connection: Connection? = null

    private val fields = parseSchemaFields(schemaFields)
    private val schema: Schema = buildReadSchema(rowkeyField, fields)
    private val familyBytes: ByteArray = Bytes.toBytes(family)

    @Setup
    fun setup() {
        connection = HBaseConnections.newConnection(zookeeperQuorum)
    }

    @Teardown
    fun tearDown() {
        runCatching { connection?.close() }
        connection = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(@Element region: HBaseRegion): OffsetRange = OffsetRange(0, 1)

    @SplitRestriction
    fun splitRestriction(
        @Element region: HBaseRegion,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        receiver.output(restriction)
    }

    @ProcessElement
    fun processElement(
        @Element region: HBaseRegion,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        if (!tracker.tryClaim(0)) {
            return
        }
        val conn = checkNotNull(connection) { "HBase 连接未初始化" }
        val table = conn.getTable(TableName.valueOf(region.table))
        try {
            val scan = Scan(region.startKey, region.stopKey).apply { caching = scanCaching }
            table.getScanner(scan).use { scanner ->
                var count = 0L
                var result: Result? = scanner.next()
                while (result != null) {
                    receiver.output(resultToRow(result))
                    count++
                    result = scanner.next()
                }
                RECORDS_READ.inc(count)
                LOGGER.info("region[{}, {}) 读完 {} 条", Bytes.toStringBinary(region.startKey), Bytes.toStringBinary(region.stopKey), count)
            }
        } finally {
            runCatching { table.close() }
        }
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun resultToRow(result: Result): Row {
        val builder = Row.withSchema(schema).addValue(Bytes.toString(result.row))
        for (field in fields) {
            val raw = result.getValue(familyBytes, Bytes.toBytes(field.name))
            builder.addValue(decodeCell(field.normalizedType, raw))
        }
        return builder.build()
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(HBaseReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(HBaseReadFn::class.java, "records_read")
    }
}
