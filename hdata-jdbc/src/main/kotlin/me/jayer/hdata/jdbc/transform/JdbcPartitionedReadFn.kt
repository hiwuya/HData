package me.jayer.hdata.jdbc.transform

import com.zaxxer.hikari.HikariDataSource
import me.jayer.hdata.jdbc.internal.DataSources
import me.jayer.hdata.jdbc.internal.JdbcMetadata
import me.jayer.hdata.jdbc.internal.RowMapper
import me.jayer.hdata.jdbc.internal.SelectSql
import me.jayer.hdata.jdbc.partition.PartitionColumn
import me.jayer.hdata.jdbc.partition.PartitionConverter
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.util.Properties
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.roundToInt
import kotlin.math.sqrt

/**
 * 按分区列切段并行读一张表。
 *
 * 分区列的取值范围映射成 `OffsetRange`，交给 Beam 的 splittable DoFn 切分，
 * 每段再翻译回 `col >= ? AND col < ?` 的谓词。
 *
 * @author wuya
 * @date 2022-07-27
 */
@DoFn.BoundedPerElement
class JdbcPartitionedReadFn(
    private val dataSourceProperties: Properties,
    private val partitionColumn: PartitionColumn,
    private val partitionNum: Int?,
    private val fetchSize: Int,
    private val rowMapper: RowMapper,
) : DoFn<SelectSql, Row>() {

    @Transient
    private var dataSource: HikariDataSource? = null

    @Setup
    fun setup() {
        dataSource = DataSources.create(dataSourceProperties, "hdata-jdbc-partitioned-read")
    }

    @Teardown
    fun tearDown() {
        dataSource?.close()
        dataSource = null
    }

    /**
     * `@GetInitialRestriction` 可能在 `@Setup` 之外的实例上被调用，所以这里按需自建、用完即关，
     * 不复用 [dataSource]。
     */
    @GetInitialRestriction
    fun getInitialRestriction(@Element select: SelectSql): OffsetRange =
        DataSources.withConnection(dataSourceProperties, "hdata-jdbc-range") { connection ->
            val (min, max) = JdbcMetadata.partitionRange(connection, select, partitionColumn.name)
            LOGGER.info("表[{}] 分区列[{}] 取值范围: min={}, max={}", select.table, partitionColumn.name, min, max)
            if (min == null || max == null) {
                // 空表：给一个空区间，切分时自然产生 0 个分片
                OffsetRange(0, 0)
            } else {
                OffsetRange(toOffset(min), toOffset(max) + 1)
            }
        }

    @SplitRestriction
    fun splitRestriction(
        @Element select: SelectSql,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        val span = restriction.to - restriction.from
        if (span <= 0) {
            LOGGER.info("表[{}] 没有可读区间，跳过", select.table)
            return
        }
        val partitions = partitionNum ?: autoPartitionNum(span, select.table)
        val perSplit = ceil(span.toDouble() / partitions).toLong().coerceAtLeast(1)
        val splits = restriction.split(perSplit, 1)
        LOGGER.info("表[{}] 切分为 {} 个分区", select.table, splits.size)
        splits.forEach { receiver.output(it) }
    }

    /**
     * 没指定 partition_num 时按取值跨度估算：开方再除以 10，避免对着一个 RDBMS 开出成百上千条连接。
     */
    private fun autoPartitionNum(span: Long, table: String): Int {
        val num = 1.coerceAtLeast(floor(sqrt(span.toDouble()) / 10).roundToInt())
        LOGGER.info("表[{}] 未指定 partition_num，自动估算为 {}", table, num)
        return num
    }

    @ProcessElement
    fun processElement(
        @Element select: SelectSql,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        val range = tracker.currentRestriction()
        if (range.to <= range.from) {
            return
        }
        // 一次认领整段：读取本身不可中断续跑，中途放弃会重复输出
        if (!tracker.tryClaim(range.to - 1)) {
            return
        }

        val sql = select
            .withConditions("${partitionColumn.name} >= ?", "${partitionColumn.name} < ?")
            .render()
        val pool = checkNotNull(dataSource) { "数据源未初始化" }
        pool.connection.use { connection ->
            // PostgreSQL 必须关掉 autocommit 才会走游标流式读取
            connection.autoCommit = false
            connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { ps ->
                ps.fetchSize = fetchSize
                ps.setObject(1, fromOffset(range.from))
                ps.setObject(2, fromOffset(range.to))
                LOGGER.info("Executing query: {} [{}, {})", sql, fromOffset(range.from), fromOffset(range.to))
                var count = 0L
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        receiver.output(rowMapper.map(rs))
                        count++
                    }
                }
                RECORDS_READ.inc(count)
            }
        }
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    /**
     * 分区列的值来自 `min()/max()`，个别驱动给出的类型与列本身不同（例如把 INT 的 min 提成 BIGINT），
     * 强转失败时给出能定位问题的信息，而不是一句光秃秃的 ClassCastException。
     */
    private fun toOffset(value: Any): Long {
        @Suppress("UNCHECKED_CAST")
        val converter = partitionColumn.converter as PartitionConverter<Any>
        return try {
            converter.toLong(value)
        } catch (e: ClassCastException) {
            throw IllegalStateException(
                "分区列[${partitionColumn.name}] 的取值类型是 ${value.javaClass.name}，" +
                    "与列元数据推断出的类型不一致，请显式指定一个类型明确的分区列",
                e,
            )
        }
    }

    private fun fromOffset(offset: Long): Any = partitionColumn.converter.fromLong(offset)

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(JdbcPartitionedReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(JdbcPartitionedReadFn::class.java, "records_read")
    }
}
