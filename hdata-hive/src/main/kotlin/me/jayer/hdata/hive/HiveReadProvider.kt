package me.jayer.hdata.hive

import com.zaxxer.hikari.HikariDataSource
import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.jdbc.internal.DataSources
import me.jayer.hdata.jdbc.internal.JdbcMetadata
import me.jayer.hdata.jdbc.internal.RowMapper
import me.jayer.hdata.jdbc.internal.SelectSql
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.sql.ResultSet
import java.util.Properties

/**
 * `ReadFromHive`：通过 JDBC（`jdbc:hive2://...`）按分区并行读 Hive 表。
 *
 * schema 推断、类型映射、连接池全部复用 `hdata-jdbc` 的实现——Hive 的读取本质就是 JDBC，
 * 重构前这里自己维护了一份平行的、更弱的版本：
 *  - `resultSetToRow` 只认 7 种类型，DECIMAL / DATE / ARRAY 全部落到 `getObject` 上；
 *  - `DESCRIBE` 推断不出来时**静默回退成单列 `value`(STRING)**，然后拿这个名字去 `SELECT value FROM t`；
 *  - 每个分区 `DriverManager.getConnection` 各开一条连接，没有池化；
 *  - 没有 fetch size，整个结果集进内存。
 *
 * @author wuya
 */
class HiveReadProvider : TypedTransformProvider<HiveReadConfig>(HiveReadConfig::class.java) {

    override fun identifier(): String = "ReadFromHive"

    override fun description(): String = "通过 JDBC 按分区并行读取 Hive 表，使用 Splittable DoFn"

    override fun inputCollectionNames(): List<String> = emptyList()

    override fun create(
        config: HiveReadConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return HiveSource(config)
    }
}

/** 一次读取任务：一张表，外加它的分区谓词列表。 */
data class HiveSplit(val table: String, val predicates: List<String>) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

private class HiveSource(private val config: HiveReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> =
        DataSources.withConnection(config.dataSourceProperties(), "hdata-hive-metadata") { connection ->
            val predicates = HivePartitions.discover(connection, config)
            // 用一个不会返回任何行的查询探 schema，避免为了拿元数据把数据拉下来
            val probe = selectSql(predicates.first()).withConditions("1 = 0").render()
            val columns = JdbcMetadata.describe(connection, probe)
            val (schema, readers) = JdbcMetadata.toSchema(columns)
            LOGGER.info("ReadFromHive 表[{}] 共 {} 个分区，schema={}", config.qualifiedTable, predicates.size, schema)

            begin.apply("Splits", Create.of(HiveSplit(config.qualifiedTable, predicates)))
                .apply(
                    "Read",
                    ParDo.of(HiveReadFn(config.dataSourceProperties(), config, RowMapper(schema, readers))),
                )
                .setRowSchema(schema)
        }

    private fun selectSql(predicate: String): SelectSql = SelectSql(
        table = config.qualifiedTable,
        columns = config.columns.ifEmpty { listOf("*") },
        conditions = listOf(predicate, config.where),
    )

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 按分区并行读的 Splittable DoFn。
 *
 * 限制是分区下标区间，`@ProcessElement` **逐个分区认领**——重构前是
 * `OffsetRange(0, 1)` 加 `tryClaim(0)`，等于告诉 Beam"这一份不可再分"，
 * 分区之间的负载没法在运行时重新均衡。
 */
@DoFn.BoundedPerElement
class HiveReadFn(
    private val dataSourceProperties: Properties,
    private val config: HiveReadConfig,
    private val rowMapper: RowMapper,
) : DoFn<HiveSplit, Row>() {

    @Transient
    private var dataSource: HikariDataSource? = null

    @Setup
    fun setup() {
        dataSource = DataSources.create(dataSourceProperties, "hdata-hive-read")
    }

    @Teardown
    fun tearDown() {
        dataSource?.close()
        dataSource = null
    }

    @GetInitialRestriction
    fun getInitialRestriction(@Element split: HiveSplit): OffsetRange =
        OffsetRange(0, split.predicates.size.toLong())

    @SplitRestriction
    fun splitRestriction(
        @Element split: HiveSplit,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        if (restriction.to <= restriction.from) {
            return
        }
        restriction.split(1, 1).forEach { receiver.output(it) }
    }

    @ProcessElement
    fun processElement(
        @Element split: HiveSplit,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        val range = tracker.currentRestriction()
        var index = range.from
        while (index < range.to) {
            if (!tracker.tryClaim(index)) {
                return
            }
            readPartition(split, index.toInt(), receiver)
            index++
        }
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun readPartition(split: HiveSplit, index: Int, receiver: OutputReceiver<Row>) {
        val sql = SelectSql(
            table = split.table,
            columns = config.columns.ifEmpty { listOf("*") },
            conditions = listOf(split.predicates[index], config.where),
        ).render()
        val pool = checkNotNull(dataSource) { "数据源未初始化" }
        pool.connection.use { connection ->
            connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { ps ->
                // 没有 fetch size 的话整个结果集会进内存
                ps.fetchSize = config.fetchSize
                var count = 0L
                ps.executeQuery().use { rs ->
                    while (rs.next()) {
                        receiver.output(rowMapper.map(rs))
                        count++
                    }
                }
                RECORDS_READ.inc(count)
                LOGGER.info("Hive 分区[{}] 读出 {} 行 (SQL: {})", split.predicates[index], count, sql)
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val RECORDS_READ = Metrics.counter(HiveReadFn::class.java, "records_read")
    }
}

private val LOGGER = LoggerFactory.getLogger(HiveReadProvider::class.java)

/** 让 `JdbcMetadata.toSchema` 的返回值能解构。 */
private operator fun Pair<Schema, List<me.jayer.hdata.jdbc.internal.ResultSetReader>>.component1(): Schema = first
