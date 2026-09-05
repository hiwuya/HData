package me.jayer.hdata.jdbc.transform

import com.zaxxer.hikari.HikariDataSource
import me.jayer.hdata.jdbc.internal.DataSources
import me.jayer.hdata.jdbc.internal.JdbcMetadata
import me.jayer.hdata.jdbc.internal.RowMapper
import me.jayer.hdata.jdbc.internal.SelectSql
import me.jayer.hdata.jdbc.partition.PartitionColumn
import me.jayer.hdata.jdbc.partition.PartitionConverter
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.transforms.splittabledofn.SplitResult
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import java.io.Serializable
import java.sql.ResultSet
import java.util.Properties
import kotlin.math.floor
import kotlin.math.roundToInt
import kotlin.math.sqrt

/**
 * JDBC 查询不能在 ResultSet 中间安全恢复，所以把数值范围预先切成固定查询块，tracker 认领的是
 * 查询块下标。这样运行时只能在两个 SQL 查询之间切分，不会出现主任务和 residual 查询重叠区间。
 */
data class JdbcRestriction(
    val dataFrom: Long,
    val dataTo: Long,
    val chunkFrom: Long,
    val chunkTo: Long,
    val chunkCount: Long,
    val initialPartitions: Int,
    /** 分区列上是否存在 NULL 值；存在时额外跑一条 `col IS NULL` 查询把它们读出来（对齐 Trino）。 */
    val hasNulls: Boolean = false,
) : Serializable {

    /** 数值查询块数量；末尾的 NULL 查询块只占 tracker 下标，不参与数值边界均分。 */
    val numericChunkCount: Long
        get() = chunkCount - if (hasNulls) 1L else 0L

    init {
        require(dataTo >= dataFrom) { "JDBC 数据区间非法: [$dataFrom, $dataTo)" }
        require(chunkCount >= if (hasNulls) 1L else 0L) {
            "JDBC 查询块数量不足以容纳 NULL 查询块"
        }
        require(chunkFrom in 0..chunkCount && chunkTo in chunkFrom..chunkCount) {
            "JDBC 查询块区间非法: [$chunkFrom, $chunkTo) / $chunkCount"
        }
        require(initialPartitions > 0) { "JDBC 初始分区数必须 > 0" }
        require((dataTo == dataFrom) == (numericChunkCount == 0L)) {
            "空数据区间与数值查询块数量不一致"
        }
    }

    fun chunkRange(): OffsetRange = OffsetRange(chunkFrom, chunkTo)

    fun withChunkRange(range: OffsetRange): JdbcRestriction =
        copy(chunkFrom = range.from, chunkTo = range.to)

    fun dataRange(chunk: Long): OffsetRange {
        require(chunk in chunkFrom until chunkTo) { "查询块[$chunk]不在当前限制[$chunkFrom, $chunkTo)内" }
        require(chunk < numericChunkCount) { "查询块[$chunk]是 NULL 查询块，没有数值区间" }
        return OffsetRange(boundary(chunk), boundary(chunk + 1))
    }

    private fun boundary(index: Long): Long {
        require(index in 0..numericChunkCount) {
            "数值查询块边界[$index]超出范围[0, $numericChunkCount]"
        }
        val span = Math.subtractExact(dataTo, dataFrom)
        val base = span / numericChunkCount
        val remainder = span % numericChunkCount
        // base * index <= span；余数只分配给前 remainder 个块，因此整个偏移量不会超过 span。
        val offset = Math.addExact(Math.multiplyExact(base, index), minOf(index, remainder))
        return Math.addExact(dataFrom, offset)
    }

    companion object {
        private const val serialVersionUID: Long = 1

        fun empty(): JdbcRestriction = JdbcRestriction(0, 0, 0, 0, 0, 1)
    }
}

/** 把 Beam 的标准 OffsetRange tracker 包装成以查询块下标为位置的 JDBC tracker。 */
class JdbcRestrictionTracker(
    private val template: JdbcRestriction,
) : RestrictionTracker<JdbcRestriction, Long>(), RestrictionTracker.HasProgress {

    private val delegate = OffsetRangeTracker(template.chunkRange())

    override fun tryClaim(position: Long): Boolean = delegate.tryClaim(position)

    override fun currentRestriction(): JdbcRestriction = template.withChunkRange(delegate.currentRestriction())

    override fun trySplit(fractionOfRemainder: Double): SplitResult<JdbcRestriction>? {
        val split = delegate.trySplit(fractionOfRemainder) ?: return null
        return SplitResult.of(
            template.withChunkRange(checkNotNull(split.primary)),
            template.withChunkRange(checkNotNull(split.residual)),
        )
    }

    override fun checkDone() = delegate.checkDone()

    override fun isBounded(): IsBounded = delegate.isBounded

    override fun getProgress(): Progress = delegate.progress
}

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
    fun getInitialRestriction(@Element select: SelectSql): JdbcRestriction =
        DataSources.withConnection(dataSourceProperties, "hdata-jdbc-range") { connection ->
            // MIN/MAX 与 NULL 探测在同一条 SQL 里，一次扫描拿全（见 JdbcMetadata.partitionProbe）
            val probe = JdbcMetadata.partitionProbe(connection, select, partitionColumn.name)
            val min = probe.min
            val max = probe.max
            LOGGER.info("表[{}] 分区列[{}] 取值范围: min={}, max={}", select.table, partitionColumn.name, min, max)
            // 分区列上的 NULL 不会被 `col >= ? AND col < ?` 读到，单独记一笔，processElement 里补一条
            // `col IS NULL` 查询，对齐 Trino（NULL 行放进一个独立 split），不再静默丢数据。
            val hasNulls = probe.hasNulls
            if (min == null || max == null) {
                // 没有非 NULL 的分区列值：NULL 部分占一个查询块（下标 0），不需要数值区间
                val c = if (hasNulls) 1L else 0L
                JdbcRestriction(0, 0, 0, c, c, 1, hasNulls)
            } else {
                val from = toOffset(min)
                val to = try {
                    Math.addExact(toOffset(max), 1)
                } catch (e: ArithmeticException) {
                    throw IllegalArgumentException(
                        "分区列[${partitionColumn.name}] 的最大值无法表示成半开区间上界；" +
                        "请设 partition_num: 1 放弃分区读",
                        e,
                    )
                }
                val span = try {
                    Math.subtractExact(to, from)
                } catch (e: ArithmeticException) {
                    throw IllegalArgumentException("分区列取值跨度超过 Long 可切分范围，请设 partition_num: 1", e)
                }
                val partitions = partitionNum ?: autoPartitionNum(span, select.table)
                // 每个初始分区留四个可独立重查的 SQL 块，既让 Beam 能在慢任务上动态切分，
                // 又避免按每个可能的列值发一条查询。
                val chunks = minOf(span, Math.multiplyExact(partitions.toLong(), RUNTIME_SPLIT_FACTOR))
                // NULL 值不在数值区间内：在数值块之后追加一个 NULL 块（下标 = chunks）
                val totalChunks = chunks + if (hasNulls) 1 else 0
                JdbcRestriction(from, to, 0, totalChunks, totalChunks, partitions, hasNulls)
            }
        }

    @SplitRestriction
    fun splitRestriction(
        @Element select: SelectSql,
        @Restriction restriction: JdbcRestriction,
        receiver: OutputReceiver<JdbcRestriction>,
    ) {
        val chunks = restriction.chunkTo - restriction.chunkFrom
        if (chunks <= 0) {
            LOGGER.info("表[{}] 没有可读区间，跳过", select.table)
            return
        }
        val splitsWanted = minOf(restriction.initialPartitions.toLong(), chunks)
        val perSplit = Math.floorDiv(chunks - 1, splitsWanted) + 1
        val splits = restriction.chunkRange().split(perSplit, 1)
        LOGGER.info("表[{}] 切分为 {} 个分区", select.table, splits.size)
        splits.forEach { receiver.output(restriction.withChunkRange(it)) }
    }

    /**
     * 没指定 partition_num 时按取值跨度估算：开方再除以 10，避免对着一个 RDBMS 开出成百上千条连接。
     * 但跨度极大时开方/10 仍会爆掉（例如 1e12 跨度的列会算出 10 万），所以再夹一个上限，
     * 与 MongoDB 分桶的 [MAX_PARTITIONS] 取同一量级，避免一个作业同时发起海量并行查询把库打挂。
     */
    internal fun autoPartitionNum(span: Long, table: String): Int = Companion.autoPartitionNum(span, table)

    @ProcessElement
    fun processElement(
        @Element select: SelectSql,
        tracker: RestrictionTracker<JdbcRestriction, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        val restriction = tracker.currentRestriction()
        if (restriction.chunkTo <= restriction.chunkFrom) {
            return
        }
        val name = partitionColumn.name
        // 最后一个查询块的上界是 dataTo = toOffset(max) + 1，回灌成列值时可能超出列类型表示范围
        // （例如 INT 列最大值为 2147483647 时，上界 2147483648 被 INT.fromLong 回绕成负数，
        // 于是 `col < 负数` 把边界那一行悄悄丢掉）。边界之后本就没有更大的值，所以最后一个块只下
        // 推 `col >= ?`、不再带 `< ?` 上界——语义等价且不会越界。
        val sqlLowerOnly = select.withConditions("$name >= ?").render()
        val sqlBounded = select.withConditions("$name >= ?", "$name < ?").render()
        val sqlNull = select.withConditions("$name IS NULL").render()
        // 分区列上的 NULL 不在任何数值区间内：在查询块下标末尾追加一个 NULL 块，processElement 里
        // 认领到它时跑 `col IS NULL`（对齐 Trino 把 NULL 行放进一个独立 split），不静默丢数据。
        val nullChunkIndex = if (restriction.hasNulls) restriction.chunkCount - 1 else -1
        val pool = checkNotNull(dataSource) { "数据源未初始化" }
        pool.connection.use { connection ->
            // PostgreSQL 必须关掉 autocommit 才会走游标流式读取；其他库保持默认 autocommit，
            // 每条语句自己提交——bundle 会连跑几十个块，没必要也不应该攒一个长事务。
            val postgres = connection.metaData.databaseProductName.contains("postgresql", ignoreCase = true)
            if (postgres) connection.autoCommit = false
            var count = 0L
            for (chunk in restriction.chunkFrom until restriction.chunkTo) {
                // 查询块是最小可恢复单元：先认领，再执行对应的、互不重叠的查询。
                if (!tracker.tryClaim(chunk)) break
                if (chunk == nullChunkIndex) {
                    connection.prepareStatement(sqlNull, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { ps ->
                        ps.fetchSize = fetchSize
                        LOGGER.info("Executing query (NULL 分区值): {}", sqlNull)
                        ps.executeQuery().use { rs ->
                            while (rs.next()) {
                                receiver.output(rowMapper.map(rs))
                                count++
                            }
                        }
                    }
                    // 快照周期缩到单个块：读完一块就提交，别让整个 bundle 抓着同一个快照
                    if (postgres) connection.commit()
                    continue
                }
                val range = restriction.dataRange(chunk)
                val from = fromOffset(range.from)
                val isLast = range.to == restriction.dataTo
                val sql = if (isLast) sqlLowerOnly else sqlBounded
                connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { ps ->
                    ps.fetchSize = fetchSize
                    ps.setObject(1, from)
                    if (!isLast) ps.setObject(2, fromOffset(range.to))
                    LOGGER.info("Executing query: {} [{}, {}{}", sql, from, range.to, if (isLast) "]" else ")")
                    ps.executeQuery().use { rs ->
                        while (rs.next()) {
                            receiver.output(rowMapper.map(rs))
                            count++
                        }
                    }
                }
                // 快照周期缩到单个块：读完一块就提交，别让整个 bundle 抓着同一个快照
                if (postgres) connection.commit()
            }
            RECORDS_READ.inc(count)
        }
    }

    @NewTracker
    fun newTracker(@Restriction restriction: JdbcRestriction): JdbcRestrictionTracker =
        JdbcRestrictionTracker(restriction)

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<JdbcRestriction> = SerializableCoder.of(JdbcRestriction::class.java)

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
        private const val RUNTIME_SPLIT_FACTOR: Long = 4
        internal const val MAX_PARTITIONS: Int = 1000
        private val LOGGER = LoggerFactory.getLogger(JdbcPartitionedReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(JdbcPartitionedReadFn::class.java, "records_read")

        /**
         * 见 [autoPartitionNum] 的约定；抽到 companion 以便单测直接验证上限钳制行为。
         */
        internal fun autoPartitionNum(span: Long, table: String): Int {
            val num = 1.coerceAtLeast(floor(sqrt(span.toDouble()) / 10).roundToInt())
                .coerceAtMost(MAX_PARTITIONS)
            LOGGER.info("表[{}] 未指定 partition_num，自动估算为 {}", table, num)
            return num
        }
    }
}
