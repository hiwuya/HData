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
 * A JDBC query cannot be safely resumed in the middle of a ResultSet, so the numeric range is pre-split into fixed query chunks
 * and the tracker claims chunk indexes. Splitting then only happens between two SQL queries, never inside an overlapping range.
 */
data class JdbcRestriction(
    val dataFrom: Long,
    val dataTo: Long,
    val chunkFrom: Long,
    val chunkTo: Long,
    val chunkCount: Long,
    val initialPartitions: Int,
    /** Whether the partition column contains NULL values; when it does, an extra `col IS NULL` query reads those rows (matching Trino). */
    val hasNulls: Boolean = false,
) : Serializable {

    /** Number of numeric query chunks; the trailing NULL query chunk only occupies a tracker index and takes no part in dividing the numeric boundaries. */
    val numericChunkCount: Long
        get() = chunkCount - if (hasNulls) 1L else 0L

    init {
        require(dataTo >= dataFrom) { "illegal JDBC data range: [$dataFrom, $dataTo)" }
        require(chunkCount >= if (hasNulls) 1L else 0L) {
            "the number of JDBC query chunks is too small to hold the NULL query chunk"
        }
        require(chunkFrom in 0..chunkCount && chunkTo in chunkFrom..chunkCount) {
            "illegal JDBC query chunk range: [$chunkFrom, $chunkTo) / $chunkCount"
        }
        require(initialPartitions > 0) { "the JDBC initial partition count must be > 0" }
        require((dataTo == dataFrom) == (numericChunkCount == 0L)) {
            "an empty data range is inconsistent with the number of numeric query chunks"
        }
    }

    fun chunkRange(): OffsetRange = OffsetRange(chunkFrom, chunkTo)

    fun withChunkRange(range: OffsetRange): JdbcRestriction =
        copy(chunkFrom = range.from, chunkTo = range.to)

    fun dataRange(chunk: Long): OffsetRange {
        require(chunk in chunkFrom until chunkTo) { "query chunk [$chunk] is outside the current restriction [$chunkFrom, $chunkTo)" }
        require(chunk < numericChunkCount) { "query chunk [$chunk] is the NULL query chunk and has no numeric range" }
        return OffsetRange(boundary(chunk), boundary(chunk + 1))
    }

    private fun boundary(index: Long): Long {
        require(index in 0..numericChunkCount) {
            "numeric query chunk boundary [$index] is out of range [0, $numericChunkCount]"
        }
        val span = Math.subtractExact(dataTo, dataFrom)
        val base = span / numericChunkCount
        val remainder = span % numericChunkCount
        // base * index <= span; the remainder is handed out to the first remainder chunks only, so the total offset never exceeds span.
        val offset = Math.addExact(Math.multiplyExact(base, index), minOf(index, remainder))
        return Math.addExact(dataFrom, offset)
    }

    companion object {
        private const val serialVersionUID: Long = 1

        fun empty(): JdbcRestriction = JdbcRestriction(0, 0, 0, 0, 0, 1)
    }
}

/** Wraps Beam's standard OffsetRange tracker into a JDBC tracker positioned by query chunk index. */
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
 * Reads a table in parallel, split by the partition column.
 *
 * The value range of the partition column is mapped onto an `OffsetRange` and handed to Beam's splittable DoFn for splitting;
 * each piece is then translated back into a `col >= ? AND col < ?` predicate.
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
     * `@GetInitialRestriction` may be called on an instance other than the one from `@Setup`, so we build one on demand and close it
     * right after use instead of reusing [dataSource].
     */
    @GetInitialRestriction
    fun getInitialRestriction(@Element select: SelectSql): JdbcRestriction =
        DataSources.withConnection(dataSourceProperties, "hdata-jdbc-range") { connection ->
            // MIN/MAX and NULL detection live in the same SQL statement, so one scan gets everything (see JdbcMetadata.partitionProbe)
            val probe = JdbcMetadata.partitionProbe(connection, select, partitionColumn.name)
            val min = probe.min
            val max = probe.max
            LOGGER.info("value range of the partition column [{}] of table [{}]: min={}, max={}", select.table, partitionColumn.name, min, max)
            // NULLs in the partition column are not covered by `col >= ? AND col < ?`; note it here and issue an extra
            // `col IS NULL` query in processElement, matching Trino (NULL rows go into a dedicated split), so no data is silently lost.
            val hasNulls = probe.hasNulls
            if (min == null || max == null) {
                // There is no non-NULL partition column value: the NULL part takes one query chunk (index 0) and needs no numeric range
                val c = if (hasNulls) 1L else 0L
                JdbcRestriction(0, 0, 0, c, c, 1, hasNulls)
            } else {
                val from = toOffset(min)
                val to = try {
                    Math.addExact(toOffset(max), 1)
                } catch (e: ArithmeticException) {
                    throw IllegalArgumentException(
                        "the maximum value of the partition column [${partitionColumn.name}] cannot be expressed as a half-open range upper bound; " +
                        "please set partition_num: 1 to give up partitioned reads",
                        e,
                    )
                }
                val span = try {
                    Math.subtractExact(to, from)
                } catch (e: ArithmeticException) {
                    throw IllegalArgumentException("the value span of the partition column exceeds the splittable range of Long, please set partition_num: 1", e)
                }
                val partitions = partitionNum ?: autoPartitionNum(span, select.table)
                // Every initial partition keeps four independently re-runnable SQL chunks, so Beam can split dynamically on slow
                // tasks without issuing one query per possible column value.
                val chunks = minOf(span, Math.multiplyExact(partitions.toLong(), RUNTIME_SPLIT_FACTOR))
                // NULL values are outside the numeric range: append a NULL chunk after the numeric chunks (index = chunks)
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
            LOGGER.info("table [{}] has no readable range, skipping", select.table)
            return
        }
        val splitsWanted = minOf(restriction.initialPartitions.toLong(), chunks)
        val perSplit = Math.floorDiv(chunks - 1, splitsWanted) + 1
        val splits = restriction.chunkRange().split(perSplit, 1)
        LOGGER.info("table [{}] split into {} partitions", select.table, splits.size)
        splits.forEach { receiver.output(restriction.withChunkRange(it)) }
    }

    /**
     * When partition_num is not given, estimate from the value span: take the square root and divide by 10, to avoid opening
     * hundreds or thousands of connections against a single RDBMS. An extreme span still blows up with sqrt/10 though (a column
     * spanning 1e12 would yield 100000), so an upper bound is clamped as well, on the same order of magnitude as MongoDB's [MAX_PARTITIONS] bucketing, to keep one job from flooding the database with a huge number of parallel queries.
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
        // The last query chunk's upper bound is dataTo = toOffset(max) + 1, which may exceed the column type's representable range
        // when converted back into a column value (with an INT column whose maximum is 2147483647, the bound 2147483648 is wrapped
        // into a negative number by INT.fromLong, so `col < negative` silently drops the boundary row). There is no larger value
        // beyond the boundary anyway, so the last chunk only pushes down `col >= ?` and no `< ?` upper bound — semantically equivalent and cannot overflow.
        val sqlLowerOnly = select.withConditions("$name >= ?").render()
        val sqlBounded = select.withConditions("$name >= ?", "$name < ?").render()
        val sqlNull = select.withConditions("$name IS NULL").render()
        // NULLs in the partition column are outside every numeric range: append a NULL chunk at the end of the query chunk indexes,
        // and processElement runs `col IS NULL` when it claims it (matching Trino's dedicated split for NULL rows), so no data is silently lost.
        val nullChunkIndex = if (restriction.hasNulls) restriction.chunkCount - 1 else -1
        val pool = checkNotNull(dataSource) { "data source is not initialized" }
        pool.connection.use { connection ->
            // PostgreSQL must have autocommit disabled to stream through a cursor; other databases keep the default autocommit and
            // commit per statement — a bundle runs dozens of chunks, so there is no need and no reason to hold one long transaction.
            val postgres = connection.metaData.databaseProductName.contains("postgresql", ignoreCase = true)
            if (postgres) connection.autoCommit = false
            var count = 0L
            for (chunk in restriction.chunkFrom until restriction.chunkTo) {
                // A query chunk is the smallest recoverable unit: claim it first, then run the corresponding non-overlapping query.
                if (!tracker.tryClaim(chunk)) break
                if (chunk == nullChunkIndex) {
                    connection.prepareStatement(sqlNull, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY).use { ps ->
                        ps.fetchSize = fetchSize
                        LOGGER.info("Executing query (NULL partition value): {}", sqlNull)
                        ps.executeQuery().use { rs ->
                            while (rs.next()) {
                                receiver.output(rowMapper.map(rs))
                                count++
                            }
                        }
                    }
                    // Shorten the snapshot cycle to a single chunk: commit once a chunk is done, instead of holding one snapshot for the whole bundle
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
                // Shorten the snapshot cycle to a single chunk: commit once a chunk is done, instead of holding one snapshot for the whole bundle
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
     * Partition column values come from `min()/max()`, and some drivers report a type different from the column itself (promoting
     * the min of an INT to BIGINT, for example); when the cast fails, report something that locates the problem instead of a bare
     */
    private fun toOffset(value: Any): Long {
        @Suppress("UNCHECKED_CAST")
        val converter = partitionColumn.converter as PartitionConverter<Any>
        return try {
            converter.toLong(value)
        } catch (e: ClassCastException) {
            throw IllegalStateException(
                "the value type of the partition column [${partitionColumn.name}] is ${value.javaClass.name}, " +
                    "which is inconsistent with the type inferred from the column metadata; please specify a partition column with an unambiguous type",
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
         * See the contract of [autoPartitionNum]; pulled into the companion so unit tests can verify the clamping behaviour directly.
         */
        internal fun autoPartitionNum(span: Long, table: String): Int {
            val num = 1.coerceAtLeast(floor(sqrt(span.toDouble()) / 10).roundToInt())
                .coerceAtMost(MAX_PARTITIONS)
            LOGGER.info("no partition_num given for table [{}], automatically estimated as {}", table, num)
            return num
        }
    }
}
