package io.jayer.hdata.jdbc

import com.zaxxer.hikari.HikariDataSource
import io.jayer.hdata.jdbc.handler.RowHandler
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import kotlin.math.ceil
import kotlin.math.floor
import kotlin.math.roundToInt
import kotlin.math.sqrt


/**
 * @author wuya
 * @date 2022-07-27
 */
@BoundedPerElement
class JdbcSourceSplittableDoFn<T>(
    private val rowHandler: RowHandler,
    private val partitionConverter: PartitionConverter<T>
) : DoFn<JdbcSourceDescriptor, Row>() {

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(JdbcSourceSplittableDoFn::class.java)
    }

    private var dataSource: HikariDataSource? = null

    private fun getDataSource(sourceDescriptor: JdbcSourceDescriptor): HikariDataSource {
        if (dataSource == null || dataSource!!.isClosed) {
            dataSource = JdbcUtils.createDataSource(sourceDescriptor.dataSourceConfig)
        }

        return dataSource!!
    }

    @GetInitialRestriction
    fun getInitialRestriction(@Element sourceDescriptor: JdbcSourceDescriptor): OffsetRange {
        getDataSource(sourceDescriptor).also { dataSource ->
            dataSource.connection.use { connection ->
                val (_, _, table, where, partitionColumn) = sourceDescriptor
                val range = JdbcUtils.queryPartitionRange(connection, table, where, partitionColumn)
                val min = range.first
                val max = range.second
                LOGGER.info("Partition range for table[$table]: min=$min, max=$max")
                if (min == null || max == null) {
                    // table has no data
                    return OffsetRange(0, 0)
                }
                return OffsetRange(partitionConverter.toLong(min as T), partitionConverter.toLong(max as T) + 1)
            }
        }
    }

    @SplitRestriction
    fun splitRestriction(
        @Element sourceDescriptor: JdbcSourceDescriptor,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>
    ) {
        val from = restriction.from
        val to = restriction.to
        val (_, _, table, _, _, partitionNum) = sourceDescriptor
        val numPartitions = if (partitionNum != null) {
            partitionNum
        } else {
            // In this case, we use the table row count to infer a number of
            // partitions.
            // We take the square root of the number of rows, and divide it by 10
            // to keep a relatively low number of partitions, given that an RDBMS
            // cannot usually accept a very large number of connections.
            val num = 1.coerceAtLeast(floor(sqrt((to - from).toDouble()) / 10).roundToInt())
            LOGGER.info("Automatically calculate partitionNum for table[$table]: $num")
            num
        }

        val desiredNumOffsetsPerSplit = ceil((to - from).toDouble() / numPartitions).toLong()
        val splits = restriction.split(desiredNumOffsetsPerSplit, 1)
        LOGGER.info("Split size: {}", splits.size)
        for ((index, split) in splits.withIndex()) {
            LOGGER.info("Split-$index OffsetRange: {}", split)
            receiver.output(split)
        }
    }

    @ProcessElement
    fun processElement(
        @Element sourceDescriptor: JdbcSourceDescriptor,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>
    ) {
        val range = tracker.currentRestriction()
        val (_, _, _, where, partitionColumn, _, _, fetchSize) = sourceDescriptor
        val sql = if (where.isBlank()) {
            sourceDescriptor.createQuery() + " WHERE $partitionColumn >= ? AND $partitionColumn < ?"
        } else {
            sourceDescriptor.createQuery() + " AND $partitionColumn >= ? AND $partitionColumn < ?"
        }

        if (tracker.tryClaim(range.to - 1)) {
            getDataSource(sourceDescriptor).also { dataSource ->
                dataSource.connection.use { connection ->
                    // PostgreSQL requires autocommit to be disabled to enable cursor streaming
                    // see https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
                    connection.autoCommit = false
                    connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
                        .use { ps ->
                            ps.fetchSize = fetchSize
                            ps.setObject(1, partitionConverter.fromLong(range.from))
                            ps.setObject(2, partitionConverter.fromLong(range.to))

                            LOGGER.info("Executing query: {}", sql)
                            ps.executeQuery().use { rs ->
                                while (rs.next()) {
                                    receiver.output(rowHandler.handle(rs))
                                }
                            }
                        }
                }
            }
        }
    }

    @NewTracker
    fun newTracker(@Restriction offsetRange: OffsetRange): OffsetRangeTracker {
        return offsetRange.newTracker()
    }

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> {
        return OffsetRange.Coder()
    }

    @Teardown
    fun tearDown() {
        dataSource?.close()
    }
}