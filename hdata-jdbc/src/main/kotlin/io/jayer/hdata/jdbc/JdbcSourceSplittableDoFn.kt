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
class JdbcSourceSplittableDoFn(private val rowHandler: RowHandler) : DoFn<JdbcSourceDescriptor, Row>() {

    companion object {
        private const val serialVersionUID: Long = 1
        private val NONE_SPLIT_RANGE = OffsetRange(Long.MIN_VALUE, Long.MAX_VALUE)
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
        val (_, _, table, where, partitionColumn, partitionNum, query) = sourceDescriptor
        if (query.isNotBlank() || partitionNum == 1 || partitionColumn.isBlank()) {
            return NONE_SPLIT_RANGE
        }

        getDataSource(sourceDescriptor).use { dataSource ->
            dataSource.connection.use { connection ->
                val range = JdbcUtils.queryPartitionRange(connection, table, where, partitionColumn)
                LOGGER.info("Initial restriction range for table[$table]: $range")
                return range
            }
        }
    }

    @SplitRestriction
    fun splitRestriction(
        @Element sourceDescriptor: JdbcSourceDescriptor,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>
    ) {
        if (restriction == NONE_SPLIT_RANGE) {
            receiver.output(restriction)
        } else {
            val (_, _, table, _, _, partitionNum) = sourceDescriptor
            val numPartitions = if (partitionNum != null) {
                partitionNum
            } else {
                // In this case, we use the table row count to infer a number of
                // partitions.
                // We take the square root of the number of rows, and divide it by 10
                // to keep a relatively low number of partitions, given that an RDBMS
                // cannot usually accept a very large number of connections.
                val num = 1.coerceAtLeast(floor(sqrt((restriction.to - restriction.from).toDouble()) / 10).roundToInt())
                LOGGER.info("Auto calculate partitionNum for table[$table]: $num")
                num
            }

            val splits =
                restriction.split(ceil((restriction.to - restriction.from).toDouble() / numPartitions).toLong(), 1)
            LOGGER.info("Split size: {}", splits.size)
            for ((index, split) in splits.withIndex()) {
                LOGGER.info("Split-$index OffsetRange: {}", split)
                receiver.output(split)
            }
        }
    }

    @ProcessElement
    fun processElement(
        @Element sourceDescriptor: JdbcSourceDescriptor,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>
    ) {
        val range = tracker.currentRestriction()
        val (_, _, _, where, partitionColumn, _, query, fetchSize) = sourceDescriptor
        val sql = when {
            query.isNotBlank() -> query
            partitionColumn.isNotBlank() && where.isBlank() -> sourceDescriptor.createSchemaQuery() + " WHERE `$partitionColumn` >= ${range.from} AND `$partitionColumn` < ${range.to}"
            partitionColumn.isNotBlank() && where.isNotBlank() -> sourceDescriptor.createSchemaQuery() + " AND `$partitionColumn` >= ${range.from} AND `$partitionColumn` < ${range.to}"
            else -> sourceDescriptor.createSchemaQuery()
        }

        if (tracker.tryClaim(range.to - 1)) {
            getDataSource(sourceDescriptor).use { dataSource ->
                dataSource.connection.use { connection ->
                    // PostgreSQL requires autocommit to be disabled to enable cursor streaming
                    // see https://jdbc.postgresql.org/documentation/head/query.html#query-with-cursor
                    connection.autoCommit = false
                    val ps = connection.prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
                    ps.fetchSize = fetchSize
                    ps.use {
                        LOGGER.info("Executing query: {}", sql)
                        it.executeQuery().use { rs ->
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
        return OffsetRangeTracker(offsetRange)
    }

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> {
        return OffsetRange.Coder()
    }

    @Teardown
    fun tearDown() {
        dataSource?.let {
            if (!it.isClosed) {
                it.close()
            }
        }
    }
}