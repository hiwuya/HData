package io.jayer.hdata.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import java.sql.ResultSet
import java.util.*


/**
 * @author wuya
 * @date 2022-07-27
 */
@BoundedPerElement
class JdbcSourceSplittableDoFn(
    private val rowMapper: BeamRowMapper,
    private val dataSourceConfig: Properties,
    private val columns: List<String>,
    private val where: String,
    private val partitionColumn: String,
    private val fetchSize: Int,
) : DoFn<String, Row>() {

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(JdbcSourceSplittableDoFn::class.java)
    }

    @GetInitialRestriction
    fun getInitialRestriction(@Element table: String): OffsetRange {
        HikariDataSource(HikariConfig(dataSourceConfig)).use { dataSource ->
            dataSource.connection.use { connection ->
                val range = JdbcUtils.queryPartitionRange(connection, table, where, partitionColumn)
                return if (range == null) {
                    LOGGER.info("Table[{}] is empty", table)
                    OffsetRange(0, 0)
                } else {
                    LOGGER.info("Initial restriction range: {}", range)
                    range
                }
            }
        }
    }

    @SplitRestriction
    fun splitRestriction(@Restriction restriction: OffsetRange, splitReceiver: OutputReceiver<OffsetRange>) {
        val splits = restriction.split(fetchSize.toLong(), 1)
        LOGGER.info("Split size: {}", splits.size)
        for ((index, split) in splits.withIndex()) {
            LOGGER.info("Split-$index OffsetRange: {}", split)
            splitReceiver.output(split)
        }
    }

    @ProcessElement
    fun processElement(
        @Element table: String,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>
    ) {
        val range = tracker.currentRestriction()
        val query =
            "SELECT ${columns.joinToString(",")} FROM `$table` WHERE `$partitionColumn` >= ${range.from} AND `$partitionColumn` < ${range.to}"
        if (tracker.tryClaim(range.to - 1)) {
            HikariDataSource(HikariConfig(dataSourceConfig)).use { dataSource ->
                dataSource.connection.use { connection ->
                    val ps = connection.prepareStatement(query, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY)
                    ps.fetchSize = fetchSize
                    ps.use {
                        LOGGER.info("Executing query: {}", query)
                        it.executeQuery().use { rs ->
                            while (rs.next()) {
                                receiver.output(rowMapper.mapRow(rs))
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
}