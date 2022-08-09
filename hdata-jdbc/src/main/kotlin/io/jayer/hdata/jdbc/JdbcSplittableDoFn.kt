package io.jayer.hdata.jdbc

import com.zaxxer.hikari.HikariConfig
import com.zaxxer.hikari.HikariDataSource
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.DoFn.BoundedPerElement
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory


/**
 * @author wuya
 * @date 2022-07-27
 */
@BoundedPerElement
class JdbcSplittableDoFn(private val sourceDescriptor: JdbcSourceDescriptor) : DoFn<Void, Row>() {

    companion object {
        private const val serialVersionUID: Long = 1
        private val logger = LoggerFactory.getLogger(JdbcSplittableDoFn::class.java)
    }

    private lateinit var dataSource: HikariDataSource

    @Setup
    fun setup() {
        dataSource = HikariDataSource(HikariConfig(sourceDescriptor.dataSourceConfig))
    }

    @GetInitialRestriction
    fun getInitialRestriction(): OffsetRange {
        val partitionLowerBound = sourceDescriptor.partitionLowerBound
        val partitionUpperBound = sourceDescriptor.partitionUpperBound
        if (partitionLowerBound != null && partitionUpperBound != null) {
            return OffsetRange(partitionLowerBound, partitionUpperBound)
        }

        val table = sourceDescriptor.table
        var partitionColumn = sourceDescriptor.partitionColumn
        if (partitionColumn.isBlank()) {
            partitionColumn = JdbcUtils.getFirstNumericPrimaryKey(dataSource, table) ?: ""
        }

        if (partitionColumn.isNotBlank()) {
            val range = JdbcUtils.queryPartitionRange(dataSource, table, sourceDescriptor.where, partitionColumn)
            logger.info("Initial restriction range: {}", range)
            return range
        }

        return OffsetRange(0, 0)
    }

    @SplitRestriction
    fun splitRestriction(@Restriction restriction: OffsetRange, splitReceiver: OutputReceiver<OffsetRange>) {
        val fetchSize = sourceDescriptor.fetchSize.toLong()
        val splits = restriction.split(fetchSize, fetchSize)
        logger.info("Split size: {}", splits.size)
        for (split in splits) {
            logger.info("Split: {}", split)
            splitReceiver.output(split)
        }
    }

    @ProcessElement
    fun processElement(c: ProcessContext, tracker: RestrictionTracker<OffsetRange, Long>) {
        var sql =
            "SELECT ${sourceDescriptor.columns.joinToString(",")} FROM `${sourceDescriptor.table}` WHERE ${sourceDescriptor.partitionColumn} BETWEEN ? AND ?"

        val range = tracker.currentRestriction()
        dataSource.connection.use { connection ->
            val ps = connection.prepareStatement(sql)
            ps.setLong(1, range.from)
            ps.setLong(2, range.to)
            ps.use {
                ps.executeQuery().use { rs ->
                    val schema = JdbcUtils.convertToBeamSchema(rs.metaData)
                    val columns = rs.metaData.columnCount
                    while (rs.next()) {
                        val row = Row.withSchema(schema)
                        for (i in 1..columns) {
                            when (schema.getField(i - 1).type) {
                                Schema.FieldType.DECIMAL -> row.addValue(rs.getBigDecimal(i))
                                else -> row.addValue(rs.getObject(i))
                            }
                        }
                        c.output(row.build())
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
        dataSource.close()
    }
}