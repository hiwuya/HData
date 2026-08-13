package me.jayer.hdata.hive

import com.zaxxer.hikari.HikariDataSource
import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.jdbc.internal.DataSources
import me.jayer.hdata.jdbc.internal.InsertSql
import me.jayer.hdata.jdbc.internal.RowBinder
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.windowing.BoundedWindow
import org.apache.beam.sdk.transforms.windowing.PaneInfo
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.ValueInSingleWindow
import org.slf4j.LoggerFactory
import java.sql.BatchUpdateException
import java.util.Properties

/**
 * `WriteToHive`：通过 JDBC 批量 `INSERT INTO` 写 Hive 表，支持死信输出。
 *
 * @author wuya
 */
class HiveWriteProvider : TypedTransformProvider<HiveWriteConfig>(HiveWriteConfig::class.java) {

    override fun identifier(): String = "WriteToHive"

    override fun description(): String = "通过 JDBC 批量写入 Hive 表，支持死信输出"

    override fun outputCollectionNames(): List<String> = listOf(Tags.ERROR_OUTPUT)

    override fun create(
        config: HiveWriteConfig,
        context: TransformConfig,
    ): PTransform<PCollectionRowTuple, PCollectionRowTuple> {
        config.validate()
        return HiveSink(config, context.errorHandling != null, context.transformName)
    }
}

private class HiveSink(
    private val config: HiveWriteConfig,
    private val deadLetter: Boolean,
    private val transformName: String,
) : RowSink() {

    override fun write(input: PCollection<Row>): PCollection<Row>? {
        val errorSchema = ErrorSchemas.of(input.schema)
        val errors = input
            .apply(
                "Write",
                ParDo.of(HiveWriteFn(config.dataSourceProperties(), config, input.schema, errorSchema, deadLetter, transformName)),
            )
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 攒批写入 Hive。
 *
 * 走 JDBC 的 `addBatch` / `executeBatch`，绑定值复用 `hdata-jdbc` 的 [RowBinder]。
 * 重构前这里对**每一行**都 `prepareStatement(...).executeUpdate()`：
 * `batch_size` 只是攒在内存里，真正发出去还是一行一个往返；而且用的是 `ps.setObject(i, value)`，
 * 遇到 Beam 的 joda `Instant` 或 `LocalDate` 这类值，驱动根本不认。
 */
class HiveWriteFn(
    private val dataSourceProperties: Properties,
    private val config: HiveWriteConfig,
    private val inputSchema: Schema,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var dataSource: HikariDataSource? = null

    @Transient
    private var binder: RowBinder? = null

    @Transient
    private var buffered: MutableList<ValueInSingleWindow<Row>>? = null

    @Transient
    private var failures: MutableList<ValueInSingleWindow<Row>>? = null

    @Setup
    fun setup() {
        dataSource = DataSources.create(dataSourceProperties, "hdata-hive-write")
        binder = RowBinder.of(inputSchema)
        buffered = mutableListOf()
        failures = mutableListOf()
    }

    @Teardown
    fun tearDown() {
        dataSource?.close()
        dataSource = null
    }

    @StartBundle
    fun startBundle() {
        buffered?.clear()
        failures?.clear()
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        val queue = checkNotNull(buffered) { "写入器未初始化" }
        queue.add(ValueInSingleWindow.of(row, timestamp, window, pane))
        if (queue.size >= config.batchSize) {
            flush()
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        flush()
        val rejected = checkNotNull(failures)
        rejected.forEach { context.output(it.value, it.timestamp, it.window) }
        rejected.clear()
    }

    private fun flush() {
        val queue = checkNotNull(buffered)
        if (queue.isEmpty()) {
            return
        }
        val sql = InsertSql.render(config.qualifiedTable, inputSchema.fieldNames)
        val pool = checkNotNull(dataSource) { "数据源未初始化" }
        try {
            pool.connection.use { connection ->
                connection.prepareStatement(sql).use { ps ->
                    queue.forEach { record ->
                        checkNotNull(binder).bind(ps, record.value)
                        ps.addBatch()
                    }
                    val results = ps.executeBatch()
                    RECORDS_WRITTEN.inc(results.size.toLong())
                }
            }
        } catch (e: BatchUpdateException) {
            // updateCounts 里 EXECUTE_FAILED 的下标就是失败的那几行，能精确到行
            rejectFailed(queue, e.updateCounts, e)
        } catch (e: Exception) {
            queue.forEach { reject(it, e) }
        } finally {
            queue.clear()
        }
    }

    private fun rejectFailed(
        queue: List<ValueInSingleWindow<Row>>,
        updateCounts: IntArray?,
        cause: Exception,
    ) {
        if (updateCounts == null || updateCounts.size != queue.size) {
            queue.forEach { reject(it, cause) }
            return
        }
        queue.forEachIndexed { index, record ->
            if (updateCounts[index] == java.sql.Statement.EXECUTE_FAILED) {
                reject(record, cause)
            } else {
                RECORDS_WRITTEN.inc()
            }
        }
    }

    private fun reject(record: ValueInSingleWindow<Row>, e: Exception) {
        if (!deadLetter) {
            throw e
        }
        LOGGER.warn("写入 Hive 失败，转入死信: {}", e.message)
        RECORDS_REJECTED.inc()
        checkNotNull(failures).add(
            ValueInSingleWindow.of(
                ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                record.timestamp,
                record.window,
                record.paneInfo,
            )
        )
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(HiveWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(HiveWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(HiveWriteFn::class.java, "records_rejected")
    }
}
