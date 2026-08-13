package me.jayer.hdata.hive

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
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
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties

/**
 * `WriteToHive`：通过 JDBC 批量 `INSERT INTO` 写 Hive 表，支持死信输出。
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
        val inputSchema = input.schema
        val errorSchema = ErrorSchemas.of(inputSchema)
        val errors = input
            .apply("Write", ParDo.of(HiveWriteFn(config, inputSchema, errorSchema, deadLetter, transformName)))
            .setRowSchema(errorSchema)
        return if (deadLetter) errors else null
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 逐条（攒批）写入 Hive。写失败且开了死信时单条转入死信流；没开死信时异常直接抛出，作业失败。
 */
class HiveWriteFn(
    private val config: HiveWriteConfig,
    private val inputSchema: Schema,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
) : DoFn<Row, Row>() {

    @Transient
    private var connection: Connection? = null

    private val buffered = mutableListOf<ValueInSingleWindow<Row>>()
    private val failures = mutableListOf<ValueInSingleWindow<Row>>()

    @Setup
    fun setup() {
        connection = newConnection()
    }

    @StartBundle
    fun startBundle() {
        buffered.clear()
        failures.clear()
    }

    @ProcessElement
    fun processElement(
        @Element row: Row,
        @Timestamp timestamp: org.joda.time.Instant,
        window: BoundedWindow,
        pane: PaneInfo,
    ) {
        buffered.add(ValueInSingleWindow.of(row, timestamp, window, pane))
        if (buffered.size >= config.batchSize) {
            flush()
        }
    }

    @FinishBundle
    fun finishBundle(context: FinishBundleContext) {
        flush()
        failures.forEach { context.output(it.value, it.timestamp, it.window) }
        failures.clear()
    }

    @Teardown
    fun tearDown() {
        runCatching { connection?.close() }
        connection = null
    }

    private fun flush() {
        if (buffered.isEmpty()) {
            return
        }
        val conn = checkNotNull(connection) { "数据库连接未初始化" }
        val qualified = if (config.database.isNotBlank()) "${config.database}.${config.table}" else config.table
        val columns = inputSchema.columnNames().joinToString(", ") { it }
        val placeholders = inputSchema.columnNames().joinToString(", ") { "?" }
        val sql = "INSERT INTO $qualified ($columns) VALUES ($placeholders)"
        buffered.forEach { record ->
            try {
                conn.prepareStatement(sql).use { ps ->
                    val row = record.value
                    for (i in 0 until inputSchema.fieldCount) {
                        val value = row.getValue<Any?>(i)
                        ps.setObject(i + 1, value)
                    }
                    ps.executeUpdate()
                    RECORDS_WRITTEN.inc()
                }
            } catch (e: Exception) {
                if (!deadLetter) {
                    throw e
                }
                LOGGER.warn("写入 Hive 失败，转入死信: {}", e.message)
                RECORDS_REJECTED.inc()
                failures.add(
                    ValueInSingleWindow.of(
                        ErrorSchemas.failure(errorSchema, record.value, e, transformName),
                        record.timestamp,
                        record.window,
                        record.paneInfo,
                    )
                )
            }
        }
        buffered.clear()
    }

    private fun newConnection(): Connection {
        val props = Properties().apply {
            if (config.user.isNotBlank()) this["user"] = config.user
            if (config.password.isNotBlank()) this["password"] = config.password
        }
        return DriverManager.getConnection(config.url, props)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(HiveWriteFn::class.java)
        private val RECORDS_WRITTEN = Metrics.counter(HiveWriteFn::class.java, "records_written")
        private val RECORDS_REJECTED = Metrics.counter(HiveWriteFn::class.java, "records_rejected")
    }
}
