package me.jayer.hdata.hive

import me.jayer.hdata.core.spi.RowSource
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import org.apache.beam.sdk.coders.Coder
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.metrics.Metrics
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.splittabledofn.OffsetRangeTracker
import org.apache.beam.sdk.transforms.splittabledofn.RestrictionTracker
import org.apache.beam.sdk.values.PBegin
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.slf4j.LoggerFactory
import java.sql.Connection
import java.sql.DriverManager
import java.util.Properties

private val LOGGER = LoggerFactory.getLogger(HiveReadProvider::class.java)

/**
 * `ReadFromHive`：通过 JDBC（`jdbc:hive2://...`）按分区并行读 Hive 表，使用 Splittable DoFn。
 *
 * 分区列表在构图阶段通过 `SHOW PARTITIONS` 或显式 `partitions` 拿到；每个分区是一个元素，
 * 限制 `OffsetRange(0, 1)` 表示每个分区只切出一份（不可再分），由 Beam 在 worker 上并行执行。
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

private class HiveSource(private val config: HiveReadConfig) : RowSource() {

    override fun read(begin: PBegin): PCollection<Row> {
        val schema = resolveSchema()
        val partitions = resolvePartitions()
        LOGGER.info("ReadFromHive 共 {} 个分区: {}", partitions.size, partitions)
        return begin.apply("Partitions", Create.of(partitions))
            .apply("Read", ParDo.of(HiveReadFn(config, schema)))
            .setRowSchema(schema)
    }

    /** 输出 schema：优先显式字段，其次 DESCRIBE 推断，最后回退单列 value(STRING)。 */
    private fun resolveSchema(): Schema =
        buildSchemaFromFields(config.schemaFields)
            ?: withConnection { conn -> deriveSchema(conn, config.database, config.table) }
            ?: Schema.builder().addNullableField("value", Schema.FieldType.STRING).build()

    /** 分区谓词片段列表：显式优先；为空则 SHOW PARTITIONS；仍为空（未分区表）给一个空片段。 */
    private fun resolvePartitions(): List<String> {
        if (config.partitions.isNotEmpty()) {
            return config.partitions
        }
        val discovered = withConnection { conn ->
            val qualified = if (config.database.isNotBlank()) "${config.database}.${config.table}" else config.table
            runCatching {
                conn.createStatement().use { st ->
                    st.executeQuery("SHOW PARTITIONS $qualified").use { rs ->
                        val list = mutableListOf<String>()
                        while (rs.next()) {
                            val raw = rs.getString(1) ?: continue
                            list.add(raw)
                        }
                        list
                    }
                }
            }.getOrDefault(emptyList())
        }
        return discovered.ifEmpty { listOf("") }
    }

    private fun <T> withConnection(block: (Connection) -> T): T {
        val props = Properties().apply {
            if (config.user.isNotBlank()) this["user"] = config.user
            if (config.password.isNotBlank()) this["password"] = config.password
        }
        DriverManager.getConnection(config.url, props).use { return block(it) }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 每个元素是一个分区谓词片段（直接拼进 WHERE），限制固定 `OffsetRange(0, 1)`，
 * 表示该分区一次读完（读本身不可中断续跑，用 `tryClaim(to - 1)` 一次性认领）。
 */
@DoFn.BoundedPerElement
class HiveReadFn(
    private val config: HiveReadConfig,
    private val schema: Schema,
) : DoFn<String, Row>() {

    @GetInitialRestriction
    fun getInitialRestriction(@Element spec: String): OffsetRange = OffsetRange(0, 1)

    @SplitRestriction
    fun splitRestriction(
        @Element spec: String,
        @Restriction restriction: OffsetRange,
        receiver: OutputReceiver<OffsetRange>,
    ) {
        val span = restriction.to - restriction.from
        if (span <= 0) {
            return
        }
        restriction.split(1, 1).forEach { receiver.output(it) }
    }

    @ProcessElement
    fun processElement(
        @Element spec: String,
        tracker: RestrictionTracker<OffsetRange, Long>,
        receiver: OutputReceiver<Row>,
    ) {
        val range = tracker.currentRestriction()
        if (range.to <= range.from) {
            return
        }
        if (!tracker.tryClaim(range.to - 1)) {
            return
        }
        queryPartition(spec, receiver)
    }

    @NewTracker
    fun newTracker(@Restriction restriction: OffsetRange): OffsetRangeTracker = restriction.newTracker()

    @GetRestrictionCoder
    fun restrictionCoder(): Coder<OffsetRange> = OffsetRange.Coder()

    private fun queryPartition(spec: String, receiver: OutputReceiver<Row>) {
        val qualified = if (config.database.isNotBlank()) "${config.database}.${config.table}" else config.table
        val columns = schema.columnNames().joinToString(", ") { it }
        val sql = buildString {
            append("SELECT ").append(columns).append(" FROM ").append(qualified)
            if (spec.isNotBlank()) {
                append(" WHERE ").append(spec)
            }
        }
        val props = Properties().apply {
            if (config.user.isNotBlank()) this["user"] = config.user
            if (config.password.isNotBlank()) this["password"] = config.password
        }
        DriverManager.getConnection(config.url, props).use { conn ->
            conn.createStatement().use { st ->
                st.executeQuery(sql).use { rs ->
                    var count = 0L
                    while (rs.next()) {
                        receiver.output(resultSetToRow(schema, rs))
                        count++
                    }
                    RECORDS_READ.inc(count)
                    LOGGER.info("Hive 分区[{}] 读完 {} 条 (SQL: {})", spec, count, sql)
                }
            }
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(HiveReadFn::class.java)
        private val RECORDS_READ = Metrics.counter(HiveReadFn::class.java, "records_read")
    }
}
