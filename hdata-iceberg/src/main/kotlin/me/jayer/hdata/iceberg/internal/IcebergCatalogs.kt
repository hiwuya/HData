package me.jayer.hdata.iceberg.internal

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Metrics
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.Record
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.io.FileAppender
import org.apache.iceberg.types.Conversions
import org.apache.iceberg.types.Type
import org.apache.iceberg.exceptions.CommitFailedException
import java.io.Serializable
import java.nio.ByteBuffer
import java.util.UUID
import java.util.concurrent.ThreadLocalRandom

/**
 * 打开本地 HadoopCatalog、加载 / 按需创建 Iceberg 表，以及把一个 bundle 的行落盘并提交。
 *
 * @author wuya
 */
object IcebergCatalogs : Serializable {

    /**
     * `catalog_name` 要真的传下去：它会出现在 Iceberg 的报错、指标与表标识里，
     * 用两参数构造函数的话名字被写死成 `hadoop`，配置项等于收下就丢掉。
     */
    fun openCatalog(warehouse: String, catalogName: String): HadoopCatalog {
        val catalog = HadoopCatalog()
        catalog.setConf(Configuration())
        catalog.initialize(catalogName, mapOf("warehouse" to warehouse))
        return catalog
    }

    fun loadTable(catalog: Catalog, table: String): Table =
        catalog.loadTable(TableIdentifier.parse(table))

    fun ensureTable(catalog: Catalog, table: String, schema: org.apache.iceberg.Schema): Table {
        val id = TableIdentifier.parse(table)
        val resolved = if (catalog.tableExists(id)) catalog.loadTable(id) else try {
            catalog.createTable(id, schema)
        } catch (e: org.apache.iceberg.exceptions.AlreadyExistsException) {
            catalog.loadTable(id)
        }
        validateWritableTable(resolved, schema)
        return resolved
    }

    /** 写入器目前只支持无分区表，并要求声明字段与现有表按名字、顺序和类型一致。 */
    fun validateWritableTable(table: Table, expected: org.apache.iceberg.Schema) {
        require(table.spec().isUnpartitioned) {
            "Iceberg 表[${table.name()}]是分区表，当前写入器尚未实现分区数据文件，请改用无分区表"
        }
        val actualFields = table.schema().columns()
        val expectedFields = expected.columns()
        require(actualFields.size == expectedFields.size) {
            "Iceberg 表[${table.name()}]字段数为 ${actualFields.size}，schema_fields 声明了 ${expectedFields.size} 个"
        }
        actualFields.zip(expectedFields).forEach { (actual, declared) ->
            require(actual.name() == declared.name() && actual.type() == declared.type()) {
                "Iceberg 表[${table.name()}]字段[${actual.name()}:${actual.type()}]与 " +
                    "schema_fields[${declared.name()}:${declared.type()}]不一致"
            }
        }
    }

    /**
     * 清空表里已有的数据，`write_mode: overwrite` 用。
     *
     * 用 `newDelete().deleteFromRowFilter(alwaysTrue())` 而不是自己删文件：Iceberg 的删除是
     * 一次原子提交，读的人要么看到旧快照要么看到空表，不会读到删了一半的中间状态。
     * 这一步是**幂等**的——空表上再删一次什么也不会发生，所以 bundle 重试是安全的。
     */
    fun truncate(table: Table) {
        table.newDelete().deleteFromRowFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue()).commit()
    }

    fun writeRecords(table: Table, rows: List<Record>, icebergSchema: org.apache.iceberg.Schema) {
        if (rows.isEmpty()) return
        val format = org.apache.iceberg.FileFormat.AVRO
        val location = table.location() + "/data/" + UUID.randomUUID().toString() + ".avro"
        val outputFile = table.io().newOutputFile(location)
        val appender: FileAppender<Record> = org.apache.iceberg.InternalData.write(format, outputFile)
            .schema(icebergSchema)
            .build()
        // 中途失败也要关掉，否则临时文件的句柄一直留着；length()/metrics() 必须在 close 之后取
        appender.use { rows.forEach(it::add) }
        // InternalData.write 在 Iceberg 1.10 的 AVRO appender 上拿不到列统计（lower/upper_bounds 为空），
        // 而聚合/过滤下推依赖数据文件元数据，所以这里用本 bundle 的行自己算一份统计写进 DataFile。
        val metrics = metricsFromRows(rows, icebergSchema)
        val dataFile = org.apache.iceberg.DataFiles.builder(table.spec())
            .withPath(outputFile.location())
            .withFileSizeInBytes(appender.length())
            .withFormat(format)
            .withRecordCount(rows.size.toLong())
            .withMetrics(metrics)
            .build()
        appendWithRetry(table, dataFile)
    }

    /**
     * 多个 Beam bundle 会在不同 worker 上同时提交快照。Iceberg 自带的有限次乐观重试在并发 bundle
     * 较多时仍可能耗尽，HadoopCatalog 随后以 `Version N already exists` 失败。这里只重试明确表示
     * “提交未发生”的 [CommitFailedException]，每次先刷新表并加随机退避；提交状态未知的异常绝不
     * 重试，否则可能把同一个数据文件追加两次。
     */
    private fun appendWithRetry(table: Table, dataFile: org.apache.iceberg.DataFile) {
        var conflicts = 0
        while (true) {
            try {
                table.refresh()
                table.newAppend().appendFile(dataFile).commit()
                return
            } catch (e: CommitFailedException) {
                if (conflicts >= MAX_COMMIT_CONFLICT_RETRIES) throw e
                val cap = minOf(MAX_COMMIT_BACKOFF_MILLIS, INITIAL_COMMIT_BACKOFF_MILLIS shl minOf(conflicts, 10))
                val delay = ThreadLocalRandom.current().nextLong(cap / 2 + 1, cap + 1)
                try {
                    Thread.sleep(delay)
                } catch (interrupted: InterruptedException) {
                    Thread.currentThread().interrupt()
                    throw IllegalStateException("等待 Iceberg 并发提交重试时被中断", interrupted)
                }
                conflicts++
            }
        }
    }

    /**
     * 用本 bundle 的行自己算 Iceberg 列统计。InternalData.write 在 1.10 的 AVRO appender 上返回空统计，
     * 而聚合/过滤下推要靠数据文件元数据，这里补一份：每列的最小/最大值按 Iceberg 类型序列化进
     * lower/upper_bounds，空值计数进 null_value_counts。嵌套/非标量列不参与最值（聚合下推也不支持）。
     */
    internal fun metricsFromRows(rows: List<Record>, schema: org.apache.iceberg.Schema): Metrics {
        val nullCounts = mutableMapOf<Int, Long>()
        val valueCounts = mutableMapOf<Int, Long>()
        val lower = mutableMapOf<Int, ByteBuffer>()
        val upper = mutableMapOf<Int, ByteBuffer>()
        schema.columns().forEach { field ->
            val id = field.fieldId()
            val type = field.type()
            var minV: Comparable<Any>? = null
            var maxV: Comparable<Any>? = null
            var nulls = 0L
            for (r in rows) {
                val v = r.getField(field.name())
                if (v == null) {
                    nulls++
                    continue
                }
                @Suppress("UNCHECKED_CAST")
                val c = (v as? Comparable<Any>) ?: return@forEach
                if (minV == null || minV.compareTo(c) > 0) minV = c
                if (maxV == null || maxV.compareTo(c) < 0) maxV = c
            }
            nullCounts[id] = nulls
            // Iceberg 的 value_counts 是“该字段在文件中的值总数”，其中包含 null；
            // null_value_counts 才单独描述空值数。写成 non-null 数会让 inclusive metrics
            // evaluator 把“1 null + 1 非 null”误判成整列全 null，进而裁掉仍有匹配行的文件。
            valueCounts[id] = rows.size.toLong()
            if (minV != null) lower[id] = Conversions.toByteBuffer(type, minV)
            if (maxV != null) upper[id] = Conversions.toByteBuffer(type, maxV)
        }
        return Metrics(
            rows.size.toLong(),
            emptyMap(),
            valueCounts,
            nullCounts,
            emptyMap(),
            lower,
            upper,
        )
    }

    private const val MAX_COMMIT_CONFLICT_RETRIES = 20
    private const val INITIAL_COMMIT_BACKOFF_MILLIS = 25L
    private const val MAX_COMMIT_BACKOFF_MILLIS = 1_000L
}
