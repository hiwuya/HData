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
import java.io.Serializable
import java.nio.ByteBuffer
import java.util.UUID

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
        table.newAppend().appendFile(dataFile).commit()
    }

    /**
     * 用本 bundle 的行自己算 Iceberg 列统计。InternalData.write 在 1.10 的 AVRO appender 上返回空统计，
     * 而聚合/过滤下推要靠数据文件元数据，这里补一份：每列的最小/最大值按 Iceberg 类型序列化进
     * lower/upper_bounds，空值计数进 null_value_counts。嵌套/非标量列不参与最值（聚合下推也不支持）。
     */
    private fun metricsFromRows(rows: List<Record>, schema: org.apache.iceberg.Schema): Metrics {
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
            valueCounts[id] = rows.size.toLong() - nulls
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
}
