package me.jayer.hdata.iceberg.internal

import org.apache.hadoop.conf.Configuration
import org.apache.iceberg.Table
import org.apache.iceberg.catalog.Catalog
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.data.Record
import org.apache.iceberg.hadoop.HadoopCatalog
import org.apache.iceberg.io.FileAppender
import java.io.Serializable
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
        if (catalog.tableExists(id)) return catalog.loadTable(id)
        return try {
            catalog.createTable(id, schema)
        } catch (e: org.apache.iceberg.exceptions.AlreadyExistsException) {
            catalog.loadTable(id)
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
        val dataFile = org.apache.iceberg.DataFiles.builder(table.spec())
            .withPath(outputFile.location())
            .withFileSizeInBytes(appender.length())
            .withFormat(format)
            .withRecordCount(rows.size.toLong())
            .withMetrics(appender.metrics())
            .build()
        table.newAppend().appendFile(dataFile).commit()
    }
}
