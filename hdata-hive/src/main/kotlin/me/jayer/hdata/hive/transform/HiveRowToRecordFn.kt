package me.jayer.hdata.hive.transform

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.hive.metastore.HiveColumn
import me.jayer.hdata.hive.metastore.PartitionNames
import me.jayer.hdata.hive.type.HiveValues
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFn
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.TupleTag
import org.slf4j.LoggerFactory

/**
 * 把上游的一行拆成"分区名 + 数据行"，对应 Trino `HivePageSink` 里算分区键那一步。
 *
 * 两件容易写错的事在这里一次说清：
 *
 *  1. **数据文件里没有分区列**。Hive 的分区值只存在于目录名 `dt=2024-01-01` 上，
 *     写进文件反而会让 Hive 读出错位的列。所以这里把分区列从行里摘出去，只留数据列。
 *  2. **按列名对齐，不按下标**。上游的 Row 来自任意 transform，字段顺序和 Hive 表一致纯属巧合，
 *     按下标写会静默地把数据写错列——这种错误在下游查出来之前根本发现不了。
 *
 * @author wuya
 */
class HiveRowToRecordFn(
    private val fileSchema: Schema,
    private val partitionColumns: List<HiveColumn>,
    private val errorSchema: Schema,
    private val deadLetter: Boolean,
    private val transformName: String,
    private val errorTag: TupleTag<Row>,
) : DoFn<Row, KV<String, Row>>() {

    @ProcessElement
    fun processElement(@Element row: Row, context: ProcessContext) {
        try {
            context.output(KV.of(partitionName(row), HiveValues.align(row, fileSchema)))
        } catch (e: Exception) {
            if (!deadLetter) {
                throw e
            }
            LOGGER.warn("行无法写入 Hive，转入死信: {}", e.message)
            context.output(errorTag, ErrorSchemas.failure(errorSchema, row, e, transformName))
        }
    }

    /** 非分区表返回空串，[me.jayer.hdata.hive.HiveWriteProvider] 会把它当成"直接写表目录"。 */
    private fun partitionName(row: Row): String {
        if (partitionColumns.isEmpty()) {
            return ""
        }
        val values = partitionColumns.map { column ->
            val field = row.schema.fields.firstOrNull { it.name.equals(column.name, ignoreCase = true) }
            requireNotNull(field) { "上游数据里没有分区列 ${column.name}，无法决定这一行写到哪个分区" }
            HiveValues.toPartitionLiteral(row.getValue<Any?>(field.name))
        }
        return PartitionNames.makePartName(partitionColumns.map { it.name }, values)
    }

    companion object {
        private const val serialVersionUID: Long = 1
        private val LOGGER = LoggerFactory.getLogger(HiveRowToRecordFn::class.java)
    }
}
