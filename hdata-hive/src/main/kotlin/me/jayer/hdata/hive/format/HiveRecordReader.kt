package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.metastore.HiveColumn
import me.jayer.hdata.hive.metastore.HiveTable
import me.jayer.hdata.hive.split.HiveFile
import me.jayer.hdata.hive.type.HiveTypes
import me.jayer.hdata.hive.type.HiveValues
import org.apache.beam.sdk.io.range.OffsetRange
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import java.io.Serializable

/**
 * 一次读取任务的列布局：从文件里读哪几列、分区列怎么补、输出成什么 schema。
 *
 * 输出顺序固定为**投影到的数据列 + 投影到的分区列**，与 Hive `SELECT *` 的顺序一致
 * （Hive 把分区列排在最后）。要别的顺序用 `MapToFields`，不在读取端做。
 *
 * 数据列与分区列都保留**全表的列表**加一份投影下标，而不是只留投影后的列：
 * 位置敏感的格式（TEXTFILE / CSV / RCFile / `_col0` 式的 ORC）靠物理下标定位列，
 * 分区值也是按分区列的位置存在目录名里的，丢掉原始位置就对不回去了。
 *
 * @author wuya
 */
data class HiveReadSpec(
    val dataColumns: List<HiveColumn>,
    val projectedDataIndexes: List<Int>,
    val partitionColumns: List<HiveColumn>,
    val projectedPartitionIndexes: List<Int>,
    /** 表属性，`skip.header.line.count` 这类在这里。 */
    val tableParameters: Map<String, String> = emptyMap(),
) : Serializable {

    /** 被投影到的数据列，按输出顺序。 */
    val projectedDataColumns: List<HiveColumn> get() = projectedDataIndexes.map { dataColumns[it] }

    val projectedPartitionColumns: List<HiveColumn> get() = projectedPartitionIndexes.map { partitionColumns[it] }

    val outputSchema: Schema get() = HiveTypes.schemaOf(projectedDataColumns + projectedPartitionColumns)

    /** 文件里每一列的 Beam 类型，下标与 [dataColumns] 对齐。 */
    val dataFieldTypes: List<Schema.FieldType> get() = dataColumns.map { HiveTypes.parse(it.type) }

    /** 表属性 `skip.header.line.count`，CSV 外表常用。 */
    val headerLineCount: Int get() = tableParameters["skip.header.line.count"]?.toIntOrNull() ?: 0

    val footerLineCount: Int get() = tableParameters["skip.footer.line.count"]?.toIntOrNull() ?: 0

    companion object {
        private const val serialVersionUID: Long = 1

        /**
         * 按配置里的 `columns` 建投影。列名大小写不敏感（Hive 的列名一律存成小写）。
         *
         * @param columns 空表示读全部列
         */
        fun of(table: HiveTable, columns: List<String> = emptyList()): HiveReadSpec {
            if (columns.isEmpty()) {
                return HiveReadSpec(
                    dataColumns = table.dataColumns,
                    projectedDataIndexes = table.dataColumns.indices.toList(),
                    partitionColumns = table.partitionColumns,
                    projectedPartitionIndexes = table.partitionColumns.indices.toList(),
                    tableParameters = table.parameters,
                )
            }
            val wanted = columns.map { it.trim().lowercase() }
            val unknown = wanted.filterNot { name -> table.columns.any { it.name.equals(name, ignoreCase = true) } }
            require(unknown.isEmpty()) {
                "表 ${table.qualifiedName} 上没有这些列: $unknown；可选: ${table.columns.map { it.name }}"
            }
            return HiveReadSpec(
                dataColumns = table.dataColumns,
                projectedDataIndexes = table.dataColumns.indices.filter { table.dataColumns[it].name.lowercase() in wanted },
                partitionColumns = table.partitionColumns,
                projectedPartitionIndexes = table.partitionColumns.indices
                    .filter { table.partitionColumns[it].name.lowercase() in wanted },
                tableParameters = table.parameters,
            )
        }
    }
}

/** 认领一个偏移量。返回 false 表示这段已经不归本分片了，读取器要立刻停下来。 */
fun interface OffsetClaim {
    fun tryClaim(offset: Long): Boolean
}

/**
 * 按字节区间读一个 Hive 数据文件。
 *
 * 每种格式的"可认领边界"粒度不一样，但协议是统一的：**读一批之前先认领这一批的起始偏移量**。
 * ORC 是 stripe、Parquet 是 row group、Avro / SequenceFile / RCFile 是同步块、文本是行。
 * 这样运行时才能在读到一半时把剩下的活分给空闲 worker——
 * 一次性 `tryClaim(range.to - 1)` 等于告诉 Beam"这段不可再分"，是明确要避免的写法。
 */
abstract class HiveRecordReader(
    protected val spec: HiveReadSpec,
    /** 已经按分区列类型解析好的分区值，每行原样附在数据列后面。 */
    private val partitionValues: List<Any?>,
) : AutoCloseable {

    private val outputSchema: Schema = spec.outputSchema

    /**
     * 读完本区间。
     *
     * @return true 表示数据读完了；false 表示 [OffsetClaim.tryClaim] 拒绝了，
     *   调用方**不要**再补认领区间末尾，否则 `OffsetRangeTracker` 会因为
     *   "认领的偏移量小于上一次尝试"直接抛异常。
     */
    abstract fun read(claim: OffsetClaim, output: (Row) -> Unit): Boolean

    /** [values] 的长度是投影到的数据列个数。 */
    protected fun toRow(values: Array<Any?>): Row {
        val builder = Row.withSchema(outputSchema)
        values.forEach(builder::addValue)
        partitionValues.forEach(builder::addValue)
        return builder.build()
    }

    override fun close() {}
}

/**
 * 按存储格式挑读取器，对应 Trino 的 `HivePageSourceProvider` 挑 `HivePageSourceFactory`。
 */
object HiveRecordReaders {

    fun open(
        file: HiveFile,
        range: OffsetRange,
        spec: HiveReadSpec,
        configuration: Configuration,
    ): HiveRecordReader {
        val storage = file.partition.storage
        val format = HiveStorageFormat.of(storage.storageFormat)
        val serdeParameters = storage.serdeParameters
        val partitionValues = partitionValues(spec, file)
        return when (format) {
            HiveStorageFormat.TEXTFILE ->
                TextRecordReader(file, range, spec, partitionValues, serdeParameters, configuration)

            HiveStorageFormat.CSV ->
                CsvRecordReader(file, spec, partitionValues, serdeParameters, configuration)

            HiveStorageFormat.SEQUENCEFILE ->
                SequenceFileRecordReader(file, range, spec, partitionValues, serdeParameters, configuration)

            HiveStorageFormat.RCTEXT, HiveStorageFormat.RCBINARY ->
                RcFileRecordReader(file, range, spec, partitionValues, format, serdeParameters, configuration)

            HiveStorageFormat.ORC ->
                OrcRecordReader(file, range, spec, partitionValues, configuration)

            HiveStorageFormat.PARQUET ->
                ParquetRecordReader(file, range, spec, partitionValues, configuration)

            HiveStorageFormat.AVRO ->
                AvroRecordReader(file, range, spec, partitionValues, configuration)
        }
    }

    /**
     * 分区目录名里的字面量按分区列的类型还原成值。
     *
     * 分区值是**位置相关**的：`HivePartition.values` 与表的分区列一一对应，
     * 所以投影下标可以直接拿来取值。
     */
    fun partitionValues(spec: HiveReadSpec, file: HiveFile): List<Any?> =
        spec.projectedPartitionIndexes.map { index ->
            val literal = file.partition.values.getOrNull(index)
                ?: throw IllegalStateException(
                    "分区[${file.partition.name}] 的值个数(${file.partition.values.size})" +
                        "与表的分区列个数(${spec.partitionColumns.size})对不上"
                )
            HiveValues.fromPartitionLiteral(literal, HiveTypes.parse(spec.partitionColumns[index].type))
        }
}
