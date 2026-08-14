package me.jayer.hdata.hive

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spi.RowSink
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.spi.TypedTransformProvider
import me.jayer.hdata.hive.format.HiveFileSinks
import me.jayer.hdata.hive.format.HiveStorageFormat
import me.jayer.hdata.hive.metastore.HiveMetastores
import me.jayer.hdata.hive.metastore.HiveTable
import me.jayer.hdata.hive.split.HivePaths
import me.jayer.hdata.hive.transform.HiveCommitPartitionFn
import me.jayer.hdata.hive.transform.HiveRowToRecordFn
import me.jayer.hdata.hive.type.HiveTypes
import org.apache.beam.sdk.coders.KvCoder
import org.apache.beam.sdk.coders.StringUtf8Coder
import org.apache.beam.sdk.io.FileIO
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.schemas.SchemaCoder
import org.apache.beam.sdk.transforms.Contextful
import org.apache.beam.sdk.transforms.GroupByKey
import org.apache.beam.sdk.transforms.PTransform
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.KV
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.PCollectionTuple
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.TupleTag
import org.apache.beam.sdk.values.TupleTagList
import org.slf4j.LoggerFactory

/**
 * `WriteToHive`：按表的存储格式直接往表目录里写文件，写完把新分区注册进 metastore。
 *
 * 形状对齐 Trino 的写入路径（`HivePageSink` 写文件 + `finishInsert` 提交元数据）：
 *
 * ```
 * 输入 Row
 *   -> ToRecord(DoFn)          -> KV<分区名, 只含数据列的行>，失败行进死信
 *   -> FileIO.writeDynamic()   -> 按分区名分目录写，各分片先写临时文件、全部成功后原子改名
 *   -> GroupByKey + Commit     -> 覆盖模式清旧文件、新分区注册进 metastore
 * ```
 *
 * 分区是**动态**的：每行按自己的分区列取值决定落到哪个分区，一次作业写出任意多个分区，
 * 与 Hive 的动态分区插入一致。`write_mode` 决定已有数据怎么处理（见 [HiveWriteMode]）。
 *
 * 用 `FileIO.writeDynamic()` 而不是自己在 DoFn 里开文件，是因为后者在作业重试时会把
 * 已经写好的结果截断——Beam 不保证 `@Setup`/`@Teardown` 每个 worker 只走一次。
 *
 * 重构前这里走的是 JDBC `INSERT INTO`：每批一次网络往返，HiveServer2 那边还要为每条
 * INSERT 起一个作业，同时也没法控制落盘的文件格式。
 *
 * @author wuya
 */
class HiveWriteProvider : TypedTransformProvider<HiveWriteConfig>(HiveWriteConfig::class.java) {

    override fun identifier(): String = "WriteToHive"

    override fun description(): String = "按表的存储格式直接写表目录下的文件，并把新分区注册进 metastore"

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

    override fun write(input: PCollection<Row>): PCollection<Row>? =
        HiveMetastores.withMetastore(config.metastoreSpec()) { metastore ->
            val table = requireNotNull(metastore.getTable(config.database, config.table)) {
                "表不存在: ${config.qualifiedTable}。WriteToHive 不建表，请先在 Hive 里建好"
            }
            checkWritable(table)
            val format = HiveStorageFormat.of(table.storage.storageFormat)
            // 数据文件里只有数据列，分区列的值编码在目录名上
            val fileSchema = HiveTypes.schemaOf(table.dataColumns)
            val errorSchema = ErrorSchemas.of(input.schema)
            LOGGER.info(
                "WriteToHive 表[{}] 格式={} 目录={} 分区列={}",
                table.qualifiedName,
                format,
                table.storage.location,
                table.partitionColumns.map { it.name },
            )

            val errorTag = TupleTag<Row>()
            val mainTag = object : TupleTag<KV<String, Row>>() {}
            val outputs: PCollectionTuple = input.apply(
                "ToRecord",
                ParDo.of(
                    HiveRowToRecordFn(
                        fileSchema,
                        table.partitionColumns,
                        errorSchema,
                        deadLetter,
                        transformName,
                        errorTag,
                    )
                ).withOutputTags(mainTag, TupleTagList.of(errorTag)),
            )

            val records = outputs.get(mainTag)
                .setCoder(KvCoder.of(StringUtf8Coder.of(), SchemaCoder.of(fileSchema)))
            writeFiles(records, table, format, fileSchema)

            val errors = outputs.get(errorTag).setRowSchema(errorSchema)
            if (deadLetter) errors else null
        }

    private fun writeFiles(
        records: PCollection<KV<String, Row>>,
        table: HiveTable,
        format: HiveStorageFormat,
        fileSchema: Schema,
    ) {
        val sink = HiveFileSinks.of(
            format = format,
            columns = table.dataColumns,
            schema = fileSchema,
            serdeParameters = table.storage.serdeParameters,
            configuration = config.hadoopConf,
        )
        val extension = format.fileExtension
        // Beam 的 defaultNaming 只按 "前缀-分片号-of-总数" 命名，同一张表跑两次会生成**一模一样**的文件名，
        // 第二次直接把第一次的结果盖掉——既不是追加也不是覆盖，是悄悄丢数据。
        // 每次作业带一个唯一标记，追加写入才真的是追加。
        val prefix = "${config.filePrefix}-${runToken()}"
        var write = FileIO.writeDynamic<String, KV<String, Row>>()
            .by { it.key }
            .withDestinationCoder(StringUtf8Coder.of())
            .via(Contextful.fn<KV<String, Row>, Row> { element -> element.value }, sink)
            .to(HivePaths.forBeamIO(table.storage.location))
            // 分区名本身就是相对表目录的子路径 dt=2024-01-01/hr=01，直接拼进文件名
            .withNaming { destination ->
                FileIO.Write.defaultNaming(
                    if (destination.isEmpty()) prefix else "$destination/$prefix",
                    extension,
                )
            }
        if (config.numShards > 0) {
            write = write.withNumShards(config.numShards)
        }

        val result = records.apply("WriteFiles", write)
        val mode = config.mode()
        if (mode == HiveWriteMode.APPEND && (!config.createPartitions || !table.partitioned)) {
            // 追加写入 + 不用注册分区，落盘完就没有别的事了
            return
        }
        result.perDestinationOutputFilenames
            // 按分区聚起来：提交这一步要知道"这个分区这次写出了哪些文件"，
            // 覆盖模式才能把不在这个集合里的旧文件删掉
            .apply("GroupByPartition", GroupByKey.create())
            .apply(
                "CommitPartitions",
                ParDo.of(
                    HiveCommitPartitionFn(
                        config.metastoreSpec(),
                        config.database,
                        config.table,
                        table.partitionColumns.map { it.name },
                        table.storage,
                        mode,
                        config.createPartitions,
                        config.hadoopConf,
                    )
                ),
            )
    }

    /** 一次作业一个标记，构图阶段生成后随 transform 固定下来，作业重试时不会变。 */
    private fun runToken(): String = java.util.UUID.randomUUID().toString().replace("-", "").take(12)

    private fun checkWritable(table: HiveTable) {
        require(table.tableType != HiveTable.VIRTUAL_VIEW) {
            "${table.qualifiedName} 是视图，不能写入"
        }
        require(table.parameters["transactional"]?.toBoolean() != true) {
            "${table.qualifiedName} 是事务表(ACID)，暂不支持写入：ACID 表要写 delta 目录并维护写事务号"
        }
        require(table.storage.location.isNotBlank()) {
            "${table.qualifiedName} 在 metastore 上没有 location，无法确定往哪里写"
        }
    }

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

private val LOGGER = LoggerFactory.getLogger(HiveWriteProvider::class.java)
