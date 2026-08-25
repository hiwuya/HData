package me.jayer.hdata.hive

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.format.AggSpec
import me.jayer.hdata.hive.format.AggType
import me.jayer.hdata.hive.format.HiveAggregateFn
import me.jayer.hdata.hive.format.HiveFileSinks
import me.jayer.hdata.hive.format.HiveReadSpec
import me.jayer.hdata.hive.format.HiveStorageFormat
import me.jayer.hdata.hive.format.mergeAggregatePartials
import me.jayer.hdata.hive.metastore.HiveColumn
import me.jayer.hdata.hive.metastore.HiveMetastoreSpec
import me.jayer.hdata.hive.metastore.HiveTable
import me.jayer.hdata.hive.metastore.Storage
import me.jayer.hdata.hive.split.HiveFile
import me.jayer.hdata.hive.split.HivePartitionSpec
import me.jayer.hdata.hive.transform.HiveCommitPartitionFn
import me.jayer.hdata.hive.transform.HiveListFilesFn
import me.jayer.hdata.hive.transform.HiveReadFn
import me.jayer.hdata.hive.transform.HiveRowToRecordFn
import me.jayer.hdata.hive.type.HiveTypes
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.TupleTag
import kotlin.test.Test

/**
 * 所有随作业下发的对象都必须可序列化。
 *
 * 这类问题**只在提交作业时**才炸，单独调 `@ProcessElement` 的单测永远发现不了：
 * DoFn 里不小心捕获了一个 Hadoop `Configuration`、一个 thrift 客户端、
 * 或者一个普通的 Kotlin lambda（`Function1`），本地跑得好好的，一上集群就
 * `NotSerializableException`。ES 8 的 `Query` 就是这么混进去的。
 *
 * @author wuya
 */
class HiveSerializationTest {

    private val dataColumns = listOf(HiveColumn("id", "bigint"), HiveColumn("name", "string"))
    private val partitionColumns = listOf(HiveColumn("dt", "string"))

    private val storage = Storage(
        storageFormat = HiveStorageFormat.ORC.toStorageFormat(),
        location = "hdfs://nn:8020/user/hive/warehouse/t",
        serdeParameters = mapOf("field.delim" to ","),
    )

    private val table = HiveTable(
        databaseName = "default",
        tableName = "t_order",
        tableType = HiveTable.MANAGED_TABLE,
        dataColumns = dataColumns,
        partitionColumns = partitionColumns,
        storage = storage,
    )

    private val schema: Schema = HiveTypes.schemaOf(dataColumns)

    @Test
    fun `元数据模型可序列化`() {
        SerializableUtils.ensureSerializable(table)
        SerializableUtils.ensureSerializable(storage)
        SerializableUtils.ensureSerializable(HivePartitionSpec("dt=2024-01-01", listOf("2024-01-01"), storage))
        SerializableUtils.ensureSerializable(
            HiveFile("hdfs://nn:8020/x/part-0", 100L, HivePartitionSpec.unpartitioned(storage))
        )
        SerializableUtils.ensureSerializable(HiveMetastoreSpec("thrift://localhost:9083"))
        SerializableUtils.ensureSerializable(HiveReadSpec.of(table))
    }

    @Test
    fun `配置类可序列化`() {
        SerializableUtils.ensureSerializable(
            HiveReadConfig(metastoreUri = "thrift://localhost:9083", table = "t_order")
        )
        SerializableUtils.ensureSerializable(
            HiveWriteConfig(metastoreUri = "thrift://localhost:9083", table = "t_order")
        )
    }

    @Test
    fun `读写两端的 DoFn 可序列化`() {
        val spec = HiveReadSpec.of(table)
        SerializableUtils.ensureSerializableByCoder(
            org.apache.beam.sdk.coders.SerializableCoder.of(HiveReadFn::class.java),
            HiveReadFn(spec, mapOf("fs.defaultFS" to "hdfs://nn:8020"), 64L * 1024 * 1024),
            "HiveReadFn",
        )
        SerializableUtils.ensureSerializable(HiveListFilesFn(emptyMap(), false))
        // 聚合下推的 DoFn / 归并函数：它们随作业下发，捕获了 Hadoop Configuration 这类不可序列化对象会
        // 只在提交作业时才炸。
        SerializableUtils.ensureSerializable(
            HiveAggregateFn(
                listOf(AggSpec(AggType.COUNT, null, FieldTypes.INT64, "count"), AggSpec(AggType.MIN, "id", FieldTypes.INT64, "min_id")),
                emptyMap(),
            )
        )
        val viewPipeline = org.apache.beam.sdk.Pipeline.create()
        val dummySchema = org.apache.beam.sdk.schemas.Schema.builder()
            .addField("c", me.jayer.hdata.core.type.FieldTypes.INT64)
            .build()
        val aggView = viewPipeline
            .apply(
                org.apache.beam.sdk.transforms.Create.of(
                    listOf(
                        org.apache.beam.sdk.values.Row.withSchema(dummySchema).addValue(0L).build(),
                    ),
                ),
            )
            .apply(org.apache.beam.sdk.transforms.View.asList())
        SerializableUtils.ensureSerializable(
            me.jayer.hdata.hive.HiveMergeFn(
                listOf(AggSpec(AggType.COUNT, null, FieldTypes.INT64, "count"), AggSpec(AggType.MIN, "id", FieldTypes.INT64, "min_id")),
                aggView,
            )
        )
        SerializableUtils.ensureSerializable(
            HiveRowToRecordFn(
                schema,
                partitionColumns,
                ErrorSchemas.of(schema),
                true,
                "WriteToHive",
                TupleTag<Row>(),
            )
        )
        SerializableUtils.ensureSerializable(
            HiveCommitPartitionFn(
                HiveMetastoreSpec("thrift://localhost:9083"),
                "default",
                "t_order",
                listOf("dt"),
                storage,
                HiveWriteMode.OVERWRITE,
                true,
                mapOf("fs.defaultFS" to "hdfs://nn:8020"),
            )
        )
    }

    @Test
    fun `八种格式的写入器都可序列化`() {
        // FileIO.Sink 会随作业下发，写 ORC/Parquet 的那几个尤其容易捕获到不可序列化的对象
        HiveStorageFormat.entries.forEach { format ->
            SerializableUtils.ensureSerializable(
                HiveFileSinks.of(
                    format = format,
                    columns = dataColumns,
                    schema = schema,
                    serdeParameters = mapOf("field.delim" to ","),
                    configuration = mapOf("fs.defaultFS" to "hdfs://nn:8020"),
                )
            )
        }
    }

    @Test
    fun `文本编解码器可序列化`() {
        SerializableUtils.ensureSerializable(
            me.jayer.hdata.hive.format.LazySimpleCodec(mapOf("field.delim" to ","))
        )
    }

    @Test
    fun `schema 推断出来的字段类型正确`() {
        val full = HiveTypes.schemaOf(table)
        // 分区列排在数据列之后
        kotlin.test.assertEquals(listOf("id", "name", "dt"), full.fieldNames)
        kotlin.test.assertEquals(FieldTypes.INT64, full.getField("id").type.withNullable(false))
    }
}
