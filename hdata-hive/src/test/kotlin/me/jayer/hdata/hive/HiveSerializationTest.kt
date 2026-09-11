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
import org.apache.beam.sdk.io.range.OffsetRange
import kotlin.test.assertEquals
import kotlin.test.Test

/**
 * Every object shipped with the job must be serializable.
 *
 * This kind of problem **only blows up when the job is submitted**; a unit test that calls `@ProcessElement` on its own will
 * never find it: a DoFn that accidentally captured a Hadoop `Configuration`, a thrift client,
 * or a plain Kotlin lambda (`Function1`) works fine locally and then throws
 * `NotSerializableException` on a cluster. That is how ES 8's `Query` slipped in.
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
    fun `metadata models are serializable`() {
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
    fun `config classes are serializable`() {
        SerializableUtils.ensureSerializable(
            HiveReadConfig(metastoreUri = "thrift://localhost:9083", table = "t_order")
        )
        SerializableUtils.ensureSerializable(
            HiveWriteConfig(metastoreUri = "thrift://localhost:9083", table = "t_order")
        )
    }

    @Test
    fun `an unsplittable Hive file uses a single logical restriction`() {
        assertEquals(OffsetRange(0, 1), HiveReadFn.initialRestriction(1_000_000, false))
        assertEquals(OffsetRange(0, 1_000_000), HiveReadFn.initialRestriction(1_000_000, true))
    }

    @Test
    fun `read- and write-side DoFns are serializable`() {
        val spec = HiveReadSpec.of(table)
        SerializableUtils.ensureSerializableByCoder(
            org.apache.beam.sdk.coders.SerializableCoder.of(HiveReadFn::class.java),
            HiveReadFn(spec, mapOf("fs.defaultFS" to "hdfs://nn:8020"), 64L * 1024 * 1024),
            "HiveReadFn",
        )
        SerializableUtils.ensureSerializable(HiveListFilesFn(emptyMap(), false))
        // The aggregation pushdown DoFn / merge function: they are shipped with the job, and capturing something non-serializable
        // such as a Hadoop Configuration only blows up when the job is submitted.
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
    fun `all eight formats' file sinks are serializable`() {
        // A FileIO.Sink is shipped with the job; the ORC/Parquet ones are especially likely to capture non-serializable objects
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
    fun `the text codec is serializable`() {
        SerializableUtils.ensureSerializable(
            me.jayer.hdata.hive.format.LazySimpleCodec(mapOf("field.delim" to ","))
        )
    }

    @Test
    fun `the inferred schema has the right field types`() {
        val full = HiveTypes.schemaOf(table)
        // Partition columns come after the data columns
        kotlin.test.assertEquals(listOf("id", "name", "dt"), full.fieldNames)
        kotlin.test.assertEquals(FieldTypes.INT64, full.getField("id").type.withNullable(false))
    }
}
