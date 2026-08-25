package me.jayer.hdata.hive

import java.math.BigDecimal
import java.nio.file.Path as NioPath
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.format.HiveStorageFormat
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.metrics.MetricsFilter
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path as HadoopPath
import org.apache.orc.CompressionKind
import org.apache.orc.OrcFile
import org.apache.orc.TypeDescription
import org.apache.orc.Writer
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.Types
import kotlin.test.Test
import kotlin.test.assertTrue
import tools.jackson.databind.node.ObjectNode

/**
 * IS NULL / IS NOT NULL 的**列统计整段跳过**端到端用例。
 *
 * 行级过滤（[me.jayer.hdata.hive.format.PredicateEvaluator.matches]）的正确性由
 * [me.jayer.hdata.hive.HivePipelineTest] 覆盖；这里要证明的是更靠前的"统计跳过"真的会触发——
 * 即 `orcStripesSkipped` / `parquetRowGroupsSkipped` 计数器真的涨上去，而不是只靠行级过滤兜底。
 *
 * 为此本测试绕开 HiveWriteProvider（它默认压缩、又不可调 stripe/row group 大小，难以稳定产出多段），
 * 直接用 ORC/Parquet 低级 API 写出**未压缩 + 极小 stripe/row group** 的文件，并把空值集中放在
 * 文件首尾，确保有一部分单元整段无 NULL 或整段全 NULL，从而确定性地触发跳过。
 *
 * @author wuya
 */
class PredicateNullSkipE2ETest {

    private val schema: Schema = Schema.builder()
        .addNullableField("id", FieldTypes.INT64)
        .addNullableField("name", FieldTypes.STRING)
        .addNullableField("amount", FieldTypes.DECIMAL)
        .build()

    private val dataColumns = listOf("id" to "bigint", "name" to "string", "amount" to "decimal(10,2)")
    private val orcTypeStr = "struct<id:bigint,name:string,amount:decimal(10,2)>"

    private fun config(yaml: String): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    /** name 列在 [nullFrom, nullTo]（含）行为 NULL，其余为非 NULL。 */
    private fun rowsWithNullName(count: Int, nullFrom: Int, nullTo: Int): List<Row> = (0 until count).map { i ->
        Row.withSchema(schema).apply {
            addValue(i.toLong())
            addValue(if (i in nullFrom..nullTo) null else "name-$i")
            addValue(BigDecimal("$i.50"))
        }.build()
    }

    private fun writeOrc(file: NioPath, rows: List<Row>) {
        val conf = Configuration()
        val type = TypeDescription.fromString(orcTypeStr)
        val opts = OrcFile.writerOptions(conf).setSchema(type).useUTCTimestamp(true)
            .compress(CompressionKind.NONE).stripeSize(4096)
        val writer: Writer = OrcFile.createWriter(HadoopPath(file.toString()), opts)
        val batch = type.createRowBatch()
        val idVec = batch.cols[0] as org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
        val nameVec = batch.cols[1] as org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
        val amtVec = batch.cols[2] as org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector
        for (r in rows) {
            val i = batch.size
            idVec.vector[i] = r.getValue<Long>("id")
            val name = r.getValue<Any?>("name")
            if (name == null) {
                nameVec.noNulls = false
                nameVec.isNull[i] = true
            } else {
                nameVec.isNull[i] = false
                nameVec.setVal(i, (name as String).toByteArray())
            }
            amtVec.set(i, org.apache.hadoop.hive.common.type.HiveDecimal.create(r.getValue<BigDecimal>("amount")))
            batch.size++
            if (batch.size == batch.maxSize) {
                writer.addRowBatch(batch)
                batch.reset()
            }
        }
        if (batch.size > 0) writer.addRowBatch(batch)
        writer.close()
    }

    private fun writeParquet(file: NioPath, rows: List<Row>) {
        val parquetSchema: MessageType = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64).named("id")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.stringType()).named("name")
            .required(PrimitiveType.PrimitiveTypeName.INT64).`as`(LogicalTypeAnnotation.decimalType(2, 10)).named("amount")
            .named("hive_table")
        val conf = Configuration()
        GroupWriteSupport.setSchema(parquetSchema, conf)
        val writer = ExampleParquetWriter.builder(HadoopPath(file.toString()))
            .withType(parquetSchema)
            .withConf(conf)
            .withRowGroupSize(4096L)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
        for (r in rows) {
            val g = SimpleGroup(parquetSchema)
            g.add("id", r.getValue<Long>("id"))
            val name = r.getValue<Any?>("name")
            if (name != null) g.add("name", name as String)
            val unscaled = r.getValue<BigDecimal>("amount")!!.multiply(BigDecimal(100)).toLong()
            g.add("amount", unscaled)
            writer.write(g)
        }
        writer.close()
    }

    private fun runAndAssertSkip(
        hive: TestHive,
        table: String,
        format: HiveStorageFormat,
        rows: List<Row>,
        predicateYaml: String,
        expected: Int,
        counterName: String,
        expectSkipped: Boolean = true,
    ) {
        hive.createTable(table, format, dataColumns)
        val file = hive.warehouse.resolve("default.db").resolve(table).resolve("data.${format.name.lowercase()}")
        when (format) {
            HiveStorageFormat.ORC -> writeOrc(file, rows)
            HiveStorageFormat.PARQUET -> writeParquet(file, rows)
            else -> throw IllegalArgumentException("本测试只覆盖 ORC / Parquet：$format")
        }
        val pipeline = Pipeline.create()
        val configYaml = buildString {
            appendLine("""metastore_uri: "${hive.metastoreUri}"""")
            appendLine("table: $table")
            appendLine(predicateYaml.trimIndent())
        }
        val output: PCollection<Row> = HiveReadProvider().from(config(configYaml))
            .expand(PCollectionRowTuple.empty(pipeline)).get(Tags.MAIN_OUTPUT)
        // 行级过滤的结果必须正确（统计跳过只是性能优化，不能改变结果）
        PAssert.that(output.apply("Count", Count.globally())).containsInAnyOrder(expected.toLong())
        val result = pipeline.run()
        val skipped = result.metrics().queryMetrics(MetricsFilter.builder().build())
            .counters.firstOrNull { it.name.name == counterName }?.attempted?.toInt() ?: 0
        if (expectSkipped) {
            assertTrue(skipped > 0, "统计跳过未触发（$counterName），预期至少有 1 个 stripe / row group 被跳过")
        } else {
            // 该格式无法从列统计判定"整列全 NULL"（拿不到 null 计数），只能靠行级过滤，跳过必然为 0。
            assertTrue(skipped == 0, "该格式不应触发整段跳过（$counterName），却跳过了 $skipped 个")
        }
    }

    @Test
    fun `ORC IS NULL 跳过整段无空值的 stripe`() {
        // name 仅前 500 行为 NULL，其余 1500 行非 NULL → 后半段 stripe 无 NULL，IS NULL 应跳过
        TestHive().use { hive ->
            runAndAssertSkip(
                hive,
                "t_isnull_orc",
                HiveStorageFormat.ORC,
                rowsWithNullName(20000, 0, 499),
                "predicates:\n  - column: name\n    op: \"is null\"",
                500,
                "orcStripesSkipped",
            )
        }
    }

    @Test
    fun `Parquet IS NULL 跳过整段无空值的 row group`() {
        TestHive().use { hive ->
            runAndAssertSkip(
                hive,
                "t_isnull_parquet",
                HiveStorageFormat.PARQUET,
                rowsWithNullName(20000, 0, 499),
                "predicates:\n  - column: name\n    op: \"is null\"",
                500,
                "parquetRowGroupsSkipped",
            )
        }
    }

    @Test
    fun `ORC IS NOT NULL 只走行级过滤（ORC 列统计拿不到 null 计数，无法判定整段全空）`() {
        // ORC 的 ColumnStatistics 只暴露 hasNull（是否有 NULL），没有 null 计数，
        // 因此无法证明"整列全 NULL"，IS NOT NULL 不能整段跳过——只靠行级过滤，结果仍正确。
        // Parquet 因为统计里有 numNulls，能整段跳过（见下方 Parquet 用例）。
        TestHive().use { hive ->
            runAndAssertSkip(
                hive,
                "t_isnotnull_orc",
                HiveStorageFormat.ORC,
                rowsWithNullName(20000, 1500, 1999),
                "predicates:\n  - column: name\n    op: \"is not null\"",
                19500,
                "orcStripesSkipped",
                expectSkipped = false,
            )
        }
    }

    @Test
    fun `Parquet IS NOT NULL 跳过整段全空值的 row group`() {
        TestHive().use { hive ->
            runAndAssertSkip(
                hive,
                "t_isnotnull_parquet",
                HiveStorageFormat.PARQUET,
                rowsWithNullName(20000, 1500, 1999),
                "predicates:\n  - column: name\n    op: \"is not null\"",
                19500,
                "parquetRowGroupsSkipped",
            )
        }
    }
}
