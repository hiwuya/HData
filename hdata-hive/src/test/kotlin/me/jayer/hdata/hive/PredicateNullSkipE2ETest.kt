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
 * End-to-end cases for **whole-block skipping by column statistics** of IS NULL / IS NOT NULL.
 *
 * The correctness of row-level filtering ([me.jayer.hdata.hive.format.PredicateEvaluator.matches]) is covered by
 * [me.jayer.hdata.hive.HivePipelineTest]; what this proves is that the earlier "statistics skipping" really triggers —
 * i.e. the `orcStripesSkipped` / `parquetRowGroupsSkipped` counters really go up instead of only the row-level filter covering it.
 *
 * For that this test bypasses HiveWriteProvider (which compresses by default and cannot tune stripe/row group sizes, making it hard
 * to produce several blocks reliably) and writes **uncompressed files with very small stripes / row groups** through the low-level
 * ORC/Parquet API, concentrating the nulls at the start and the end of the file so that some units have no NULL at all or are
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

    /** The name column is NULL for rows [nullFrom, nullTo] (inclusive) and non-NULL everywhere else. */
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
            else -> throw IllegalArgumentException("this test only covers ORC / Parquet: $format")
        }
        val pipeline = Pipeline.create()
        val configYaml = buildString {
            appendLine("""metastore_uri: "${hive.metastoreUri}"""")
            appendLine("table: $table")
            appendLine(predicateYaml.trimIndent())
        }
        val output: PCollection<Row> = HiveReadProvider().from(config(configYaml))
            .expand(PCollectionRowTuple.empty(pipeline)).get(Tags.MAIN_OUTPUT)
        // The row-level filter must still be correct (statistics skipping is only a performance optimization, it cannot change results)
        PAssert.that(output.apply("Count", Count.globally())).containsInAnyOrder(expected.toLong())
        val result = pipeline.run()
        val skipped = result.metrics().queryMetrics(MetricsFilter.builder().build())
            .counters.firstOrNull { it.name.name == counterName }?.attempted?.toInt() ?: 0
        if (expectSkipped) {
            assertTrue(skipped > 0, "statistics skipping did not trigger ($counterName), expected at least 1 stripe / row group to be skipped")
        } else {
            // This format cannot decide "the whole column is NULL" from its column statistics (no null count available), so it can only rely on row-level filtering and skipping is necessarily 0.
            assertTrue(skipped == 0, "this format should not trigger whole-block skipping ($counterName), yet it skipped $skipped")
        }
    }

    @Test
    fun `ORC IS NULL 跳过整段无空值的 stripe`() {
        // name is NULL only in the first 500 rows and non-NULL in the remaining 1500 -> the later stripes hold no NULL, IS NULL should skip
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
        // ORC's ColumnStatistics only exposes hasNull (whether NULLs exist) and no null count, so "the whole column is NULL" cannot be
        // proven and IS NOT NULL cannot skip whole blocks — only the row-level filter applies, and the result is still correct.
        // Parquet carries numNulls in its statistics and can skip whole blocks (see the Parquet case below).
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
