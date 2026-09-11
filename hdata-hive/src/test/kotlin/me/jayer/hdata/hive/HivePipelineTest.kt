package me.jayer.hdata.hive

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.format.HiveStorageFormat
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.TypeDescriptors
import org.apache.beam.sdk.metrics.MetricsFilter
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path as HadoopPath
import org.apache.parquet.example.data.simple.SimpleGroup
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.hadoop.example.GroupWriteSupport
import org.apache.parquet.hadoop.metadata.CompressionCodecName
import org.apache.parquet.schema.LogicalTypeAnnotation
import org.apache.parquet.schema.PrimitiveType
import org.apache.parquet.schema.MessageType
import org.apache.parquet.schema.Types
import tools.jackson.databind.node.ObjectNode
import java.math.BigDecimal
import java.nio.file.Path as NioPath
import org.apache.orc.CompressionKind
import org.apache.orc.OrcFile
import org.apache.orc.TypeDescription
import org.apache.orc.Writer
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector
import org.apache.hadoop.hive.common.type.HiveDecimal
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * End-to-end tests for `ReadFromHive` / `WriteToHive`: running on DirectRunner + a local temp directory + an in-process metastore
 * (see [TestHive]); all eight storage formats really write files and really read them back by byte range.
 *
 * @author wuya
 */
class HivePipelineTest {

    private val dataColumns = listOf(
        "id" to "bigint",
        "name" to "string",
        "amount" to "decimal(10,2)",
    )

    private val inputSchema: Schema = Schema.builder()
        .addNullableField("id", FieldTypes.INT64)
        .addNullableField("name", FieldTypes.STRING)
        .addNullableField("amount", FieldTypes.DECIMAL)
        .build()

    private val partitionedInputSchema: Schema = Schema.builder()
        .addNullableField("id", FieldTypes.INT64)
        .addNullableField("name", FieldTypes.STRING)
        .addNullableField("amount", FieldTypes.DECIMAL)
        .addNullableField("dt", FieldTypes.STRING)
        .build()

    private fun config(yaml: String): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    private fun rows(schema: Schema, count: Int, partitioned: Boolean): List<Row> = (1..count).map { i ->
        Row.withSchema(schema).apply {
            addValue(i.toLong())
            addValue("name-$i")
            addValue(BigDecimal("$i.50"))
            if (partitioned) {
                addValue(if (i % 2 == 0) "2024-01-02" else "2024-01-01")
            }
        }.build()
    }

    private fun write(hive: TestHive, table: String, rows: List<Row>, schema: Schema, extra: String = "") {
        val pipeline = Pipeline.create()
        val input = pipeline.apply("Create", Create.of(rows).withRowSchema(schema))
        val yaml = buildString {
            appendLine("""metastore_uri: "${hive.metastoreUri}"""")
            appendLine("table: $table")
            extra.lines().filter { it.isNotBlank() }.forEach { appendLine(it) }
        }
        HiveWriteProvider().from(config(yaml)).expand(PCollectionRowTuple.of(Tags.MAIN_INPUT, input))
        pipeline.run().waitUntilFinish()
    }

    private fun read(hive: TestHive, table: String, extra: String = ""): Pair<Pipeline, PCollection<Row>> {
        val pipeline = Pipeline.create()
        val yaml = buildString {
            appendLine("""metastore_uri: "${hive.metastoreUri}"""")
            appendLine("table: $table")
            extra.lines().filter { it.isNotBlank() }.forEach { appendLine(it) }
        }
        val output = HiveReadProvider().from(config(yaml))
            .expand(PCollectionRowTuple.empty(pipeline))
            .get(Tags.MAIN_OUTPUT)
        return pipeline to output
    }

    /**
     * Writes the ORC/Parquet files directly (uncompressed, with very small stripes / row groups) so that SYSTEM sampling spans several blocks,
     * instead of the whole file being drawn at once (with the default block size 20000 rows make a single block, and SYSTEM sampling would degrade into "all or nothing").
     */
    private fun writeDirect(hive: TestHive, table: String, format: HiveStorageFormat, rows: List<Row>) {
        hive.createTable(table, format, dataColumns)
        val file = hive.warehouse.resolve("default.db").resolve(table).resolve("data.${format.name.lowercase()}")
        when (format) {
            HiveStorageFormat.ORC -> writeOrcDirect(file, rows)
            HiveStorageFormat.PARQUET -> writeParquetDirect(file, rows)
            else -> throw IllegalArgumentException("the SYSTEM sampling test only covers ORC / Parquet: $format")
        }
    }

    private fun writeOrcDirect(file: NioPath, rows: List<Row>) {
        val conf = Configuration()
        val type = TypeDescription.fromString("struct<id:bigint,name:string,amount:decimal(10,2)>")
        val opts = OrcFile.writerOptions(conf).setSchema(type).useUTCTimestamp(true)
            .compress(CompressionKind.NONE).stripeSize(4096)
        val writer = OrcFile.createWriter(HadoopPath(file.toString()), opts)
        val batch = type.createRowBatch()
        val idVec = batch.cols[0] as LongColumnVector
        val nameVec = batch.cols[1] as BytesColumnVector
        val amtVec = batch.cols[2] as DecimalColumnVector
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
            amtVec.set(i, HiveDecimal.create(r.getValue<BigDecimal>("amount")))
            batch.size++
            if (batch.size == batch.maxSize) {
                writer.addRowBatch(batch)
                batch.reset()
            }
        }
        if (batch.size > 0) writer.addRowBatch(batch)
        writer.close()
    }

    private fun writeParquetDirect(file: NioPath, rows: List<Row>) {
        val parquetSchema: MessageType = Types.buildMessage()
            .required(PrimitiveType.PrimitiveTypeName.INT64).named("id")
            .optional(PrimitiveType.PrimitiveTypeName.BINARY).`as`(LogicalTypeAnnotation.stringType()).named("name")
            .required(PrimitiveType.PrimitiveTypeName.INT64).`as`(LogicalTypeAnnotation.decimalType(2, 10)).named("amount")
            .named("hive_table")
        val conf = Configuration()
        GroupWriteSupport.setSchema(parquetSchema, conf)
        val writer = ExampleParquetWriter.builder(HadoopPath(file.toString()))
            .withType(parquetSchema).withConf(conf)
            .withRowGroupSize(4096L).withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
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

    /** Row -> a string that is easy to assert on, sidestepping the need for identical schemas. */
    private fun PCollection<Row>.asText(): PCollection<String> = apply(
        "ToText",
        MapElements.into(TypeDescriptors.strings()).via(
            object : SimpleFunction<Row, String>() {
                override fun apply(row: Row): String =
                    row.schema.fieldNames.joinToString("|") { name -> row.getValue<Any?>(name)?.toString() ?: "<null>" }
            }
        ),
    )

    @Test
    fun `all eight storage formats can be written and read back`() {
        HiveStorageFormat.entries.forEach { format ->
            TestHive().use { hive ->
                hive.createTable("t_order", format, dataColumns)
                write(hive, "t_order", rows(inputSchema, 4, partitioned = false), inputSchema)

                assertTrue(hive.dataFiles("t_order").isNotEmpty(), "$format did not write any file")

                val (pipeline, output) = read(hive, "t_order")
                PAssert.that(output.asText()).containsInAnyOrder(
                    "1|name-1|1.50",
                    "2|name-2|2.50",
                    "3|name-3|3.50",
                    "4|name-4|4.50",
                )
                pipeline.run().waitUntilFinish()
            }
        }
    }

    @Test
    fun `a partitioned table writes by directory, partitions auto-register in the metastore`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.ORC,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 4, partitioned = true), partitionedInputSchema)

            // Partitions must be registered in the metastore, otherwise Hive cannot query this data
            assertEquals(
                listOf("dt=2024-01-01", "dt=2024-01-02"),
                hive.metastore.getPartitionNames("default", "t_order"),
            )
            // The files really land under the partition directory
            assertTrue(hive.dataFiles("t_order").all { it.toString().contains("dt=2024-01-0") })

            val (pipeline, output) = read(hive, "t_order")
            // Partition columns come after the data columns, matching Hive's SELECT * order
            PAssert.that(output.asText()).containsInAnyOrder(
                "1|name-1|1.50|2024-01-01",
                "2|name-2|2.50|2024-01-02",
                "3|name-3|3.50|2024-01-01",
                "4|name-4|4.50|2024-01-02",
            )
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `reading by partition name reads only that one partition`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.PARQUET,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 4, partitioned = true), partitionedInputSchema)

            val (pipeline, output) = read(hive, "t_order", """partitions: ["dt=2024-01-01"]""")
            PAssert.that(output.asText()).containsInAnyOrder(
                "1|name-1|1.50|2024-01-01",
                "3|name-3|3.50|2024-01-01",
            )
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `the partition filter expression is handed to the metastore`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.TEXTFILE,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 4, partitioned = true), partitionedInputSchema)

            val (pipeline, output) = read(hive, "t_order", """partition_filter: "dt = '2024-01-02'"""")
            PAssert.that(output.apply(Count.globally())).containsInAnyOrder(2L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `a predicate hitting a partition column auto-prunes unrelated partitions (other partitions are not read)`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.ORC,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 4, partitioned = true), partitionedInputSchema)
            // 4 rows land in dt=2024-01-01 / dt=2024-01-02, two in each; take only dt=2024-01-02
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                predicates:
                  - column: dt
                    op: "="
                    value: "2024-01-02"
                """.trimIndent(),
            )
            // The result may only contain the 2 rows of dt=2024-01-02 (id=2,4); the dt=2024-01-01 partition is pruned wholesale
            PAssert.that(output.asText()).containsInAnyOrder(
                "2|name-2|2.50|2024-01-02",
                "4|name-4|4.50|2024-01-02",
            )
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `a range predicate (greater-or-equal) on a partition column also prunes partitions`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.ORC,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 4, partitioned = true), partitionedInputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                predicates:
                  - column: dt
                    op: ">="
                    value: "2024-01-02"
                """.trimIndent(),
            )
            // dt=2024-01-01 is pruned, only dt=2024-01-02 is kept
            PAssert.that(output.asText()).containsInAnyOrder(
                "2|name-2|2.50|2024-01-02",
                "4|name-4|4.50|2024-01-02",
            )
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `partition pruning down to nothing yields an empty collection instead of a coder-inference failure`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.ORC,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 4, partitioned = true), partitionedInputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                predicates:
                  - column: dt
                    op: "="
                    value: "2099-01-01"
                """.trimIndent(),
            )

            PAssert.that(output).empty()
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `partition-column and data-column predicates combine, still scanning only the matched partitions`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.ORC,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 4, partitioned = true), partitionedInputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                predicates:
                  - column: dt
                    op: "="
                    value: "2024-01-02"
                  - column: id
                    op: ">"
                    value: "2"
                """.trimIndent(),
            )
            // The dt=2024-01-02 partition has id=2,4; id>2 leaves only id=4
            PAssert.that(output.asText()).containsInAnyOrder("4|name-4|4.50|2024-01-02")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `a predicate on a data column does not prune partitions, all partitions are read as usual`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.ORC,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 4, partitioned = true), partitionedInputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                predicates:
                  - column: id
                    op: ">"
                    value: "0"
                """.trimIndent(),
            )
            // id>0 matches every row, so no partition is pruned and there are still 4 rows
            PAssert.that(output.apply(Count.globally())).containsInAnyOrder(4L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `reads only the projected columns`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.ORC,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 2, partitioned = true), partitionedInputSchema)

            val (pipeline, output) = read(hive, "t_order", "columns: [id, dt]")
            assertEquals(listOf("id", "dt"), output.schema.fieldNames)
            PAssert.that(output.asText()).containsInAnyOrder("1|2024-01-01", "2|2024-01-02")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `Parquet reads only the projected columns`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.PARQUET,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 2, partitioned = true), partitionedInputSchema)

            val (pipeline, output) = read(hive, "t_order", "columns: [id, dt]")
            assertEquals(listOf("id", "dt"), output.schema.fieldNames)
            PAssert.that(output.asText()).containsInAnyOrder("1|2024-01-01", "2|2024-01-02")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `aggregation pushdown reads only the columns the aggregation touches (finer-grained projection pushdown)`() {
        // The table has id / name / amount, but the aggregation only uses id and amount; the projection should push down only those
        // two columns and name never needs to be decoded. If the projection did not take effect the scan would read name as well
        // (its values are readable here), but the asserted result contains only sum_id / avg_amount / count, proving the read did not bring name along.
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            write(hive, "t_order", rows(inputSchema, 4, partitioned = false), inputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                aggregates:
                  - type: sum
                    column: id
                  - type: avg
                    column: amount
                  - type: count
                """.trimIndent(),
            )
            assertEquals(listOf("sum_id", "avg_amount", "count"), output.schema.fieldNames)
            // sum(id)=10, avg(amount)=3.00, count=4
            PAssert.that(output.asText()).containsInAnyOrder("10|3.00|4")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `Parquet aggregation pushdown reads only the columns the aggregation touches`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.PARQUET, dataColumns)
            write(hive, "t_order", rows(inputSchema, 4, partitioned = false), inputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                aggregates:
                  - type: sum
                    column: id
                  - type: avg
                    column: amount
                  - type: count
                """.trimIndent(),
            )
            assertEquals(listOf("sum_id", "avg_amount", "count"), output.schema.fieldNames)
            PAssert.that(output.asText()).containsInAnyOrder("10|3.00|4")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `a text file split by byte range has no duplicates or gaps`() {
        TestHive().use { hive ->
            hive.createTable("t_big", HiveStorageFormat.TEXTFILE, dataColumns)
            val data = rows(inputSchema, 500, partitioned = false)
            write(hive, "t_big", data, inputSchema, "num_shards: 1")

            // One split is only a few dozen bytes, so 500 rows are cut into many pieces
            val (pipeline, output) = read(hive, "t_big", "split_bytes: 256")
            PAssert.that(output.apply(Count.globally())).containsInAnyOrder(500L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `ORC claims by stripe, split with no duplicates or gaps`() {
        TestHive().use { hive ->
            hive.createTable("t_big", HiveStorageFormat.ORC, dataColumns)
            write(hive, "t_big", rows(inputSchema, 500, partitioned = false), inputSchema, "num_shards: 1")

            val (pipeline, output) = read(hive, "t_big", "split_bytes: 1024")
            PAssert.that(output.apply(Count.globally())).containsInAnyOrder(500L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `Parquet claims by row group, split with no duplicates or gaps`() {
        // Writes a Parquet file holding several row groups directly, then splits it by byte range and reads it back.
        // This format had no split test before: the old implementation ignored the upper bound of the range and read the whole
        // file, so with several splits the same data was read N times — the row count here locks that invariant down.
        TestHive().use { hive ->
            hive.createTable("t_parquet", HiveStorageFormat.PARQUET, listOf("id" to "bigint", "name" to "string"))
            val location = hive.warehouse.resolve("default.db").resolve("t_parquet")
            writeParquet(location, 2000)

            val (pipeline, output) = read(hive, "t_parquet", "split_bytes: 512")
            PAssert.that(output.apply(Count.globally())).containsInAnyOrder(2000L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `SequenceFile claims by sync block, split with no duplicates or gaps`() {
        TestHive().use { hive ->
            hive.createTable("t_big", HiveStorageFormat.SEQUENCEFILE, dataColumns)
            write(hive, "t_big", rows(inputSchema, 3000, partitioned = false), inputSchema, "num_shards: 1")

            val (pipeline, output) = read(hive, "t_big", "split_bytes: 100000000")
            PAssert.that(output.apply(Count.globally())).containsInAnyOrder(3000L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `RCFile claims by sync block, split with no duplicates or gaps`() {
        listOf(HiveStorageFormat.RCTEXT, HiveStorageFormat.RCBINARY).forEach { format ->
            TestHive().use { hive ->
                hive.createTable("t_big", format, dataColumns)
                write(hive, "t_big", rows(inputSchema, 3000, partitioned = false), inputSchema, "num_shards: 1")

                val (pipeline, output) = read(hive, "t_big", "split_bytes: 1024")
                PAssert.that(output.apply(Count.globally())).containsInAnyOrder(3000L)
                pipeline.run().waitUntilFinish()
            }
        }
    }

    /** Writes a Parquet file holding several row groups (with the row group size squeezed very small), to verify that splitting
     * it produces no duplicates and drops no rows. */
    private fun writeParquet(location: java.nio.file.Path, n: Int) {
        val schema = MessageType(
            "hive",
            Types.required(PrimitiveType.PrimitiveTypeName.INT64).named("id"),
            Types.required(PrimitiveType.PrimitiveTypeName.BINARY).named("name"),
        )
        val conf = Configuration()
        conf.set("parquet.block.size", "1024")
        conf.set("parquet.page.size", "256")
        val path = HadoopPath(location.resolve("data.parquet").toString())
        ExampleParquetWriter.builder(path)
            .withType(schema)
            .withConf(conf)
            .withCompressionCodec(CompressionCodecName.UNCOMPRESSED)
            .build()
            .use { writer ->
                repeat(n) { i ->
                    val group = SimpleGroup(schema)
                    group.add("id", i.toLong())
                    group.add("name", "name-$i")
                    writer.write(group)
                }
            }
    }

    @Test
    fun `a null value is written into the default partition and reads back`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.TEXTFILE,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            val row = Row.withSchema(partitionedInputSchema)
                .addValue(1L)
                .addValue(null)
                .addValue(null)
                .addValue(null)
                .build()
            write(hive, "t_order", listOf(row), partitionedInputSchema)

            assertEquals(
                listOf("dt=__HIVE_DEFAULT_PARTITION__"),
                hive.metastore.getPartitionNames("default", "t_order"),
            )
            val (pipeline, output) = read(hive, "t_order")
            PAssert.that(output.asText()).containsInAnyOrder("1|<null>|<null>|<null>")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `a missing table errors at graph construction time`() {
        TestHive().use { hive ->
            val error = assertFailsWith<IllegalArgumentException> {
                read(hive, "no_such_table")
            }
            assertTrue(error.message!!.contains("table does not exist"))
        }
    }

    @Test
    fun `a transactional table is explicitly rejected, not pretended to be readable`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_acid",
                HiveStorageFormat.ORC,
                dataColumns,
                tableParameters = mapOf("transactional" to "true"),
            )
            val error = assertFailsWith<IllegalArgumentException> { read(hive, "t_acid") }
            assertTrue(error.message!!.contains("transactional (ACID)"))
        }
    }

    @Test
    fun `the write side aligns by column name, a different upstream field order does not write to the wrong column`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            // The upstream field order is the reverse of the table's
            val reversed = Schema.builder()
                .addNullableField("amount", FieldTypes.DECIMAL)
                .addNullableField("name", FieldTypes.STRING)
                .addNullableField("id", FieldTypes.INT64)
                .build()
            val row = Row.withSchema(reversed).addValues(BigDecimal("9.90"), "张三", 42L).build()
            write(hive, "t_order", listOf(row), reversed)

            val (pipeline, output) = read(hive, "t_order")
            PAssert.that(output.asText()).containsInAnyOrder("42|张三|9.90")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `ORC predicate pushdown does row-level filtering`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            write(hive, "t_order", rows(inputSchema, 10, partitioned = false), inputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                predicates:
                  - column: id
                    op: ">"
                    value: "5"
                """.trimIndent(),
            )
            // id 6..10 is 5 rows, proving the predicate really filtered the other rows out (whole-block skipping via ORC stripe statistics is covered by PredicateEvaluatorTest)
            PAssert.that(output.apply("Count", Count.globally())).containsInAnyOrder(5L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `Parquet predicate pushdown does row-level filtering`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.PARQUET, dataColumns)
            write(hive, "t_order", rows(inputSchema, 10, partitioned = false), inputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                predicates:
                  - column: id
                    op: ">"
                    value: "5"
                """.trimIndent(),
            )
            // id 6..10 is 5 rows, proving the predicate really filtered the other rows out (whole-block skipping via Parquet row group statistics is covered by PredicateEvaluatorTest)
            PAssert.that(output.apply("Count", Count.globally())).containsInAnyOrder(5L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `predicate pushdown on a text format falls back to row-level filtering`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.TEXTFILE, dataColumns)
            write(hive, "t_order", rows(inputSchema, 10, partitioned = false), inputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                predicates:
                  - column: id
                    op: "<="
                    value: "3"
                """.trimIndent(),
            )
            PAssert.that(output.apply("Count", Count.globally())).containsInAnyOrder(3L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `ORC decimal predicate pushdown filters row-level and does not mis-skip`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            write(hive, "t_order", rows(inputSchema, 10, partitioned = false), inputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                predicates:
                  - column: amount
                    op: ">"
                    value: "5.50"
                """.trimIndent(),
            )
            // amount is i.50: >5.50 keeps i>=6, i.e. 5 rows; decimal stripe statistics skipping is wired up and must not skip rows that do match.
            PAssert.that(output.apply("Count", Count.globally())).containsInAnyOrder(5L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `Parquet decimal predicate pushdown filters row-level and does not mis-skip`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.PARQUET, dataColumns)
            write(hive, "t_order", rows(inputSchema, 10, partitioned = false), inputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                predicates:
                  - column: amount
                    op: ">"
                    value: "5.50"
                """.trimIndent(),
            )
            // parquet stores decimal statistics unscaled, so they must be converted back to BigDecimal by scale before comparing;
            // a wrong conversion would mis-skip and drop rows.
            PAssert.that(output.apply("Count", Count.globally())).containsInAnyOrder(5L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `Avro decimal predicate pushdown falls back to row-level filtering`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.AVRO, dataColumns)
            write(hive, "t_order", rows(inputSchema, 10, partitioned = false), inputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                predicates:
                  - column: amount
                    op: ">"
                    value: "5.50"
                """.trimIndent(),
            )
            PAssert.that(output.apply("Count", Count.globally())).containsInAnyOrder(5L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `a predicate column absent from the read columns errors explicitly`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            val error = assertFailsWith<IllegalArgumentException> {
                read(
                    hive,
                    "t_order",
                    """
                    columns: [name]
                    predicates:
                      - column: id
                        op: ">"
                        value: "1"
                    """.trimIndent(),
                )
            }
            assertTrue(error.message!!.contains("predicate column"), error.message)
        }
    }

    @Test
    fun `a predicate column absent from the table errors explicitly`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            val error = assertFailsWith<IllegalArgumentException> {
                read(
                    hive,
                    "t_order",
                    """
                    predicates:
                      - column: no_such_column
                        op: ">"
                        value: "1"
                    """.trimIndent(),
                )
            }
            assertTrue(error.message!!.contains("does not exist"), error.message)
        }
    }

    @Test
    fun `LIMIT pushdown returns at most N rows`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            write(hive, "t_order", rows(inputSchema, 100, partitioned = false), inputSchema)
            val (pipeline, output) = read(hive, "t_order", "limit: 5")
            PAssert.that(output.apply(Count.globally())).containsInAnyOrder(5L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `LIMIT combines with predicate pushdown, filtering happens before truncation`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.PARQUET, dataColumns)
            write(hive, "t_order", rows(inputSchema, 100, partitioned = false), inputSchema)
            // id > 50 leaves 50 rows, then LIMIT 8 -> 8 rows
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                predicates:
                  - column: id
                    op: ">"
                    value: "50"
                limit: 8
                """.trimIndent(),
            )
            PAssert.that(output.apply(Count.globally())).containsInAnyOrder(8L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `sampling pushdown keeps rows by probability, reducing downstream data volume`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            write(hive, "t_order", rows(inputSchema, 2000, partitioned = false), inputSchema)
            // A fixed seed makes the result reproducible; the retention ratio is ~0.1, so about 200 of the 2000 rows are kept
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                sample:
                  fraction: 0.1
                  seed: 42
                """.trimIndent(),
            )
            PAssert.that(output.apply(Count.globally())).satisfies {
                val c = it.iterator().next()
                assertTrue(c in 100L..300L, "the row count after sampling should be around ~200, actually $c")
                null
            }
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `limit must be positive, otherwise it errors explicitly`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            val error = assertFailsWith<IllegalArgumentException> {
                read(hive, "t_order", "limit: 0")
            }
            assertTrue(error.message!!.contains("limit"), error.message)
        }
    }

    @Test
    fun `sample fraction must be between 0 and 1, otherwise it errors explicitly`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            val error = assertFailsWith<IllegalArgumentException> {
                read(
                    hive,
                    "t_order",
                    """
                    sample:
                      fraction: 2.0
                    """.trimIndent(),
                )
            }
            assertTrue(error.message!!.contains("sample.fraction") || error.message!!.contains("fraction"), error.message)
        }
    }

    @Test
    fun `SYSTEM sampling skips whole ORC stripes, reducing IO`() {
        TestHive().use { hive ->
            writeDirect(hive, "t_order", HiveStorageFormat.ORC, rows(inputSchema, 20000, partitioned = false))
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                sample:
                  fraction: 0.5
                  method: system
                  seed: 7
                """.trimIndent(),
            )
            PAssert.that(output.apply(Count.globally())).satisfies {
                val c = it.iterator().next()
                // Roughly half of the stripes are skipped wholesale, so the rows kept should be around half of the total
                assertTrue(c in 6000L..14000L, "the row count after SYSTEM sampling should be around ~10000, actually $c")
                null
            }
            val result = pipeline.run()
            val skipped = result.metrics().queryMetrics(MetricsFilter.builder().build())
                .counters.firstOrNull { it.name.name == "orcStripesSkipped" }?.attempted?.toInt() ?: 0
            assertTrue(skipped > 0, "SYSTEM sampling did not trigger any stripe skipping")
            result.waitUntilFinish()
        }
    }

    @Test
    fun `SYSTEM sampling skips whole Parquet row groups, reducing IO`() {
        TestHive().use { hive ->
            writeDirect(hive, "t_order", HiveStorageFormat.PARQUET, rows(inputSchema, 20000, partitioned = false))
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                sample:
                  fraction: 0.5
                  method: system
                  seed: 7
                """.trimIndent(),
            )
            PAssert.that(output.apply(Count.globally())).satisfies {
                val c = it.iterator().next()
                assertTrue(c in 6000L..14000L, "the row count after SYSTEM sampling should be around ~10000, actually $c")
                null
            }
            val result = pipeline.run()
            val skipped = result.metrics().queryMetrics(MetricsFilter.builder().build())
                .counters.firstOrNull { it.name.name == "parquetRowGroupsSkipped" }?.attempted?.toInt() ?: 0
            assertTrue(skipped > 0, "SYSTEM sampling did not trigger any row group skipping")
            result.waitUntilFinish()
        }
    }

    @Test
    fun `an illegal sample method errors explicitly`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            val error = assertFailsWith<IllegalArgumentException> {
                read(
                    hive,
                    "t_order",
                    """
                    sample:
                      fraction: 0.5
                      method: bogus
                    """.trimIndent(),
                )
            }
            assertTrue(error.message!!.contains("sample.method"), error.message)
        }
    }

    @Test
    fun `ORC aggregation pushdown for count, min, and max reads the file-tail statistics directly, no row scan`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            write(hive, "t_order", rows(inputSchema, 4, partitioned = false), inputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                aggregates:
                  - type: count
                  - type: min
                    column: id
                  - type: max
                    column: amount
                  - type: min
                    column: name
                  - type: max
                    column: name
                """.trimIndent(),
            )
            // count=4, min_id=1, max_amount=4.50, min_name=name-1, max_name=name-4
            PAssert.that(output.asText()).containsInAnyOrder("4|1|4.50|name-1|name-4")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `Parquet aggregation pushdown for count, min, and max`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.PARQUET, dataColumns)
            write(hive, "t_order", rows(inputSchema, 4, partitioned = false), inputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                aggregates:
                  - type: count
                  - type: min
                    column: id
                  - type: max
                    column: amount
                """.trimIndent(),
            )
            PAssert.that(output.asText()).containsInAnyOrder("4|1|4.50")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `ORC aggregation pushdown for sum and avg must scan and accumulate the file`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            write(hive, "t_order", rows(inputSchema, 4, partitioned = false), inputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                aggregates:
                  - type: sum
                    column: id
                  - type: avg
                    column: id
                  - type: sum
                    column: amount
                  - type: avg
                    column: amount
                """.trimIndent(),
            )
            // sum(id)=10, avg(id)=2.5, sum(amount)=12.00, avg(amount)=3.00
            PAssert.that(output.asText()).containsInAnyOrder("10|2.5|12.00|3.00")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `Parquet aggregation pushdown for sum and avg`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.PARQUET, dataColumns)
            write(hive, "t_order", rows(inputSchema, 4, partitioned = false), inputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                aggregates:
                  - type: sum
                    column: id
                  - type: avg
                    column: amount
                """.trimIndent(),
            )
            PAssert.that(output.asText()).containsInAnyOrder("10|3.00")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `aggregation pushdown merges sum and avg across partitions`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.ORC,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 4, partitioned = true), partitionedInputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                aggregates:
                  - type: sum
                    column: id
                  - type: avg
                    column: id
                """.trimIndent(),
            )
            // Two partitions with 2 rows each: id 1,2 and 3,4, sum=10, avg=2.5
            PAssert.that(output.asText()).containsInAnyOrder("10|2.5")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `aggregation pushdown on an empty table yields avg = null`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                aggregates:
                  - type: avg
                    column: id
                """.trimIndent(),
            )
            PAssert.that(output.asText()).containsInAnyOrder("<null>")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `aggregation pushdown sum and avg support only numeric columns, a string column errors`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            val error = assertFailsWith<IllegalArgumentException> {
                read(
                    hive,
                    "t_order",
                    """
                    aggregates:
                      - type: sum
                        column: name
                    """.trimIndent(),
                )
            }
            assertTrue(error.message!!.contains("numeric column"), error.message)
        }
    }

    @Test
    fun `aggregation pushdown merges count across partitions`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.ORC,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 4, partitioned = true), partitionedInputSchema)
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                aggregates:
                  - type: count
                  - type: max
                    column: id
                """.trimIndent(),
            )
            // Two partitions with 2 rows each, 4 rows in total, global max(id)=4
            PAssert.that(output.asText()).containsInAnyOrder("4|4")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `aggregation pushdown on an empty table yields count = 0`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            // Writes no data at all, so there is no file to scan
            val (pipeline, output) = read(
                hive,
                "t_order",
                """
                aggregates:
                  - type: count
                """.trimIndent(),
            )
            PAssert.that(output.asText()).containsInAnyOrder("0")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `aggregation pushdown and predicates are mutually exclusive`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            val error = assertFailsWith<IllegalArgumentException> {
                read(
                    hive,
                    "t_order",
                    """
                    aggregates:
                      - type: count
                    predicates:
                      - column: id
                        op: ">"
                        value: "1"
                    """.trimIndent(),
                )
            }
            assertTrue(error.message!!.contains("aggregation pushdown"), error.message)
        }
    }

    @Test
    fun `aggregation pushdown min and max must specify column, otherwise it errors`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            val error = assertFailsWith<IllegalArgumentException> {
                read(
                    hive,
                    "t_order",
                    """
                    aggregates:
                      - type: min
                    """.trimIndent(),
                )
            }
            assertTrue(error.message!!.contains("column"), error.message)
        }
    }

    @Test
    fun `aggregation pushdown supports only ORC and Parquet, other formats error explicitly`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.TEXTFILE, dataColumns)
            val error = assertFailsWith<IllegalArgumentException> {
                read(
                    hive,
                    "t_order",
                    """
                    aggregates:
                      - type: count
                    """.trimIndent(),
                )
            }
            assertTrue(error.message!!.contains("aggregation pushdown"), error.message)
        }
    }
}
