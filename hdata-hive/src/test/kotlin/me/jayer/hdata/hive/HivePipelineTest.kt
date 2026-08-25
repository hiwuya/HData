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
 * `ReadFromHive` / `WriteToHive` 的端到端测试：跑在 DirectRunner + 本地临时目录 + 进程内 metastore 上
 * （见 [TestHive]），八种存储格式全部真的写文件、再真的按字节区间读回来。
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
     * 直接写出 ORC/Parquet 文件（未压缩 + 极小 stripe/row group），好让 SYSTEM 采样能跨多个块，
     * 而不是整文件被一次抽中（默认块太大时 20000 行只有一个块，SYSTEM 采样会退化成"全留或全丢"）。
     */
    private fun writeDirect(hive: TestHive, table: String, format: HiveStorageFormat, rows: List<Row>) {
        hive.createTable(table, format, dataColumns)
        val file = hive.warehouse.resolve("default.db").resolve(table).resolve("data.${format.name.lowercase()}")
        when (format) {
            HiveStorageFormat.ORC -> writeOrcDirect(file, rows)
            HiveStorageFormat.PARQUET -> writeParquetDirect(file, rows)
            else -> throw IllegalArgumentException("SYSTEM 采样测试只覆盖 ORC / Parquet：$format")
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

    /** Row -> 便于断言的字符串，绕开 schema 完全一致才能比的麻烦。 */
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
    fun `八种存储格式都能写出去再读回来`() {
        HiveStorageFormat.entries.forEach { format ->
            TestHive().use { hive ->
                hive.createTable("t_order", format, dataColumns)
                write(hive, "t_order", rows(inputSchema, 4, partitioned = false), inputSchema)

                assertTrue(hive.dataFiles("t_order").isNotEmpty(), "$format 没有写出任何文件")

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
    fun `分区表按目录写出，分区自动注册进 metastore`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.ORC,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 4, partitioned = true), partitionedInputSchema)

            // 分区必须注册进 metastore，否则 Hive 查不到这些数据
            assertEquals(
                listOf("dt=2024-01-01", "dt=2024-01-02"),
                hive.metastore.getPartitionNames("default", "t_order"),
            )
            // 文件真的落在分区目录下
            assertTrue(hive.dataFiles("t_order").all { it.toString().contains("dt=2024-01-0") })

            val (pipeline, output) = read(hive, "t_order")
            // 分区列排在数据列之后，与 Hive SELECT * 的顺序一致
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
    fun `按分区名读只读那一个分区`() {
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
    fun `分区过滤表达式交给 metastore`() {
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
    fun `谓词命中分区列时自动裁剪无关分区（不读其它分区的数据）`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.ORC,
                dataColumns,
                partitionColumns = listOf("dt" to "string"),
            )
            write(hive, "t_order", rows(partitionedInputSchema, 4, partitioned = true), partitionedInputSchema)
            // 4 行落在 dt=2024-01-01 / dt=2024-01-02 两个分区各 2 行；只取 dt=2024-01-02
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
            // 结果里只能有 dt=2024-01-02 的 2 行（id=2,4），dt=2024-01-01 的分区被整段裁剪
            PAssert.that(output.asText()).containsInAnyOrder(
                "2|name-2|2.50|2024-01-02",
                "4|name-4|4.50|2024-01-02",
            )
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `分区列范围谓词（大于等于）同样能裁剪分区`() {
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
            // dt=2024-01-01 被裁剪，只保留 dt=2024-01-02
            PAssert.that(output.asText()).containsInAnyOrder(
                "2|name-2|2.50|2024-01-02",
                "4|name-4|4.50|2024-01-02",
            )
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `分区列谓词与数据列谓词可叠加，仍只扫命中分区`() {
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
            // dt=2024-01-02 分区里 id=2,4；id>2 只剩 id=4
            PAssert.that(output.asText()).containsInAnyOrder("4|name-4|4.50|2024-01-02")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `谓词在数据列上时不裁剪分区，所有分区照常读`() {
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
            // id>0 命中所有行，分区不被裁剪，仍 4 行
            PAssert.that(output.apply(Count.globally())).containsInAnyOrder(4L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `只读投影到的列`() {
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
    fun `文本文件按字节区间切分后不重不漏`() {
        TestHive().use { hive ->
            hive.createTable("t_big", HiveStorageFormat.TEXTFILE, dataColumns)
            val data = rows(inputSchema, 500, partitioned = false)
            write(hive, "t_big", data, inputSchema, "num_shards: 1")

            // 一个分片只有几十字节，500 行会被切成很多段
            val (pipeline, output) = read(hive, "t_big", "split_bytes: 256")
            PAssert.that(output.apply(Count.globally())).containsInAnyOrder(500L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `ORC 按 stripe 认领，切分后不重不漏`() {
        TestHive().use { hive ->
            hive.createTable("t_big", HiveStorageFormat.ORC, dataColumns)
            write(hive, "t_big", rows(inputSchema, 500, partitioned = false), inputSchema, "num_shards: 1")

            val (pipeline, output) = read(hive, "t_big", "split_bytes: 1024")
            PAssert.that(output.apply(Count.globally())).containsInAnyOrder(500L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `Parquet 按 row group 认领，切分后不重不漏`() {
        // 直接写一个含多个 row group 的 Parquet 文件（row group 很小），再按字节区间切开读回来。
        // 这个格式之前没有做过分片测试：旧实现会忽略区间上界、把整个文件读一遍，
        // 多分片时同一批数据会被重复读 N 倍——这里用行数锁死这个不变量。
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
    fun `SequenceFile 按同步块认领，切分后不重不漏`() {
        TestHive().use { hive ->
            hive.createTable("t_big", HiveStorageFormat.SEQUENCEFILE, dataColumns)
            write(hive, "t_big", rows(inputSchema, 3000, partitioned = false), inputSchema, "num_shards: 1")

            val (pipeline, output) = read(hive, "t_big", "split_bytes: 100000000")
            PAssert.that(output.apply(Count.globally())).containsInAnyOrder(3000L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `RCFile 按同步块认领，切分后不重不漏`() {
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

    /** 写一个含多个 row group 的 Parquet 文件（把 row group 大小压得很小），用来验证分片后不重不漏。 */
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
    fun `null 值写进默认分区并能读回来`() {
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
    fun `表不存在时构图阶段就报错`() {
        TestHive().use { hive ->
            val error = assertFailsWith<IllegalArgumentException> {
                read(hive, "no_such_table")
            }
            assertTrue(error.message!!.contains("表不存在"))
        }
    }

    @Test
    fun `事务表明确拒绝，不装作能读`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_acid",
                HiveStorageFormat.ORC,
                dataColumns,
                tableParameters = mapOf("transactional" to "true"),
            )
            val error = assertFailsWith<IllegalArgumentException> { read(hive, "t_acid") }
            assertTrue(error.message!!.contains("事务表"))
        }
    }

    @Test
    fun `写入端按列名对齐，上游字段顺序不同也不会写错列`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            // 上游字段顺序与表相反
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
    fun `ORC 谓词下推做行级过滤`() {
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
            // id 6..10 共 5 行，证明谓词真的把其余行过滤掉了（ORC stripe 统计的整段跳过见 PredicateEvaluatorTest）
            PAssert.that(output.apply("Count", Count.globally())).containsInAnyOrder(5L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `Parquet 谓词下推做行级过滤`() {
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
            // id 6..10 共 5 行，证明谓词真的把其余行过滤掉了（Parquet row group 统计的整段跳过见 PredicateEvaluatorTest）
            PAssert.that(output.apply("Count", Count.globally())).containsInAnyOrder(5L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `文本格式上的谓词下推走行级过滤`() {
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
    fun `ORC decimal 谓词下推做行级过滤且不会误跳`() {
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
            // amount 为 i.50：>5.50 保留 i>=6 共 5 行；decimal 的 stripe 统计跳过已接上，不能把命中的行跳掉。
            PAssert.that(output.apply("Count", Count.globally())).containsInAnyOrder(5L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `Parquet decimal 谓词下推做行级过滤且不会误跳`() {
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
            // parquet 的 decimal 统计是未缩放值，要按 scale 换回 BigDecimal 才能比较；换算错了会误跳导致丢行。
            PAssert.that(output.apply("Count", Count.globally())).containsInAnyOrder(5L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `Avro decimal 谓词下推走行级过滤`() {
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
    fun `谓词列不在读取列里时显式报错`() {
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
            assertTrue(error.message!!.contains("谓词列"), error.message)
        }
    }

    @Test
    fun `谓词列在表上不存在时显式报错`() {
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
            assertTrue(error.message!!.contains("不存在"), error.message)
        }
    }

    @Test
    fun `LIMIT 下推最多返回 N 行`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            write(hive, "t_order", rows(inputSchema, 100, partitioned = false), inputSchema)
            val (pipeline, output) = read(hive, "t_order", "limit: 5")
            PAssert.that(output.apply(Count.globally())).containsInAnyOrder(5L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `LIMIT 与谓词下推叠加，先过滤再截断`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.PARQUET, dataColumns)
            write(hive, "t_order", rows(inputSchema, 100, partitioned = false), inputSchema)
            // id > 50 还剩 50 行，再 LIMIT 8 → 8 行
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
    fun `采样下推按概率保留行，减少下游数据量`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            write(hive, "t_order", rows(inputSchema, 2000, partitioned = false), inputSchema)
            // 固定种子 → 结果可复现；保留比例≈0.1，2000 行里大约 200 行被留下
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
                assertTrue(c in 100L..300L, "采样后行数应在 ~200 附近，实际 $c")
                null
            }
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `limit 必须为正数否则显式报错`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            val error = assertFailsWith<IllegalArgumentException> {
                read(hive, "t_order", "limit: 0")
            }
            assertTrue(error.message!!.contains("limit"), error.message)
        }
    }

    @Test
    fun `sample fraction 必须在 0 到 1 之间否则显式报错`() {
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
    fun `SYSTEM 采样按 ORC stripe 整段跳过，减少 IO`() {
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
                // 整段跳过约一半 stripe，留下的行数应在总量附近的一半
                assertTrue(c in 6000L..14000L, "SYSTEM 采样后行数应在 ~10000 附近，实际 $c")
                null
            }
            val result = pipeline.run()
            val skipped = result.metrics().queryMetrics(MetricsFilter.builder().build())
                .counters.firstOrNull { it.name.name == "orcStripesSkipped" }?.attempted?.toInt() ?: 0
            assertTrue(skipped > 0, "SYSTEM 采样未触发任何 stripe 跳过")
            result.waitUntilFinish()
        }
    }

    @Test
    fun `SYSTEM 采样按 Parquet row group 整段跳过，减少 IO`() {
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
                assertTrue(c in 6000L..14000L, "SYSTEM 采样后行数应在 ~10000 附近，实际 $c")
                null
            }
            val result = pipeline.run()
            val skipped = result.metrics().queryMetrics(MetricsFilter.builder().build())
                .counters.firstOrNull { it.name.name == "parquetRowGroupsSkipped" }?.attempted?.toInt() ?: 0
            assertTrue(skipped > 0, "SYSTEM 采样未触发任何 row group 跳过")
            result.waitUntilFinish()
        }
    }

    @Test
    fun `sample method 非法时显式报错`() {
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
    fun `ORC 聚合下推 count min max 直接读文件尾统计，不扫行`() {
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
    fun `Parquet 聚合下推 count min max`() {
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
    fun `ORC 聚合下推 sum avg 必须扫文件累加`() {
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
    fun `Parquet 聚合下推 sum avg`() {
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
    fun `聚合下推跨分区归并 sum avg`() {
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
            // 两个分区各 2 行：id 1,2 与 3,4，sum=10，avg=2.5
            PAssert.that(output.asText()).containsInAnyOrder("10|2.5")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `空表聚合下推 avg 为 null`() {
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
    fun `聚合下推 sum avg 只支持数值列，字符串列报错`() {
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
            assertTrue(error.message!!.contains("数值列"), error.message)
        }
    }

    @Test
    fun `聚合下推跨分区归并 count`() {
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
            // 两个分区各 2 行共 4 行，全局 max(id)=4
            PAssert.that(output.asText()).containsInAnyOrder("4|4")
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `空表聚合下推 count 为 0`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.ORC, dataColumns)
            // 不写任何数据，没有文件可扫
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
    fun `聚合下推与谓词互斥`() {
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
            assertTrue(error.message!!.contains("聚合下推"), error.message)
        }
    }

    @Test
    fun `聚合下推 min 与 max 必须指定 column 否则报错`() {
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
    fun `聚合下推仅支持 ORC 与 Parquet，其它格式显式报错`() {
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
            assertTrue(error.message!!.contains("聚合下推"), error.message)
        }
    }
}
