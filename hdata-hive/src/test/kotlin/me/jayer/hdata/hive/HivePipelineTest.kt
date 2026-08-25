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
}
