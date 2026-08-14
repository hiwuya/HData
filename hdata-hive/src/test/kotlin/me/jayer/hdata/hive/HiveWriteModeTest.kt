package me.jayer.hdata.hive

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.format.HiveStorageFormat
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.MapElements
import org.apache.beam.sdk.transforms.SimpleFunction
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.apache.beam.sdk.values.TypeDescriptors
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 追加写入 / 覆盖写入 / 动态分区。
 *
 * 这三件事只靠单跑一次作业是测不出来的：问题都出在**第二次写同一张表**的时候。
 * 最早的实现就栽在这里——Beam 的 `defaultNaming` 只按"前缀-分片号-of-总数"命名，
 * 两次作业生成一模一样的文件名，第二次把第一次的结果直接盖掉，
 * 既不是追加也不是覆盖，而且作业状态还是成功。
 *
 * @author wuya
 */
class HiveWriteModeTest {

    private val schema: Schema = Schema.builder()
        .addNullableField("id", FieldTypes.INT64)
        .addNullableField("dt", FieldTypes.STRING)
        .build()

    private fun config(yaml: String): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    /** 写一批 `id -> dt` 的行。 */
    private fun write(hive: TestHive, rows: List<Pair<Long, String>>, extra: String = "") {
        val pipeline = Pipeline.create()
        val input = pipeline.apply(
            Create.of(rows.map { (id, dt) -> Row.withSchema(schema).addValues(id, dt).build() })
                .withRowSchema(schema)
        )
        val yaml = buildString {
            appendLine("""metastore_uri: "${hive.metastoreUri}"""")
            appendLine("table: t_order")
            extra.lines().filter { it.isNotBlank() }.forEach { appendLine(it.trimIndent()) }
        }
        HiveWriteProvider().from(config(yaml)).expand(PCollectionRowTuple.of(Tags.MAIN_INPUT, input))
        pipeline.run().waitUntilFinish()
    }

    private fun assertContent(hive: TestHive, vararg expected: String) {
        val pipeline = Pipeline.create()
        val output = HiveReadProvider()
            .from(config("""
                metastore_uri: "${hive.metastoreUri}"
                table: t_order
            """.trimIndent()))
            .expand(PCollectionRowTuple.empty(pipeline))
            .get(Tags.MAIN_OUTPUT)
        val text = output.apply(
            MapElements.into(TypeDescriptors.strings()).via(
                object : SimpleFunction<Row, String>() {
                    override fun apply(input: Row): String =
                        input.schema.fieldNames.joinToString("|") { input.getValue<Any?>(it)?.toString() ?: "<null>" }
                }
            )
        )
        PAssert.that(text).containsInAnyOrder(*expected)
        pipeline.run().waitUntilFinish()
    }

    private fun partitionedTable(hive: TestHive, format: HiveStorageFormat = HiveStorageFormat.TEXTFILE) {
        hive.createTable(
            "t_order",
            format,
            listOf("id" to "bigint"),
            partitionColumns = listOf("dt" to "string"),
        )
    }

    @Test
    fun `追加写入：第二次作业不会盖掉第一次的文件`() {
        TestHive().use { hive ->
            partitionedTable(hive)
            write(hive, listOf(1L to "2024-01-01", 2L to "2024-01-01"))
            write(hive, listOf(3L to "2024-01-01", 4L to "2024-01-01"))

            assertContent(
                hive,
                "1|2024-01-01", "2|2024-01-01", "3|2024-01-01", "4|2024-01-01",
            )
        }
    }

    @Test
    fun `覆盖写入：只留下最后一次写的数据`() {
        TestHive().use { hive ->
            partitionedTable(hive)
            write(hive, listOf(1L to "2024-01-01", 2L to "2024-01-01"))
            write(hive, listOf(3L to "2024-01-01"), "write_mode: overwrite")

            assertContent(hive, "3|2024-01-01")
        }
    }

    @Test
    fun `覆盖写入只动本次写到的分区，其余分区不受影响`() {
        TestHive().use { hive ->
            partitionedTable(hive)
            write(hive, listOf(1L to "2024-01-01", 2L to "2024-01-02"))
            // 只往 01-01 写，01-02 的数据必须原样留着——这是 Hive 动态分区覆盖的语义
            write(hive, listOf(9L to "2024-01-01"), "write_mode: overwrite")

            assertContent(hive, "9|2024-01-01", "2|2024-01-02")
        }
    }

    @Test
    fun `动态分区：一次作业按行的取值写出多个分区并全部注册`() {
        TestHive().use { hive ->
            partitionedTable(hive, HiveStorageFormat.ORC)
            write(
                hive,
                listOf(
                    1L to "2024-01-01",
                    2L to "2024-01-02",
                    3L to "2024-01-03",
                    4L to "2024-01-03",
                ),
            )

            assertEquals(
                listOf("dt=2024-01-01", "dt=2024-01-02", "dt=2024-01-03"),
                hive.metastore.getPartitionNames("default", "t_order"),
            )
            assertContent(hive, "1|2024-01-01", "2|2024-01-02", "3|2024-01-03", "4|2024-01-03")
        }
    }

    @Test
    fun `动态分区遇到新分区会补注册，老分区不会重复报错`() {
        TestHive().use { hive ->
            partitionedTable(hive)
            write(hive, listOf(1L to "2024-01-01"))
            // 第二次既写老分区也写新分区：老分区已存在不能报错，新分区要补上
            write(hive, listOf(2L to "2024-01-01", 3L to "2024-01-02"))

            assertEquals(
                listOf("dt=2024-01-01", "dt=2024-01-02"),
                hive.metastore.getPartitionNames("default", "t_order"),
            )
            assertContent(hive, "1|2024-01-01", "2|2024-01-01", "3|2024-01-02")
        }
    }

    @Test
    fun `非分区表也支持覆盖写入`() {
        TestHive().use { hive ->
            hive.createTable("t_order", HiveStorageFormat.PARQUET, listOf("id" to "bigint", "dt" to "string"))
            write(hive, listOf(1L to "a", 2L to "b"))
            write(hive, listOf(3L to "c"), "write_mode: overwrite")

            assertContent(hive, "3|c")
            assertEquals(1, hive.dataFiles("t_order").size)
        }
    }

    @Test
    fun `追加写入产生的文件名互不相同`() {
        TestHive().use { hive ->
            partitionedTable(hive)
            write(hive, listOf(1L to "2024-01-01"), "num_shards: 1")
            write(hive, listOf(2L to "2024-01-01"), "num_shards: 1")

            val names = hive.dataFiles("t_order").map { it.fileName.toString() }
            assertEquals(2, names.size, "两次作业应各写出一个文件，实际: $names")
            assertEquals(names.size, names.toSet().size, "文件名撞了: $names")
        }
    }

    @Test
    fun `write_mode 写错时在构图阶段就报错`() {
        val error = assertFailsWith<IllegalArgumentException> {
            HiveWriteConfig(metastoreUri = "thrift://h:9083", table = "t", writeMode = "upsert").validate()
        }
        assertTrue(error.message!!.contains("write_mode"))
    }
}
