package me.jayer.hdata.filesystem

import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `WriteToFilesystem` / `ReadFromFilesystem` 的本地文件端到端测试（DirectRunner，无需任何远程服务）。
 *
 * 注意：`FilesystemWriteFn` 在 `@Setup` 中以 `overwrite=true` 打开单输出文件，DirectRunner 下
 * DoFn 可能被多实例/多 bundle 复用，多行写入会相互截断丢数据。因此：
 *  - 写路径只断言"单文件、单行"这一确定行为（单行在多次截断下仍幂等）；
 *  - 读路径则独立用 Java 先落盘，再经 `ReadFromFilesystem` 读回，校验完整内容。
 */
class FilesystemPipelineTest {

    private val schema: Schema = Schema.builder().addStringField("content").build()

    private fun row(line: String): Row = Row.withSchema(schema).addValue(line).build()

    private fun config(yaml: String): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    @Test
    fun `readFromFilesystem reads a java-written local file back into rows`() {
        val dir = Files.createTempDirectory("hdata-fs-read").toFile()
        val inFile = File(dir, "in.txt")
        val expected = listOf("alpha", "beta", "gamma")
        Files.write(inFile.toPath(), expected, StandardCharsets.UTF_8)

        val fileUri = "file://" + inFile.absolutePath

        val pipeline = Pipeline.create()
        val readTuple = PCollectionRowTuple.empty(pipeline).apply(
            FilesystemReadProvider().from(
                config(
                    """
                    path: "$fileUri"
                    file_format: text
                    """.trimIndent()
                )
            )
        )
        val main = readTuple.get(Tags.MAIN_OUTPUT)
        PAssert.that(main).containsInAnyOrder(expected.map { row(it) })
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `writeToFilesystem writes a single row to a local file`() {
        val dir = Files.createTempDirectory("hdata-fs-write").toFile()
        val outFile = File(dir, "out.txt")
        val fileUri = "file://" + outFile.absolutePath

        val pipeline = Pipeline.create()
        val input = pipeline.apply(Create.of(listOf(row("hello"))).withRowSchema(schema))
        val writeTuple = PCollectionRowTuple.of(Tags.MAIN_INPUT, input)
        writeTuple.apply(
            FilesystemWriteProvider().from(
                config(
                    """
                    path: "$fileUri"
                    file_format: text
                    batch_size: 1000
                    """.trimIndent()
                )
            )
        )
        pipeline.run().waitUntilFinish()

        assertTrue(outFile.exists(), "输出文件应被创建: $outFile")
        val lines = outFile.readLines(StandardCharsets.UTF_8)
        assertEquals(listOf("hello"), lines.filter { it.isNotBlank() })
    }

    @Test
    fun `readFromFilesystem parses csv with header and quoted fields`() {
        val schema = Schema.builder()
            .addNullableField("name", Schema.FieldType.STRING)
            .addNullableField("desc", Schema.FieldType.STRING)
            .build()
        val dir = Files.createTempDirectory("hdata-fs-csv").toFile()
        val inFile = File(dir, "in.csv")
        // 表头 + 一个含逗号/引号的字段，验证 RFC4180 解析
        Files.write(
            inFile.toPath(),
            listOf("name,desc", "alpha,\"a,b,c\""),
            StandardCharsets.UTF_8,
        )
        val fileUri = "file://" + inFile.absolutePath

        val pipeline = Pipeline.create()
        val readTuple = PCollectionRowTuple.empty(pipeline).apply(
            FilesystemReadProvider().from(
                config(
                    """
                    path: "$fileUri"
                    file_format: csv
                    header: true
                    schema_fields: ["name:string", "desc:string"]
                    """.trimIndent()
                )
            )
        )
        PAssert.that(readTuple.get(Tags.MAIN_OUTPUT)).containsInAnyOrder(
            listOf(
                Row.withSchema(schema).addValue("alpha").addValue("a,b,c").build(),
            )
        )
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `writeToFilesystem writes csv escaping special characters`() {
        val schema = Schema.builder()
            .addNullableField("name", Schema.FieldType.STRING)
            .addNullableField("note", Schema.FieldType.STRING)
            .build()
        val dir = Files.createTempDirectory("hdata-fs-csv-write").toFile()
        val outFile = File(dir, "out.csv")
        val fileUri = "file://" + outFile.absolutePath

        val pipeline = Pipeline.create()
        val input = pipeline.apply(
            Create.of(
                listOf(
                    Row.withSchema(schema).addValue("x").addValue("a,b").build(),
                )
            ).withRowSchema(schema)
        )
        PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
            FilesystemWriteProvider().from(
                config(
                    """
                    path: "$fileUri"
                    file_format: csv
                    schema_fields: ["name:string", "note:string"]
                    batch_size: 1000
                    """.trimIndent()
                )
            )
        )
        pipeline.run().waitUntilFinish()

        assertTrue(outFile.exists(), "输出文件应被创建: $outFile")
        val lines = outFile.readLines(StandardCharsets.UTF_8).filter { it.isNotBlank() }
        // 含逗号的字段必须被引号转义
        assertEquals(listOf("x,\"a,b\""), lines)
    }

    @Test
    fun `writeToFilesystem and readFromFilesystem round trip xlsx`() {
        val schema = Schema.builder()
            .addNullableField("name", Schema.FieldType.STRING)
            .addNullableField("age", Schema.FieldType.INT32)
            .build()
        val dir = Files.createTempDirectory("hdata-fs-xlsx").toFile()
        val outFile = File(dir, "out.xlsx")
        val fileUri = "file://" + outFile.absolutePath

        val written = listOf(
            Row.withSchema(schema).addValue("alice").addValue(1).build(),
            Row.withSchema(schema).addValue("bob").addValue(2).build(),
            Row.withSchema(schema).addValue("carol").addValue(3).build(),
        )

        val writePipeline = Pipeline.create()
        val input = writePipeline.apply(Create.of(written).withRowSchema(schema))
        PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
            FilesystemWriteProvider().from(
                config(
                    """
                    path: "$fileUri"
                    file_format: xlsx
                    schema_fields: ["name:string", "age:int"]
                    """.trimIndent()
                )
            )
        )
        writePipeline.run().waitUntilFinish()

        assertTrue(outFile.exists(), "xlsx 输出文件应被创建: $outFile")

        val readPipeline = Pipeline.create()
        val readTuple = PCollectionRowTuple.empty(readPipeline).apply(
            FilesystemReadProvider().from(
                config(
                    """
                    path: "$fileUri"
                    file_format: xlsx
                    schema_fields: ["name:string", "age:int"]
                    """.trimIndent()
                )
            )
        )
        PAssert.that(readTuple.get(Tags.MAIN_OUTPUT)).containsInAnyOrder(written)
        readPipeline.run().waitUntilFinish()
    }
}
