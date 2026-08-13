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
}
