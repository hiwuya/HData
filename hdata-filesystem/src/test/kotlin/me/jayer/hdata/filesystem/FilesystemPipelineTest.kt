package me.jayer.hdata.filesystem

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import tools.jackson.databind.node.ObjectNode
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `ReadFromFilesystem` / `WriteToFilesystem` 的端到端测试，跑在 DirectRunner 上，全部走本地文件。
 *
 * 重构前这个测试类的注释里写着一句话：
 *
 * > `FilesystemWriteFn` 在 `@Setup` 中以 `overwrite=true` 打开单输出文件，DirectRunner 下
 * > DoFn 可能被多实例/多 bundle 复用，多行写入会相互截断丢数据。因此写路径只断言"单文件、单行"。
 *
 * 也就是说，测试是**绕着这个 bug**写的。落盘换成 Beam 的 `FileIO.write()` 之后，
 * 分片各写各的临时文件、全部成功才原子改名，多行写入不再互相截断，
 * 所以这里直接断言完整内容。
 *
 * @author wuya
 */
class FilesystemPipelineTest {

    private val textSchema: Schema = FilesystemSchemas.TEXT_SCHEMA

    private val personSchema: Schema = Schema.builder()
        .addNullableStringField("name")
        .addNullableInt32Field("age")
        .build()

    private fun line(text: String): Row = Row.withSchema(textSchema).addValue(text).build()

    private fun person(name: String?, age: Int?): Row =
        Row.withSchema(personSchema).addValue(name).addValue(age).build()

    private fun config(yaml: String): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    private fun tempDir(prefix: String): File = Files.createTempDirectory(prefix).toFile()

    private fun uri(file: File): String = "file://" + file.absolutePath

    private fun read(yaml: String) = Pipeline.create().let { pipeline ->
        pipeline to PCollectionRowTuple.empty(pipeline)
            .apply(FilesystemReadProvider().from(config(yaml)))
            .get(Tags.MAIN_OUTPUT)
    }

    private fun write(rows: List<Row>, schema: Schema, yaml: String): Pipeline {
        val pipeline = Pipeline.create()
        val input = pipeline.apply(Create.of(rows).withRowSchema(schema))
        PCollectionRowTuple.of(Tags.MAIN_INPUT, input)
            .apply(FilesystemWriteProvider().from(config(yaml)))
        return pipeline
    }

    /** 读出输出目录下的所有分片，按内容合并。 */
    private fun outputLines(dir: File): List<String> =
        (dir.listFiles() ?: emptyArray())
            .filter { it.isFile && !it.name.startsWith(".") }
            .flatMap { it.readLines() }

    private fun outputFiles(dir: File): List<File> =
        (dir.listFiles() ?: emptyArray()).filter { it.isFile && !it.name.startsWith(".") }

    // ---------- text ----------

    @Test
    fun `text 读出文件的每一行`() {
        val dir = tempDir("fs-text-read")
        val file = File(dir, "in.txt")
        Files.write(file.toPath(), listOf("alpha", "beta", "gamma"), StandardCharsets.UTF_8)

        val (pipeline, rows) = read("""path: "${uri(file)}"""")
        PAssert.that(rows).containsInAnyOrder(listOf("alpha", "beta", "gamma").map { line(it) })
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `通配符一次读多个文件`() {
        val dir = tempDir("fs-glob")
        Files.write(File(dir, "a.txt").toPath(), listOf("a1", "a2"), StandardCharsets.UTF_8)
        Files.write(File(dir, "b.txt").toPath(), listOf("b1"), StandardCharsets.UTF_8)
        // 不匹配的后缀不该被读进来
        Files.write(File(dir, "c.log").toPath(), listOf("c1"), StandardCharsets.UTF_8)

        val (pipeline, rows) = read("""path: "${uri(dir)}/${'*'}.txt"""")
        PAssert.that(rows).containsInAnyOrder(listOf("a1", "a2", "b1").map { line(it) })
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `text 多行写出后能完整读回，不会互相截断`() {
        // 这正是重构前测试刻意绕开的场景：多行 + 多 bundle
        val dir = tempDir("fs-text-write")
        val expected = (1..200).map { "line-$it" }

        write(expected.map { line(it) }, textSchema, """
            path: "${uri(dir)}"
            file_format: text
            num_shards: 1
        """.trimIndent()).run().waitUntilFinish()

        assertEquals(expected.sorted(), outputLines(dir).sorted())
    }

    @Test
    fun `num_shards 决定输出文件个数`() {
        val dir = tempDir("fs-shards")

        write((1..20).map { line("l-$it") }, textSchema, """
            path: "${uri(dir)}"
            num_shards: 3
        """.trimIndent()).run().waitUntilFinish()

        assertEquals(3, outputFiles(dir).size)
        assertEquals(20, outputLines(dir).size)
    }

    @Test
    fun `file_prefix 与扩展名体现在输出文件名上`() {
        val dir = tempDir("fs-naming")

        write(listOf(line("x")), textSchema, """
            path: "${uri(dir)}"
            file_prefix: "orders"
            num_shards: 1
        """.trimIndent()).run().waitUntilFinish()

        val name = outputFiles(dir).single().name
        assertTrue(name.startsWith("orders") && name.endsWith(".txt"), "实际文件名: $name")
    }

    // ---------- csv ----------

    @Test
    fun `csv 按 schema_fields 解析并跳过表头`() {
        val dir = tempDir("fs-csv-read")
        val file = File(dir, "in.csv")
        Files.write(file.toPath(), listOf("name,age", "张三,30", "李四,25"), StandardCharsets.UTF_8)

        val (pipeline, rows) = read("""
            path: "${uri(file)}"
            file_format: csv
            header: true
            schema_fields: ["name:string", "age:int"]
        """.trimIndent())
        PAssert.that(rows).containsInAnyOrder(person("张三", 30), person("李四", 25))
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `csv 里带引号的逗号与换行不会把一条记录劈开`() {
        // 正是因为这个，csv 不能像 text 那样按字节区间切分
        val dir = tempDir("fs-csv-quote")
        val file = File(dir, "in.csv")
        file.writeText("name,age\n\"张,三\",30\n\"李\n四\",25\n", StandardCharsets.UTF_8)

        val (pipeline, rows) = read("""
            path: "${uri(file)}"
            file_format: csv
            header: true
            schema_fields: ["name:string", "age:int"]
        """.trimIndent())
        PAssert.that(rows).containsInAnyOrder(person("张,三", 30), person("李\n四", 25))
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `csv_delimiter 真的会被用上`() {
        // 重构前 CSVFormat.DEFAULT 是写死的，配置里的分隔符根本没传下去
        val dir = tempDir("fs-csv-delim")
        val file = File(dir, "in.csv")
        Files.write(file.toPath(), listOf("张三|30", "李四|25"), StandardCharsets.UTF_8)

        val (pipeline, rows) = read("""
            path: "${uri(file)}"
            file_format: csv
            csv_delimiter: "|"
            schema_fields: ["name:string", "age:int"]
        """.trimIndent())
        PAssert.that(rows).containsInAnyOrder(person("张三", 30), person("李四", 25))
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `csv 写出再读回，值保持一致`() {
        val dir = tempDir("fs-csv-roundtrip")
        val rows = listOf(person("张,三", 30), person("李四", 25), person(null, null))

        write(rows, personSchema, """
            path: "${uri(dir)}"
            file_format: csv
            schema_fields: ["name:string", "age:int"]
            num_shards: 1
        """.trimIndent()).run().waitUntilFinish()

        val (pipeline, back) = read("""
            path: "${uri(dir)}/${'*'}.csv"
            file_format: csv
            schema_fields: ["name:string", "age:int"]
        """.trimIndent())
        PAssert.that(back).containsInAnyOrder(rows)
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `csv 写出可以带表头`() {
        val dir = tempDir("fs-csv-header")

        write(listOf(person("张三", 30)), personSchema, """
            path: "${uri(dir)}"
            file_format: csv
            header: true
            schema_fields: ["name:string", "age:int"]
            num_shards: 1
        """.trimIndent()).run().waitUntilFinish()

        assertEquals(listOf("name,age", "张三,30"), outputLines(dir))
    }

    // ---------- xlsx ----------

    @Test
    fun `xlsx 写出再读回，值保持一致`() {
        val dir = tempDir("fs-xlsx")
        val rows = (1..50).map { person("name-$it", it) }

        write(rows, personSchema, """
            path: "${uri(dir)}"
            file_format: xlsx
            schema_fields: ["name:string", "age:int"]
            num_shards: 1
        """.trimIndent()).run().waitUntilFinish()

        val (pipeline, back) = read("""
            path: "${uri(dir)}/${'*'}.xlsx"
            file_format: xlsx
            schema_fields: ["name:string", "age:int"]
        """.trimIndent())
        PAssert.that(back).containsInAnyOrder(rows)
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `xlsx 带表头写出时首行是字段名`() {
        val dir = tempDir("fs-xlsx-header")

        write(listOf(person("张三", 30)), personSchema, """
            path: "${uri(dir)}"
            file_format: xlsx
            header: true
            schema_fields: ["name:string", "age:int"]
            num_shards: 1
        """.trimIndent()).run().waitUntilFinish()

        val (pipeline, back) = read("""
            path: "${uri(dir)}/${'*'}.xlsx"
            file_format: xlsx
            header: true
            schema_fields: ["name:string", "age:int"]
        """.trimIndent())
        PAssert.that(back).containsInAnyOrder(person("张三", 30))
        pipeline.run().waitUntilFinish()
    }

    // ---------- 解析错误 ----------

    @Test
    fun `解析失败的报错带上文件名 行号 列名`() {
        // 重构前只会抛一句 NumberFormatException: For input string: "abc"，
        // 几百万行的 CSV 里根本无从查起
        val dir = tempDir("fs-parse-error")
        val file = File(dir, "in.csv")
        Files.write(file.toPath(), listOf("张三,30", "李四,abc"), StandardCharsets.UTF_8)

        val (pipeline, rows) = read("""
            path: "${uri(file)}"
            file_format: csv
            schema_fields: ["name:string", "age:int"]
        """.trimIndent())
        PAssert.that(rows).empty()

        val error = runCatching { pipeline.run().waitUntilFinish() }.exceptionOrNull()
        val message = generateSequence(error) { it.cause }.mapNotNull { it.message }.joinToString(" | ")

        assertTrue("in.csv" in message, "报错里应带上文件名: $message")
        assertTrue("第 2 行" in message, "报错里应带上行号: $message")
        assertTrue("age" in message, "报错里应带上列名: $message")
    }
}
