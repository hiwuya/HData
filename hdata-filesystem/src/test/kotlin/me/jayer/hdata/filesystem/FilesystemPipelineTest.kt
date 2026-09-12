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
import org.apache.poi.xssf.usermodel.XSSFWorkbook
import tools.jackson.databind.node.ObjectNode
import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * End-to-end tests for `ReadFromFilesystem` / `WriteToFilesystem`, running on DirectRunner against
 * local files only.
 *
 * Before the refactor this test class carried this comment:
 *
 * > `FilesystemWriteFn` opens a single output file with `overwrite=true` in `@Setup`; under
 * > DirectRunner the DoFn may be reused across instances / bundles, so multi-row writes truncate
 * > each other and lose data. The write path therefore only asserts "one file, one row".
 *
 * In other words the test was written **around that bug**. Once the actual writing moved to Beam's
 * `FileIO.write()`, every shard writes its own temp file and the rename only happens when all of
 * them succeeded, so multi-row writes no longer truncate each other and the full content can be
 * asserted directly here.
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

    /** Reads every shard under the output directory and merges them by content. */
    private fun outputLines(dir: File): List<String> =
        (dir.listFiles() ?: emptyArray())
            .filter { it.isFile && !it.name.startsWith(".") }
            .flatMap { it.readLines() }

    private fun outputFiles(dir: File): List<File> =
        (dir.listFiles() ?: emptyArray()).filter { it.isFile && !it.name.startsWith(".") }

    // ---------- text ----------

    @Test
    fun `text reads every line of the file`() {
        val dir = tempDir("fs-text-read")
        val file = File(dir, "in.txt")
        Files.write(file.toPath(), listOf("alpha", "beta", "gamma"), StandardCharsets.UTF_8)

        val (pipeline, rows) = read("""path: "${uri(file)}"""")
        PAssert.that(rows).containsInAnyOrder(listOf("alpha", "beta", "gamma").map { line(it) })
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `a wildcard reads several files at once`() {
        val dir = tempDir("fs-glob")
        Files.write(File(dir, "a.txt").toPath(), listOf("a1", "a2"), StandardCharsets.UTF_8)
        Files.write(File(dir, "b.txt").toPath(), listOf("b1"), StandardCharsets.UTF_8)
        // a non-matching suffix must not be read in
        Files.write(File(dir, "c.log").toPath(), listOf("c1"), StandardCharsets.UTF_8)

        val (pipeline, rows) = read("""path: "${uri(dir)}/${'*'}.txt"""")
        PAssert.that(rows).containsInAnyOrder(listOf("a1", "a2", "b1").map { line(it) })
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `text round-trips multiple written rows completely without truncating each other`() {
        // exactly the scenario the pre-refactor test deliberately avoided: multiple rows + multiple bundles
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
    fun `num_shards determines the number of output files`() {
        val dir = tempDir("fs-shards")

        write((1..20).map { line("l-$it") }, textSchema, """
            path: "${uri(dir)}"
            num_shards: 3
        """.trimIndent()).run().waitUntilFinish()

        assertEquals(3, outputFiles(dir).size)
        assertEquals(20, outputLines(dir).size)
    }

    @Test
    fun `file_prefix and the extension show up in the output file name`() {
        val dir = tempDir("fs-naming")

        write(listOf(line("x")), textSchema, """
            path: "${uri(dir)}"
            file_prefix: "orders"
            num_shards: 1
        """.trimIndent()).run().waitUntilFinish()

        val name = outputFiles(dir).single().name
        assertTrue(name.startsWith("orders") && name.endsWith(".txt"), "actual file name: $name")
    }

    // ---------- csv ----------

    @Test
    fun `csv parses by schema_fields and skips the header`() {
        val dir = tempDir("fs-csv-read")
        val file = File(dir, "in.csv")
        Files.write(file.toPath(), listOf("name,age", "Alice,30", "Bob,25"), StandardCharsets.UTF_8)

        val (pipeline, rows) = read("""
            path: "${uri(file)}"
            file_format: csv
            header: true
            schema_fields: ["name:string", "age:int"]
        """.trimIndent())
        PAssert.that(rows).containsInAnyOrder(person("Alice", 30), person("Bob", 25))
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `a quoted comma and newline in csv do not split a record`() {
        // precisely because of this, csv cannot be split by byte range the way text can
        val dir = tempDir("fs-csv-quote")
        val file = File(dir, "in.csv")
        file.writeText("name,age\n\"Alice, Jr.\",30\n\"Bob\nSmith\",25\n", StandardCharsets.UTF_8)

        val (pipeline, rows) = read("""
            path: "${uri(file)}"
            file_format: csv
            header: true
            schema_fields: ["name:string", "age:int"]
        """.trimIndent())
        PAssert.that(rows).containsInAnyOrder(person("Alice, Jr.", 30), person("Bob\nSmith", 25))
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `csv_delimiter really takes effect`() {
        // before the refactor CSVFormat.DEFAULT was hardcoded and the configured delimiter was never passed down
        val dir = tempDir("fs-csv-delim")
        val file = File(dir, "in.csv")
        Files.write(file.toPath(), listOf("Alice|30", "Bob|25"), StandardCharsets.UTF_8)

        val (pipeline, rows) = read("""
            path: "${uri(file)}"
            file_format: csv
            csv_delimiter: "|"
            schema_fields: ["name:string", "age:int"]
        """.trimIndent())
        PAssert.that(rows).containsInAnyOrder(person("Alice", 30), person("Bob", 25))
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `csv values stay consistent after a write and read back`() {
        val dir = tempDir("fs-csv-roundtrip")
        val rows = listOf(person("Alice, Jr.", 30), person("Bob", 25), person(null, null))

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
    fun `csv output can include a header`() {
        val dir = tempDir("fs-csv-header")

        write(listOf(person("Alice", 30)), personSchema, """
            path: "${uri(dir)}"
            file_format: csv
            header: true
            schema_fields: ["name:string", "age:int"]
            num_shards: 1
        """.trimIndent()).run().waitUntilFinish()

        assertEquals(listOf("name,age", "Alice,30"), outputLines(dir))
    }

    // ---------- xlsx ----------

    @Test
    fun `xlsx values stay consistent after a write and read back`() {
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
    fun `xlsx INT64 round-trips without precision loss beyond the exact Double range`() {
        val dir = tempDir("fs-xlsx-int64")
        val schema = Schema.builder().addInt64Field("id").build()
        val rows = listOf(Long.MIN_VALUE, -9_007_199_254_740_992L, 9_007_199_254_740_992L, Long.MAX_VALUE)
            .map { Row.withSchema(schema).addValue(it).build() }

        write(rows, schema, """
            path: "${uri(dir)}"
            file_format: xlsx
            schema_fields: ["id:long"]
            num_shards: 1
        """.trimIndent()).run().waitUntilFinish()

        val (pipeline, back) = read("""
            path: "${uri(dir)}/${'*'}.xlsx"
            file_format: xlsx
            schema_fields: ["id:long"]
        """.trimIndent())
        PAssert.that(back).containsInAnyOrder(rows)
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `xlsx written with a header has the field names in the first row`() {
        val dir = tempDir("fs-xlsx-header")

        write(listOf(person("Alice", 30)), personSchema, """
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
        PAssert.that(back).containsInAnyOrder(person("Alice", 30))
        pipeline.run().waitUntilFinish()
    }

    @Test
    fun `xlsx formulas are read by their cached result type`() {
        val dir = tempDir("fs-xlsx-formula")
        val file = File(dir, "formula.xlsx")
        XSSFWorkbook().use { workbook ->
            val row = workbook.createSheet("data").createRow(0)
            row.createCell(0).setCellFormula("1=1")
            row.createCell(1).setCellFormula("1+2")
            val evaluator = workbook.creationHelper.createFormulaEvaluator()
            row.forEach { evaluator.evaluateFormulaCell(it) }
            Files.newOutputStream(file.toPath()).use { workbook.write(it) }
        }

        val schema = Schema.builder().addBooleanField("matched").addInt32Field("total").build()
        val expected = Row.withSchema(schema).addValues(true, 3).build()
        val (pipeline, rows) = read("""
            path: "${uri(file)}"
            file_format: xlsx
            schema_fields: ["matched:boolean", "total:int"]
        """.trimIndent())

        PAssert.that(rows).containsInAnyOrder(expected)
        pipeline.run().waitUntilFinish()
    }

    // ---------- parse errors ----------

    @Test
    fun `a parse failure error carries the file name line number and column name`() {
        // before the refactor it only threw NumberFormatException: For input string: "abc",
        // which is impossible to trace in a CSV of millions of rows
        val dir = tempDir("fs-parse-error")
        val file = File(dir, "in.csv")
        Files.write(file.toPath(), listOf("Alice,30", "Bob,abc"), StandardCharsets.UTF_8)

        val (pipeline, rows) = read("""
            path: "${uri(file)}"
            file_format: csv
            schema_fields: ["name:string", "age:int"]
        """.trimIndent())
        PAssert.that(rows).empty()

        val error = runCatching { pipeline.run().waitUntilFinish() }.exceptionOrNull()
        val message = generateSequence(error) { it.cause }.mapNotNull { it.message }.joinToString(" | ")

        assertTrue("in.csv" in message, "the error should carry the file name: $message")
        assertTrue("line 2" in message, "the error should carry the line number: $message")
        assertTrue("age" in message, "the error should carry the column name: $message")
    }
}
