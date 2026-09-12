package me.jayer.hdata.ftp

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
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * End-to-end tests for `ReadFromFtp` / `WriteToFtp`, running on DirectRunner + an in-process FTP server.
 *
 * @author wuya
 */
class FtpPipelineTest {

    private val personSchema: Schema = Schema.builder()
        .addNullableStringField("name")
        .addNullableInt32Field("age")
        .build()

    private fun line(text: String): Row = Row.withSchema(FTP_TEXT_SCHEMA).addValue(text).build()

    private fun person(name: String?, age: Int?): Row =
        Row.withSchema(personSchema).addValue(name).addValue(age).build()

    private fun config(yaml: String): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    private fun read(yaml: String): Pair<Pipeline, org.apache.beam.sdk.values.PCollection<Row>> {
        val pipeline = Pipeline.create()
        return pipeline to PCollectionRowTuple.empty(pipeline)
            .apply(FtpReadProvider().from(config(yaml)))
            .get(Tags.MAIN_OUTPUT)
    }

    private fun write(rows: List<Row>, schema: Schema, yaml: String): Pipeline {
        val pipeline = Pipeline.create()
        val input = pipeline.apply(Create.of(rows).withRowSchema(schema))
        PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(FtpWriteProvider().from(config(yaml)))
        return pipeline
    }

    @Test
    fun `text reads every line of the file`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("data/in.txt", "alpha\nbeta\ngamma\n")

            val (pipeline, rows) = read(ftp.readConfigYaml("/data/in.txt"))
            PAssert.that(rows).containsInAnyOrder(listOf("alpha", "beta", "gamma").map { line(it) })
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `the last line without a trailing newline is read too`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("data/in.txt", "alpha\nbeta")

            val (pipeline, rows) = read(ftp.readConfigYaml("/data/in.txt"))
            PAssert.that(rows).containsInAnyOrder(line("alpha"), line("beta"))
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `CRLF newlines do not leave a carriage return at line end`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("data/in.txt", "alpha\r\nbeta\r\n")

            val (pipeline, rows) = read(ftp.readConfigYaml("/data/in.txt"))
            PAssert.that(rows).containsInAnyOrder(line("alpha"), line("beta"))
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `adjacent shards after a byte range split have no overlap and no gaps`() {
        // This is the core of this refactor: the restriction changed from the fixed OffsetRange(0,1)
        // to a real byte range. If the row-ownership rule (the line crossing from belongs to the
        // previous shard) is wrong, this test would miss or duplicate lines.
        EmbeddedFtpServer().use { ftp ->
            val expected = (1..500).map { "line-%04d".format(it) }
            ftp.put("data/big.txt", expected.joinToString("\n", postfix = "\n"))

            val fn = me.jayer.hdata.ftp.transform.FtpReadFn(
                FtpConnection(host = "127.0.0.1", port = ftp.port, user = ftp.user, password = ftp.password),
                FtpReadConfig(host = "127.0.0.1", port = ftp.port, user = ftp.user, password = ftp.password, path = "/data/big.txt"),
            )
            val size = ftp.read("data/big.txt").toByteArray().size.toLong()
            val file = me.jayer.hdata.ftp.transform.FtpFile("/data/big.txt", size)

            // split into several segments by hand (simulating runtime splitting), read each and concat
            val bounds = listOf(0L, size / 3, size * 2 / 3, size)
            val collected = mutableListOf<String>()
            fn.setup()
            try {
                for (i in 0 until bounds.size - 1) {
                    val range = org.apache.beam.sdk.io.range.OffsetRange(bounds[i], bounds[i + 1])
                    fn.processElement(file, range.newTracker(), CollectingRows { collected.add(it.getString("content")!!) })
                }
            } finally {
                fn.tearDown()
            }

            assertEquals(expected, collected)
        }
    }

    @Test
    fun `when the split point lands exactly at a line start that line must not be lost`() {
        // A boundary exactly equal to a line's start offset is the easiest case to lose data:
        // the previous shard stopped when its position reached from; if this shard reads from from
        // and drops the first line, the whole line is read by neither side, yet the job still succeeds.
        // So we must read from from-1.
        EmbeddedFtpServer().use { ftp ->
            // each line is a fixed 10 bytes ("line-0001" + "\n"); split points at multiples of 10
            // must land at a line start
            val expected = (1..100).map { "line-%04d".format(it) }
            ftp.put("data/aligned.txt", expected.joinToString("\n", postfix = "\n"))

            val fn = me.jayer.hdata.ftp.transform.FtpReadFn(
                FtpConnection(host = "127.0.0.1", port = ftp.port, user = ftp.user, password = ftp.password),
                FtpReadConfig(host = "127.0.0.1", port = ftp.port, user = ftp.user, password = ftp.password, path = "/data/aligned.txt"),
            )
            val size = ftp.read("data/aligned.txt").toByteArray().size.toLong()
            assertEquals(1000L, size, "each line should be 10 bytes")
            val file = me.jayer.hdata.ftp.transform.FtpFile("/data/aligned.txt", size)

            val bounds = listOf(0L, 250L, 500L, 750L, size)
            val collected = mutableListOf<String>()
            fn.setup()
            try {
                for (i in 0 until bounds.size - 1) {
                    val range = org.apache.beam.sdk.io.range.OffsetRange(bounds[i], bounds[i + 1])
                    val tracker = range.newTracker()
                    fn.processElement(file, tracker, CollectingRows { collected.add(it.getString("content")!!) })
                    // each shard must satisfy checkDone()'s contract, otherwise the runtime throws directly
                    tracker.checkDone()
                }
            } finally {
                fn.tearDown()
            }

            assertEquals(expected, collected)
        }
    }

    @Test
    fun `wildcard filters the files under a directory`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("data/a.txt", "a1\n")
            ftp.put("data/b.txt", "b1\n")
            ftp.put("data/c.log", "c1\n")

            val (pipeline, rows) = read(ftp.readConfigYaml("/data", "file_pattern: \"*.txt\""))
            PAssert.that(rows).containsInAnyOrder(line("a1"), line("b1"))
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `csv parses by the declared type instead of always treating values as strings`() {
        // before the refactor csvLineToRow was row.addValue(raw): regardless of the declared type,
        // the pushed-in value was a string, so file_format=csv combined with any non-STRING field was broken
        EmbeddedFtpServer().use { ftp ->
            ftp.put("data/in.csv", "name,age\nAlice,30\nBob,25\n")

            val (pipeline, rows) = read(
                ftp.readConfigYaml(
                    "/data/in.csv",
                    """
                    file_format: csv
                    header: true
                    schema_fields: ["name:string", "age:int"]
                    """.trimIndent(),
                )
            )
            PAssert.that(rows).containsInAnyOrder(person("Alice", 30), person("Bob", 25))
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `a quoted comma in csv does not split the field`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("data/in.csv", "\"Alice, Jr.\",30\n")

            val (pipeline, rows) = read(
                ftp.readConfigYaml(
                    "/data/in.csv",
                    """
                    file_format: csv
                    schema_fields: ["name:string", "age:int"]
                    """.trimIndent(),
                )
            )
            PAssert.that(rows).containsInAnyOrder(person("Alice, Jr.", 30))
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `an unreadable file raises an error instead of being silently skipped`() {
        // before the refactor retrieveFile returning false only logged a warning and returned; the
        // whole file was dropped yet the job succeeded
        EmbeddedFtpServer().use { ftp ->
            ftp.put("data/in.txt", "x\n")

            val fn = me.jayer.hdata.ftp.transform.FtpReadFn(
                FtpConnection(host = "127.0.0.1", port = ftp.port, user = ftp.user, password = ftp.password),
                FtpReadConfig(host = "127.0.0.1", port = ftp.port, user = ftp.user, password = ftp.password, path = "/data"),
            )
            fn.setup()
            try {
                val error = runCatching {
                    fn.processElement(
                        me.jayer.hdata.ftp.transform.FtpFile("/data/missing.txt", 10),
                        org.apache.beam.sdk.io.range.OffsetRange(0, 10).newTracker(),
                        CollectingRows { },
                    )
                }.exceptionOrNull()
                assertTrue(error != null, "a file that cannot be read must throw an exception")
                assertTrue("missing.txt" in error.message!!, "the error must indicate which file: ${error.message}")
            } finally {
                fn.tearDown()
            }
        }
    }

    @Test
    fun `text can be read back completely after writing`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("out/.keep", "")
            val expected = (1..100).map { "line-$it" }

            write(expected.map { line(it) }, FTP_TEXT_SCHEMA, ftp.readConfigYaml("/out", "file_prefix: \"data\""))
                .run().waitUntilFinish()

            val written = ftp.list("out").filter { it.startsWith("data") }
                .flatMap { ftp.read("out/$it").lines() }
                .filter { it.isNotEmpty() }
            assertEquals(expected.sorted(), written.sorted())
        }
    }

    @Test
    fun `concurrent write shards do not interfere and the total row count is exact`() {
        // before the refactor every instance appended to the same file_name via appendFile: concurrent
        // appends would interleave the contents. DirectRunner splits 100 rows into many bundles, each
        // bundle a shard file.
        EmbeddedFtpServer().use { ftp ->
            ftp.put("out/.keep", "")
            val rows = (1..100).map { line("l-$it") }

            write(rows, FTP_TEXT_SCHEMA, ftp.readConfigYaml("/out", "file_prefix: \"data\"")).run().waitUntilFinish()

            val written = ftp.list("out").filter { it.startsWith("data") }
                .flatMap { ftp.read("out/$it").lines() }
                .filter { it.isNotEmpty() }
            assertEquals(rows.size, written.size, "total row count should be exactly right; actual shards: ${ftp.list("out")}")
            assertEquals((1..100).map { "l-$it" }.sorted(), written.sorted())
        }
    }

    @Test
    fun `no half-written tmp files are left behind`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("out/.keep", "")

            write((1..5).map { line("l-$it") }, FTP_TEXT_SCHEMA, ftp.readConfigYaml("/out"))
                .run().waitUntilFinish()

            assertTrue(ftp.list("out").none { it.endsWith(".tmp") }, "actual: ${ftp.list("out")}")
        }
    }

    @Test
    fun `csv values stay consistent after a write and read back`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("out/.keep", "")
            val rows = listOf(person("Alice, Jr.", 30), person("Bob", 25))

            write(rows, personSchema, ftp.readConfigYaml("/out", """
                file_prefix: "data"
                file_format: csv
                schema_fields: ["name:string", "age:int"]
            """.trimIndent())).run().waitUntilFinish()

            val (pipeline, back) = read(ftp.readConfigYaml("/out", """
                file_pattern: "data-*.csv"
                file_format: csv
                schema_fields: ["name:string", "age:int"]
            """.trimIndent()))
            PAssert.that(back).containsInAnyOrder(rows)
            pipeline.run().waitUntilFinish()
        }
    }
}

/** An [org.apache.beam.sdk.transforms.DoFn.OutputReceiver] that only collects output rows. */
private class CollectingRows(private val sink: (Row) -> Unit) : org.apache.beam.sdk.transforms.DoFn.OutputReceiver<Row> {
    override fun builder(value: Row): org.apache.beam.sdk.values.OutputBuilder<Row> = Builder(value, sink)

    private class Builder(
        private val value: Row,
        private val sink: (Row) -> Unit,
    ) : org.apache.beam.sdk.values.OutputBuilder<Row> {
        override fun output() = sink(value)
        override fun getValue(): Row = value
        override fun getTimestamp(): org.joda.time.Instant = org.joda.time.Instant.EPOCH
        override fun getWindows(): Collection<org.apache.beam.sdk.transforms.windowing.BoundedWindow> = emptyList()
        override fun getPaneInfo(): org.apache.beam.sdk.transforms.windowing.PaneInfo =
            org.apache.beam.sdk.transforms.windowing.PaneInfo.NO_FIRING
        override fun getRecordId(): String = ""
        override fun getOpenTelemetryContext(): io.opentelemetry.context.Context = io.opentelemetry.context.Context.root()
        override fun getRecordOffset(): Long = 0L
        override fun causedByDrain(): org.apache.beam.sdk.values.CausedByDrain = org.apache.beam.sdk.values.CausedByDrain.NORMAL
        override fun getValueKind(): org.apache.beam.sdk.values.ValueKind = org.apache.beam.sdk.values.ValueKind.INSERT
        override fun explodeWindows(): Iterable<org.apache.beam.sdk.values.WindowedValue<Row>> = listOf(this)
        override fun setValue(v: Row): org.apache.beam.sdk.values.OutputBuilder<Row> = this
        override fun setTimestamp(t: org.joda.time.Instant): org.apache.beam.sdk.values.OutputBuilder<Row> = this
        override fun setWindow(w: org.apache.beam.sdk.transforms.windowing.BoundedWindow): org.apache.beam.sdk.values.OutputBuilder<Row> = this
        override fun setWindows(w: Collection<org.apache.beam.sdk.transforms.windowing.BoundedWindow>): org.apache.beam.sdk.values.OutputBuilder<Row> = this
        override fun setPaneInfo(p: org.apache.beam.sdk.transforms.windowing.PaneInfo): org.apache.beam.sdk.values.OutputBuilder<Row> = this
        override fun setRecordId(r: String?): org.apache.beam.sdk.values.OutputBuilder<Row> = this
        override fun setRecordOffset(l: Long?): org.apache.beam.sdk.values.OutputBuilder<Row> = this
        override fun setCausedByDrain(c: org.apache.beam.sdk.values.CausedByDrain): org.apache.beam.sdk.values.OutputBuilder<Row> = this
        override fun setOpenTelemetryContext(c: io.opentelemetry.context.Context?): org.apache.beam.sdk.values.OutputBuilder<Row> = this
        override fun setValueKind(k: org.apache.beam.sdk.values.ValueKind): org.apache.beam.sdk.values.OutputBuilder<Row> = this
        override fun <OtherT> withValue(o: OtherT): org.apache.beam.sdk.values.WindowedValue<OtherT> =
            throw UnsupportedOperationException()
    }
}
