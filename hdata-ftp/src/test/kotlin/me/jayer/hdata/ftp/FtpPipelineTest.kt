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
 * `ReadFromFtp` / `WriteToFtp` 的端到端测试，跑在 DirectRunner + 进程内 FTP 服务器上。
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
    fun `text 读出文件的每一行`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("data/in.txt", "alpha\nbeta\ngamma\n")

            val (pipeline, rows) = read(ftp.readConfigYaml("/data/in.txt"))
            PAssert.that(rows).containsInAnyOrder(listOf("alpha", "beta", "gamma").map { line(it) })
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `没有末尾换行的最后一行也读得出来`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("data/in.txt", "alpha\nbeta")

            val (pipeline, rows) = read(ftp.readConfigYaml("/data/in.txt"))
            PAssert.that(rows).containsInAnyOrder(line("alpha"), line("beta"))
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `CRLF 换行不会在行尾留下回车符`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("data/in.txt", "alpha\r\nbeta\r\n")

            val (pipeline, rows) = read(ftp.readConfigYaml("/data/in.txt"))
            PAssert.that(rows).containsInAnyOrder(line("alpha"), line("beta"))
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `按字节区间切分后，相邻分片不重不漏`() {
        // 这是本次改造的核心：限制从固定的 OffsetRange(0,1) 换成了真正的字节区间。
        // 行的归属规则（跨过 from 的行归上一个分片）算错的话，这里会漏行或多行。
        EmbeddedFtpServer().use { ftp ->
            val expected = (1..500).map { "line-%04d".format(it) }
            ftp.put("data/big.txt", expected.joinToString("\n", postfix = "\n"))

            val fn = me.jayer.hdata.ftp.transform.FtpReadFn(
                FtpConnection(host = "127.0.0.1", port = ftp.port, user = ftp.user, password = ftp.password),
                FtpReadConfig(host = "127.0.0.1", port = ftp.port, user = ftp.user, password = ftp.password, path = "/data/big.txt"),
            )
            val size = ftp.read("data/big.txt").toByteArray().size.toLong()
            val file = me.jayer.hdata.ftp.transform.FtpFile("/data/big.txt", size)

            // 手工切成几段（模拟运行时的切分），逐段读出来再拼起来
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
    fun `通配符筛选目录下的文件`() {
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
    fun `csv 按声明的类型解析，而不是一律当字符串`() {
        // 重构前 csvLineToRow 是 row.addValue(raw)：不管声明成什么类型塞进去的都是字符串，
        // file_format=csv 配上任何非 STRING 字段都是坏的
        EmbeddedFtpServer().use { ftp ->
            ftp.put("data/in.csv", "name,age\n张三,30\n李四,25\n")

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
            PAssert.that(rows).containsInAnyOrder(person("张三", 30), person("李四", 25))
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `csv 里带引号的逗号不会把字段劈开`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("data/in.csv", "\"张,三\",30\n")

            val (pipeline, rows) = read(
                ftp.readConfigYaml(
                    "/data/in.csv",
                    """
                    file_format: csv
                    schema_fields: ["name:string", "age:int"]
                    """.trimIndent(),
                )
            )
            PAssert.that(rows).containsInAnyOrder(person("张,三", 30))
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `读不到的文件直接报错，而不是静默跳过`() {
        // 重构前 retrieveFile 返回 false 只打一条 warn 就 return，整个文件被丢掉，作业照常成功
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
                assertTrue(error != null, "读不到的文件必须抛异常")
                assertTrue("missing.txt" in error.message!!, "报错要指出是哪个文件: ${error.message}")
            } finally {
                fn.tearDown()
            }
        }
    }

    @Test
    fun `text 写出后能完整读回`() {
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
    fun `并发写入的分片互不干扰，总行数不多不少`() {
        // 重构前所有实例都往同一个 file_name 上 appendFile：并发追加会把内容交错在一起。
        // DirectRunner 会把 100 行拆成很多 bundle，每个 bundle 一个分片文件。
        EmbeddedFtpServer().use { ftp ->
            ftp.put("out/.keep", "")
            val rows = (1..100).map { line("l-$it") }

            write(rows, FTP_TEXT_SCHEMA, ftp.readConfigYaml("/out", "file_prefix: \"data\"")).run().waitUntilFinish()

            val written = ftp.list("out").filter { it.startsWith("data") }
                .flatMap { ftp.read("out/$it").lines() }
                .filter { it.isNotEmpty() }
            assertEquals(rows.size, written.size, "总行数应不多不少，实际分片: ${ftp.list("out")}")
            assertEquals((1..100).map { "l-$it" }.sorted(), written.sorted())
        }
    }

    @Test
    fun `没有留下 tmp 半成品文件`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("out/.keep", "")

            write((1..5).map { line("l-$it") }, FTP_TEXT_SCHEMA, ftp.readConfigYaml("/out"))
                .run().waitUntilFinish()

            assertTrue(ftp.list("out").none { it.endsWith(".tmp") }, "实际: ${ftp.list("out")}")
        }
    }

    @Test
    fun `csv 写出再读回，值保持一致`() {
        EmbeddedFtpServer().use { ftp ->
            ftp.put("out/.keep", "")
            val rows = listOf(person("张,三", 30), person("李四", 25))

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

/** 只收集输出行的 [org.apache.beam.sdk.transforms.DoFn.OutputReceiver]。 */
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
