package me.jayer.hdata.iceberg

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.parseSchemaFields
import me.jayer.hdata.iceberg.parseAggregations
import me.jayer.hdata.iceberg.aggregateSchema
import me.jayer.hdata.iceberg.AggregateCombineFn
import me.jayer.hdata.iceberg.AggregateToRowFn
import me.jayer.hdata.iceberg.PartialAgg
import me.jayer.hdata.iceberg.transform.IcebergAggregateEnumeratorFn
import me.jayer.hdata.iceberg.transform.IcebergReadFileFn
import me.jayer.hdata.iceberg.transform.IcebergSplitEnumeratorFn
import me.jayer.hdata.iceberg.transform.IcebergFileSplit
import me.jayer.hdata.iceberg.transform.IcebergWriteFn
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Combine
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.coders.SerializableCoder
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import java.nio.file.Files
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.apache.iceberg.catalog.TableIdentifier
import org.apache.iceberg.types.Types

/**
 * 真正的端到端往返：在本地临时目录起一个 HadoopCatalog，把行写进 Iceberg 再读回来。
 */
class IcebergPipelineTest {

    private val fields = listOf("id:INT64", "name:STRING", "age:INT32", "score:FLOAT64", "ok:BOOLEAN")
    private val beamSchema = Schema.builder()
        .addInt64Field("id").addStringField("name").addInt32Field("age").addDoubleField("score").addBooleanField("ok")
        .build()

    @Test
    fun `写入后读出往返一致`() {
        val warehouse = Files.createTempDirectory("iceberg-wh").toString()
        val rows = listOf(
            Row.withSchema(beamSchema).addValue(1L).addValue("alice").addValue(30).addValue(1.5).addValue(true).build(),
            Row.withSchema(beamSchema).addValue(2L).addValue("bob").addValue(40).addValue(2.5).addValue(false).build(),
        )

        val writeConfig = IcebergWriteConfig(warehouse = warehouse, table = "db.users", schemaFields = fields)
        val wp = Pipeline.create()
        val win = wp.apply(Create.of(rows).withRowSchema(beamSchema))
        win.apply(
            ParDo.of(IcebergWriteFn(writeConfig, ErrorSchemas.of(beamSchema), deadLetter = false, transformName = "WriteToIceberg")),
        ).setRowSchema(ErrorSchemas.of(beamSchema))
        wp.run()

        val readConfig = IcebergReadConfig(warehouse = warehouse, table = "db.users", schemaFields = fields)
        val readSchema = readConfig.outputSchema()
        val rp = Pipeline.create()
        val trigger = rp.apply(Create.of(listOf("")))
        val out = trigger.apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig))).apply(ParDo.of(IcebergReadFileFn(readConfig, readSchema, parseSchemaFields(fields)))).setRowSchema(readSchema)
        PAssert.that(out).satisfies { output ->
            val list = output.toList()
            assertEquals(2, list.size)
            assertEquals(setOf("alice", "bob"), list.map { it.getString("name") }.toSet())
            assertEquals(setOf(1L, 2L), list.map { it.getInt64("id") }.toSet())
            null
        }
        rp.run()
    }

    @Test
    fun `外部表字段 ID 不从 1 开始时仍按表 schema 写入`() {
        val warehouse = Files.createTempDirectory("iceberg-external-ids").toString()
        IcebergCatalogs.openCatalog(warehouse, "hdata").use { catalog ->
            val externalSchema = org.apache.iceberg.Schema(
                Types.NestedField.optional(1, "obsolete", Types.StringType.get()),
                Types.NestedField.optional(2, "id", Types.LongType.get()),
                Types.NestedField.optional(3, "name", Types.StringType.get()),
            )
            val table = catalog.createTable(TableIdentifier.parse("db.external"), externalSchema)
            // HadoopCatalog 创建表时会重新分配传入 schema 的 ID；通过演进后删除旧列，才能稳定制造
            // 当前字段 ID 不从 1 开始的真实外部表。
            table.updateSchema().deleteColumn("obsolete").commit()
            assertEquals(listOf(2, 3), table.schema().columns().map { it.fieldId() })
        }
        val schema = Schema.builder().addInt64Field("id").addStringField("name").build()
        val row = Row.withSchema(schema).addValue(7L).addValue("alice").build()
        val config = IcebergWriteConfig(
            warehouse = warehouse,
            table = "db.external",
            schemaFields = listOf("id:INT64", "name:STRING"),
        )
        val pipeline = Pipeline.create()
        pipeline.apply(Create.of(row).withRowSchema(schema))
            .apply(ParDo.of(IcebergWriteFn(config, ErrorSchemas.of(schema), false, "WriteToIceberg")))
            .setRowSchema(ErrorSchemas.of(schema))
        pipeline.run().waitUntilFinish()

        IcebergCatalogs.openCatalog(warehouse, "hdata").use { catalog ->
            assertEquals(listOf(2, 3), catalog.loadTable(TableIdentifier.parse("db.external")).schema().columns().map { it.fieldId() })
        }

        val readConfig = IcebergReadConfig(
            warehouse = warehouse,
            table = "db.external",
            schemaFields = listOf("id:INT64", "name:STRING"),
        )
        val readPipeline = Pipeline.create()
        val output = readPipeline.apply(Create.of(""))
            .apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig))).apply(ParDo.of(IcebergReadFileFn(readConfig, schema, parseSchemaFields(readConfig.schemaFields))))
            .setRowSchema(schema)
        PAssert.that(output).satisfies { rows ->
            val result = rows.single()
            assertEquals(7L, result.getInt64("id"))
            assertEquals("alice", result.getString("name"))
            null
        }
        readPipeline.run().waitUntilFinish()
    }

    @Test
    fun `BYTES 列往返不丢`() {
        // Iceberg 的 binary 要 ByteBuffer、Beam 的 BYTES 要 ByteArray，
        // 两个方向的换算写反了的话，写入端会拿 ByteArray 去填 binary 列、
        // 读取端会把 ByteBuffer 塞进 Beam Row——两边都是运行期才炸
        val warehouse = Files.createTempDirectory("iceberg-bytes").toString()
        val fields = listOf("id:INT64", "payload:BYTES")
        val schema = Schema.builder().addInt64Field("id").addByteArrayField("payload").build()
        val payload = byteArrayOf(0, 1, 2, 127, -1, -128)
        val row = Row.withSchema(schema).addValue(7L).addValue(payload).build()

        val writeConfig = IcebergWriteConfig(warehouse = warehouse, table = "db.blobs", schemaFields = fields)
        val wp = Pipeline.create()
        wp.apply(Create.of(listOf(row)).withRowSchema(schema))
            .apply(ParDo.of(IcebergWriteFn(writeConfig, ErrorSchemas.of(schema), false, "WriteToIceberg")))
            .setRowSchema(ErrorSchemas.of(schema))
        wp.run().waitUntilFinish()

        val readConfig = IcebergReadConfig(warehouse = warehouse, table = "db.blobs", schemaFields = fields)
        val readSchema = readConfig.outputSchema()
        val rp = Pipeline.create()
        val out = rp.apply(Create.of(listOf("")))
            .apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig))).apply(ParDo.of(IcebergReadFileFn(readConfig, readSchema, parseSchemaFields(fields))))
            .setRowSchema(readSchema)
        PAssert.that(out).satisfies { output ->
            val list = output.toList()
            assertEquals(1, list.size)
            assertContentEquals(payload, list.single().getBytes("payload"))
            null
        }
        rp.run().waitUntilFinish()
    }

    @Test
    fun `write_mode overwrite 会先清空表，而不是悄悄追加`() {
        // overwrite 之前只是被 validate 收下就丢掉，实际走的还是 append：
        // 跑两遍就有两份数据，而作业状态一直是成功
        val warehouse = Files.createTempDirectory("iceberg-overwrite").toString()
        val first = Row.withSchema(beamSchema).addValue(1L).addValue("old").addValue(30).addValue(1.5).addValue(true).build()
        val second = Row.withSchema(beamSchema).addValue(2L).addValue("new").addValue(40).addValue(2.5).addValue(false).build()

        write(warehouse, "db.t", listOf(first), "append")
        write(warehouse, "db.t", listOf(second), "overwrite")

        val readConfig = IcebergReadConfig(warehouse = warehouse, table = "db.t", schemaFields = fields)
        val readSchema = readConfig.outputSchema()
        val rp = Pipeline.create()
        val out = rp.apply(Create.of(listOf("")))
            .apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig))).apply(ParDo.of(IcebergReadFileFn(readConfig, readSchema, parseSchemaFields(fields))))
            .setRowSchema(readSchema)
        PAssert.that(out).satisfies { output ->
            val names = output.toList().map { it.getString("name") }
            assertEquals(listOf("new"), names, "overwrite 之后表里只应剩下本次写入的数据")
            null
        }
        rp.run().waitUntilFinish()
    }

    /** 走完整的 provider 链路，这样 overwrite 的清表步骤（side input）也一并覆盖到。 */
    private fun write(warehouse: String, table: String, rows: List<Row>, mode: String) {
        val config = IcebergWriteConfig(
            warehouse = warehouse,
            table = table,
            schemaFields = fields,
            writeMode = mode,
        )
        val pipeline = Pipeline.create()
        val input = pipeline.apply(Create.of(rows).withRowSchema(beamSchema))
        PCollectionRowTuple.of(Tags.MAIN_INPUT, input)
            .apply(IcebergWriteProvider().from(TransformConfig("WriteToIceberg", configNode(config))))
        pipeline.run().waitUntilFinish()
    }

    private fun configNode(config: IcebergWriteConfig): ObjectNode =
        SpecMappers.CONFIG.valueToTree(config)

    private fun configNode(config: IcebergReadConfig): ObjectNode =
        SpecMappers.CONFIG.valueToTree(config)

    @Test
    fun `catalog_name 真的生效`() {
        // catalog_name 要真的传进 Iceberg catalog 的初始化，而不是被写死成 "hadoop"：
        // 写死的话配置项等于收下就丢掉，catalog 维度的指标/表标识全错
        val warehouse = Files.createTempDirectory("iceberg-cat").toString()
        IcebergCatalogs.openCatalog(warehouse, "mycatalog").use { catalog ->
            assertEquals("mycatalog", catalog.name())
        }
    }

    @Test
    fun `两次 append 作业数据叠加不互相覆盖`() {
        // 每个 bundle 落一个带 UUID 的数据文件，所以同一张表跑两次 append 应该叠加成 2 行，
        // 而不是因为文件名撞了把第一次的结果盖掉（那种情况作业状态还是成功，但数据丢了）
        val warehouse = Files.createTempDirectory("iceberg-append").toString()
        val r1 = Row.withSchema(beamSchema).addValue(1L).addValue("a").addValue(30).addValue(1.5).addValue(true).build()
        val r2 = Row.withSchema(beamSchema).addValue(2L).addValue("b").addValue(40).addValue(2.5).addValue(false).build()
        write(warehouse, "db.append", listOf(r1), "append")
        write(warehouse, "db.append", listOf(r2), "append")

        val readConfig = IcebergReadConfig(warehouse = warehouse, table = "db.append", schemaFields = fields)
        val readSchema = readConfig.outputSchema()
        val rp = Pipeline.create()
        val out = rp.apply(Create.of(listOf("")))
            .apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig))).apply(ParDo.of(IcebergReadFileFn(readConfig, readSchema, parseSchemaFields(fields))))
            .setRowSchema(readSchema)
        PAssert.that(out).satisfies { output ->
            val list = output.toList()
            assertEquals(2, list.size, "两次 append 应叠加成 2 行，而非互相覆盖")
            assertEquals(setOf("a", "b"), list.map { it.getString("name") }.toSet())
            null
        }
        rp.run().waitUntilFinish()
    }

    @Test
    fun `读取按数据文件切分并行`() {
        // 每次 append 落一个独立的数据文件，所以同一张表写 3 次应有 3 个数据文件；
        // 并行读的基本单元就是数据文件，枚举出来的 split 数必须等于数据文件数，
        // 否则"按文件并行"只是嘴上说说（和旧实现整表单 DoFn 读区分不开）
        val warehouse = Files.createTempDirectory("iceberg-split").toString()
        val r1 = Row.withSchema(beamSchema).addValue(1L).addValue("a").addValue(30).addValue(1.5).addValue(true).build()
        val r2 = Row.withSchema(beamSchema).addValue(2L).addValue("b").addValue(40).addValue(2.5).addValue(false).build()
        val r3 = Row.withSchema(beamSchema).addValue(3L).addValue("c").addValue(50).addValue(3.5).addValue(true).build()
        write(warehouse, "db.split", listOf(r1), "append")
        write(warehouse, "db.split", listOf(r2), "append")
        write(warehouse, "db.split", listOf(r3), "append")

        val readConfig = IcebergReadConfig(warehouse = warehouse, table = "db.split", schemaFields = fields)
        val p = Pipeline.create()
        val splits = p.apply(Create.of(listOf("")))
            .apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig)))
        PAssert.that(splits).satisfies { output ->
            val list = output.toList()
            assertEquals(3, list.size, "3 个数据文件应枚举出 3 个 split")
            require(list.all { it is IcebergFileSplit && it.path.isNotBlank() })
            null
        }
        p.run().waitUntilFinish()
    }

    @Test
    fun `大文件按 split_size 细分成多个并行分片`() {
        // 单个数据文件超过 split_size 时，枚举端要按字节区间把它切成多个互不重叠的并行分片
        // （AVRO 按同步块切分，不重不漏）；否则"按文件并行"对大文件还是退化成单线程读。
        val warehouse = Files.createTempDirectory("iceberg-rgsplit").toString()
        // 少量行（单 bundle 写出，避开多 bundle 重试时的 metadata 版本竞争），但 AVRO 文件本身
        // 带 header + sync 标记，远大于下面的 split_size，足以被切成多个并行分片
        val rows = (1L..8L).map { id ->
            Row.withSchema(beamSchema).addValue(id).addValue("name-$id").addValue(30).addValue(1.5).addValue(true).build()
        }
        write(warehouse, "db.rgsplit", rows, "append")

        // split_size 压到很小，强制单文件切成多个并行分片
        val readConfig = IcebergReadConfig(warehouse = warehouse, table = "db.rgsplit", schemaFields = fields, splitSize = 16)
        val p = Pipeline.create()
        val splits = p.apply(Create.of(listOf(""))).apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig)))
        PAssert.that(splits).satisfies { out ->
            val list = out.toList()
            assertTrue(list.size > 1, "单文件应被细分成多个 split（实际 ${list.size}）")
            list.forEach { s -> assertTrue(s.start >= 0 && s.length > 0, "split 必须落在文件内且非空") }
            null
        }
        p.run().waitUntilFinish()

        // 细分后整表读回来行数不重不漏（每个分片按同步块读各自那一份）
        val rp = Pipeline.create()
        val readSchema = readConfig.outputSchema()
        val out = rp.apply(Create.of(listOf("")))
            .apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig)))
            .apply(ParDo.of(IcebergReadFileFn(readConfig, readSchema, parseSchemaFields(fields))))
            .setRowSchema(readSchema)
        PAssert.that(out).satisfies { o ->
            assertEquals(8, o.toList().size, "细分后读出 8 行，不重不漏")
            null
        }
        rp.run().waitUntilFinish()
    }

    @Test
    fun `filter 下推裁剪不匹配的 split 并过滤行`() {
        // 谓词下推应同时做到两件事：(1) manifest 级裁剪——整文件都不匹配时直接不枚举该 split；
        // (2) 读端对每行求残留谓词，丢掉不匹配的行。否则"下推"只是嘴上说说。
        val warehouse = Files.createTempDirectory("iceberg-filter").toString()
        // 文件1 全是不匹配的行（age < 40），应被 manifest 级裁剪整个文件
        write(warehouse, "db.filter", listOf(
            Row.withSchema(beamSchema).addValue(1L).addValue("a").addValue(30).addValue(1.5).addValue(true).build(),
            Row.withSchema(beamSchema).addValue(2L).addValue("b").addValue(35).addValue(1.5).addValue(true).build(),
        ), "append")
        // 文件2 有匹配的行
        write(warehouse, "db.filter", listOf(
            Row.withSchema(beamSchema).addValue(3L).addValue("c").addValue(40).addValue(2.5).addValue(false).build(),
            Row.withSchema(beamSchema).addValue(4L).addValue("d").addValue(50).addValue(3.5).addValue(true).build(),
        ), "append")

        val baseConfig = IcebergReadConfig(warehouse = warehouse, table = "db.filter", schemaFields = fields)
        val readConfig = baseConfig.copy(filter = "age >= 40")
        // 枚举端：filter 下推后仍能正常枚举出 split（manifest 级裁剪在底层发生）
        val p = Pipeline.create()
        val splits = p.apply(Create.of(listOf(""))).apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig)))
        PAssert.that(splits).satisfies { out ->
            assertTrue(out.toList().isNotEmpty(), "filter 后才枚举出 split")
            null
        }
        p.run().waitUntilFinish()

        // 读端：只返回 age >= 40 的两行（残留谓词对每行生效，分区列/数据列都过滤）
        val rp = Pipeline.create()
        val readSchema = readConfig.outputSchema()
        val out = rp.apply(Create.of(listOf("")))
            .apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig)))
            .apply(ParDo.of(IcebergReadFileFn(readConfig, readSchema, parseSchemaFields(fields))))
            .setRowSchema(readSchema)
        PAssert.that(out).satisfies { o ->
            val list = o.toList()
            assertEquals(2, list.size, "只返回匹配 filter 的行")
            assertEquals(setOf(40, 50), list.map { it.getInt32("age") }.toSet())
            null
        }
        rp.run().waitUntilFinish()
    }

    @Test
    fun `limit 跨文件取得精确行数且在过滤后计数`() {
        // 第一个文件没有匹配行；旧实现只枚举第一个文件，会错误返回 0 行。全局 limit 必须继续扫描
        // 后续文件，并在真正产出 3 条匹配行后停止。
        val warehouse = Files.createTempDirectory("iceberg-limit").toString()
        val a = (1L..5L).map { id ->
            Row.withSchema(beamSchema).addValue(id).addValue("a$id").addValue(30).addValue(1.5).addValue(true).build()
        }
        val b = (6L..10L).map { id ->
            Row.withSchema(beamSchema).addValue(id).addValue("b$id").addValue(30).addValue(1.5).addValue(true).build()
        }
        write(warehouse, "db.limit", a, "append")
        write(warehouse, "db.limit", b, "append")
        val readConfig = IcebergReadConfig(
            warehouse = warehouse,
            table = "db.limit",
            schemaFields = fields,
            filter = "id >= 6",
            limit = 3,
        )
        val rp = Pipeline.create()
        val out = PCollectionRowTuple.empty(rp)
            .apply(IcebergReadProvider().from(TransformConfig("ReadFromIceberg", configNode(readConfig))))
            .get(Tags.MAIN_OUTPUT)
        PAssert.that(out).satisfies { o ->
            val list = o.toList()
            assertEquals(3, list.size, "limit=3 应在过滤后精确返回 3 行")
            assertTrue(list.all { checkNotNull(it.getInt64("id")) >= 6 })
            null
        }
        rp.run().waitUntilFinish()
    }

    @Test
    fun `聚合下推 count_min_max 取自文件统计不读数据`() {
        // COUNT/MIN/MAX 直接取自数据文件元数据（recordCount / lower_bounds / upper_bounds），
        // 根本不碰数据文件内容——这才是真正的存储层下推。
        val warehouse = Files.createTempDirectory("iceberg-agg").toString()
        val rows = (1L..5L).map { id ->
            Row.withSchema(beamSchema).addValue(id).addValue("n$id").addValue((10 * id).toInt()).addValue(1.5).addValue(true).build()
        }
        write(warehouse, "db.agg", rows, "append")

        val readConfig = IcebergReadConfig(
            warehouse = warehouse,
            table = "db.agg",
            schemaFields = fields,
            aggregations = listOf("count", "min:age", "max:age"),
        )
        val specs = parseAggregations(readConfig.aggregations)
        val catalog = IcebergCatalogs.openCatalog(warehouse, "hdata")
        val table = IcebergCatalogs.loadTable(catalog, "db.agg")
        val outSchema = aggregateSchema(specs, table)
        runCatching { catalog.close() }

        val rp = Pipeline.create()
        val trigger = rp.apply(Create.of(listOf("")))
        val partials = trigger.apply(ParDo.of(IcebergAggregateEnumeratorFn(readConfig, specs)))
        partials.setCoder(SerializableCoder.of(PartialAgg::class.java))
        val merged = partials.apply(Combine.globally(AggregateCombineFn(specs)))
        merged.setCoder(SerializableCoder.of(PartialAgg::class.java))
        val out = merged.apply(ParDo.of(AggregateToRowFn(specs, outSchema))).setRowSchema(outSchema)
        PAssert.that(out).satisfies { o ->
            val list = o.toList()
            assertEquals(1, list.size, "聚合应只输出一行")
            val row = list[0]
            assertEquals(5L, row.getInt64("count"))
            assertEquals(10, row.getInt32("min_age"))
            assertEquals(50, row.getInt32("max_age"))
            null
        }
        rp.run().waitUntilFinish()
    }

    @Test
    fun `聚合下推 sum_avg 按列累加并全局归并`() {
        // SUM/AVG 没有数据文件级统计，只能投影列后在读取端逐文件累加，再跨文件全局归并。
        val warehouse = Files.createTempDirectory("iceberg-agg-sum").toString()
        val rows = (1L..5L).map { id ->
            Row.withSchema(beamSchema).addValue(id).addValue("n$id").addValue((10 * id).toInt()).addValue(1.5).addValue(true).build()
        }
        write(warehouse, "db.agg2", rows, "append")

        val readConfig = IcebergReadConfig(
            warehouse = warehouse,
            table = "db.agg2",
            schemaFields = fields,
            aggregations = listOf("count", "sum:age", "avg:age"),
        )
        val specs = parseAggregations(readConfig.aggregations)
        val catalog = IcebergCatalogs.openCatalog(warehouse, "hdata")
        val table = IcebergCatalogs.loadTable(catalog, "db.agg2")
        val outSchema = aggregateSchema(specs, table)
        runCatching { catalog.close() }

        val rp = Pipeline.create()
        val trigger = rp.apply(Create.of(listOf("")))
        val partials = trigger.apply(ParDo.of(IcebergAggregateEnumeratorFn(readConfig, specs)))
        partials.setCoder(SerializableCoder.of(PartialAgg::class.java))
        val merged = partials.apply(Combine.globally(AggregateCombineFn(specs)))
        merged.setCoder(SerializableCoder.of(PartialAgg::class.java))
        val out = merged.apply(ParDo.of(AggregateToRowFn(specs, outSchema))).setRowSchema(outSchema)
        PAssert.that(out).satisfies { o ->
            val row = o.toList().single()
            assertEquals(5L, row.getInt64("count"))
            assertEquals(150.0, row.getDouble("sum_age"))
            assertEquals(30.0, row.getDouble("avg_age"))
            null
        }
        rp.run().waitUntilFinish()
    }

    @Test
    fun `聚合下推带 filter 只统计匹配行`() {
        // 回归：聚合枚举端曾完全忽略 filter——COUNT 直接拿整文件的 recordCount、
        // MIN/MAX/SUM/AVG 在不匹配的行上算，作业成功但数字是错的。
        // 数据 age = 10/20/30/40/50，filter "age > 30" 只匹配 40 与 50 两行。
        val warehouse = Files.createTempDirectory("iceberg-agg-filter").toString()
        val rows = (1L..5L).map { id ->
            Row.withSchema(beamSchema).addValue(id).addValue("n$id").addValue((10 * id).toInt()).addValue(1.5).addValue(true).build()
        }
        write(warehouse, "db.aggf", rows, "append")

        val readConfig = IcebergReadConfig(
            warehouse = warehouse,
            table = "db.aggf",
            schemaFields = fields,
            filter = "age > 30",
            aggregations = listOf("count", "min:age", "max:age", "sum:age", "avg:age"),
        )
        val specs = parseAggregations(readConfig.aggregations)
        val catalog = IcebergCatalogs.openCatalog(warehouse, "hdata")
        val table = IcebergCatalogs.loadTable(catalog, "db.aggf")
        val outSchema = aggregateSchema(specs, table)
        runCatching { catalog.close() }

        val rp = Pipeline.create()
        val trigger = rp.apply(Create.of(listOf("")))
        val partials = trigger.apply(ParDo.of(IcebergAggregateEnumeratorFn(readConfig, specs)))
        partials.setCoder(SerializableCoder.of(PartialAgg::class.java))
        val merged = partials.apply(Combine.globally(AggregateCombineFn(specs)))
        merged.setCoder(SerializableCoder.of(PartialAgg::class.java))
        val out = merged.apply(ParDo.of(AggregateToRowFn(specs, outSchema))).setRowSchema(outSchema)
        PAssert.that(out).satisfies { o ->
            val row = o.toList().single()
            assertEquals(2L, row.getInt64("count"))
            assertEquals(40, row.getInt32("min_age"))
            assertEquals(50, row.getInt32("max_age"))
            assertEquals(90.0, row.getDouble("sum_age"))
            assertEquals(45.0, row.getDouble("avg_age"))
            null
        }
        rp.run().waitUntilFinish()

        // 聚合列写错时在构图阶段就报清楚，而不是 NPE
        val badSpecs = parseAggregations(listOf("min:no_such_col"))
        val ex = assertFailsWith<IllegalArgumentException> { aggregateSchema(badSpecs, table) }
        assertTrue(ex.message!!.contains("no_such_col"))
        // sum/avg 拒绝非数值列
        val strSpecs = parseAggregations(listOf("sum:name"))
        assertFailsWith<IllegalArgumentException> { aggregateSchema(strSpecs, table) }
    }
}
