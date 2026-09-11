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
 * A genuine end-to-end round trip: spin up a HadoopCatalog in a local temp directory, write rows into Iceberg, and
 * read them back.
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
            // HadoopCatalog reassigns the IDs of the schema passed in when creating a table; only by evolving the
            // schema and then dropping the old column can we reliably produce a real external table whose current
            // field IDs do not start at 1.
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
        // Iceberg's binary wants a ByteBuffer while Beam's BYTES wants a ByteArray; if the conversion in either
        // direction is written backwards, the write side would fill a binary column with a ByteArray and the read side
        // would stuff a ByteBuffer into a Beam Row — both of which only blow up at runtime.
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
        // overwrite used to be merely accepted by validate and then discarded, with append taken in practice:
        // running twice left two copies of the data while the job status stayed SUCCESS.
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
            assertEquals(listOf("new"), names, "after overwrite the table should contain only the data written this run")
            null
        }
        rp.run().waitUntilFinish()
    }

    /** Goes through the full provider chain, so the overwrite table-clear step (side input) is covered as well. */
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
        // catalog_name must actually be passed into the Iceberg catalog's initialization, not hardcoded to "hadoop":
        // hardcoding it means the config option is accepted but discarded, and catalog-level metrics/table
        // identifiers all end up wrong.
        val warehouse = Files.createTempDirectory("iceberg-cat").toString()
        IcebergCatalogs.openCatalog(warehouse, "mycatalog").use { catalog ->
            assertEquals("mycatalog", catalog.name())
        }
    }

    @Test
    fun `两次 append 作业数据叠加不互相覆盖`() {
        // Each bundle writes a data file with a UUID, so running append twice on the same table should accumulate to
        // 2 rows, rather than a file-name collision overwriting the first run's result (in which case the job status
        // is still SUCCESS but data is lost).
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
            assertEquals(2, list.size, "two appends should accumulate to 2 rows, not overwrite each other")
            assertEquals(setOf("a", "b"), list.map { it.getString("name") }.toSet())
            null
        }
        rp.run().waitUntilFinish()
    }

    @Test
    fun `读取按数据文件切分并行`() {
        // Each append writes a separate data file, so writing 3 times to the same table should yield 3 data files;
        // the basic unit of parallel reads is the data file, so the number of enumerated splits must equal the number
        // of data files, otherwise "per-file parallelism" is just talk (indistinguishable from the old whole-table
        // single-DoFn read).
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
            assertEquals(3, list.size, "3 data files should enumerate into 3 splits")
            require(list.all { it is IcebergFileSplit && it.path.isNotBlank() })
            null
        }
        p.run().waitUntilFinish()
    }

    @Test
    fun `大文件按 split_size 细分成多个并行分片`() {
        // When a single data file exceeds split_size, the enumerator must cut it by byte range into multiple
        // non-overlapping parallel shards (AVRO splits on sync blocks, with nothing duplicated or lost); otherwise
        // "per-file parallelism" still degrades to a single-threaded read for large files.
        val warehouse = Files.createTempDirectory("iceberg-rgsplit").toString()
        // A small number of rows (written in a single bundle, avoiding the metadata version race of multi-bundle
        // retries), but the AVRO file itself carries a header + sync markers, far larger than the split_size below,
        // enough to be cut into multiple parallel shards.
        val rows = (1L..8L).map { id ->
            Row.withSchema(beamSchema).addValue(id).addValue("name-$id").addValue(30).addValue(1.5).addValue(true).build()
        }
        write(warehouse, "db.rgsplit", rows, "append")

        // Squeeze split_size very small to force a single file to be cut into multiple parallel shards.
        val readConfig = IcebergReadConfig(warehouse = warehouse, table = "db.rgsplit", schemaFields = fields, splitSize = 16)
        val p = Pipeline.create()
        val splits = p.apply(Create.of(listOf(""))).apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig)))
        PAssert.that(splits).satisfies { out ->
            val list = out.toList()
            assertTrue(list.size > 1, "a single file should be subdivided into multiple splits (actual ${list.size})")
            list.forEach { s -> assertTrue(s.start >= 0 && s.length > 0, "a split must fall within the file and be non-empty") }
            null
        }
        p.run().waitUntilFinish()

        // After subdivision, reading the whole table back yields the exact row count with nothing duplicated or lost (each shard reads its own portion by sync block).
        val rp = Pipeline.create()
        val readSchema = readConfig.outputSchema()
        val out = rp.apply(Create.of(listOf("")))
            .apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig)))
            .apply(ParDo.of(IcebergReadFileFn(readConfig, readSchema, parseSchemaFields(fields))))
            .setRowSchema(readSchema)
        PAssert.that(out).satisfies { o ->
            assertEquals(8, o.toList().size, "after subdivision, 8 rows are read with nothing duplicated or lost")
            null
        }
        rp.run().waitUntilFinish()
    }

    @Test
    fun `filter 下推裁剪不匹配的 split 并过滤行`() {
        // Predicate push-down must do two things at once: (1) manifest-level pruning — when a whole file does not
        // match, its split is simply not enumerated; (2) the read side evaluates the residual predicate per row and
        // drops non-matching rows. Otherwise "push-down" is just talk.
        val warehouse = Files.createTempDirectory("iceberg-filter").toString()
        // File 1 consists entirely of non-matching rows (age < 40), so the whole file should be pruned at the manifest level.
        write(warehouse, "db.filter", listOf(
            Row.withSchema(beamSchema).addValue(1L).addValue("a").addValue(30).addValue(1.5).addValue(true).build(),
            Row.withSchema(beamSchema).addValue(2L).addValue("b").addValue(35).addValue(1.5).addValue(true).build(),
        ), "append")
        // File 2 has matching rows.
        write(warehouse, "db.filter", listOf(
            Row.withSchema(beamSchema).addValue(3L).addValue("c").addValue(40).addValue(2.5).addValue(false).build(),
            Row.withSchema(beamSchema).addValue(4L).addValue("d").addValue(50).addValue(3.5).addValue(true).build(),
        ), "append")

        val baseConfig = IcebergReadConfig(warehouse = warehouse, table = "db.filter", schemaFields = fields)
        val readConfig = baseConfig.copy(filter = "age >= 40")
        // Enumerator side: splits are still enumerated normally after filter push-down (manifest-level pruning happens underneath).
        val p = Pipeline.create()
        val splits = p.apply(Create.of(listOf(""))).apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig)))
        PAssert.that(splits).satisfies { out ->
            assertTrue(out.toList().isNotEmpty(), "splits are still enumerated after filtering")
            null
        }
        p.run().waitUntilFinish()

        // Read side: only the two rows with age >= 40 are returned (the residual predicate applies per row, filtering both partition and data columns).
        val rp = Pipeline.create()
        val readSchema = readConfig.outputSchema()
        val out = rp.apply(Create.of(listOf("")))
            .apply(ParDo.of(IcebergSplitEnumeratorFn(readConfig)))
            .apply(ParDo.of(IcebergReadFileFn(readConfig, readSchema, parseSchemaFields(fields))))
            .setRowSchema(readSchema)
        PAssert.that(out).satisfies { o ->
            val list = o.toList()
            assertEquals(2, list.size, "only rows matching the filter are returned")
            assertEquals(setOf(40, 50), list.map { it.getInt32("age") }.toSet())
            null
        }
        rp.run().waitUntilFinish()
    }

    @Test
    fun `limit 跨文件取得精确行数且在过滤后计数`() {
        // The first file has no matching rows; the old implementation enumerated only the first file and would
        // wrongly return 0 rows. A global limit must keep scanning subsequent files and stop only after actually
        // producing 3 matching rows.
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
            assertEquals(3, list.size, "limit=3 should return exactly 3 rows after filtering")
            assertTrue(list.all { checkNotNull(it.getInt64("id")) >= 6 })
            null
        }
        rp.run().waitUntilFinish()
    }

    @Test
    fun `聚合下推 count_min_max 取自文件统计不读数据`() {
        // COUNT/MIN/MAX come directly from the data files' metadata (recordCount / lower_bounds / upper_bounds),
        // never touching the data file contents — this is genuine storage-layer push-down.
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
            assertEquals(1, list.size, "aggregation should output only one row")
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
        // SUM/AVG have no data-file-level statistics, so the column can only be projected and accumulated file by file on the read side, then merged globally across files.
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
        // Regression: the aggregation enumerator used to ignore the filter entirely — COUNT took the whole file's
        // recordCount and MIN/MAX/SUM/AVG were computed over non-matching rows, so the job succeeded but the numbers
        // were wrong. Data age = 10/20/30/40/50, and filter "age > 30" matches only the two rows 40 and 50.
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

        // A misspelled aggregation column reports clearly at graph-construction time, rather than an NPE.
        val badSpecs = parseAggregations(listOf("min:no_such_col"))
        val ex = assertFailsWith<IllegalArgumentException> { aggregateSchema(badSpecs, table) }
        assertTrue(ex.message!!.contains("no_such_col"))
        // sum/avg reject non-numeric columns.
        val strSpecs = parseAggregations(listOf("sum:name"))
        assertFailsWith<IllegalArgumentException> { aggregateSchema(strSpecs, table) }
    }
}
