package me.jayer.hdata.iceberg

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.iceberg.internal.IcebergCatalogs
import me.jayer.hdata.iceberg.internal.parseSchemaFields
import me.jayer.hdata.iceberg.transform.IcebergReadFn
import me.jayer.hdata.iceberg.transform.IcebergWriteFn
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import org.junit.jupiter.api.Test
import tools.jackson.databind.node.ObjectNode
import java.nio.file.Files
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals

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
        val out = trigger.apply(ParDo.of(IcebergReadFn(readConfig, readSchema, parseSchemaFields(fields)))).setRowSchema(readSchema)
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
            .apply(ParDo.of(IcebergReadFn(readConfig, readSchema, parseSchemaFields(fields))))
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
            .apply(ParDo.of(IcebergReadFn(readConfig, readSchema, parseSchemaFields(fields))))
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
            .apply(ParDo.of(IcebergReadFn(readConfig, readSchema, parseSchemaFields(fields))))
            .setRowSchema(readSchema)
        PAssert.that(out).satisfies { output ->
            val list = output.toList()
            assertEquals(2, list.size, "两次 append 应叠加成 2 行，而非互相覆盖")
            assertEquals(setOf("a", "b"), list.map { it.getString("name") }.toSet())
            null
        }
        rp.run().waitUntilFinish()
    }
}
