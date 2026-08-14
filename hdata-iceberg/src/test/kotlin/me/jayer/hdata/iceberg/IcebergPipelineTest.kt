package me.jayer.hdata.iceberg

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.iceberg.internal.parseSchemaFields
import me.jayer.hdata.iceberg.transform.IcebergReadFn
import me.jayer.hdata.iceberg.transform.IcebergWriteFn
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.transforms.ParDo
import org.apache.beam.sdk.values.Row
import org.junit.jupiter.api.Test
import java.nio.file.Files
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
}
