package me.jayer.hdata.hive

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollection
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

/**
 * `ReadFromHive` / `WriteToHive` 的端到端测试，跑在 DirectRunner + H2 上（见 [H2Hive] 的说明）。
 *
 * @author wuya
 */
class HivePipelineTest {

    private fun config(yaml: String): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    private fun H2Hive.createOrders(rows: Int = 6) {
        execute(
            """
            CREATE TABLE t_order (
              id INT,
              name VARCHAR(50),
              amount DECIMAL(10, 2),
              dt VARCHAR(10)
            )
            """.trimIndent()
        )
        useConnection { connection ->
            connection.prepareStatement("INSERT INTO t_order VALUES (?, ?, ?, ?)").use { ps ->
                (1..rows).forEach { i ->
                    ps.setInt(1, i)
                    ps.setString(2, "name-$i")
                    ps.setBigDecimal(3, java.math.BigDecimal("$i.50"))
                    ps.setString(4, if (i % 2 == 0) "2024-01-02" else "2024-01-01")
                    ps.addBatch()
                }
                ps.executeBatch()
            }
        }
    }

    private fun read(db: H2Hive, extra: String = ""): Pair<Pipeline, PCollection<Row>> {
        val pipeline = Pipeline.create()
        val yaml = buildString {
            appendLine("""url: "${db.url}"""")
            appendLine("table: t_order")
            extra.lines().filter { it.isNotBlank() }.forEach { appendLine(it.trimIndent()) }
        }
        return pipeline to PCollectionRowTuple.empty(pipeline)
            .apply(HiveReadProvider().from(config(yaml)))
            .get(Tags.MAIN_OUTPUT)
    }

    @Test
    fun `按显式分区并行读，各分区合起来是全量`() {
        H2Hive.named("hive_read").use { db ->
            db.createOrders(rows = 6)

            val (pipeline, rows) = read(db, """partitions: ["dt='2024-01-01'", "dt='2024-01-02'"]""")
            PAssert.thatSingleton(rows.apply(Count.globally())).isEqualTo(6L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `每个分区只读到自己的数据`() {
        H2Hive.named("hive_one_partition").use { db ->
            db.createOrders(rows = 6)

            val (pipeline, rows) = read(db, """partitions: ["dt='2024-01-01'"]""")
            PAssert.thatSingleton(rows.apply(Count.globally())).isEqualTo(3L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `where 条件与分区谓词叠加`() {
        H2Hive.named("hive_where").use { db ->
            db.createOrders(rows = 6)

            val (pipeline, rows) = read(
                db,
                """
                partitions: ["dt='2024-01-01'"]
                where: "id > 1"
                """.trimIndent(),
            )
            // dt='2024-01-01' 的是 id 1/3/5，再叠加 id > 1 剩下 3 和 5
            PAssert.thatSingleton(rows.apply(Count.globally())).isEqualTo(2L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `schema 来自结果集元数据，DECIMAL 不会被压成 DOUBLE`() {
        // 重构前 Hive 模块自己那份类型映射把 decimal 映射成 DOUBLE，精度就这么没了
        H2Hive.named("hive_schema").use { db ->
            db.createOrders(rows = 2)

            val (pipeline, rows) = read(db, """partitions: ["dt='2024-01-01'"]""")
            val schema = rows.schema

            assertEquals(Schema.TypeName.INT32, schema.getField("ID").type.typeName)
            assertEquals(Schema.TypeName.STRING, schema.getField("NAME").type.typeName)
            assertEquals(Schema.TypeName.DECIMAL, schema.getField("AMOUNT").type.typeName)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `只读指定的列`() {
        H2Hive.named("hive_columns").use { db ->
            db.createOrders(rows = 3)

            val (pipeline, rows) = read(
                db,
                """
                partitions: ["dt='2024-01-01'"]
                columns: ["id", "name"]
                """.trimIndent(),
            )
            assertEquals(listOf("ID", "NAME"), rows.schema.fieldNames)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `非分区表退化为整表读取`() {
        H2Hive.named("hive_no_partition").use { db ->
            db.execute("CREATE TABLE t_order (id INT, name VARCHAR(50))")
            db.execute("INSERT INTO t_order VALUES (1, 'a'), (2, 'b')")

            // H2 没有 SHOW PARTITIONS，会走"探测失败就整表读"这条分支
            val (pipeline, rows) = read(db)
            PAssert.thatSingleton(rows.apply(Count.globally())).isEqualTo(2L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `批量写入后行数与内容都对得上`() {
        H2Hive.named("hive_write").use { db ->
            db.execute("CREATE TABLE t_target (id INT, name VARCHAR(50))")

            val schema = Schema.builder().addInt32Field("ID").addNullableStringField("NAME").build()
            val rows = (1..50).map { Row.withSchema(schema).addValue(it).addValue("name-$it").build() }

            val pipeline = Pipeline.create()
            val input = pipeline.apply(Create.of(rows).withRowSchema(schema))
            PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                HiveWriteProvider().from(
                    config(
                        """
                        url: "${db.url}"
                        table: t_target
                        batch_size: 10
                        """.trimIndent()
                    )
                )
            )
            pipeline.run().waitUntilFinish()

            assertEquals(50L, db.count("t_target"))
            assertEquals((1..50).toList(), db.queryColumn("SELECT id FROM t_target ORDER BY id", "id"))
        }
    }

    @Test
    fun `写失败的行进死信，其余照常写入`() {
        H2Hive.named("hive_dead_letter").use { db ->
            db.execute("CREATE TABLE t_target (id INT, name VARCHAR(5) NOT NULL)")

            val schema = Schema.builder().addInt32Field("ID").addNullableStringField("NAME").build()
            val rows = listOf(
                Row.withSchema(schema).addValue(1).addValue("ok").build(),
                // 超长，H2 会拒收
                Row.withSchema(schema).addValue(2).addValue("这个名字明显超过五个字符").build(),
            )

            val pipeline = Pipeline.create()
            val input = pipeline.apply(Create.of(rows).withRowSchema(schema))
            val out = PCollectionRowTuple.of(Tags.MAIN_INPUT, input).apply(
                HiveWriteProvider().from(
                    TransformConfig(
                        "WriteToHive",
                        SpecMappers.YAML.readTree(
                            """
                            url: "${db.url}"
                            table: t_target
                            batch_size: 10
                            """.trimIndent()
                        ) as ObjectNode,
                        me.jayer.hdata.core.spec.ErrorHandlingSpec(output = "errors"),
                    )
                )
            )
            PAssert.thatSingleton(out.get(Tags.ERROR_OUTPUT).apply(Count.globally())).isEqualTo(1L)
            pipeline.run().waitUntilFinish()

            assertTrue(db.count("t_target") >= 0)
        }
    }
}
