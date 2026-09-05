package me.jayer.hdata.hive

import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.ErrorHandlingSpec
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.format.HiveStorageFormat
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.testing.PAssert
import org.apache.beam.sdk.transforms.Count
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * 配置绑定与校验，外加写入端的死信路径。
 *
 * @author wuya
 */
class HiveConfigTest {

    private fun config(yaml: String, errorHandling: ErrorHandlingSpec? = null): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode, errorHandling)

    @Test
    fun `配置键是 snake_case`() {
        val bound = config(
            """
            metastore_uri: "thrift://localhost:9083"
            database: ods
            table: t_order
            partition_filter: "dt = '2024-01-01'"
            recursive_directories: true
            split_bytes: 1048576
            hadoop_conf:
              fs.defaultFS: "hdfs://nameservice1"
            """.trimIndent()
        ).bind(HiveReadConfig::class.java)

        assertEquals("thrift://localhost:9083", bound.metastoreUri)
        assertEquals("ods.t_order", bound.qualifiedTable)
        assertEquals("dt = '2024-01-01'", bound.partitionFilter)
        assertTrue(bound.recursiveDirectories)
        assertEquals(1_048_576L, bound.splitBytes)
        assertEquals("hdfs://nameservice1", bound.hadoopConf["fs.defaultFS"])
    }

    @Test
    fun `写错配置键会报错而不是被忽略`() {
        // FAIL_ON_UNKNOWN_PROPERTIES 是开着的，写错一个键必须当场发现
        assertFailsWith<HDataException> {
            config(
                """
                metastore_uri: "thrift://localhost:9083"
                table: t
                partiton_filter: "dt = '2024-01-01'"
                """.trimIndent()
            ).bind(HiveReadConfig::class.java)
        }
    }

    @Test
    fun `必填项与互斥项在构图阶段校验`() {
        assertFailsWith<IllegalArgumentException> { HiveReadConfig(table = "t").validate() }
        assertFailsWith<IllegalArgumentException> { HiveReadConfig(metastoreUri = "thrift://h:9083").validate() }
        // partitions 与 partition_filter 只能配一个，否则谁生效是不确定的
        assertFailsWith<IllegalArgumentException> {
            HiveReadConfig(
                metastoreUri = "thrift://h:9083",
                table = "t",
                partitions = listOf("dt=2024-01-01"),
                partitionFilter = "dt = '2024-01-02'",
            ).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            HiveReadConfig(metastoreUri = "memory://x", database = " ", table = "t").validate()
        }
        assertFailsWith<IllegalArgumentException> {
            HiveReadConfig(metastoreUri = "memory://x", table = "t", metastoreTimeoutMillis = 0).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            HiveReadConfig(metastoreUri = "memory://x", table = "t", columns = listOf("id", "id")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            HiveReadConfig(
                metastoreUri = "memory://x",
                table = "t",
                partitions = listOf("dt=2024-01-01", "dt=2024-01-01"),
            ).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            HiveWriteConfig(metastoreUri = "memory://x", table = "t", filePrefix = " ").validate()
        }
    }

    @Test
    fun `聚合模式拒绝不会生效或输出重名的配置`() {
        val base = HiveReadConfig(
            metastoreUri = "memory://x",
            table = "t",
            aggregates = listOf(ConfigAggregate("count", "")),
        )
        base.validate()
        assertFailsWith<IllegalArgumentException> { base.copy(columns = listOf("id")).validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(predicates = listOf(me.jayer.hdata.hive.format.ConfigPredicate("id", ">", "0"))).validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(limit = 1).validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(sample = SampleConfig(0.5)).validate() }
        assertFailsWith<IllegalArgumentException> { base.copy(splitBytes = 1024).validate() }
        assertFailsWith<IllegalArgumentException> {
            base.copy(aggregates = listOf(ConfigAggregate("count", "id"))).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            base.copy(aggregates = listOf(ConfigAggregate("min", "id"), ConfigAggregate("MIN", "ID"))).validate()
        }
    }

    @Test
    fun `metastore_uri 的 scheme 认不出来时明确报错`() {
        val error = assertFailsWith<IllegalArgumentException> {
            me.jayer.hdata.hive.metastore.HiveMetastores.create(
                me.jayer.hdata.hive.metastore.HiveMetastoreSpec("jdbc:hive2://localhost:10000")
            )
        }
        assertTrue(error.message!!.contains("metastore_uri"))
    }

    @Test
    fun `写入失败的行进死信而不是让作业挂掉`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.TEXTFILE,
                listOf("id" to "bigint"),
                partitionColumns = listOf("dt" to "string"),
            )
            // 上游没有分区列 dt，这一行无法决定写到哪个分区
            val schema = Schema.builder().addNullableField("id", FieldTypes.INT64).build()
            val pipeline = Pipeline.create()
            val input = pipeline.apply(
                Create.of(listOf(Row.withSchema(schema).addValue(1L).build())).withRowSchema(schema)
            )
            val outputs = HiveWriteProvider()
                .from(
                    config(
                        """
                        metastore_uri: "${hive.metastoreUri}"
                        table: t_order
                        """.trimIndent(),
                        ErrorHandlingSpec("errors"),
                    )
                )
                .expand(PCollectionRowTuple.of(Tags.MAIN_INPUT, input))

            PAssert.that(outputs.get(Tags.ERROR_OUTPUT).apply(Count.globally())).containsInAnyOrder(1L)
            pipeline.run().waitUntilFinish()
        }
    }

    @Test
    fun `没配 error_handling 时写入失败就让作业失败`() {
        TestHive().use { hive ->
            hive.createTable(
                "t_order",
                HiveStorageFormat.TEXTFILE,
                listOf("id" to "bigint"),
                partitionColumns = listOf("dt" to "string"),
            )
            val schema = Schema.builder().addNullableField("id", FieldTypes.INT64).build()
            val pipeline = Pipeline.create()
            val input = pipeline.apply(
                Create.of(listOf(Row.withSchema(schema).addValue(1L).build())).withRowSchema(schema)
            )
            HiveWriteProvider()
                .from(
                    config(
                        """
                        metastore_uri: "${hive.metastoreUri}"
                        table: t_order
                        """.trimIndent()
                    )
                )
                .expand(PCollectionRowTuple.of(Tags.MAIN_INPUT, input))

            assertFailsWith<org.apache.beam.sdk.Pipeline.PipelineExecutionException> {
                pipeline.run().waitUntilFinish()
            }
        }
    }
}
