package me.jayer.hdata.hive

import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.Tags
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.format.HiveStorageFormat
import me.jayer.hdata.hive.metastore.HiveColumn
import me.jayer.hdata.hive.metastore.HiveTable
import me.jayer.hdata.hive.metastore.PartitionNames
import me.jayer.hdata.hive.metastore.Storage
import me.jayer.hdata.hive.metastore.StorageFormat
import me.jayer.hdata.hive.type.HiveTypes
import me.jayer.hdata.hive.type.HiveValues
import org.apache.beam.sdk.Pipeline
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.Create
import org.apache.beam.sdk.values.PCollectionRowTuple
import org.apache.beam.sdk.values.Row
import tools.jackson.databind.node.ObjectNode
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue

/**
 * Hive 模块的纯逻辑 / 配置校验 / 序列化边界测试。
 *
 * 专门钉住主代码里"声明了校验或配置项、但现有测试没覆盖"的分支：
 *  - 视图 / 事务表在构图阶段就被读写两端拒绝（AGENTS 点名的易回归点，之前只有读端事务表被覆盖）；
 *  - `HiveTypes` 对 `void` / 非法 decimal / struct 空字段名 / 未闭合括号的解析拒绝；
 *  - 存储格式按名字识别失败、SerDe-only 兜底判定；
 *  - `write_mode` 名字不认识、大小写不敏感；读/写配置的非正 / 负值校验；
 *  - `HiveValues` 的 binary 分区列拒绝、非整数窄化拒绝、类型完全对不上时的报错；
 *  - 分区名第二段缺等号时的解析拒绝。
 *
 * 全部不依赖任何外部服务：`memory://` 进程内 metastore + 合成对象即可触发。
 */
class HiveValidationTest {

    private fun config(yaml: String): TransformConfig =
        TransformConfig("test", SpecMappers.YAML.readTree(yaml) as ObjectNode)

    private fun tableOf(tableType: String, parameters: Map<String, String> = emptyMap()): HiveTable =
        HiveTable(
            databaseName = "default",
            tableName = "t",
            tableType = tableType,
            dataColumns = listOf(HiveColumn("id", "bigint"), HiveColumn("name", "string")),
            partitionColumns = emptyList(),
            storage = Storage(HiveStorageFormat.TEXTFILE.toStorageFormat(), "file:///tmp/hdata-validation"),
            parameters = parameters,
        )

    // ---------- 视图 / 事务表：构图即报错 ----------

    @Test
    fun `视图在构图阶段就被拒绝读取`() {
        TestHive().use { hive ->
            hive.metastore.createTable(tableOf(HiveTable.VIRTUAL_VIEW))
            val error = assertFailsWith<IllegalArgumentException> {
                HiveReadProvider()
                    .from(config("metastore_uri: \"${hive.metastoreUri}\"\ntable: t"))
                    .expand(PCollectionRowTuple.empty(Pipeline.create()))
            }
            assertTrue("视图" in error.message!!)
        }
    }

    @Test
    fun `视图在构图阶段就被拒绝写入`() {
        TestHive().use { hive ->
            hive.metastore.createTable(tableOf(HiveTable.VIRTUAL_VIEW))
            val pipeline = Pipeline.create()
            val schema = Schema.builder()
                .addNullableField("id", FieldTypes.INT64)
                .addNullableField("name", FieldTypes.STRING)
                .build()
            val input = pipeline.apply(
                "Create",
                Create.of(listOf(Row.withSchema(schema).addValues(1L, "a").build())).withRowSchema(schema),
            )
            val error = assertFailsWith<IllegalArgumentException> {
                HiveWriteProvider()
                    .from(config("metastore_uri: \"${hive.metastoreUri}\"\ntable: t"))
                    .expand(PCollectionRowTuple.of(Tags.MAIN_INPUT, input))
            }
            assertTrue("视图" in error.message!!)
        }
    }

    @Test
    fun `事务表在构图阶段就被拒绝写入`() {
        TestHive().use { hive ->
            hive.metastore.createTable(tableOf(HiveTable.MANAGED_TABLE, mapOf("transactional" to "true")))
            val pipeline = Pipeline.create()
            val schema = Schema.builder()
                .addNullableField("id", FieldTypes.INT64)
                .addNullableField("name", FieldTypes.STRING)
                .build()
            val input = pipeline.apply(
                "Create",
                Create.of(listOf(Row.withSchema(schema).addValues(1L, "a").build())).withRowSchema(schema),
            )
            val error = assertFailsWith<IllegalArgumentException> {
                HiveWriteProvider()
                    .from(config("metastore_uri: \"${hive.metastoreUri}\"\ntable: t"))
                    .expand(PCollectionRowTuple.of(Tags.MAIN_INPUT, input))
            }
            assertTrue("事务表" in error.message!!)
        }
    }

    // ---------- HiveTypes 解析拒绝分支 ----------

    @Test
    fun `void 类型直接抛而不做兜底`() {
        assertFailsWith<IllegalArgumentException> { HiveTypes.parse("void") }
    }

    @Test
    fun `decimal 参数个数不对直接抛`() {
        val error = assertFailsWith<IllegalArgumentException> { HiveTypes.decimalPrecisionAndScale("decimal(10,2,3)") }
        assertTrue("decimal 参数写法不合法" in error.message!!)
    }

    @Test
    fun `decimal 括号没闭合直接抛`() {
        val error = assertFailsWith<IllegalArgumentException> { HiveTypes.parse("decimal(10,2") }
        assertTrue("括号没有闭合" in error.message!!)
    }

    @Test
    fun `struct 字段名为空直接抛`() {
        val error = assertFailsWith<IllegalArgumentException> { HiveTypes.parse("struct<:int>") }
        assertTrue("struct 字段名不能为空" in error.message!!)
    }

    // ---------- 存储格式判定边界 ----------

    @Test
    fun `storage format 按名字识别失败直接抛`() {
        assertFailsWith<IllegalArgumentException> { HiveStorageFormat.byName("nope") }
    }

    @Test
    fun `SerDe 能唯一匹配时即使 InputFormat 写错也能判定`() {
        // 只有 ORC 用这个 SerDe，InputFormat 写错也该退到 SerDe 单一匹配，而不是抛"判不出来"
        assertEquals(
            HiveStorageFormat.ORC,
            HiveStorageFormat.of(StorageFormat("org.apache.hadoop.hive.ql.io.orc.OrcSerde", "wrong.input", "")),
        )
    }

    // ---------- write_mode 校验 ----------

    @Test
    fun `write_mode 名字不认识直接抛`() {
        val error = assertFailsWith<IllegalArgumentException> { HiveWriteMode.of("bogus") }
        assertTrue("无法识别的 write_mode" in error.message!!)
    }

    @Test
    fun `write_mode 大小写不敏感且真的生效`() {
        assertEquals(HiveWriteMode.APPEND, HiveWriteMode.of("append"))
        assertEquals(HiveWriteMode.OVERWRITE, HiveWriteMode.of("OVERWRITE"))
    }

    // ---------- 配置的非正 / 负值校验 ----------

    @Test
    fun `写端配置非法值校验`() {
        assertFailsWith<IllegalArgumentException> {
            HiveWriteConfig(metastoreUri = "thrift://h:9083", table = "t", numShards = -1).validate()
        }
        val error = assertFailsWith<IllegalArgumentException> {
            HiveWriteConfig(metastoreUri = "thrift://h:9083", table = "t", writeMode = "bogus").validate()
        }
        assertTrue("无法识别的 write_mode" in error.message!!)
    }

    @Test
    fun `读端 split_bytes 必须为正`() {
        assertFailsWith<IllegalArgumentException> {
            HiveReadConfig(metastoreUri = "thrift://h:9083", table = "t", splitBytes = 0).validate()
        }
    }

    // ---------- HiveValues 换算拒绝分支 ----------

    @Test
    fun `binary 不能作为分区列`() {
        assertFailsWith<IllegalArgumentException> { HiveValues.toPartitionLiteral(byteArrayOf(1, 2)) }
    }

    @Test
    fun `非整数的数值窄化直接抛而不是静默截断`() {
        val error = assertFailsWith<IllegalArgumentException> { HiveValues.coerce(1.5, FieldTypes.INT32) }
        assertTrue("不是整数" in error.message!!)
    }

    @Test
    fun `类型完全对不上时给出能定位的报错`() {
        val error = assertFailsWith<IllegalArgumentException> {
            HiveValues.coerce(
                1L,
                Schema.FieldType.row(Schema.builder().addNullableField("x", FieldTypes.INT64).build()),
            )
        }
        assertTrue("换算成" in error.message!!)
    }

    // ---------- 分区名解析边界 ----------

    @Test
    fun `分区名第二段缺等号时抛异常`() {
        assertFailsWith<IllegalArgumentException> { PartitionNames.toPartitionValues("dt=2024/hr01") }
    }
}
