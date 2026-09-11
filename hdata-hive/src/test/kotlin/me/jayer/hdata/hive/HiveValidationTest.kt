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
 * Pure logic / config validation / serialization boundary tests for the Hive module.
 *
 * They pin down branches in the main code that "declare a validation or config option but are not covered by existing tests":
 *  - views / transactional tables rejected by both the read and the write side at graph construction time (a regression-prone spot
 *  - `HiveTypes` rejecting `void`, illegal decimal, empty struct field names and unclosed parentheses;
 *  - storage format name recognition failures and the SerDe-only fallback decision;
 *  - an unrecognized `write_mode` name and case insensitivity; non-positive / negative value validation of the read/write configs;
 *  - `HiveValues` rejecting binary partition columns, rejecting non-integer narrowing, and failing when types do not match at all;
 *  - rejecting a partition name whose second segment is missing the equals sign.
 *
 * None of them depends on any external service: the `memory://` in-process metastore plus synthetic objects is enough to trigger them.
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

    // ---------- views / transactional tables: fail at graph construction time ----------

    @Test
    fun `视图在构图阶段就被拒绝读取`() {
        TestHive().use { hive ->
            hive.metastore.createTable(tableOf(HiveTable.VIRTUAL_VIEW))
            val error = assertFailsWith<IllegalArgumentException> {
                HiveReadProvider()
                    .from(config("metastore_uri: \"${hive.metastoreUri}\"\ntable: t"))
                    .expand(PCollectionRowTuple.empty(Pipeline.create()))
            }
            assertTrue("is a view" in error.message!!)
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
            assertTrue("is a view" in error.message!!)
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
            assertTrue("transactional (ACID)" in error.message!!)
        }
    }

    // ---------- HiveTypes parse rejection branches ----------

    @Test
    fun `void 类型直接抛而不做兜底`() {
        assertFailsWith<IllegalArgumentException> { HiveTypes.parse("void") }
    }

    @Test
    fun `decimal 参数个数不对直接抛`() {
        val error = assertFailsWith<IllegalArgumentException> { HiveTypes.decimalPrecisionAndScale("decimal(10,2,3)") }
        assertTrue("illegal decimal parameter syntax" in error.message!!)
    }

    @Test
    fun `decimal 括号没闭合直接抛`() {
        val error = assertFailsWith<IllegalArgumentException> { HiveTypes.parse("decimal(10,2") }
        assertTrue("unclosed parenthesis" in error.message!!)
    }

    @Test
    fun `struct 字段名为空直接抛`() {
        val error = assertFailsWith<IllegalArgumentException> { HiveTypes.parse("struct<:int>") }
        assertTrue("struct field name must not be empty" in error.message!!)
    }

    // ---------- storage format detection boundaries ----------

    @Test
    fun `storage format 按名字识别失败直接抛`() {
        assertFailsWith<IllegalArgumentException> { HiveStorageFormat.byName("nope") }
    }

    @Test
    fun `SerDe 能唯一匹配时即使 InputFormat 写错也能判定`() {
        // Only ORC uses this SerDe, so a wrong InputFormat should fall back to a SerDe-only match rather than throwing "cannot decide"
        assertEquals(
            HiveStorageFormat.ORC,
            HiveStorageFormat.of(StorageFormat("org.apache.hadoop.hive.ql.io.orc.OrcSerde", "wrong.input", "")),
        )
    }

    // ---------- write_mode validation ----------

    @Test
    fun `write_mode 名字不认识直接抛`() {
        val error = assertFailsWith<IllegalArgumentException> { HiveWriteMode.of("bogus") }
        assertTrue("unrecognized write_mode" in error.message!!)
    }

    @Test
    fun `write_mode 大小写不敏感且真的生效`() {
        assertEquals(HiveWriteMode.APPEND, HiveWriteMode.of("append"))
        assertEquals(HiveWriteMode.OVERWRITE, HiveWriteMode.of("OVERWRITE"))
    }

    // ---------- non-positive / negative config value validation ----------

    @Test
    fun `写端配置非法值校验`() {
        assertFailsWith<IllegalArgumentException> {
            HiveWriteConfig(metastoreUri = "thrift://h:9083", table = "t", numShards = -1).validate()
        }
        val error = assertFailsWith<IllegalArgumentException> {
            HiveWriteConfig(metastoreUri = "thrift://h:9083", table = "t", writeMode = "bogus").validate()
        }
        assertTrue("unrecognized write_mode" in error.message!!)
    }

    @Test
    fun `读端 split_bytes 必须为正`() {
        assertFailsWith<IllegalArgumentException> {
            HiveReadConfig(metastoreUri = "thrift://h:9083", table = "t", splitBytes = 0).validate()
        }
    }

    // ---------- HiveValues conversion rejection branches ----------

    @Test
    fun `binary 不能作为分区列`() {
        assertFailsWith<IllegalArgumentException> { HiveValues.toPartitionLiteral(byteArrayOf(1, 2)) }
    }

    @Test
    fun `非整数的数值窄化直接抛而不是静默截断`() {
        val error = assertFailsWith<IllegalArgumentException> { HiveValues.coerce(1.5, FieldTypes.INT32) }
        assertTrue("is not an integer" in error.message!!)
    }

    @Test
    fun `类型完全对不上时给出能定位的报错`() {
        val error = assertFailsWith<IllegalArgumentException> {
            HiveValues.coerce(
                1L,
                Schema.FieldType.row(Schema.builder().addNullableField("x", FieldTypes.INT64).build()),
            )
        }
        assertTrue("cannot convert" in error.message!!)
    }

    // ---------- partition name parsing boundaries ----------

    @Test
    fun `分区名第二段缺等号时抛异常`() {
        assertFailsWith<IllegalArgumentException> { PartitionNames.toPartitionValues("dt=2024/hr01") }
    }
}
