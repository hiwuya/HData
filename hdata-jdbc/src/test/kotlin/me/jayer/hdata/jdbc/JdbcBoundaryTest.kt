package me.jayer.hdata.jdbc

import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.core.exception.HDataException
import me.jayer.hdata.core.spec.SpecMappers
import me.jayer.hdata.core.spi.TransformConfig
import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.jdbc.internal.JdbcColumn
import me.jayer.hdata.jdbc.internal.JdbcMetadata
import me.jayer.hdata.jdbc.internal.RowMapper
import me.jayer.hdata.jdbc.internal.TypeMappings
import me.jayer.hdata.jdbc.partition.PartitionColumn
import me.jayer.hdata.jdbc.partition.PartitionConverters
import me.jayer.hdata.jdbc.transform.JdbcPartitionedReadFn
import me.jayer.hdata.jdbc.transform.JdbcWriteFn
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.util.SerializableUtils
import tools.jackson.databind.node.ObjectNode
import java.sql.JDBCType
import java.util.Properties
import kotlin.test.Test
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Pure logic / config validation / serialization boundary tests for the JDBC module.
 *
 * These pin down branches in the main code that "declare a validation or config option but are not covered by existing tests":
 *  - the pre-construction validation of `RowMapper` / `JdbcMetadata.toSchema`;
 *  - `TypeMappings.resolve` returning null (rather than silently degrading) for unsupported array element types;
 *  - config binding enabling `FAIL_ON_UNKNOWN_PROPERTIES`, so a mistyped key fails on the spot;
 *  - DoFns shipped with the job must be serializable, otherwise they only blow up when the job is submitted.
 *
 * None of them depends on an external database: type/metadata branches are triggered with synthetic [JdbcColumn]s, and the serialization branches just construct objects.
 */
class JdbcBoundaryTest {

    private fun bind(json: String): JdbcReadConfig =
        TransformConfig("Jdbc", SpecMappers.CONFIG.readTree(json) as ObjectNode).bind(JdbcReadConfig::class.java)

    private fun bigintColumn(label: String = "ID"): JdbcColumn = JdbcColumn(
        label = label,
        type = JDBCType.BIGINT,
        typeName = "BIGINT",
        typeClass = "java.lang.Long",
        precision = 0,
        scale = 0,
        nullable = false,
        autoIncrement = false,
        signed = true,
    )

    // ---------- pre-construction validation of RowMapper / JdbcMetadata ----------

    @Test
    fun `RowMapper 字段数与读取器数不一致时抛异常`() {
        val schema = Schema.builder().addNullableField("a", FieldTypes.INT64).build()
        val error = assertFailsWith<IllegalArgumentException> { RowMapper(schema, emptyList()) }
        assertTrue("schema" in error.message!! && "readers" in error.message!!)
    }

    @Test
    fun `没有任何列的查询 toSchema 直接抛异常`() {
        val error = assertFailsWith<IllegalArgumentException> { JdbcMetadata.toSchema(emptyList()) }
        assertTrue("the query returned no columns" in error.message!!)
    }

    // ---------- TypeMappings type mapping boundaries ----------

    @Test
    fun `数组元素类型不支持时解析不到字段类型（返回 null）`() {
        val column = JdbcColumn(
            label = "ids",
            type = JDBCType.ARRAY,
            typeName = "UUID ARRAY",
            typeClass = "java.util.UUID[]",
            precision = 0,
            scale = 0,
            nullable = true,
            autoIncrement = false,
            signed = false,
        )
        assertNull(TypeMappings.resolve(column))
    }

    @Test
    fun `数组元素类型不支持时 toSchema 抛可操作的异常`() {
        val column = JdbcColumn(
            label = "ids",
            type = JDBCType.ARRAY,
            typeName = "UUID ARRAY",
            typeClass = "java.util.UUID[]",
            precision = 0,
            scale = 0,
            nullable = true,
            autoIncrement = false,
            signed = false,
        )
        val error = assertFailsWith<IllegalArgumentException> { JdbcMetadata.toSchema(listOf(column)) }
        assertTrue("not supported yet" in error.message!! && "ids" in error.message!!)
    }

    // ---------- config binding: a mistyped key must fail ----------

    @Test
    fun `读端配置绑定遇到未知属性时抛异常`() {
        // FAIL_ON_UNKNOWN_PROPERTIES is on: a typo (partiton_column) must be caught on the spot instead of being silently ignored
        assertFailsWith<HDataException> {
            bind("""{"url":"jdbc:h2:mem:x","tables":["t"],"partiton_column":"id"}""")
        }
    }

    // ---------- serialization boundary: DoFns must ship with the job ----------

    @Test
    fun `分区读取 DoFn 可以被序列化下发`() {
        val codec = TypeMappings.resolve(bigintColumn())!!
        val schema = Schema.builder().addNullableField("ID", codec.fieldType).build()
        val mapper = RowMapper(schema, listOf(codec.reader))
        val fn = JdbcPartitionedReadFn(
            dataSourceProperties = Properties(),
            partitionColumn = PartitionColumn("ID", PartitionConverters.LONG.partitionConverter),
            partitionNum = null,
            fetchSize = 1000,
            rowMapper = mapper,
        )
        SerializableUtils.ensureSerializable(fn)
    }

    @Test
    fun `写入 DoFn 可以被序列化下发`() {
        val schema = Schema.builder().addNullableField("ID", FieldTypes.INT64).build()
        val config = JdbcWriteConfig(url = "jdbc:h2:mem:x", table = "t")
        val fn = JdbcWriteFn(
            config = config,
            inputSchema = schema,
            errorSchema = ErrorSchemas.of(schema),
            deadLetter = false,
            transformName = "write",
        )
        SerializableUtils.ensureSerializable(fn)
    }
}
