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
 * JDBC 模块的纯逻辑 / 配置校验 / 序列化边界测试。
 *
 * 这里专门钉住主代码里"声明了校验或配置项、但现有测试没覆盖"的分支：
 *  - `RowMapper` / `JdbcMetadata.toSchema` 的构造前校验；
 *  - 数组列元素类型不支持时 `TypeMappings.resolve` 返回 null（而非静默退化）；
 *  - 配置绑定开启 `FAIL_ON_UNKNOWN_PROPERTIES`，写错键要当场报错；
 *  - 随作业下发的 DoFn 必须可序列化，否则只在提交作业时才炸。
 *
 * 全部不依赖任何外部数据库：类型/元数据分支用合成的 [JdbcColumn] 触发，序列化分支直接构造对象。
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

    // ---------- RowMapper / JdbcMetadata 构造前校验 ----------

    @Test
    fun `RowMapper 字段数与读取器数不一致时抛异常`() {
        val schema = Schema.builder().addNullableField("a", FieldTypes.INT64).build()
        val error = assertFailsWith<IllegalArgumentException> { RowMapper(schema, emptyList()) }
        assertTrue("schema" in error.message!! && "读取器" in error.message!!)
    }

    @Test
    fun `没有任何列的查询 toSchema 直接抛异常`() {
        val error = assertFailsWith<IllegalArgumentException> { JdbcMetadata.toSchema(emptyList()) }
        assertTrue("查询没有返回任何列" in error.message!!)
    }

    // ---------- TypeMappings 类型映射边界 ----------

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
        assertTrue("暂不支持" in error.message!! && "ids" in error.message!!)
    }

    // ---------- 配置绑定：写错键必须报错 ----------

    @Test
    fun `读端配置绑定遇到未知属性时抛异常`() {
        // FAIL_ON_UNKNOWN_PROPERTIES 是开着的：拼写错误（partiton_column）必须当场发现，而不是静默忽略
        assertFailsWith<HDataException> {
            bind("""{"url":"jdbc:h2:mem:x","tables":["t"],"partiton_column":"id"}""")
        }
    }

    // ---------- 序列化边界：DoFn 必须能跟着作业下发 ----------

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
