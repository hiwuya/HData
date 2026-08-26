package me.jayer.hdata.iceberg

import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith

class IcebergReadConfigTest {

    @Test
    fun `必填项校验`() {
        assertFailsWith<IllegalArgumentException> { IcebergReadConfig(warehouse = "", table = "t", schemaFields = listOf("a:STRING")).validate() }
        assertFailsWith<IllegalArgumentException> { IcebergReadConfig(warehouse = "/wh", table = "", schemaFields = listOf("a:STRING")).validate() }
        assertFailsWith<IllegalArgumentException> { IcebergReadConfig(warehouse = "/wh", table = "t", schemaFields = emptyList()).validate() }
    }

    @Test
    fun `schema_fields 解析为输出 schema`() {
        val config = IcebergReadConfig(warehouse = "/wh", table = "t", schemaFields = listOf("id:INT64", "name:STRING", "ok:BOOLEAN"))
        val schema = config.outputSchema()
        assertEquals(Schema.TypeName.INT64, schema.getField("id").type.typeName)
        assertEquals(Schema.TypeName.STRING, schema.getField("name").type.typeName)
        assertEquals(Schema.TypeName.BOOLEAN, schema.getField("ok").type.typeName)
    }

    @Test
    fun `catalog 名与字段名会严格校验`() {
        val config = IcebergReadConfig("/wh", table = "db.t", schemaFields = listOf("id:INT64"))
        assertFailsWith<IllegalArgumentException> { config.copy(catalogName = " ").validate() }
        assertFailsWith<IllegalArgumentException> {
            config.copy(schemaFields = listOf("id:INT64", "id:STRING")).validate()
        }
    }

    @Test
    fun `split_size 默认 128MB 且必须为正`() {
        val config = IcebergReadConfig("/wh", table = "db.t", schemaFields = listOf("id:INT64"))
        assertEquals(128L * 1024 * 1024, config.splitSize)
        config.validate()
        assertFailsWith<IllegalArgumentException> { config.copy(splitSize = 0).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(splitSize = -1).validate() }
    }

    @Test
    fun `filter 和 limit 配置项校验`() {
        val config = IcebergReadConfig("/wh", table = "db.t", schemaFields = listOf("id:INT64"))
        config.copy(filter = "id >= 10 AND name = 'a'").validate()
        config.copy(limit = -1).validate()
        config.copy(limit = 5).validate()
        // 写错的过滤条件在构图阶段就报错，而不是运行时静默全读
        assertFailsWith<IllegalArgumentException> { config.copy(filter = "id >=").validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(filter = "id ~ 10").validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(limit = 0).validate() }
    }

    @Test
    fun `聚合支持 count_min_max_sum_avg`() {
        val config = IcebergReadConfig("/wh", table = "db.t", schemaFields = listOf("id:INT64", "age:INT32"))
        // count/min/max/sum/avg 都支持；min/max 需要列，count 不需要
        config.copy(aggregations = listOf("count", "min:age", "max:age", "sum:age", "avg:age")).validate()
        assertFailsWith<IllegalArgumentException> { config.copy(aggregations = listOf("mean:age")).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(aggregations = listOf("min")).validate() }
    }

    @Test
    fun `aggregates 与 limit 互斥且输出列名不能重复`() {
        val config = IcebergReadConfig("/wh", table = "db.t", schemaFields = listOf("id:INT64", "age:INT32"))
        // 聚合是全局语义，limit 对它没有意义；收了又不生效等于埋坑
        assertFailsWith<IllegalArgumentException> { config.copy(aggregations = listOf("count"), limit = 5).validate() }
        // 两条聚合落到同一个输出列，结果集会出现同名列
        assertFailsWith<IllegalArgumentException> { config.copy(aggregations = listOf("min:age", "min:age")).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(aggregations = listOf("count", "count:id")).validate() }
    }
}
