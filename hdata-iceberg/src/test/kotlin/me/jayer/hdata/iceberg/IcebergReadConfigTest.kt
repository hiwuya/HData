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
        // A malformed filter errors out at graph-construction time, rather than silently reading everything at runtime.
        assertFailsWith<IllegalArgumentException> { config.copy(filter = "id >=").validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(filter = "id ~ 10").validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(limit = 0).validate() }
    }

    @Test
    fun `聚合支持 count_min_max_sum_avg`() {
        val config = IcebergReadConfig("/wh", table = "db.t")
        // count/min/max/sum/avg are all supported; min/max need a column, count does not.
        config.copy(aggregations = listOf("count", "min:age", "max:age", "sum:age", "avg:age")).validate()
        assertFailsWith<IllegalArgumentException> { config.copy(aggregations = listOf("mean:age")).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(aggregations = listOf("min")).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(aggregations = listOf("count:age")).validate() }
    }

    @Test
    fun `aggregates 与 limit 互斥且输出列名不能重复`() {
        val config = IcebergReadConfig("/wh", table = "db.t")
        // Aggregation has global semantics, so limit is meaningless for it; accepting it without effect would be a trap.
        assertFailsWith<IllegalArgumentException> { config.copy(aggregations = listOf("count"), limit = 5).validate() }
        // Two aggregations landing on the same output column would produce duplicate column names in the result set.
        assertFailsWith<IllegalArgumentException> { config.copy(aggregations = listOf("min:age", "min:age")).validate() }
        assertFailsWith<IllegalArgumentException> { config.copy(aggregations = listOf("count", "count:id")).validate() }
        assertFailsWith<IllegalArgumentException> {
            config.copy(aggregations = listOf("count"), schemaFields = listOf("id:INT64")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            config.copy(aggregations = listOf("count"), splitSize = 1024).validate()
        }
    }

    @Test
    fun `limit 模式拒绝不会生效的 split_size`() {
        val config = IcebergReadConfig("/wh", table = "db.t", schemaFields = listOf("id:INT64"), limit = 5)
        config.validate()
        assertFailsWith<IllegalArgumentException> { config.copy(splitSize = 1024).validate() }
    }
}
