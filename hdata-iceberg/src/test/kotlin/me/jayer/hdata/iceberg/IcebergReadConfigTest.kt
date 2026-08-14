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
}
