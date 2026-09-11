package me.jayer.hdata.iceberg

import kotlin.test.Test
import kotlin.test.assertFailsWith

class IcebergWriteConfigTest {

    @Test
    fun `required fields and write_mode validation`() {
        assertFailsWith<IllegalArgumentException> { IcebergWriteConfig(warehouse = "/wh", table = "t", schemaFields = listOf("a:STRING"), writeMode = "upsert").validate() }
        IcebergWriteConfig(warehouse = "/wh", table = "t", schemaFields = listOf("a:STRING")).validate()
        IcebergWriteConfig(warehouse = "/wh", table = "t", schemaFields = listOf("a:STRING"), writeMode = "overwrite").validate()
    }

    @Test
    fun `catalog and duplicate fields are validated`() {
        assertFailsWith<IllegalArgumentException> {
            IcebergWriteConfig(warehouse = "/wh", catalogName = " ", table = "t", schemaFields = listOf("a:STRING")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            IcebergWriteConfig(warehouse = "/wh", table = "t", schemaFields = listOf("a:STRING", "a:INT64")).validate()
        }
    }
}
