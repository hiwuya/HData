package me.jayer.hdata.iceberg

import kotlin.test.Test
import kotlin.test.assertFailsWith

class IcebergWriteConfigTest {

    @Test
    fun `必填与 write_mode 校验`() {
        assertFailsWith<IllegalArgumentException> { IcebergWriteConfig(warehouse = "/wh", table = "t", schemaFields = listOf("a:STRING"), writeMode = "upsert").validate() }
        IcebergWriteConfig(warehouse = "/wh", table = "t", schemaFields = listOf("a:STRING")).validate()
        IcebergWriteConfig(warehouse = "/wh", table = "t", schemaFields = listOf("a:STRING"), writeMode = "overwrite").validate()
    }

    @Test
    fun `catalog 与重复字段会校验`() {
        assertFailsWith<IllegalArgumentException> {
            IcebergWriteConfig(warehouse = "/wh", catalogName = " ", table = "t", schemaFields = listOf("a:STRING")).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            IcebergWriteConfig(warehouse = "/wh", table = "t", schemaFields = listOf("a:STRING", "a:INT64")).validate()
        }
    }
}
