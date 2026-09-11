package me.jayer.hdata.dynamodb

import kotlin.test.Test
import kotlin.test.assertFailsWith

class DynamoDBReadConfigTest {

    @Test
    fun `default config is valid`() {
        DynamoDBReadConfig(tableName = "my-table").validate()
    }

    @Test
    fun `blank table_name errors`() {
        assertFailsWith<IllegalArgumentException> {
            DynamoDBReadConfig(tableName = "").validate()
        }
    }

    @Test
    fun `blank region errors`() {
        assertFailsWith<IllegalArgumentException> {
            DynamoDBReadConfig(tableName = "t", region = "").validate()
        }
    }

    @Test
    fun `negative max_items errors`() {
        assertFailsWith<IllegalArgumentException> {
            DynamoDBReadConfig(tableName = "t", maxItems = -1).validate()
        }
    }

    @Test
    fun `zero max_items is valid`() {
        DynamoDBReadConfig(tableName = "t", maxItems = 0).validate()
    }
}
