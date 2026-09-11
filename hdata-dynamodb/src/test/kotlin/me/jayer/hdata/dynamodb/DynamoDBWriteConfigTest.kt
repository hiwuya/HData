package me.jayer.hdata.dynamodb

import kotlin.test.Test
import kotlin.test.assertFailsWith

class DynamoDBWriteConfigTest {

    @Test
    fun `default config is valid`() {
        DynamoDBWriteConfig(tableName = "my-table").validate()
    }

    @Test
    fun `blank table_name errors`() {
        assertFailsWith<IllegalArgumentException> {
            DynamoDBWriteConfig(tableName = "").validate()
        }
    }

    @Test
    fun `blank region errors`() {
        assertFailsWith<IllegalArgumentException> {
            DynamoDBWriteConfig(tableName = "t", region = "").validate()
        }
    }

    @Test
    fun `batch_size below 1 errors`() {
        assertFailsWith<IllegalArgumentException> {
            DynamoDBWriteConfig(tableName = "t", batchSize = 0).validate()
        }
    }

    @Test
    fun `batch_size above 25 errors`() {
        assertFailsWith<IllegalArgumentException> {
            DynamoDBWriteConfig(tableName = "t", batchSize = 26).validate()
        }
    }

    @Test
    fun `batch_size 25 is valid`() {
        DynamoDBWriteConfig(tableName = "t", batchSize = 25).validate()
    }

    @Test
    fun `negative max_retries errors`() {
        assertFailsWith<IllegalArgumentException> {
            DynamoDBWriteConfig(tableName = "t", maxRetries = -1).validate()
        }
    }

    @Test
    fun `zero retry_delay_ms errors`() {
        assertFailsWith<IllegalArgumentException> {
            DynamoDBWriteConfig(tableName = "t", retryDelayMs = 0).validate()
        }
    }
}
