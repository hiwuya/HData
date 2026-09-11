package me.jayer.hdata.sqs

import kotlin.test.Test
import kotlin.test.assertFailsWith

class SQSWriteConfigTest {

    @Test
    fun `default config is valid`() {
        SQSWriteConfig(queueUrl = "http://localhost:4566/queue/test").validate()
    }

    @Test
    fun `blank queue_url errors`() {
        assertFailsWith<IllegalArgumentException> { SQSWriteConfig(queueUrl = "").validate() }
    }

    @Test
    fun `blank body_field errors`() {
        assertFailsWith<IllegalArgumentException> {
            SQSWriteConfig(queueUrl = "q", bodyField = "").validate()
        }
    }

    @Test
    fun `batch_size out of range errors`() {
        assertFailsWith<IllegalArgumentException> {
            SQSWriteConfig(queueUrl = "q", batchSize = 0).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            SQSWriteConfig(queueUrl = "q", batchSize = 11).validate()
        }
    }

    @Test
    fun `blank region errors`() {
        assertFailsWith<IllegalArgumentException> {
            SQSWriteConfig(queueUrl = "q", region = "").validate()
        }
    }
}
