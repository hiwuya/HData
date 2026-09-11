package me.jayer.hdata.sqs

import kotlin.test.Test
import kotlin.test.assertFailsWith

class SQSReadConfigTest {

    @Test
    fun `default config is valid`() {
        SQSReadConfig(queueUrl = "http://localhost:4566/queue/test").validate()
    }

    @Test
    fun `blank queue_url errors`() {
        assertFailsWith<IllegalArgumentException> { SQSReadConfig(queueUrl = "").validate() }
    }

    @Test
    fun `blank region errors`() {
        assertFailsWith<IllegalArgumentException> {
            SQSReadConfig(queueUrl = "q", region = "").validate()
        }
    }

    @Test
    fun `batch_size out of range errors`() {
        assertFailsWith<IllegalArgumentException> {
            SQSReadConfig(queueUrl = "q", batchSize = 0).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            SQSReadConfig(queueUrl = "q", batchSize = 11).validate()
        }
    }

    @Test
    fun `negative max_messages errors`() {
        assertFailsWith<IllegalArgumentException> {
            SQSReadConfig(queueUrl = "q", maxMessages = -1).validate()
        }
    }
}
