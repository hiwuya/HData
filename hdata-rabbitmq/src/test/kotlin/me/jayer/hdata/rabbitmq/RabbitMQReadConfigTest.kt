package me.jayer.hdata.rabbitmq

import kotlin.test.Test
import kotlin.test.assertFailsWith

class RabbitMQReadConfigTest {

    @Test
    fun `default config is valid`() {
        RabbitMQReadConfig(queue = "test-queue").validate()
    }

    @Test
    fun `blank host errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQReadConfig(host = "").validate()
        }
    }

    @Test
    fun `invalid port errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQReadConfig(port = 0).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            RabbitMQReadConfig(port = 70000).validate()
        }
    }

    @Test
    fun `blank virtual host errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQReadConfig(virtualHost = "").validate()
        }
    }

    @Test
    fun `blank username errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQReadConfig(username = "").validate()
        }
    }

    @Test
    fun `blank queue errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQReadConfig(queue = "").validate()
        }
    }

    @Test
    fun `negative max_messages errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQReadConfig(maxMessages = -1).validate()
        }
    }

    @Test
    fun `negative wait_timeout_ms errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQReadConfig(waitTimeoutMs = -1).validate()
        }
    }

    @Test
    fun `streaming requires unlimited max messages`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQReadConfig(queue = "test-queue", streaming = true, maxMessages = 1).validate()
        }
        RabbitMQReadConfig(queue = "test-queue", streaming = true, maxMessages = 0).validate()
    }
}
