package me.jayer.hdata.rabbitmq

import kotlin.test.Test
import kotlin.test.assertFailsWith

class RabbitMQWriteConfigTest {

    @Test
    fun `default config is valid`() {
        RabbitMQWriteConfig(queue = "test-queue").validate()
    }

    @Test
    fun `blank host errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQWriteConfig(host = "").validate()
        }
    }

    @Test
    fun `invalid port errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQWriteConfig(port = 0).validate()
        }
        assertFailsWith<IllegalArgumentException> {
            RabbitMQWriteConfig(port = 70000).validate()
        }
    }

    @Test
    fun `blank virtual host errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQWriteConfig(virtualHost = "").validate()
        }
    }

    @Test
    fun `blank queue errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQWriteConfig(queue = "").validate()
        }
    }

    @Test
    fun `blank body_field errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQWriteConfig(bodyField = "").validate()
        }
    }

    @Test
    fun `blank routing_key_field errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQWriteConfig(routingKeyField = "").validate()
        }
    }

    @Test
    fun `blank exchange_field errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQWriteConfig(exchangeField = "").validate()
        }
    }

    @Test
    fun `blank message_id_field errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQWriteConfig(messageIdField = "").validate()
        }
    }

    @Test
    fun `zero batch_size errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQWriteConfig(batchSize = 0).validate()
        }
    }

    @Test
    fun `invalid exchange_type when declare_exchange is true errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQWriteConfig(declareExchange = true, exchangeType = "bogus").validate()
        }
    }

    @Test
    fun `negative message_ttl_ms errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQWriteConfig(messageTtlMs = -1).validate()
        }
    }

    @Test
    fun `zero message_ttl_ms errors`() {
        assertFailsWith<IllegalArgumentException> {
            RabbitMQWriteConfig(messageTtlMs = 0).validate()
        }
    }
}
