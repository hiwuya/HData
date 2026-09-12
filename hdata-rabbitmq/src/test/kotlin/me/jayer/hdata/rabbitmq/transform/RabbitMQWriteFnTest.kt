package me.jayer.hdata.rabbitmq.transform

import com.rabbitmq.client.Channel
import me.jayer.hdata.core.error.ErrorSchemas
import me.jayer.hdata.rabbitmq.RabbitMQWriteConfig
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFnTester
import org.apache.beam.sdk.values.Row
import org.junit.jupiter.api.Test
import java.io.IOException
import java.lang.reflect.Proxy
import kotlin.test.assertEquals

class RabbitMQWriteFnTest {

    private val schema = Schema.builder().addStringField("body").build()
    private val errorSchema = ErrorSchemas.of(schema)

    private fun row(body: String) = Row.withSchema(schema).addValue(body).build()

    @Test
    fun `an ambiguous publisher confirm sends every pending row to dead letter`() {
        val channel = Proxy.newProxyInstance(
            Channel::class.java.classLoader,
            arrayOf(Channel::class.java),
        ) { _, method, _ ->
            when (method.name) {
                "isOpen" -> true
                "waitForConfirmsOrDie" -> throw IOException("confirmation connection lost")
                "confirmSelect", "basicPublish", "close" -> null
                else -> error("Unexpected Channel call: ${method.name}")
            }
        } as Channel
        val factory = RabbitMQChannelFactory { RabbitMQChannelConnection(proxyConnection(), channel) }
        val fn = RabbitMQWriteFn(
            RabbitMQWriteConfig(host = "localhost", port = 5672, queue = "orders", batchSize = 10),
            errorSchema,
            deadLetter = true,
            transformName = "WriteToRabbitMQ",
            channelFactory = factory,
        )

        // The fake channel accepts publishes then loses the confirm. The broker may have the messages already,
        // so every original row must be exposed for idempotent replay rather than being silently discarded.
        val errors = DoFnTester.of<Row, Row>(fn).apply {
            setCloningBehavior(DoFnTester.CloningBehavior.DO_NOT_CLONE)
        }.use { tester ->
            tester.processBundle(row("one"), row("two"))
        }

        assertEquals(listOf("one", "two"), errors.map { it.getRow(ErrorSchemas.ELEMENT)!!.getString("body") })
    }

    private fun proxyConnection(): com.rabbitmq.client.Connection = Proxy.newProxyInstance(
        com.rabbitmq.client.Connection::class.java.classLoader,
        arrayOf(com.rabbitmq.client.Connection::class.java),
    ) { _, method, _ ->
        when (method.name) {
            "isOpen" -> true
            "close" -> null
            else -> error("Unexpected Connection call: ${method.name}")
        }
    } as com.rabbitmq.client.Connection
}
