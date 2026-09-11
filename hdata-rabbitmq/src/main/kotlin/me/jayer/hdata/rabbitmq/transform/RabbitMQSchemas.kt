package me.jayer.hdata.rabbitmq.transform

import org.apache.beam.sdk.schemas.Schema

/**
 * Fixed output schema for `ReadFromRabbitMQ`.
 *
 * @author wuya
 */
val RABBITMQ_READ_SCHEMA: Schema = Schema.builder()
    .addStringField("exchange")
    .addStringField("routing_key")
    .addStringField("body")
    .addNullableStringField("message_id")
    .addInt64Field("delivery_tag")
    .build()

/** Column names for the read schema. */
internal object RabbitMQReadFields {
    const val EXCHANGE = "exchange"
    const val ROUTING_KEY = "routing_key"
    const val BODY = "body"
    const val MESSAGE_ID = "message_id"
    const val DELIVERY_TAG = "delivery_tag"
}
