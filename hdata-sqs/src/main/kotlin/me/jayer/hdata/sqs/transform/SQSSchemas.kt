package me.jayer.hdata.sqs.transform

import org.apache.beam.sdk.schemas.Schema

/**
 * Fixed output schema for `ReadFromSQS`.
 */
val SQS_READ_SCHEMA: Schema = Schema.builder()
    .addStringField("message_id")
    .addStringField("body")
    .addStringField("receipt_handle")
    .addNullableMapField("attributes", Schema.FieldType.STRING, Schema.FieldType.STRING)
    .build()
