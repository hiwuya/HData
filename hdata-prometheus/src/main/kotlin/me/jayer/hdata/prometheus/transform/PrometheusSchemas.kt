package me.jayer.hdata.prometheus.transform

import org.apache.beam.sdk.schemas.Schema

/**
 * Fixed output schema for `ReadFromPrometheus`.
 */
val PROMETHEUS_READ_SCHEMA: Schema = Schema.builder()
    .addStringField("metric_name")
    .addNullableMapField("labels", Schema.FieldType.STRING, Schema.FieldType.STRING)
    .addDoubleField("value")
    .addDoubleField("timestamp")
    .build()
