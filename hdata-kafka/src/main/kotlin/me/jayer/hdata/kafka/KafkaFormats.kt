package me.jayer.hdata.kafka

import org.apache.beam.sdk.schemas.Schema
import java.nio.charset.StandardCharsets

/**
 * Encoding/decoding method for key/value.
 *
 * Before this refactor `key_format` / `value_format` were dead parameters that were **accepted and
 * then dropped**: whatever you filled in, it was always handled as `StringDeserializer`, and binary
 * messages were silently replaced with the U+FFFD replacement character. Now they genuinely
 * determine the field type, and an unrecognized value fails immediately.
 *
 * @author wuya
 */
enum class KafkaFormat(val configValue: String, val fieldType: Schema.FieldType) {

    /** Decode as STRING using UTF-8. Binary messages lose bytes; do not use this for them. */
    STRING("string", Schema.FieldType.STRING),

    /** Keep as-is as BYTES. */
    RAW("raw", Schema.FieldType.BYTES),
    ;

    fun decode(bytes: ByteArray?): Any? = when {
        bytes == null -> null
        this == STRING -> String(bytes, StandardCharsets.UTF_8)
        else -> bytes
    }

    fun encode(value: Any?): ByteArray? = when (value) {
        null -> null
        is ByteArray -> value
        is String -> value.toByteArray(StandardCharsets.UTF_8)
        else -> throw IllegalArgumentException(
            "$configValue format cannot encode ${value.javaClass.name}; the field type should be ${fieldType.typeName}"
        )
    }
}

/**
 * The fixed schema of the emitted row, plus key/value format parsing.
 */
object KafkaFormats {

    const val KEY = "key"
    const val VALUE = "value"
    const val TOPIC = "topic"
    const val PARTITION = "partition"
    const val OFFSET = "offset"
    const val TIMESTAMP = "timestamp"
    const val TIMESTAMP_TYPE = "timestamp_type"
    const val HEADERS = "headers"

    fun of(value: String, configKey: String): KafkaFormat =
        KafkaFormat.entries.firstOrNull { it.configValue == value }
            ?: throw IllegalArgumentException(
                "$configKey is invalid: $value; allowed values: ${KafkaFormat.entries.joinToString { it.configValue }}"
            )

    /**
     * The type of `key` / `value` is decided by the format; the rest are metadata columns with the
     * same names as in the Flink Kafka connector.
     *
     * `headers` is `MAP<STRING, BYTES>` just like Flink: Kafka allows a header with the same name to
     * appear multiple times, and when converting to a map only the last one is kept.
     */
    fun readSchema(keyFormat: KafkaFormat, valueFormat: KafkaFormat): Schema = Schema.builder()
        .addNullableField(KEY, keyFormat.fieldType)
        .addNullableField(VALUE, valueFormat.fieldType)
        .addStringField(TOPIC)
        .addInt32Field(PARTITION)
        .addInt64Field(OFFSET)
        .addInt64Field(TIMESTAMP)
        .addStringField(TIMESTAMP_TYPE)
        // A Kafka header's value can legitimately be null; we must not turn null into empty bytes,
        // otherwise the two kinds of messages become indistinguishable.
        .addNullableField(
            HEADERS,
            Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.BYTES.withNullable(true)),
        )
        .build()

    fun readSchema(config: KafkaReadConfig): Schema =
        readSchema(of(config.keyFormat, "key_format"), of(config.valueFormat, "value_format"))
}
