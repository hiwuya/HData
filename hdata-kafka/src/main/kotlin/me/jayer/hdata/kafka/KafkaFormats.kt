package me.jayer.hdata.kafka

import org.apache.beam.sdk.schemas.Schema
import java.nio.charset.StandardCharsets

/**
 * key/value 的编解码方式。
 *
 * 重构前 `key_format` / `value_format` 是**收下就丢掉**的死参数：无论填什么都按 `StringDeserializer`
 * 处理，二进制消息会被静默替换成 U+FFFD 替换字符。现在它真正决定字段类型，填了不认识的值直接报错。
 *
 * @author wuya
 */
enum class KafkaFormat(val configValue: String, val fieldType: Schema.FieldType) {

    /** 按 UTF-8 解码成 STRING。二进制消息会丢字节，别用这个。 */
    STRING("string", Schema.FieldType.STRING),

    /** 原样保留成 BYTES。 */
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
            "$configValue 格式无法编码 ${value.javaClass.name}，字段类型应为 ${fieldType.typeName}"
        )
    }
}

/**
 * 读出行的固定 schema，以及 key/value 格式的解析。
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
                "$configKey 取值非法: $value，可选 ${KafkaFormat.entries.joinToString { it.configValue }}"
            )

    /**
     * `key` / `value` 的类型由格式决定，其余是 Flink Kafka connector 同名的元数据列。
     *
     * `headers` 与 Flink 一样是 `MAP<STRING, BYTES>`：Kafka 允许同名 header 出现多次，
     * 转成 map 时只保留最后一个。
     */
    fun readSchema(keyFormat: KafkaFormat, valueFormat: KafkaFormat): Schema = Schema.builder()
        .addNullableField(KEY, keyFormat.fieldType)
        .addNullableField(VALUE, valueFormat.fieldType)
        .addStringField(TOPIC)
        .addInt32Field(PARTITION)
        .addInt64Field(OFFSET)
        .addInt64Field(TIMESTAMP)
        .addStringField(TIMESTAMP_TYPE)
        .addNullableField(HEADERS, Schema.FieldType.map(Schema.FieldType.STRING, Schema.FieldType.BYTES))
        .build()

    fun readSchema(config: KafkaReadConfig): Schema =
        readSchema(of(config.keyFormat, "key_format"), of(config.valueFormat, "value_format"))
}
