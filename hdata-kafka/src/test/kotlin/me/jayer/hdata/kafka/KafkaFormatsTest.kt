package me.jayer.hdata.kafka

import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * key/value 格式。
 *
 * 重构前这两个参数是收下就丢掉的：无论填什么都按 UTF-8 解码，二进制消息会被替换字符毁掉且不报错。
 *
 * @author wuya
 */
class KafkaFormatsTest {

    @Test
    fun `string 格式按 UTF-8 双向转换`() {
        assertEquals("你好", KafkaFormat.STRING.decode("你好".toByteArray()))
        assertContentEquals("你好".toByteArray(), KafkaFormat.STRING.encode("你好"))
    }

    @Test
    fun `raw 格式原样保留字节`() {
        // 这串字节不是合法 UTF-8，按 string 解码会变成替换字符
        val bytes = byteArrayOf(0x00, 0xFF.toByte(), 0x7F, 0x80.toByte())

        assertContentEquals(bytes, KafkaFormat.RAW.decode(bytes) as ByteArray)
        assertTrue((KafkaFormat.STRING.decode(bytes) as String).toByteArray().size != bytes.size)
    }

    @Test
    fun `null 在两个方向上都保持为 null`() {
        assertNull(KafkaFormat.STRING.decode(null))
        assertNull(KafkaFormat.RAW.decode(null))
        assertNull(KafkaFormat.STRING.encode(null))
    }

    @Test
    fun `格式名不认识时报错并列出可选值`() {
        val error = assertFailsWith<IllegalArgumentException> { KafkaFormats.of("json", "value_format") }

        assertTrue("value_format" in error.message!! && "string" in error.message!! && "raw" in error.message!!)
    }

    @Test
    fun `schema 里 key 与 value 的类型跟着格式走`() {
        val schema = KafkaFormats.readSchema(KafkaFormat.RAW, KafkaFormat.STRING)

        assertEquals(Schema.TypeName.BYTES, schema.getField("key").type.typeName)
        assertEquals(Schema.TypeName.STRING, schema.getField("value").type.typeName)
        assertEquals(Schema.TypeName.MAP, schema.getField("headers").type.typeName)
        // 元数据列与 Flink Kafka connector 同名
        assertTrue(listOf("topic", "partition", "offset", "timestamp", "timestamp_type").all { schema.hasField(it) })
    }

    @Test
    fun `编码类型与格式不符时报错`() {
        val error = assertFailsWith<IllegalArgumentException> { KafkaFormat.STRING.encode(42) }

        assertTrue("Integer" in error.message!!)
    }
}
