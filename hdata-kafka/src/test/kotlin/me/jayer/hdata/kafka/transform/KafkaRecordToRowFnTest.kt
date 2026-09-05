package me.jayer.hdata.kafka.transform

import org.apache.beam.sdk.io.kafka.KafkaRecord
import org.apache.beam.sdk.io.kafka.KafkaTimestampType
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.transforms.DoFnTester
import org.apache.beam.sdk.values.Row
import org.apache.kafka.common.header.internals.RecordHeaders
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * 读路径对 `key_format` / `value_format` 的落实。
 *
 * 这是 AGENTS 点名的易回归点：重构前这两个参数是收下就丢掉的死参数，无论填什么都按 UTF-8 解码，
 * 二进制消息会被静默替换成替换字符且不报错。这里必须证明读出来的行**真的**因格式不同而不同。
 *
 * @author wuya
 */
class KafkaRecordToRowFnTest {

    private fun record(
        key: ByteArray?,
        value: ByteArray?,
        headers: RecordHeaders = RecordHeaders(),
    ): KafkaRecord<ByteArray, ByteArray> =
        KafkaRecord("orders", 0, 5L, 1000L, KafkaTimestampType.CREATE_TIME, headers, key, value)

    private fun tester(fn: KafkaRecordToRowFn): DoFnTester<KafkaRecord<ByteArray, ByteArray>, Row> =
        DoFnTester.of(fn).apply { setCloningBehavior(DoFnTester.CloningBehavior.DO_NOT_CLONE) }

    @Test
    fun `value_format=raw 原样保留非 UTF-8 字节`() {
        // 这串字节不是合法 UTF-8，若按 string 解码会被替换字符毁掉；raw 必须原样保留
        val bytes = byteArrayOf(0x00, 0xFF.toByte(), 0x80.toByte())
        val out = tester(KafkaRecordToRowFn("string", "raw")).processBundle(record(null, bytes))

        val got = out.single().getValue<Any?>("value") as ByteArray
        assertContentEquals(bytes, got)
        assertEquals(Schema.TypeName.BYTES, out.single().schema.getField("value").type.typeName)
    }

    @Test
    fun `value_format=string 按 UTF-8 解码`() {
        val bytes = "你好".toByteArray()
        val out = tester(KafkaRecordToRowFn("string", "string")).processBundle(record(null, bytes))

        assertEquals("你好", out.single().getValue("value"))
        assertEquals(Schema.TypeName.STRING, out.single().schema.getField("value").type.typeName)
    }

    @Test
    fun `key_format=raw 时 key 是字节，string 时解码成文本`() {
        val bytes = "k".toByteArray()
        val rawKey = tester(KafkaRecordToRowFn("raw", "string"))
            .processBundle(record(bytes, "v".toByteArray())).single()
        assertTrue(rawKey.getValue<Any?>("key") is ByteArray)

        val strKey = tester(KafkaRecordToRowFn("string", "string"))
            .processBundle(record(bytes, "v".toByteArray())).single()
        assertEquals("k", strKey.getValue<Any?>("key"))
    }

    @Test
    fun `null header value 保持为 null 而不是变成空字节`() {
        val headers = RecordHeaders()
        headers.add("nullable", null)
        headers.add("empty", ByteArray(0))

        val row = tester(KafkaRecordToRowFn("string", "string"))
            .processBundle(record(null, "v".toByteArray(), headers)).single()
        val values = row.getMap<String, ByteArray?>("headers")!!

        assertTrue(values.containsKey("nullable"))
        assertNull(values["nullable"])
        assertContentEquals(ByteArray(0), values["empty"])
    }
}
