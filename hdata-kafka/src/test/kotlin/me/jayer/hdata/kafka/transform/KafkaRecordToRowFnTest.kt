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
 * How the read path actually honors `key_format` / `value_format`.
 *
 * This is the regression-prone spot called out in AGENTS: before the refactor these two parameters were accepted and
 * then dropped, everything was decoded as UTF-8 whatever they said, and binary messages were silently replaced with
 * the replacement character without any error. Here we must prove the rows read out are **really** different per
 * format.
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
    fun `value_format=raw preserves non-UTF-8 bytes verbatim`() {
        // These bytes are not valid UTF-8; decoding them as string would wreck them with replacement characters — raw must keep them as-is
        val bytes = byteArrayOf(0x00, 0xFF.toByte(), 0x80.toByte())
        val out = tester(KafkaRecordToRowFn("string", "raw")).processBundle(record(null, bytes))

        val got = out.single().getValue<Any?>("value") as ByteArray
        assertContentEquals(bytes, got)
        assertEquals(Schema.TypeName.BYTES, out.single().schema.getField("value").type.typeName)
    }

    @Test
    fun `value_format=string decodes as UTF-8`() {
        val bytes = "café".toByteArray()
        val out = tester(KafkaRecordToRowFn("string", "string")).processBundle(record(null, bytes))

        assertEquals("café", out.single().getValue("value"))
        assertEquals(Schema.TypeName.STRING, out.single().schema.getField("value").type.typeName)
    }

    @Test
    fun `key_format=raw keeps the key as bytes, string decodes it into text`() {
        val bytes = "k".toByteArray()
        val rawKey = tester(KafkaRecordToRowFn("raw", "string"))
            .processBundle(record(bytes, "v".toByteArray())).single()
        assertTrue(rawKey.getValue<Any?>("key") is ByteArray)

        val strKey = tester(KafkaRecordToRowFn("string", "string"))
            .processBundle(record(bytes, "v".toByteArray())).single()
        assertEquals("k", strKey.getValue<Any?>("key"))
    }

    @Test
    fun `a null header value stays null instead of becoming empty bytes`() {
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
