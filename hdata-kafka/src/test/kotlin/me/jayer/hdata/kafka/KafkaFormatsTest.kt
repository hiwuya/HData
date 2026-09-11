package me.jayer.hdata.kafka

import org.apache.beam.sdk.schemas.Schema
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * key/value formats.
 *
 * Before the refactor these two parameters were accepted and then dropped: whatever was filled in, everything was
 * decoded as UTF-8, and binary messages were wrecked by the replacement character without any error.
 *
 * @author wuya
 */
class KafkaFormatsTest {

    @Test
    fun `the string format converts in both directions as UTF-8`() {
        assertEquals("café", KafkaFormat.STRING.decode("café".toByteArray()))
        assertContentEquals("café".toByteArray(), KafkaFormat.STRING.encode("café"))
    }

    @Test
    fun `the raw format preserves bytes verbatim`() {
        // These bytes are not valid UTF-8; decoding them as string turns them into replacement characters
        val bytes = byteArrayOf(0x00, 0xFF.toByte(), 0x7F, 0x80.toByte())

        assertContentEquals(bytes, KafkaFormat.RAW.decode(bytes) as ByteArray)
        assertTrue((KafkaFormat.STRING.decode(bytes) as String).toByteArray().size != bytes.size)
    }

    @Test
    fun `null stays null in both directions`() {
        assertNull(KafkaFormat.STRING.decode(null))
        assertNull(KafkaFormat.RAW.decode(null))
        assertNull(KafkaFormat.STRING.encode(null))
    }

    @Test
    fun `an unrecognized format name fails and lists the valid values`() {
        val error = assertFailsWith<IllegalArgumentException> { KafkaFormats.of("json", "value_format") }

        assertTrue("value_format" in error.message!! && "string" in error.message!! && "raw" in error.message!!)
    }

    @Test
    fun `the key and value types in the schema follow the format`() {
        val schema = KafkaFormats.readSchema(KafkaFormat.RAW, KafkaFormat.STRING)

        assertEquals(Schema.TypeName.BYTES, schema.getField("key").type.typeName)
        assertEquals(Schema.TypeName.STRING, schema.getField("value").type.typeName)
        assertEquals(Schema.TypeName.MAP, schema.getField("headers").type.typeName)
        // The metadata columns share their names with the Flink Kafka connector
        assertTrue(listOf("topic", "partition", "offset", "timestamp", "timestamp_type").all { schema.hasField(it) })
    }

    @Test
    fun `an encoded type that does not match the format fails`() {
        val error = assertFailsWith<IllegalArgumentException> { KafkaFormat.STRING.encode(42) }

        assertTrue("Integer" in error.message!!)
    }
}
