package me.jayer.hdata.hive.format

import me.jayer.hdata.core.type.FieldTypes
import me.jayer.hdata.hive.metastore.StorageFormat
import me.jayer.hdata.hive.split.HiveFileSystems
import org.apache.beam.sdk.schemas.Schema
import org.apache.beam.sdk.values.Row
import java.math.BigDecimal
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Storage format detection, splittability detection, and the text codec of `LazySimpleSerDe`.
 *
 * @author wuya
 */
class HiveFormatsTest {

    @Test
    fun `determines the format by SerDe and InputFormat`() {
        assertEquals(
            HiveStorageFormat.ORC,
            HiveStorageFormat.of(
                StorageFormat(
                    "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
                    "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
                    "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
                )
            ),
        )
    }

    @Test
    fun `LazySimpleSerDe is both TEXTFILE and SEQUENCEFILE, distinguished by InputFormat`() {
        val serde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe"
        assertEquals(
            HiveStorageFormat.TEXTFILE,
            HiveStorageFormat.of(StorageFormat(serde, "org.apache.hadoop.mapred.TextInputFormat", "")),
        )
        assertEquals(
            HiveStorageFormat.SEQUENCEFILE,
            HiveStorageFormat.of(StorageFormat(serde, "org.apache.hadoop.mapred.SequenceFileInputFormat", "")),
        )
    }

    @Test
    fun `RCFile's two encodings are distinguished by SerDe`() {
        val inputFormat = "org.apache.hadoop.hive.ql.io.RCFileInputFormat"
        assertEquals(
            HiveStorageFormat.RCTEXT,
            HiveStorageFormat.of(
                StorageFormat("org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe", inputFormat, "")
            ),
        )
        assertEquals(
            HiveStorageFormat.RCBINARY,
            HiveStorageFormat.of(
                StorageFormat("org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe", inputFormat, "")
            ),
        )
    }

    @Test
    fun `an undeterminable format throws directly, no guessing`() {
        assertFailsWith<IllegalArgumentException> {
            HiveStorageFormat.of(StorageFormat("com.example.MySerde", "com.example.MyInputFormat", ""))
        }
    }

    @Test
    fun `compressed text cannot be split by byte range`() {
        val configuration = HiveFileSystems.configurationOf(emptyMap())
        assertTrue(HiveFileSystems.isSplittable(HiveStorageFormat.TEXTFILE, "/data/part-0", configuration))
        assertFalse(HiveFileSystems.isSplittable(HiveStorageFormat.TEXTFILE, "/data/part-0.gz", configuration))
        // Compression in columnar formats is intra-block and does not affect splitting
        assertTrue(HiveFileSystems.isSplittable(HiveStorageFormat.ORC, "/data/part-0.orc", configuration))
        // A quoted CSV field can contain newlines, so it is never splittable
        assertFalse(HiveFileSystems.isSplittable(HiveStorageFormat.CSV, "/data/part-0", configuration))
    }

    @Test
    fun `hidden and metadata files are skipped`() {
        assertTrue(HiveFileSystems.isHidden("_SUCCESS"))
        assertTrue(HiveFileSystems.isHidden(".hive-staging_xxx"))
        assertFalse(HiveFileSystems.isHidden("part-00000"))
    }

    @Test
    fun `text codec round-trips`() {
        val codec = LazySimpleCodec(emptyMap())
        val schema = Schema.builder()
            .addNullableField("id", FieldTypes.INT64)
            .addNullableField("name", FieldTypes.STRING)
            .addNullableField("amount", FieldTypes.DECIMAL)
            .build()
        val row = Row.withSchema(schema).addValues(1L, "张三", BigDecimal("1.50")).build()

        val encoded = codec.encodeRow(row)
        assertEquals("1\u0001张三\u00011.50", encoded)

        val decoded = codec.decodeRow(encoded, schema.fields.map { it.type }, listOf(0, 1, 2))
        assertEquals(1L, decoded[0])
        assertEquals("张三", decoded[1])
        assertEquals(BigDecimal("1.50"), decoded[2])
    }

    @Test
    fun `null is written as backslash-N`() {
        val codec = LazySimpleCodec(emptyMap())
        assertEquals("\\N", codec.encodeField(null, FieldTypes.STRING, level = 1))
        assertNull(codec.decodeField("\\N", FieldTypes.STRING, level = 1))
    }

    @Test
    fun `nested types use hierarchical delimiters`() {
        val codec = LazySimpleCodec(emptyMap())
        val arrayType = Schema.FieldType.array(FieldTypes.STRING)
        // Array elements use the level-1 delimiter \002
        assertEquals("a\u0002b", codec.encodeField(listOf("a", "b"), arrayType, level = 1))
        assertEquals(listOf("a", "b"), codec.decodeField("a\u0002b", arrayType, level = 1))

        val mapType = Schema.FieldType.map(FieldTypes.STRING, FieldTypes.INT32)
        // Map entries use \002, with \003 between the key and the value
        assertEquals(mapOf("a" to 1), codec.decodeField("a\u0003" + "1", mapType, level = 1))
    }

    @Test
    fun `a custom delimiter takes effect`() {
        val codec = LazySimpleCodec(mapOf("field.delim" to ","))
        val schema = Schema.builder()
            .addNullableField("a", FieldTypes.STRING)
            .addNullableField("b", FieldTypes.STRING)
            .build()
        val decoded = codec.decodeRow("x,y", schema.fields.map { it.type }, listOf(0, 1))
        assertEquals("x", decoded[0])
        assertEquals("y", decoded[1])
    }

    @Test
    fun `missing trailing columns are padded with null`() {
        // Old files written before a column was added are readable only thanks to this semantics
        val codec = LazySimpleCodec(emptyMap())
        val types = listOf(FieldTypes.STRING, FieldTypes.STRING, FieldTypes.INT32)
        val decoded = codec.decodeRow("a\u0001b", types, listOf(0, 1, 2))
        assertEquals("a", decoded[0])
        assertEquals("b", decoded[1])
        assertNull(decoded[2])
    }
}
