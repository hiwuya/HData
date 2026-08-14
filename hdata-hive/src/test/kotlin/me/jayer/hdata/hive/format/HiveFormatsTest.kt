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
 * 存储格式判定、可切分性判定、以及 `LazySimpleSerDe` 的文本编解码。
 *
 * @author wuya
 */
class HiveFormatsTest {

    @Test
    fun `按 SerDe 与 InputFormat 判定格式`() {
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
    fun `LazySimpleSerDe 同时是 TEXTFILE 与 SEQUENCEFILE，靠 InputFormat 区分`() {
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
    fun `RCFile 的两种编码靠 SerDe 区分`() {
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
    fun `判不出格式直接抛，不猜`() {
        assertFailsWith<IllegalArgumentException> {
            HiveStorageFormat.of(StorageFormat("com.example.MySerde", "com.example.MyInputFormat", ""))
        }
    }

    @Test
    fun `压缩过的文本不可按字节切分`() {
        val configuration = HiveFileSystems.configurationOf(emptyMap())
        assertTrue(HiveFileSystems.isSplittable(HiveStorageFormat.TEXTFILE, "/data/part-0", configuration))
        assertFalse(HiveFileSystems.isSplittable(HiveStorageFormat.TEXTFILE, "/data/part-0.gz", configuration))
        // 列式格式的压缩是块内压缩，不影响切分
        assertTrue(HiveFileSystems.isSplittable(HiveStorageFormat.ORC, "/data/part-0.orc", configuration))
        // CSV 的引号字段可以内嵌换行，任何时候都不可切
        assertFalse(HiveFileSystems.isSplittable(HiveStorageFormat.CSV, "/data/part-0", configuration))
    }

    @Test
    fun `隐藏文件与元数据文件要跳过`() {
        assertTrue(HiveFileSystems.isHidden("_SUCCESS"))
        assertTrue(HiveFileSystems.isHidden(".hive-staging_xxx"))
        assertFalse(HiveFileSystems.isHidden("part-00000"))
    }

    @Test
    fun `文本编解码往返`() {
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
    fun `null 写成反斜杠 N`() {
        val codec = LazySimpleCodec(emptyMap())
        assertEquals("\\N", codec.encodeField(null, FieldTypes.STRING, level = 1))
        assertNull(codec.decodeField("\\N", FieldTypes.STRING, level = 1))
    }

    @Test
    fun `嵌套类型用分层分隔符`() {
        val codec = LazySimpleCodec(emptyMap())
        val arrayType = Schema.FieldType.array(FieldTypes.STRING)
        // 数组元素用第 1 层分隔符 \002
        assertEquals("a\u0002b", codec.encodeField(listOf("a", "b"), arrayType, level = 1))
        assertEquals(listOf("a", "b"), codec.decodeField("a\u0002b", arrayType, level = 1))

        val mapType = Schema.FieldType.map(FieldTypes.STRING, FieldTypes.INT32)
        // map 条目用 \002，key 与 value 之间用 \003
        assertEquals(mapOf("a" to 1), codec.decodeField("a\u0003" + "1", mapType, level = 1))
    }

    @Test
    fun `自定义分隔符生效`() {
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
    fun `列数比表定义少时缺的列补 null`() {
        // 加列之后的老文件全靠这个语义才读得动
        val codec = LazySimpleCodec(emptyMap())
        val types = listOf(FieldTypes.STRING, FieldTypes.STRING, FieldTypes.INT32)
        val decoded = codec.decodeRow("a\u0001b", types, listOf(0, 1, 2))
        assertEquals("a", decoded[0])
        assertEquals("b", decoded[1])
        assertNull(decoded[2])
    }
}
