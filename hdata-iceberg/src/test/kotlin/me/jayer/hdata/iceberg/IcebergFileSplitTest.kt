package me.jayer.hdata.iceberg

import me.jayer.hdata.iceberg.transform.IcebergFileSplit
import me.jayer.hdata.iceberg.transform.icebergReadProjection
import me.jayer.hdata.iceberg.transform.icebergPartitionValue
import me.jayer.hdata.iceberg.transform.serializablePartitionValue
import org.apache.beam.sdk.util.SerializableUtils
import org.apache.iceberg.types.Types
import java.nio.ByteBuffer
import kotlin.test.Test
import kotlin.test.assertContentEquals
import kotlin.test.assertEquals
import kotlin.test.assertIs

class IcebergFileSplitTest {

    @Test
    fun `普通读取只投影输出数据列且过滤读取完整数据列`() {
        val fullSchema = org.apache.iceberg.Schema(
            Types.NestedField.optional(1, "id", Types.LongType.get()),
            Types.NestedField.optional(2, "name", Types.StringType.get()),
            Types.NestedField.optional(3, "region", Types.StringType.get()),
        )

        val projected = icebergReadProjection(fullSchema, setOf("name", "region"), setOf("region"), false)
        assertEquals(listOf("name"), projected.columns().map { it.name() })

        val filtered = icebergReadProjection(fullSchema, setOf("name"), setOf("region"), true)
        assertEquals(listOf("id", "name"), filtered.columns().map { it.name() })
    }

    @Test
    fun `binary 分区值可序列化并能恢复 ByteBuffer`() {
        val source = ByteBuffer.wrap(byteArrayOf(1, 2, 3))
        source.position(1)
        val encoded = serializablePartitionValue(source)
        assertContentEquals(byteArrayOf(2, 3), assertIs<ByteArray>(encoded))
        assertEquals(1, source.position(), "normalization must not change the position of the original buffer Iceberg provided")

        val split = IcebergFileSplit("a.avro", "AVRO", 0, 1, 0, listOf("payload"), listOf(encoded))
        SerializableUtils.ensureSerializable(split)

        val restored = assertIs<ByteBuffer>(icebergPartitionValue(encoded, Types.BinaryType.get()))
        val bytes = ByteArray(restored.remaining()).also { restored.get(it) }
        assertContentEquals(byteArrayOf(2, 3), bytes)
    }
}
