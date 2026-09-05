package me.jayer.hdata.iceberg.transform

import java.io.Serializable
import java.nio.ByteBuffer
import org.apache.iceberg.types.Type

/**
 * 一个 Iceberg 数据文件（或文件内的一个字节区间）的可序列化描述，作为并行读的基本单元。
 *
 * 对标 Trino：Iceberg 的 split 按 `data file` 划分，大文件再按 row group 细分。这里按文件切分
 * （`start`/`length` 来自 [org.apache.iceberg.FileScanTask]，对 AVRO 数据文件通常是 0 / 整文件大小），
 * 每个文件交给独立的 DoFn 实例读，从而把整表扫描并行起来。
 *
 * 分区元数据记在 `FileScanTask.partition()` 里；这里把分区字段的（名字, 值）一并带下来。
 * 值必须先规范化成可序列化表示，因为 Iceberg 的 binary 分区值是不可序列化的 [ByteBuffer]。
 *
 * @author wuya
 */
data class IcebergFileSplit(
    val path: String,
    val format: String,
    val start: Long,
    val length: Long,
    val partitionSpecId: Int,
    val partitionNames: List<String>,
    val partitionValues: List<Serializable?>,
) : Serializable {

    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/** 把 Iceberg 分区元数据规范化成能安全穿过 Beam coder 的 JVM 值。 */
internal fun serializablePartitionValue(value: Any?): Serializable? = when (value) {
    null -> null
    is ByteBuffer -> {
        val copy = value.duplicate()
        ByteArray(copy.remaining()).also { copy.get(it) }
    }
    is CharSequence -> value.toString()
    is Serializable -> value
    else -> throw IllegalArgumentException(
        "Iceberg 分区值类型[${value.javaClass.name}]不可序列化，无法生成并行读取 split",
    )
}

/** 恢复 Iceberg Record 所需的内部表示；binary/fixed 字段必须使用 ByteBuffer。 */
internal fun icebergPartitionValue(value: Serializable?, type: Type): Any? = when {
    value is ByteArray && type.typeId() in setOf(Type.TypeID.BINARY, Type.TypeID.FIXED) -> ByteBuffer.wrap(value)
    else -> value
}
