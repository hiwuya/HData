package me.jayer.hdata.iceberg.transform

import java.io.Serializable
import java.nio.ByteBuffer
import org.apache.iceberg.types.Type

/**
 * A serializable description of one Iceberg data file (or a byte range within a file), serving as the basic unit of
 * parallel reads.
 *
 * Modeled on Trino: Iceberg splits are divided by `data file`, with large files further subdivided by row group.
 * Here we split by file (`start`/`length` come from [org.apache.iceberg.FileScanTask], which for AVRO data files is
 * usually 0 / the whole file size), handing each file to an independent DoFn instance to parallelize the full-table
 * scan.
 *
 * Partition metadata is recorded in `FileScanTask.partition()`; here we carry the partition fields' (name, value)
 * along too. The value must first be normalized into a serializable representation, because Iceberg's binary
 * partition values are non-serializable [ByteBuffer]s.
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

/** Normalizes Iceberg partition metadata into JVM values that can safely pass through a Beam coder. */
internal fun serializablePartitionValue(value: Any?): Serializable? = when (value) {
    null -> null
    is ByteBuffer -> {
        val copy = value.duplicate()
        ByteArray(copy.remaining()).also { copy.get(it) }
    }
    is CharSequence -> value.toString()
    is Serializable -> value
    else -> throw IllegalArgumentException(
        "Iceberg partition value type [${value.javaClass.name}] is not serializable, cannot generate a parallel-read split",
    )
}

/** The internal representation required to reconstruct an Iceberg Record; binary/fixed fields must use ByteBuffer. */
internal fun icebergPartitionValue(value: Serializable?, type: Type): Any? = when {
    value is ByteArray && type.typeId() in setOf(Type.TypeID.BINARY, Type.TypeID.FIXED) -> ByteBuffer.wrap(value)
    else -> value
}
