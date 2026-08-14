package me.jayer.hdata.hive.split

import me.jayer.hdata.hive.metastore.HivePartition
import me.jayer.hdata.hive.metastore.Storage
import java.io.Serializable

/**
 * 一个待读的分区（非分区表就是整张表），带上它自己的存储描述。
 *
 * 存储描述**必须逐分区带着走**：Hive 允许 `ALTER TABLE t PARTITION (dt='...') SET FILEFORMAT ORC`，
 * 一张表里前半年的分区是 TEXTFILE、后半年是 ORC 完全合法。只看表级格式的读取器
 * 会把 ORC 文件按文本读，读出来的是一堆二进制乱码。
 *
 * @param name `dt=2024-01-01/hr=01`，非分区表是空串
 * @param values 与表的分区列一一对应的原始值（已反转义），非分区表是空列表
 *
 * @author wuya
 */
data class HivePartitionSpec(
    val name: String,
    val values: List<String>,
    val storage: Storage,
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1

        fun of(name: String, partition: HivePartition) =
            HivePartitionSpec(name, partition.values, partition.storage)

        /** 非分区表：整张表当成一个"分区"。 */
        fun unpartitioned(storage: Storage) = HivePartitionSpec("", emptyList(), storage)
    }
}

/**
 * 分区目录下的一个数据文件。SDF 的限制是它内部的字节区间。
 */
data class HiveFile(
    val path: String,
    val length: Long,
    val partition: HivePartitionSpec,
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}
