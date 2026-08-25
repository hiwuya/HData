package me.jayer.hdata.iceberg.transform

import java.io.Serializable

/**
 * 一个 Iceberg 数据文件（或文件内的一个字节区间）的可序列化描述，作为并行读的基本单元。
 *
 * 对标 Trino：Iceberg 的 split 按 `data file` 划分，大文件再按 row group 细分。这里按文件切分
 * （`start`/`length` 来自 [org.apache.iceberg.FileScanTask]，对 AVRO 数据文件通常是 0 / 整文件大小），
 * 每个文件交给独立的 DoFn 实例读，从而把整表扫描并行起来。
 *
 * 分区列的值不存进数据文件，而是记在 `FileScanTask.partition()` 里；读单文件时要把它们回填进行，
 * 所以这里把分区列的（名字, 值）一并带下来。
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
    val partitionValues: List<Any?>,
) : Serializable {

    companion object {
        private const val serialVersionUID: Long = 1
    }
}
