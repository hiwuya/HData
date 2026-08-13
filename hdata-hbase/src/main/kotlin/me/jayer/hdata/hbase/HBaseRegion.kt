package me.jayer.hdata.hbase

import java.io.Serializable

/**
 * 一个被切分的读取单元：某个表的一段 `[startKey, stopKey)` region 范围。
 *
 * 真正的 region 边界在构图阶段通过 [org.apache.hadoop.hbase.client.Admin.getRegionLocations]
 * 拿到，这里只描述"要扫哪段"，交给 [me.jayer.hdata.hbase.transform.HBaseReadFn] 用 Scan 读出。
 */
data class HBaseRegion(
    val table: String,
    val startKey: ByteArray,
    val stopKey: ByteArray,
) : Serializable {
    override fun equals(other: Any?): Boolean {
        if (this === other) return true
        if (javaClass != other?.javaClass) return false
        other as HBaseRegion
        return table == other.table &&
            startKey.contentEquals(other.startKey) &&
            stopKey.contentEquals(other.stopKey)
    }

    override fun hashCode(): Int {
        var result = table.hashCode()
        result = 31 * result + startKey.contentHashCode()
        result = 31 * result + stopKey.contentHashCode()
        return result
    }
}
