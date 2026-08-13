package me.jayer.hdata.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory

/**
 * HBase 连接相关的共用工具：从 `zookeeper_quorum` 构造 [Configuration] / [Connection]，
 * 以及构图阶段用来切分 region 的边界发现。
 */
object HBaseConnections {

    fun newConfiguration(zookeeperQuorum: String): Configuration =
        HBaseConfiguration.create().apply {
            set("hbase.zookeeper.quorum", zookeeperQuorum)
        }

    fun newConnection(zookeeperQuorum: String): Connection =
        ConnectionFactory.createConnection(newConfiguration(zookeeperQuorum))

    /** 取表里每个 region 的 `[startKey, stopKey)` 范围，作为 Splittable DoFn 的切分单元。 */
    fun listRegions(zookeeperQuorum: String, table: String): List<HBaseRegion> {
        val tableName = TableName.valueOf(table)
        val connection = newConnection(zookeeperQuorum)
        try {
            val locator = connection.getRegionLocator(tableName)
            try {
                val keys: org.apache.hadoop.hbase.util.Pair<Array<ByteArray>, Array<ByteArray>> = locator.startEndKeys
                val startKeys = keys.first
                val endKeys = keys.second
                return startKeys.indices.map { i -> HBaseRegion(table, startKeys[i], endKeys[i]) }
            } finally {
                runCatching { locator.close() }
            }
        } finally {
            runCatching { connection.close() }
        }
    }
}
