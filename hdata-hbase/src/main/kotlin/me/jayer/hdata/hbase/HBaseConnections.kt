package me.jayer.hdata.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory

/**
 * 从配置构造 HBase 的 [Configuration] / [Connection]。
 *
 * region 边界发现（原来的 `listRegions`）已经删掉：读取端改用 Beam 的 `HBaseIO.readAll()`，
 * 它的 `HBaseReadSplittableDoFn` 自己按 region 切分，而且用 `ByteKeyRangeTracker` 支持
 * 扫描过程中的动态再切分，比在构图阶段一次性切死更均衡。
 *
 * @author wuya
 */
object HBaseConnections {

    fun newConfiguration(
        zookeeperQuorum: String,
        znodeParent: String = "",
        properties: Map<String, String> = emptyMap(),
    ): Configuration = HBaseConfiguration.create().apply {
        set("hbase.zookeeper.quorum", zookeeperQuorum)
        if (znodeParent.isNotBlank()) {
            set("zookeeper.znode.parent", znodeParent)
        }
        properties.forEach { (key, value) -> set(key, value) }
    }

    fun newConnection(configuration: Configuration): Connection =
        ConnectionFactory.createConnection(configuration)
}
