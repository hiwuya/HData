package me.jayer.hdata.hbase

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.Connection
import org.apache.hadoop.hbase.client.ConnectionFactory

/**
 * Builds HBase [Configuration] and [Connection] instances from configuration.
 *
 * Beam's `HBaseIO.readAll()` handles region discovery and uses `ByteKeyRangeTracker` to split scans dynamically.
 *
 * @author wuya
 */
object HBaseConnections {

    fun newConfiguration(
        zookeeperQuorum: String,
        znodeParent: String = "",
        properties: Map<String, String> = emptyMap(),
    ): Configuration = HBaseConfiguration.create().apply {
        properties.forEach { (key, value) -> set(key, value) }
        // Apply explicit fields last so identically named properties cannot silently override them.
        set("hbase.zookeeper.quorum", zookeeperQuorum)
        if (znodeParent.isNotBlank()) {
            set("zookeeper.znode.parent", znodeParent)
        }
    }

    fun newConnection(configuration: Configuration): Connection =
        ConnectionFactory.createConnection(configuration)
}
