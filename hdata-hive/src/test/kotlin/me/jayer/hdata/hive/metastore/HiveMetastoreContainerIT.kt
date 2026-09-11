package me.jayer.hdata.hive.metastore

import org.junit.jupiter.api.Tag
import org.junit.jupiter.api.Test
import org.testcontainers.containers.GenericContainer
import org.testcontainers.containers.wait.strategy.Wait
import org.testcontainers.utility.DockerImageName
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

/**
 * Exercises [ThriftHiveMetastore] against a real Hive Metastore service. This is the one Hive test
 * [InMemoryHiveMetastore] cannot stand in for: it proves the raw thrift calls (`get_table_req`,
 * `add_partition`, `get_partitions_by_filter`, `get_partitions_by_names`) actually match a real Hive
 * 4.x metastore's wire protocol, not just our own in-memory model of it.
 */
@Tag("integration")
class HiveMetastoreContainerIT {
    @Test
    fun `round-trips a partitioned table through a real Hive metastore`() {
        HiveMetastoreContainer().use { metastore ->
            metastore.start()
            val spec = HiveMetastoreSpec(uri = "thrift://${metastore.host}:${metastore.getMappedPort(PORT)}")
            ThriftHiveMetastore.connect(spec).use { client ->
                assertNull(client.getTable("default", "orders"))

                val table = HiveTable(
                    databaseName = "default",
                    tableName = "orders",
                    tableType = HiveTable.MANAGED_TABLE,
                    dataColumns = listOf(HiveColumn("id", "bigint"), HiveColumn("name", "string")),
                    partitionColumns = listOf(HiveColumn("dt", "string")),
                    storage = Storage(
                        storageFormat = me.jayer.hdata.hive.format.HiveStorageFormat.TEXTFILE.toStorageFormat(),
                        location = "/tmp/hdata-hive-it/default.db/orders",
                    ),
                )
                client.createTable(table)

                val fetched = requireNotNull(client.getTable("default", "orders"))
                assertEquals(listOf(HiveColumn("id", "bigint"), HiveColumn("name", "string")), fetched.dataColumns)
                assertEquals(listOf(HiveColumn("dt", "string")), fetched.partitionColumns)

                val partitionStorage = Storage(
                    storageFormat = me.jayer.hdata.hive.format.HiveStorageFormat.TEXTFILE.toStorageFormat(),
                    location = "/tmp/hdata-hive-it/default.db/orders/dt=2024-01-01",
                )
                val added = client.addPartitions(
                    "default",
                    "orders",
                    mapOf("dt=2024-01-01" to HivePartition(values = listOf("2024-01-01"), storage = partitionStorage)),
                )
                assertEquals(listOf("dt=2024-01-01"), added)
                // Re-adding the same partition must be a no-op, not an error — the write side can retry a bundle.
                assertTrue(
                    client.addPartitions(
                        "default",
                        "orders",
                        mapOf("dt=2024-01-01" to HivePartition(values = listOf("2024-01-01"), storage = partitionStorage)),
                    ).isEmpty(),
                )

                assertEquals(listOf("dt=2024-01-01"), client.getPartitionNames("default", "orders"))
                assertEquals(
                    listOf("dt=2024-01-01"),
                    client.getPartitionNamesByFilter("default", "orders", "dt = \"2024-01-01\""),
                )
                val byName = client.getPartitionsByNames("default", "orders", listOf("dt=2024-01-01"))
                assertEquals(listOf("2024-01-01"), byName.getValue("dt=2024-01-01").values)
            }
        }
    }

    private class HiveMetastoreContainer :
        GenericContainer<HiveMetastoreContainer>(DockerImageName.parse("apache/hive:standalone-metastore-4.2.1")) {
        init {
            withEnv("SERVICE_NAME", "metastore")
            withExposedPorts(PORT)
            waitingFor(Wait.forLogMessage(".*Started the new metaserver on port.*\\n", 1))
        }
    }

    private companion object {
        const val PORT = 9083
    }
}
