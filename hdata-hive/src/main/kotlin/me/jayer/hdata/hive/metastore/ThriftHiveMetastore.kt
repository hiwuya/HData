package me.jayer.hdata.hive.metastore

import org.apache.hadoop.hive.metastore.api.AlreadyExistsException
import org.apache.hadoop.hive.metastore.api.FieldSchema
import org.apache.hadoop.hive.metastore.api.GetTableRequest
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException
import org.apache.hadoop.hive.metastore.api.SerDeInfo
import org.apache.hadoop.hive.metastore.api.StorageDescriptor
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore as ThriftApi
import org.apache.thrift.TConfiguration
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.transport.TSocket
import org.apache.thrift.transport.TTransport
import org.slf4j.LoggerFactory
import java.net.URI
import org.apache.hadoop.hive.metastore.api.Partition as ThriftPartition
import org.apache.hadoop.hive.metastore.api.Table as ThriftTable

/**
 * Speaks the metastore's thrift protocol directly, the same route as Trino's `ThriftHiveMetastoreClient`: it uses
 * `ThriftHiveMetastore.Client` (the one generated from the thrift IDL), not `HiveMetaStoreClient`.
 *
 * Why not `HiveMetaStoreClient`: it is a facade for Hive itself and pulls in `MetastoreConf`, a retry proxy, ZooKeeper service
 * discovery and a whole transaction/lock manager, dragging derby, grpc and curator into the dependency tree, while all we need
 * is six read-only calls plus `add_partitions`.
 *
 * It is also a different thing from HiveServer2: this connects to the metastore (port 9083 by default), parses no SQL and starts
 *
 * @author wuya
 */
class ThriftHiveMetastore private constructor(
    private val transport: TTransport,
    private val client: ThriftApi.Client,
    private val endpoint: String,
) : HiveMetastore {

    /**
     * Uses `get_table_req` rather than the old `get_table`: the latter was removed from Hive 4's thrift IDL, while
     * `get_table_req` has been available since Hive 3.0. In other words the server must be at least 3.0.
     */
    override fun getTable(databaseName: String, tableName: String): HiveTable? = try {
        toHiveTable(client.get_table_req(GetTableRequest(databaseName, tableName)).table)
    } catch (e: NoSuchObjectException) {
        LOGGER.debug("table does not exist: {}.{} ({})", databaseName, tableName, e.message)
        null
    } catch (e: org.apache.thrift.TApplicationException) {
        if (e.type == org.apache.thrift.TApplicationException.UNKNOWN_METHOD) {
            throw IllegalStateException("metastore[$endpoint] does not support get_table_req, a Hive 3.0+ metastore is required", e)
        }
        throw e
    }

    override fun getPartitionNames(databaseName: String, tableName: String): List<String> =
        client.get_partition_names(databaseName, tableName, ALL_PARTITIONS)

    override fun getPartitionNamesByFilter(
        databaseName: String,
        tableName: String,
        filter: String,
    ): List<String> {
        // get_partitions_by_filter returns full partition objects while we only need the names; on a table with many partitions
        // this pass is not cheap, but still far better than fetching all partition names and filtering client-side
        val partitions = client.get_partitions_by_filter(databaseName, tableName, filter, ALL_PARTITIONS)
        val table = requireNotNull(getTable(databaseName, tableName)) { "table does not exist: $databaseName.$tableName" }
        val columnNames = table.partitionColumns.map { it.name }
        return partitions.map { PartitionNames.makePartName(columnNames, it.values) }
    }

    override fun getPartitionsByNames(
        databaseName: String,
        tableName: String,
        partitionNames: List<String>,
    ): Map<String, HivePartition> {
        if (partitionNames.isEmpty()) {
            return emptyMap()
        }
        val table = requireNotNull(getTable(databaseName, tableName)) { "table does not exist: $databaseName.$tableName" }
        val columnNames = table.partitionColumns.map { it.name }
        // Asking for too many partitions at once blows up the thrift message size, so fetch in batches
        return partitionNames.chunked(BATCH_SIZE).flatMap { batch ->
            client.get_partitions_by_names(databaseName, tableName, batch).map { partition ->
                PartitionNames.makePartName(columnNames, partition.values) to toHivePartition(partition)
            }
        }.toMap()
    }

    override fun addPartitions(
        databaseName: String,
        tableName: String,
        partitions: Map<String, HivePartition>,
    ): List<String> {
        if (partitions.isEmpty()) {
            return emptyList()
        }
        val existing = getPartitionsByNames(databaseName, tableName, partitions.keys.toList()).keys
        val missing = partitions.filterKeys { it !in existing }
        if (missing.isEmpty()) {
            return emptyList()
        }
        val added = mutableListOf<String>()
        missing.forEach { (name, partition) ->
            try {
                // Add one by one instead of a single add_partitions: the latter fails the whole batch when any partition already
                // exists, while "the partition was already created by another bundle" is the norm in parallel writes, not an error
                client.add_partition(toThriftPartition(databaseName, tableName, partition))
                added += name
            } catch (e: AlreadyExistsException) {
                LOGGER.debug("partition already exists, skipping: {}.{} {} ({})", databaseName, tableName, name, e.message)
            }
        }
        return added
    }

    override fun createTable(table: HiveTable) {
        client.create_table(toThriftTable(table))
    }

    override fun close() {
        transport.close()
    }

    companion object {
        private val LOGGER = LoggerFactory.getLogger(ThriftHiveMetastore::class.java)

        /** thrift's `max_parts` is a short; -1 means unlimited. */
        private const val ALL_PARTITIONS: Short = -1

        private const val BATCH_SIZE = 500

        /**
         * thrift's default maximum message size is 100MB. One `get_partitions_by_names` on a table with tens of thousands of
         * partitions can exceed that, so relax it to 1GB here — over the limit the error is `MaxMessageSize reached`, and working
         * back from that error to "too many partitions" is not obvious.
         */
        private const val MAX_MESSAGE_SIZE = 1024 * 1024 * 1024

        fun connect(spec: HiveMetastoreSpec): ThriftHiveMetastore {
            // Supports the HA form thrift://h1:9083,thrift://h2:9083; try each in turn
            val endpoints = spec.uri.split(',').map { it.trim() }.filter { it.isNotBlank() }
            require(endpoints.isNotEmpty()) { "metastore_uri must not be blank" }
            var lastError: Exception? = null
            endpoints.forEach { endpoint ->
                try {
                    return connectOne(endpoint, spec.timeoutMillis)
                } catch (e: Exception) {
                    LOGGER.warn("failed to connect to metastore[{}]: {}", endpoint, e.message)
                    lastError = e
                }
            }
            throw IllegalStateException("cannot connect to any metastore address: ${spec.uri}", lastError)
        }

        private fun connectOne(endpoint: String, timeoutMillis: Int): ThriftHiveMetastore {
            val uri = URI(endpoint)
            require(uri.scheme == "thrift") { "a metastore address must start with thrift://: $endpoint" }
            val port = if (uri.port > 0) uri.port else 9083
            val configuration = TConfiguration(
                MAX_MESSAGE_SIZE,
                TConfiguration.DEFAULT_MAX_FRAME_SIZE,
                TConfiguration.DEFAULT_RECURSION_DEPTH,
            )
            val transport = TSocket(configuration, uri.host, port, timeoutMillis)
            transport.open()
            val client = ThriftApi.Client(TBinaryProtocol(transport))
            LOGGER.info("connected to Hive metastore: {}:{}", uri.host, port)
            return ThriftHiveMetastore(transport, client, "${uri.host}:$port")
        }

        internal fun toHiveTable(table: ThriftTable): HiveTable = HiveTable(
            databaseName = table.dbName,
            tableName = table.tableName,
            tableType = table.tableType ?: HiveTable.MANAGED_TABLE,
            dataColumns = table.sd.cols.map { it.toHiveColumn() },
            partitionColumns = table.partitionKeys.orEmpty().map { it.toHiveColumn() },
            storage = table.sd.toStorage(),
            parameters = table.parameters.orEmpty(),
        )

        internal fun toHivePartition(partition: ThriftPartition): HivePartition = HivePartition(
            values = partition.values,
            storage = partition.sd.toStorage(),
            parameters = partition.parameters.orEmpty(),
        )

        private fun FieldSchema.toHiveColumn() = HiveColumn(name, type, comment)

        private fun StorageDescriptor.toStorage() = Storage(
            storageFormat = StorageFormat(
                serde = serdeInfo?.serializationLib.orEmpty(),
                inputFormat = inputFormat.orEmpty(),
                outputFormat = outputFormat.orEmpty(),
            ),
            location = location.orEmpty(),
            serdeParameters = serdeInfo?.parameters.orEmpty(),
        )

        private fun toThriftTable(table: HiveTable): ThriftTable = ThriftTable().apply {
            dbName = table.databaseName
            tableName = table.tableName
            tableType = table.tableType
            sd = toStorageDescriptor(table.storage, table.dataColumns)
            partitionKeys = table.partitionColumns.map { FieldSchema(it.name, it.type, it.comment) }
            parameters = table.parameters
            owner = System.getProperty("user.name")
        }

        private fun toThriftPartition(
            databaseName: String,
            tableName: String,
            partition: HivePartition,
        ): ThriftPartition = ThriftPartition().apply {
            dbName = databaseName
            this.tableName = tableName
            values = partition.values
            // A partition carries its own StorageDescriptor; column information comes from the table — partition-level column
            sd = toStorageDescriptor(partition.storage, emptyList())
            parameters = partition.parameters
            createTime = (System.currentTimeMillis() / 1000).toInt()
        }

        private fun toStorageDescriptor(storage: Storage, columns: List<HiveColumn>): StorageDescriptor =
            StorageDescriptor().apply {
                cols = columns.map { FieldSchema(it.name, it.type, it.comment) }
                location = storage.location
                inputFormat = storage.storageFormat.inputFormat
                outputFormat = storage.storageFormat.outputFormat
                serdeInfo = SerDeInfo().apply {
                    name = storage.storageFormat.serde.substringAfterLast('.')
                    serializationLib = storage.storageFormat.serde
                    parameters = storage.serdeParameters
                }
                setCompressed(false)
                numBuckets = -1
                bucketCols = emptyList()
                sortCols = emptyList()
                parameters = emptyMap()
            }
    }
}
