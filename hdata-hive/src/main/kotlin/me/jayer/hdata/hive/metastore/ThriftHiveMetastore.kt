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
 * 直接说 metastore 的 thrift 协议，和 Trino 的 `ThriftHiveMetastoreClient` 一个路子：
 * 用 `ThriftHiveMetastore.Client`（thrift IDL 生成的那个），不用 `HiveMetaStoreClient`。
 *
 * 为什么不用 `HiveMetaStoreClient`：它是给 Hive 自己用的门面，会拉起 `MetastoreConf`、
 * 重试代理、ZooKeeper 服务发现、事务/锁管理一整套东西，依赖树里 derby、grpc、curator 全都要跟着进来，
 * 而我们只需要 6 个只读调用加一个 `add_partitions`。
 *
 * 和 HiveServer2 也不是一回事：这里连的是 metastore（默认 9083 端口），不解析 SQL，不起 MR/Tez 作业。
 *
 * @author wuya
 */
class ThriftHiveMetastore private constructor(
    private val transport: TTransport,
    private val client: ThriftApi.Client,
    private val endpoint: String,
) : HiveMetastore {

    /**
     * 用 `get_table_req` 而不是老的 `get_table`：后者在 Hive 4 的 thrift IDL 里已经删掉了，
     * `get_table_req` 从 Hive 3.0 开始提供。也就是说服务端至少要 3.0。
     */
    override fun getTable(databaseName: String, tableName: String): HiveTable? = try {
        toHiveTable(client.get_table_req(GetTableRequest(databaseName, tableName)).table)
    } catch (e: NoSuchObjectException) {
        LOGGER.debug("表不存在: {}.{} ({})", databaseName, tableName, e.message)
        null
    } catch (e: org.apache.thrift.TApplicationException) {
        if (e.type == org.apache.thrift.TApplicationException.UNKNOWN_METHOD) {
            throw IllegalStateException("metastore[$endpoint] 不支持 get_table_req，需要 Hive 3.0 及以上的 metastore", e)
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
        // get_partitions_by_filter 返回的是完整分区对象，这里只要名字；
        // 分区数多的表这一趟不便宜，但比拉全量分区名再在客户端过滤仍然划算得多
        val partitions = client.get_partitions_by_filter(databaseName, tableName, filter, ALL_PARTITIONS)
        val table = requireNotNull(getTable(databaseName, tableName)) { "表不存在: $databaseName.$tableName" }
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
        val table = requireNotNull(getTable(databaseName, tableName)) { "表不存在: $databaseName.$tableName" }
        val columnNames = table.partitionColumns.map { it.name }
        // 一次要太多分区会把 thrift 的消息体撑爆，分批取
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
                // 逐个加而不是一把 add_partitions：后者只要有一个分区已存在就整批失败，
                // 而"分区已经被别的 bundle 建好了"在并行写入里是常态，不是错误
                client.add_partition(toThriftPartition(databaseName, tableName, partition))
                added += name
            } catch (e: AlreadyExistsException) {
                LOGGER.debug("分区已存在，跳过: {}.{} {} ({})", databaseName, tableName, name, e.message)
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

        /** thrift 的 `max_parts` 是 short，-1 表示不限。 */
        private const val ALL_PARTITIONS: Short = -1

        private const val BATCH_SIZE = 500

        /**
         * thrift 默认的最大消息体是 100MB。分区数上万的表一次 `get_partitions_by_names`
         * 就可能超过，这里放宽到 1GB——超限时报的是 `MaxMessageSize reached`，
         * 从这个错误反推到"分区太多"并不直观。
         */
        private const val MAX_MESSAGE_SIZE = 1024 * 1024 * 1024

        fun connect(spec: HiveMetastoreSpec): ThriftHiveMetastore {
            // 支持 thrift://h1:9083,thrift://h2:9083 这种 HA 写法，逐个试
            val endpoints = spec.uri.split(',').map { it.trim() }.filter { it.isNotBlank() }
            require(endpoints.isNotEmpty()) { "metastore_uri 不能为空" }
            var lastError: Exception? = null
            endpoints.forEach { endpoint ->
                try {
                    return connectOne(endpoint, spec.timeoutMillis)
                } catch (e: Exception) {
                    LOGGER.warn("连接 metastore[{}] 失败: {}", endpoint, e.message)
                    lastError = e
                }
            }
            throw IllegalStateException("所有 metastore 地址都连不上: ${spec.uri}", lastError)
        }

        private fun connectOne(endpoint: String, timeoutMillis: Int): ThriftHiveMetastore {
            val uri = URI(endpoint)
            require(uri.scheme == "thrift") { "metastore 地址应以 thrift:// 开头: $endpoint" }
            val port = if (uri.port > 0) uri.port else 9083
            val configuration = TConfiguration(
                MAX_MESSAGE_SIZE,
                TConfiguration.DEFAULT_MAX_FRAME_SIZE,
                TConfiguration.DEFAULT_RECURSION_DEPTH,
            )
            val transport = TSocket(configuration, uri.host, port, timeoutMillis)
            transport.open()
            val client = ThriftApi.Client(TBinaryProtocol(transport))
            LOGGER.info("已连接 Hive metastore: {}:{}", uri.host, port)
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
            // 分区自己带一份 StorageDescriptor，列信息取自表——分区级的列变更（ALTER ... CASCADE 之外）不在支持范围内
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
