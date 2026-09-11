package me.jayer.hdata.hive.split

import me.jayer.hdata.hive.metastore.HivePartition
import me.jayer.hdata.hive.metastore.Storage
import java.io.Serializable

/**
 * A partition waiting to be read (for a non-partitioned table, the whole table), carrying its own storage description.
 *
 * The storage description **must travel per partition**: Hive allows `ALTER TABLE t PARTITION (dt='...') SET FILEFORMAT ORC`, so
 * a table whose first half-year partitions are TEXTFILE and later ones ORC is perfectly legal. A reader that only looks at the
 * table-level format would read ORC files as text and get a pile of binary garbage.
 *
 * @param name `dt=2024-01-01/hr=01`, empty for a non-partitioned table
 * @param values raw values (unescaped) corresponding one-to-one to the table's partition columns; empty for a non-partitioned
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

        /** Non-partitioned table: the whole table counts as one "partition". */
        fun unpartitioned(storage: Storage) = HivePartitionSpec("", emptyList(), storage)
    }
}

/**
 * One data file under a partition directory. The SDF restriction is a byte range inside it.
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
