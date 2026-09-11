package me.jayer.hdata.hive.metastore

import java.io.Serializable

/**
 * One column of a Hive table. [type] is the Hive type string (`string` / `decimal(10,2)` / `array<struct<a:int>>`); parsing it
 * into a Beam type is [me.jayer.hdata.hive.type.HiveTypes]' job.
 *
 * @author wuya
 */
data class HiveColumn(
    val name: String,
    val type: String,
    val comment: String? = null,
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * A storage format declaration, i.e. the group of strings formed by `SerDeInfo.serializationLib` + `StorageDescriptor.inputFormat`
 * in the metastore.
 *
 * The raw strings are kept here instead of being normalized into an enum right away: Hive allows the same physical format with
 * different SerDes (RCFile's `ColumnarSerDe` and `LazyBinaryColumnarSerDe` are two completely different column encodings), and
 * the decision is left to [me.jayer.hdata.hive.format.HiveStorageFormat.of].
 */
data class StorageFormat(
    val serde: String,
    val inputFormat: String,
    val outputFormat: String,
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1

        fun of(serde: String, inputFormat: String, outputFormat: String) =
            StorageFormat(serde, inputFormat, outputFormat)
    }
}

/**
 * Storage description of a table or a partition.
 *
 * **Every partition has its own**: Hive allows `ALTER TABLE ... PARTITION (...) SET FILEFORMAT`, and tables whose old partitions
 * are TEXTFILE while the newer ones are ORC are common in production. So format detection on the read side must happen at
 * partition level and cannot just look at the table-level [HiveTable.storage].
 */
data class Storage(
    val storageFormat: StorageFormat,
    val location: String,
    /** SerDe parameters: `field.delim`, `serialization.format`, `avro.schema.literal` and so on. */
    val serdeParameters: Map<String, String> = emptyMap(),
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * A Hive table. The fields come from the metastore's `Table` struct, keeping only what reads and writes really use.
 *
 * [dataColumns] and [partitionColumns] are **separate**, matching Hive's storage model: partition column values are not in the
 * data files, they are encoded in the directory name `dt=2024-01-01`.
 */
data class HiveTable(
    val databaseName: String,
    val tableName: String,
    val tableType: String,
    val dataColumns: List<HiveColumn>,
    val partitionColumns: List<HiveColumn>,
    val storage: Storage,
    val parameters: Map<String, String> = emptyMap(),
) : Serializable {

    val qualifiedName: String get() = "$databaseName.$tableName"

    val partitioned: Boolean get() = partitionColumns.isNotEmpty()

    /** Data columns + partition columns, in the same order as `SELECT *` (Hive puts partition columns last). */
    val columns: List<HiveColumn> get() = dataColumns + partitionColumns

    companion object {
        private const val serialVersionUID: Long = 1

        const val EXTERNAL_TABLE = "EXTERNAL_TABLE"
        const val MANAGED_TABLE = "MANAGED_TABLE"
        const val VIRTUAL_VIEW = "VIRTUAL_VIEW"
    }
}

/**
 * One partition. [values] corresponds one-to-one to [HiveTable.partitionColumns] and holds the **unescaped** raw values.
 */
data class HivePartition(
    val values: List<String>,
    val storage: Storage,
    val parameters: Map<String, String> = emptyMap(),
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}
