package me.jayer.hdata.hive.metastore

import java.io.Serializable

/**
 * Hive 表的一列。[type] 是 Hive 的类型字符串（`string` / `decimal(10,2)` / `array<struct<a:int>>`），
 * 解析成 Beam 类型的活交给 [me.jayer.hdata.hive.type.HiveTypes]。
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
 * 一份存储格式声明，也就是 metastore 里 `SerDeInfo.serializationLib` + `StorageDescriptor.inputFormat`
 * 那一组字符串。
 *
 * 这里保留原始字符串而不是直接归一成枚举：Hive 允许同一种物理格式配不同的 SerDe
 * （RCFile 的 `ColumnarSerDe` 与 `LazyBinaryColumnarSerDe` 就是两种完全不同的列编码），
 * 判定交给 [me.jayer.hdata.hive.format.HiveStorageFormat.of]。
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
 * 表或分区的存储描述。
 *
 * **每个分区都有自己的一份**：Hive 允许 `ALTER TABLE ... PARTITION (...) SET FILEFORMAT`，
 * 老分区是 TEXTFILE、新分区是 ORC 的表在生产里很常见。所以读取端的格式判定必须落到分区级，
 * 不能只看表级的 [HiveTable.storage]。
 */
data class Storage(
    val storageFormat: StorageFormat,
    val location: String,
    /** SerDe 参数：`field.delim`、`serialization.format`、`avro.schema.literal` 等。 */
    val serdeParameters: Map<String, String> = emptyMap(),
) : Serializable {
    companion object {
        private const val serialVersionUID: Long = 1
    }
}

/**
 * 一张 Hive 表。字段取自 metastore 的 `Table` 结构体，只留读写真正用得上的部分。
 *
 * [dataColumns] 与 [partitionColumns] 是**分开**的，和 Hive 的存储模型一致：
 * 分区列的值不在数据文件里，而是编码在目录名 `dt=2024-01-01` 上。
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

    /** 数据列 + 分区列，顺序与 `SELECT *` 一致（Hive 把分区列排在最后）。 */
    val columns: List<HiveColumn> get() = dataColumns + partitionColumns

    companion object {
        private const val serialVersionUID: Long = 1

        const val EXTERNAL_TABLE = "EXTERNAL_TABLE"
        const val MANAGED_TABLE = "MANAGED_TABLE"
        const val VIRTUAL_VIEW = "VIRTUAL_VIEW"
    }
}

/**
 * 一个分区。[values] 与 [HiveTable.partitionColumns] 一一对应，是**未转义**的原始值。
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
