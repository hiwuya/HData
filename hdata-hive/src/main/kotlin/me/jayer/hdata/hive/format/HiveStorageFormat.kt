package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.metastore.StorageFormat

/**
 * Hive 支持的存储格式，等价于 Trino 的 `HiveStorageFormat`。
 *
 * metastore 里没有"格式"这个字段，只有 SerDe 类名 + InputFormat 类名 + OutputFormat 类名三个字符串，
 * 判定要靠这三个的组合：`LazySimpleSerDe` 既可能是 TEXTFILE 也可能是 SEQUENCEFILE，
 * 差别只在 InputFormat；`RCFileInputFormat` 既可能是 RCTEXT 也可能是 RCBINARY，差别只在 SerDe。
 *
 * @param splittable 文件本身是否支持从中间某个字节位置开始读。
 *   ORC / Parquet 有 stripe / row group 索引，Avro / SequenceFile / RCFile 有同步标记，
 *   纯文本可以扫到下一个换行——都可切。真正不可切的是**压缩**（见 [Compressions]）。
 */
enum class HiveStorageFormat(
    val serde: String,
    val inputFormat: String,
    val outputFormat: String,
    val splittable: Boolean,
) {
    TEXTFILE(
        serde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        inputFormat = "org.apache.hadoop.mapred.TextInputFormat",
        outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        splittable = true,
    ),
    CSV(
        serde = "org.apache.hadoop.hive.serde2.OpenCSVSerde",
        inputFormat = "org.apache.hadoop.mapred.TextInputFormat",
        outputFormat = "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
        // 带引号的字段可以内嵌换行，从任意字节位置切开会把一条记录劈成两半
        splittable = false,
    ),
    SEQUENCEFILE(
        serde = "org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe",
        inputFormat = "org.apache.hadoop.mapred.SequenceFileInputFormat",
        outputFormat = "org.apache.hadoop.hive.ql.io.HiveSequenceFileOutputFormat",
        splittable = true,
    ),
    RCTEXT(
        serde = "org.apache.hadoop.hive.serde2.columnar.ColumnarSerDe",
        inputFormat = "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
        outputFormat = "org.apache.hadoop.hive.ql.io.RCFileOutputFormat",
        splittable = true,
    ),
    RCBINARY(
        serde = "org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe",
        inputFormat = "org.apache.hadoop.hive.ql.io.RCFileInputFormat",
        outputFormat = "org.apache.hadoop.hive.ql.io.RCFileOutputFormat",
        splittable = true,
    ),
    ORC(
        serde = "org.apache.hadoop.hive.ql.io.orc.OrcSerde",
        inputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcInputFormat",
        outputFormat = "org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat",
        splittable = true,
    ),
    PARQUET(
        serde = "org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe",
        inputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat",
        outputFormat = "org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat",
        splittable = true,
    ),
    AVRO(
        serde = "org.apache.hadoop.hive.serde2.avro.AvroSerDe",
        inputFormat = "org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat",
        outputFormat = "org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat",
        splittable = true,
    ),
    ;

    /** 建表时用的存储声明。 */
    fun toStorageFormat(): StorageFormat = StorageFormat(serde, inputFormat, outputFormat)

    /** 落盘文件的后缀。Hive 不靠后缀识别格式，加上只是为了人看着方便。 */
    val fileExtension: String
        get() = when (this) {
            TEXTFILE -> ".txt"
            CSV -> ".csv"
            SEQUENCEFILE -> ".seq"
            RCTEXT, RCBINARY -> ".rc"
            ORC -> ".orc"
            PARQUET -> ".parquet"
            AVRO -> ".avro"
        }

    companion object {

        /**
         * 从 metastore 的三元组判定格式。
         *
         * 先按 SerDe + InputFormat 精确匹配，再退到只按 SerDe，最后退到只按 InputFormat——
         * 有些工具（Spark、Impala、各种 ETL 脚本）建表时只写全其中一个。
         *
         * 判不出来就抛，**不做兜底**：猜错格式读出来的是一堆乱码，比读不出来更糟。
         */
        fun of(format: StorageFormat): HiveStorageFormat {
            val serde = format.serde.trim()
            val inputFormat = format.inputFormat.trim()
            entries.firstOrNull { it.serde == serde && it.inputFormat == inputFormat }?.let { return it }
            entries.filter { it.serde == serde }.let { matched ->
                if (matched.size == 1) return matched.single()
            }
            entries.filter { it.inputFormat == inputFormat }.let { matched ->
                if (matched.size == 1) return matched.single()
            }
            // 同一个 InputFormat 对多个格式时（RCFile），SerDe 说了算
            entries.firstOrNull { it.inputFormat == inputFormat && it.serde == serde }?.let { return it }
            throw IllegalArgumentException(
                "暂不支持的 Hive 存储格式: serde=$serde inputFormat=$inputFormat；" +
                    "已支持: ${entries.joinToString { it.name }}"
            )
        }

        /** 配置里写的格式名（`orc` / `parquet` / `textfile` …）。 */
        fun byName(name: String): HiveStorageFormat {
            val normalized = name.trim().uppercase()
            return entries.firstOrNull { it.name == normalized }
                ?: throw IllegalArgumentException("无法识别的存储格式: $name，可选: ${entries.joinToString { it.name.lowercase() }}")
        }
    }
}
