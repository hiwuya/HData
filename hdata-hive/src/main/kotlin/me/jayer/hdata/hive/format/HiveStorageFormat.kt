package me.jayer.hdata.hive.format

import me.jayer.hdata.hive.metastore.StorageFormat

/**
 * Storage formats supported by Hive, equivalent to Trino's `HiveStorageFormat`.
 *
 * The metastore has no "format" field, only three strings — the SerDe class name, the InputFormat class name and the OutputFormat
 * class name — so the decision rests on the combination of the three: `LazySimpleSerDe` may be either TEXTFILE or SEQUENCEFILE,
 * differing only in InputFormat; `RCFileInputFormat` may be either RCTEXT or RCBINARY, differing only in SerDe.
 *
 * @param splittable whether the file itself can be read starting from some byte position in the middle.
 *   ORC / Parquet have stripe / row group indexes, Avro / SequenceFile / RCFile have sync markers, and plain text can scan to
 *   the next newline — all splittable. What is really unsplittable is **compression** (see [Compressions]).
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
        // A quoted field can contain newlines, so cutting at an arbitrary byte position would split a record in half
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

    /** The storage declaration used when creating a table. */
    fun toStorageFormat(): StorageFormat = StorageFormat(serde, inputFormat, outputFormat)

    /** Suffix of the files written to disk. Hive does not recognize formats by suffix; it is only there for human convenience. */
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
         * Determines the format from the metastore triple.
         *
         * Match SerDe + InputFormat exactly first, then fall back to SerDe only, and finally to InputFormat only — some tools
         * (Spark, Impala, various ETL scripts) only fill in one of them when creating a table.
         *
         * If it cannot decide, throw — **no fallback**: guessing the format wrong reads a pile of garbage, worse than not reading.
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
            // When one InputFormat maps to several formats (RCFile), the SerDe decides
            entries.firstOrNull { it.inputFormat == inputFormat && it.serde == serde }?.let { return it }
            throw IllegalArgumentException(
                "unsupported Hive storage format: serde=$serde inputFormat=$inputFormat; " +
                    "supported: ${entries.joinToString { it.name }}"
            )
        }

        /** The format name written in the config (`orc` / `parquet` / `textfile` …). */
        fun byName(name: String): HiveStorageFormat {
            val normalized = name.trim().uppercase()
            return entries.firstOrNull { it.name == normalized }
                ?: throw IllegalArgumentException("unrecognized storage format: $name, choose from: ${entries.joinToString { it.name.lowercase() }}")
        }
    }
}
