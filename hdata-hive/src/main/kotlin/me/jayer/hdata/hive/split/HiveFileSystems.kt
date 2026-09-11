package me.jayer.hdata.hive.split

import me.jayer.hdata.hive.format.HiveStorageFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.slf4j.LoggerFactory

/**
 * File enumeration and splittability decisions for a table directory, mirroring Trino's `BackgroundHiveSplitLoader` +
 *
 * @author wuya
 */
object HiveFileSystems {

    private val LOGGER = LoggerFactory.getLogger(HiveFileSystems::class.java)

    /**
     * Builds a Hadoop [Configuration] from the config items.
     *
     * `Configuration` is not serializable, so the config stores a `Map<String, String>` that is restored in `@Setup` on the
     * worker — the same treatment as a connection pool.
     */
    fun configurationOf(properties: Map<String, String>): Configuration = Configuration().apply {
        properties.forEach { (key, value) -> set(key, value) }
    }

    /**
     * Lists the data files under a directory.
     *
     * **Do not call `fs.close()`**: `FileSystem.get` returns a **process-wide shared instance** cached by URI + user, and closing
     * it affects every other piece of code in the same JVM that uses that file system. `hdata-filesystem` hit this trap before.
     *
     * @param recursive Hive's `hive.mapred.supports.subdirectories`. Off by default: subdirectories under a partition directory
     *   usually mean an ACID table or a union write result, and those need extra merge semantics.
     */
    fun listFiles(configuration: Configuration, location: String, recursive: Boolean): List<FileStatus> {
        val path = Path(location)
        val fs = path.getFileSystem(configuration)
        if (!fs.exists(path)) {
            LOGGER.warn("path does not exist, treating it as an empty partition: {}", location)
            return emptyList()
        }
        val result = mutableListOf<FileStatus>()
        collect(fs, path, recursive, result)
        return result
    }

    private fun collect(fs: FileSystem, path: Path, recursive: Boolean, into: MutableList<FileStatus>) {
        fs.listStatus(path).forEach { status ->
            when {
                isHidden(status.path.name) -> LOGGER.debug("skipping hidden file: {}", status.path)
                status.isDirectory -> if (recursive) {
                    collect(fs, status.path, true, into)
                } else {
                    LOGGER.debug("skipping subdirectory (recursive_directories is off): {}", status.path)
                }
                // A zero-length file yields nothing in any format, and ORC / Parquet even fail outright for lack of a file tail
                status.len <= 0 -> LOGGER.debug("skipping empty file: {}", status.path)
                else -> into += status
            }
        }
    }

    /**
     * Hive / Hadoop convention: names starting with `.` or `_` are metadata or temporary files (`_SUCCESS`, `.hive-staging_*`,
     * MapReduce's `_temporary`), not data.
     */
    fun isHidden(name: String): Boolean = name.startsWith('.') || name.startsWith('_')

    /**
     * Whether this file can be read starting from some byte position in the middle.
     *
     * A splittable format is only a necessary condition: once a TEXTFILE is gzip / snappy compressed as a whole it can only be
     * decompressed from the start, and then parallelism can only come from the number of files. bzip2 is theoretically splittable
     * (it has block boundaries), but locating them requires `SplittableCompressionCodec`, so it is treated as unsplittable here
     * too — better to make one cut less than to split a record in half.
     *
     * Compression in ORC / Parquet / Avro / SequenceFile / RCFile is **intra-block**, so the file always keeps an index or sync
     * markers and is unaffected.
     */
    fun isSplittable(format: HiveStorageFormat, path: String, configuration: Configuration): Boolean {
        if (!format.splittable) {
            return false
        }
        if (format != HiveStorageFormat.TEXTFILE) {
            return true
        }
        return CompressionCodecFactory(configuration).getCodec(Path(path)) == null
    }
}
