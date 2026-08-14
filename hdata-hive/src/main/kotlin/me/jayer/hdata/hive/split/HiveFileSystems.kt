package me.jayer.hdata.hive.split

import me.jayer.hdata.hive.format.HiveStorageFormat
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.FileStatus
import org.apache.hadoop.fs.FileSystem
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.compress.CompressionCodecFactory
import org.slf4j.LoggerFactory

/**
 * 表目录的文件枚举与可切分性判定，对应 Trino 的 `BackgroundHiveSplitLoader` + `InternalHiveSplitFactory`。
 *
 * @author wuya
 */
object HiveFileSystems {

    private val LOGGER = LoggerFactory.getLogger(HiveFileSystems::class.java)

    /**
     * 从配置项建一个 Hadoop [Configuration]。
     *
     * `Configuration` 不可序列化，所以配置里存的是 `Map<String, String>`，
     * 到了 worker 上的 `@Setup` 里再还原——和连接池的处理方式一样。
     */
    fun configurationOf(properties: Map<String, String>): Configuration = Configuration().apply {
        properties.forEach { (key, value) -> set(key, value) }
    }

    /**
     * 列出一个目录下的数据文件。
     *
     * **不要 `fs.close()`**：`FileSystem.get` 返回的是按 URI + 用户缓存的**进程内共享实例**，
     * 关掉它会波及同一个 JVM 里所有正在用同一文件系统的代码。`hdata-filesystem` 重构前就踩过这个坑。
     *
     * @param recursive Hive 的 `hive.mapred.supports.subdirectories`。默认不递归：
     *   分区目录下出现子目录通常意味着这是一张 ACID 表或者 union 写出的结果，那些需要额外的合并语义。
     */
    fun listFiles(configuration: Configuration, location: String, recursive: Boolean): List<FileStatus> {
        val path = Path(location)
        val fs = path.getFileSystem(configuration)
        if (!fs.exists(path)) {
            LOGGER.warn("路径不存在，按空分区处理: {}", location)
            return emptyList()
        }
        val result = mutableListOf<FileStatus>()
        collect(fs, path, recursive, result)
        return result
    }

    private fun collect(fs: FileSystem, path: Path, recursive: Boolean, into: MutableList<FileStatus>) {
        fs.listStatus(path).forEach { status ->
            when {
                isHidden(status.path.name) -> LOGGER.debug("跳过隐藏文件: {}", status.path)
                status.isDirectory -> if (recursive) {
                    collect(fs, status.path, true, into)
                } else {
                    LOGGER.debug("跳过子目录（未开启 recursive_directories）: {}", status.path)
                }
                // 长度为 0 的文件在所有格式下都读不出东西，ORC / Parquet 还会因为没有文件尾直接报错
                status.len <= 0 -> LOGGER.debug("跳过空文件: {}", status.path)
                else -> into += status
            }
        }
    }

    /**
     * Hive / Hadoop 的约定：`.` 与 `_` 开头的都是元数据或临时文件（`_SUCCESS`、`.hive-staging_*`、
     * MapReduce 的 `_temporary`），不是数据。
     */
    fun isHidden(name: String): Boolean = name.startsWith('.') || name.startsWith('_')

    /**
     * 这个文件能不能从中间某个字节位置开始读。
     *
     * 格式本身可切只是必要条件：TEXTFILE 一旦整文件用 gzip / snappy 压过，
     * 就只能从头解压，这时并行度只能来自文件个数。bzip2 理论上可切（有块边界），
     * 但要靠 `SplittableCompressionCodec` 才能定位，这里一并按不可切处理——
     * 宁可少切一刀，也不要把一条记录劈成两半。
     *
     * ORC / Parquet / Avro / SequenceFile / RCFile 的压缩是**块内**压缩，
     * 文件本身始终带索引或同步标记，不受影响。
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
