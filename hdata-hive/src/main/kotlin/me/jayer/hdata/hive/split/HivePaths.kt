package me.jayer.hdata.hive.split

/**
 * 表目录/分区目录的路径归一化。
 *
 * metastore 里存的 location 是**带 scheme 的 URI**（`file:///user/hive/warehouse/t`、
 * `hdfs://nameservice1/user/hive/warehouse/t`），读取端交给 Hadoop `FileSystem` 处理没有问题，
 * 但写入端要把它交给 Beam 的 `FileIO`，而 Beam 的 `LocalFileSystem` 对 `file://` 前缀的处理
 * **读写不一致**：
 *  - `FileIO.match("file:///tmp/a")` 能正常匹配；
 *  - `FileIO.write().to("file:///tmp/a")` 却把整个字符串当**相对路径**，
 *    数据写到当前工作目录下一个名叫 `file:` 的目录里（`./file:/tmp/a/...`），
 *    而且作业状态照样是 DONE——数据"写成功"了，只是不在表目录里。
 *
 * `hdata-filesystem` 里已经踩过同一个坑（见 `FilesystemPaths`），这里对 metastore 给的
 * location 做同样的处理。`hdfs://` / `s3a://` / `oss://` 这些有专门 FileSystem 实现的 scheme 原样保留。
 *
 * @author wuya
 */
object HivePaths {

    private const val LOCAL_SCHEME = "file:"

    /** 交给 Beam `FileIO.write()` 之前必须过这一道。 */
    fun forBeamIO(location: String): String {
        if (!location.startsWith(LOCAL_SCHEME)) {
            return location
        }
        // file:///tmp/x -> /tmp/x；file:/tmp/x（Hadoop Path 的写法）也一样
        val rest = location.removePrefix(LOCAL_SCHEME).trimStart('/')
        return "/$rest"
    }
}
