package me.jayer.hdata.filesystem

/**
 * 路径归一化。
 *
 * Beam 的 `LocalFileSystem` 对 `file://` 前缀的处理**读写不一致**：
 *  - `FileIO.match("file:///tmp/a.txt")` 能正常匹配到文件；
 *  - `FileIO.write().to("file:///tmp/out")` 却把整个字符串当成**相对路径**，
 *    结果写到当前工作目录下一个名叫 `file:` 的目录里（`./file:/tmp/out/...`），
 *    而且作业状态照样是 DONE。
 *
 * 也就是说，配置里写 `file://` 前缀时，读得好好的，写出去的数据却悄悄躺在别处。
 * 这里统一把本地路径的 scheme 去掉——`LocalFileSystem` 对裸路径的读写行为是一致的。
 * `hdfs://` / `gs://` / `s3://` 这些有专门 FileSystem 实现的 scheme 原样保留。
 *
 * @author wuya
 */
object FilesystemPaths {

    private const val LOCAL_SCHEME = "file://"

    fun normalize(path: String): String {
        if (!path.startsWith(LOCAL_SCHEME)) {
            return path
        }
        // file:///tmp/x -> /tmp/x；file://tmp/x（少写一个斜杠）-> /tmp/x
        val rest = path.removePrefix(LOCAL_SCHEME)
        return if (rest.startsWith("/")) rest else "/$rest"
    }
}
