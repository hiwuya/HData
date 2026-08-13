package me.jayer.hdata.filesystem

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * 本地路径归一化。
 *
 * Beam 的 `LocalFileSystem` 对 `file://` 前缀读写不一致：`FileIO.match` 认，
 * `FileIO.write().to()` 却当成相对路径，把数据写进当前目录下一个叫 `file:` 的目录里，
 * 而且作业状态还是 DONE。见 [FilesystemPaths] 的注释。
 *
 * @author wuya
 */
class FilesystemPathsTest {

    @Test
    fun `本地 scheme 被剥掉`() {
        assertEquals("/tmp/out", FilesystemPaths.normalize("file:///tmp/out"))
        assertEquals("/tmp/out/a.csv", FilesystemPaths.normalize("file:///tmp/out/a.csv"))
    }

    @Test
    fun `少写一个斜杠也能归一化`() {
        assertEquals("/tmp/out", FilesystemPaths.normalize("file://tmp/out"))
    }

    @Test
    fun `裸路径原样保留`() {
        assertEquals("/tmp/out", FilesystemPaths.normalize("/tmp/out"))
        assertEquals("out", FilesystemPaths.normalize("out"))
    }

    @Test
    fun `有专门实现的 scheme 原样保留`() {
        // 这些 scheme 由各自的 Beam FileSystem 处理，不能动
        listOf(
            "hdfs://namenode:8020/data",
            "gs://bucket/path",
            "s3://bucket/path",
        ).forEach { assertEquals(it, FilesystemPaths.normalize(it)) }
    }
}
