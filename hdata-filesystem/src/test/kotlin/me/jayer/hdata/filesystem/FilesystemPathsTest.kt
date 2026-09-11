package me.jayer.hdata.filesystem

import kotlin.test.Test
import kotlin.test.assertEquals

/**
 * Local path normalization.
 *
 * Beam's `LocalFileSystem` is inconsistent about the `file://` prefix: `FileIO.match` accepts it,
 * while `FileIO.write().to()` treats it as a relative path and writes the data into a directory
 * named `file:` under the current directory -- and the job still reports DONE. See the comments on
 * [FilesystemPaths].
 *
 * @author wuya
 */
class FilesystemPathsTest {

    @Test
    fun `the local scheme is stripped`() {
        assertEquals("/tmp/out", FilesystemPaths.normalize("file:///tmp/out"))
        assertEquals("/tmp/out/a.csv", FilesystemPaths.normalize("file:///tmp/out/a.csv"))
    }

    @Test
    fun `a missing slash still normalizes`() {
        assertEquals("/tmp/out", FilesystemPaths.normalize("file://tmp/out"))
        assertEquals("/tmp/out", FilesystemPaths.normalize("file:/tmp/out"))
    }

    @Test
    fun `bare paths are kept as-is`() {
        assertEquals("/tmp/out", FilesystemPaths.normalize("/tmp/out"))
        assertEquals("out", FilesystemPaths.normalize("out"))
    }

    @Test
    fun `schemes with their own implementation are kept as-is`() {
        // these schemes are handled by their own Beam FileSystem and must not be touched
        listOf(
            "hdfs://namenode:8020/data",
            "gs://bucket/path",
            "s3://bucket/path",
        ).forEach { assertEquals(it, FilesystemPaths.normalize(it)) }
    }

    @Test
    fun `bare paths use a non-local default_fs`() {
        assertEquals(
            "hdfs://namenode:8020/data/in/*.csv",
            FilesystemPaths.normalize("/data/in/*.csv", "hdfs://namenode:8020"),
        )
        assertEquals("gs://bucket/in", FilesystemPaths.normalize("in", "gs://bucket"))
    }

    @Test
    fun `an explicit scheme takes precedence over default_fs`() {
        assertEquals(
            "s3://bucket/data",
            FilesystemPaths.normalize("s3://bucket/data", "hdfs://namenode:8020"),
        )
    }
}
