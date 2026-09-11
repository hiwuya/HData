package me.jayer.hdata.hive.split

/**
 * Path normalization of table / partition directories.
 *
 * The location stored in the metastore is a **URI with a scheme** (`file:///user/hive/warehouse/t`,
 * `hdfs://nameservice1/user/hive/warehouse/t`). Handing it to Hadoop's `FileSystem` on the read side is fine, but the write side
 * has to hand it to Beam's `FileIO`, and Beam's `LocalFileSystem` treats the `file://` prefix **inconsistently for read and write**:
 *  - `FileIO.match("file:///tmp/a")` matches fine;
 *  - `FileIO.write().to("file:///tmp/a")` treats the whole string as a **relative path** and writes the data into a directory
 *    writing the data into a directory literally named `file:` under the current working directory (`./file:/tmp/a/...`),
 *    and the job status is still DONE —
 *    the data was "written successfully", just not into the table directory.
 *
 * `hdata-filesystem` already hit the same trap (see `FilesystemPaths`), so the location given by the metastore gets the same
 * treatment here. Schemes with dedicated FileSystem implementations such as `hdfs://` / `s3a://` / `oss://` are kept as they are.
 *
 * @author wuya
 */
object HivePaths {

    private const val LOCAL_SCHEME = "file:"

    /** Must be applied before a path is handed to Beam's `FileIO.write()`. */
    fun forBeamIO(location: String): String {
        if (!location.startsWith(LOCAL_SCHEME)) {
            return location
        }
        // file:///tmp/x -> /tmp/x; the same for file:/tmp/x (Hadoop Path's spelling)
        val rest = location.removePrefix(LOCAL_SCHEME).trimStart('/')
        return "/$rest"
    }
}
