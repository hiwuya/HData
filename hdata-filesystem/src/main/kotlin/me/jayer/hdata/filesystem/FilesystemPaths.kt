package me.jayer.hdata.filesystem

/**
 * Path normalization.
 *
 * Beam's `LocalFileSystem` handles the `file://` prefix **inconsistently between reads and writes**:
 *  - `FileIO.match("file:///tmp/a.txt")` finds the file just fine;
 *  - `FileIO.write().to("file:///tmp/out")` treats the whole string as a **relative path** instead,
 *    so the data lands in a directory literally named `file:` under the current working directory
 *    (`./file:/tmp/out/...`), and the job still reports DONE.
 *
 * In other words, with a `file://` prefix in the config reads work fine while the written data
 * quietly ends up somewhere else. Here the scheme is stripped from local paths instead --
 * `LocalFileSystem` behaves consistently for bare paths on both read and write.
 * Schemes with their own FileSystem implementation (`hdfs://` / `gs://` / `s3://`) are left as-is.
 *
 * @author wuya
 */
object FilesystemPaths {

    private const val LOCAL_SCHEME = "file:"

    fun normalize(path: String, defaultFs: String = "file:///"): String {
        val resolved = if (SCHEME.matches(path.substringBefore('/', path))) {
            path
        } else if (defaultFs.startsWith(LOCAL_SCHEME, ignoreCase = true)) {
            path
        } else {
            "${defaultFs.trimEnd('/')}/${path.trimStart('/')}"
        }
        if (!resolved.startsWith(LOCAL_SCHEME, ignoreCase = true)) {
            return resolved
        }
        // file:///tmp/x and file:/tmp/x -> /tmp/x; file://tmp/x (one slash missing) -> /tmp/x
        val rest = resolved.substring(LOCAL_SCHEME.length).removePrefix("//")
        return if (rest.startsWith("/")) rest else "/$rest"
    }

    fun validateDefaultFs(defaultFs: String) {
        require(SCHEME.matches(defaultFs.substringBefore('/', defaultFs))) {
            "default_fs must contain a URI scheme, for example file:/// or hdfs://namenode:8020; got: $defaultFs"
        }
    }

    private val SCHEME = Regex("^[A-Za-z][A-Za-z0-9+.-]*:$")
}
